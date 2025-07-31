import duckdb
import json
import logging
import pandas as pd
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

from duckdb import DuckDBPyConnection
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict, Iterator, Optional

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging

# --- Setup ---
setup_logging('logger.yaml')
logger = logging.getLogger(__name__)

BASE_DATA_PATH = Path(etl_settings.DUCKDB_PATH).parent
STATE_FILE = Path(etl_settings.STATE_FILE)

# --- Các hàm quản lý trạng thái ---
def load_etl_state() -> Dict[str, str]:
    """Tải trạng thái ETL cuối cùng từ file JSON."""
    if not STATE_FILE.exists():
        return {}

    try:
        with STATE_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Could not read state file '{STATE_FILE}'. Starting fresh.\n")
        return {}

def save_etl_state(state: Dict[str, str]):
    """Lưu trạng thái ETL hiện tại vào file JSON."""
    # Đảm bảo thư mục tồn tại trước khi ghi file
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open('w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

# --- Các hàm trong pipeline ETL ---
def extract(sql_engine: Engine, config: TableConfig, last_timestamp: str) -> Iterator[pd.DataFrame]:
    """Trích xuất dữ liệu từ SQL Server theo từng chunk."""
    query = f"SELECT * FROM {config.source_table}"
    params = {}

    if config.incremental and config.timestamp_col:
        query += f" WHERE {config.timestamp_col} > :last_ts ORDER BY {config.timestamp_col}"
        params = {"last_ts": last_timestamp}

    logger.info(f"Extracting data from '{config.source_table}' with high-water-mark > '{last_timestamp}'.")

    return pd.read_sql(text(query), sql_engine, params=params, chunksize=etl_settings.ETL_CHUNK_SIZE)

def transform(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Áp dụng các bước transform cơ bản cho DataFrame."""
    df = df.rename(columns=config.rename_map)

    if 'store_name' in df.columns:
        df['store_name'] = df['store_name'].astype(str).str.rstrip()

    ts_col = config.rename_map.get(config.timestamp_col) if config.timestamp_col else None
    
    if ts_col and ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
        df.dropna(subset=[ts_col], inplace=True)
        if not df.empty and config.partition_cols:
            df['year'] = df[ts_col].dt.year
            df['month'] = df[ts_col].dt.month
    return df

def update_duckdb_table(conn: DuckDBPyConnection, config: TableConfig):
    """Tạo hoặc thay thế bảng trong DuckDB từ nguồn Parquet."""
    staging_dir = BASE_DATA_PATH / config.dest_table

    read_path = str(staging_dir / '**' / '*.parquet').replace('\\', '/')

    is_partitioned = bool(config.partition_cols)
    hive_param = ", hive_partitioning=1" if is_partitioned else ""

    read_statement = f"read_parquet('{read_path}'{hive_param})"
    create_sql = f"CREATE OR REPLACE TABLE {config.dest_table} AS SELECT * FROM {read_statement};"

    logger.info(f"Refreshing table '{config.dest_table}' in DuckDB from Parquet source at '{staging_dir}'.")
    conn.execute(create_sql)

def process_table(sql_engine: Engine, duckdb_conn: DuckDBPyConnection, config: TableConfig, etl_state: Dict[str, str]):
    """
    Điều phối toàn bộ quy trình ETL cho một bảng duy nhất:
    Extract -> Transform -> Load (to Parquet) -> Update DuckDB -> Update State
    """
    logger.info(f"Processing table: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")
    
    dest_path = BASE_DATA_PATH / config.dest_table

    if not config.incremental and dest_path.exists():
        if dest_path.is_dir():
            shutil.rmtree(dest_path)
            logger.info(f"Full-load: Removed old staging directory: {dest_path}")
        else:
            dest_path.unlink()
            logger.info(f"Full-load: Removed old staging file: {dest_path}")

    dest_path.mkdir(parents=True, exist_ok=True)

    last_timestamp = etl_state.get(config.dest_table, etl_settings.ETL_DEFAULT_TIMESTAMP)
    
    total_rows = 0
    max_timestamp_in_run: Optional[pd.Timestamp] = None
    writer: Optional[pq.ParquetWriter] = None
    
    try:
        for chunk in extract(sql_engine, config, last_timestamp):
            if chunk.empty:
                continue

            transformed_chunk = transform(chunk, config)
            if transformed_chunk.empty:
                continue

            arrow_table = pa.Table.from_pandas(transformed_chunk, preserve_index=False)

            if config.partition_cols:
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(dest_path),
                    partition_cols=config.partition_cols,
                    existing_data_behavior='overwrite_or_ignore'
                )
            else:
                if writer is None:
                    output_file = dest_path / 'data.parquet'
                    writer = pq.ParquetWriter(str(output_file), arrow_table.schema)
                writer.write_table(arrow_table)

            total_rows += len(transformed_chunk)

            if config.incremental:
                ts_col = config.rename_map.get(config.timestamp_col)
                if ts_col:
                    current_max = transformed_chunk[ts_col].max()
                    if max_timestamp_in_run is None or current_max > max_timestamp_in_run:
                        max_timestamp_in_run = current_max
    finally:
        if writer:
            writer.close()

    if total_rows == 0:
        logger.info(f"No new data found for table '{config.dest_table}'.")
        return

    logger.info(f"Processed {total_rows} rows. Staging to Parquet complete.")

    update_duckdb_table(duckdb_conn, config)
    logger.info(f"Successfully loaded {total_rows} rows into DuckDB table '{config.dest_table}'.")

    if config.incremental and max_timestamp_in_run:
        etl_state[config.dest_table] = max_timestamp_in_run.isoformat(sep=' ')

# --- Hàm chạy chính ---
def run_etl():
    """Hàm chính điều phối toàn bộ quy trình ETL."""
    etl_state = load_etl_state()
    sql_engine = None
    duckdb_conn = None

    logger.info("ETL process started...")
    try:
        sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
        duckdb_path_str = str(Path(etl_settings.DUCKDB_PATH).resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path_str, read_only=False)
        logger.info(f"Successfully connected to SQL Server and DuckDB ('{duckdb_path_str}').\n")

        for table_name, config in etl_settings.TABLE_CONFIG.items():
            try:
                process_table(sql_engine, duckdb_conn, config, etl_state)

                # Lưu state ngay sau khi một bảng xử lý thành công
                save_etl_state(etl_state)
                logger.info(f"Successfully processed table '{config.source_table}'. State has been saved.\n")
            except Exception as e:
                logger.error(f"Failed to process table '{config.source_table}'. Error: {e}\n", exc_info=True)
    except Exception as e:
        logger.critical(f"A critical error occurred in the main ETL process: {e}\n", exc_info=True)
    finally:
        if sql_engine:
            sql_engine.dispose()
            logger.info("SQLAlchemy connection pool disposed.")
        if duckdb_conn:
            duckdb_conn.close()
            logger.info("DuckDB connection closed.")
        logger.info("ETL process finished.\n")

if __name__ == '__main__':
    run_etl()
