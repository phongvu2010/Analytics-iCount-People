import duckdb
import json
import pandas as pd
import shutil
import pyarrow.parquet as pq
import pyarrow as pa

from duckdb import DuckDBPyConnection
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Any, Dict, Iterator, Optional # List

from app.core.config import etl_settings, TableConfig
from app.utils.logger import get_logger

# --- Setup ---
logger = get_logger('ETL', 'etl_app')
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
        logger.warning(f"Could not read state file '{STATE_FILE}'. Starting fresh.")
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

    logger.info(f"Extracting data from '{config.source_table}' with high-water-mark > '{last_timestamp}'")

    return pd.read_sql(text(query), sql_engine, params=params, chunksize=etl_settings.ETL_CHUNK_SIZE)

def transform(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Áp dụng các bước transform cơ bản cho DataFrame."""
    df.rename(columns=config.rename_map, inplace=True)

    if 'store_name' in df.columns:
        df['store_name'] = df['store_name'].astype(str).str.rstrip()

    if config.partition_cols and config.dest_timestamp_col:
        ts_col = config.dest_timestamp_col
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
        df.dropna(subset=[ts_col], inplace=True)

        if not df.empty:
            df['year'] = df[ts_col].dt.year
            df['month'] = df[ts_col].dt.month

    return df

def load_to_staging(df: pd.DataFrame, config: TableConfig):
    """Ghi DataFrame vào file/thư mục Parquet."""
    staging_dir = BASE_DATA_PATH / config.dest_table

    if not df.empty:
        if config.partition_cols:
            df.to_parquet(
                path=staging_dir,
                engine='pyarrow',
                compression='snappy',
                partition_cols=config.partition_cols
            )
        else:
            parquet_file = staging_dir / f"{config.dest_table}.parquet"
            df.to_parquet(parquet_file, engine='pyarrow', compression='snappy', index=False)

def update_duckdb_table(conn: DuckDBPyConnection, config: TableConfig):
    """Tạo hoặc thay thế bảng trong DuckDB từ nguồn Parquet."""
    staging_dir = BASE_DATA_PATH / config.dest_table

    if config.partition_cols:
        # Sử dụng glob để quét tất cả các file parquet trong các thư mục con
        # hive_partitioning=1 tự động nhận diện `year=.../month=...`
        read_path = str(staging_dir / '**' / '*.parquet').replace('\\', '/')
        read_statement = f"read_parquet('{read_path}', hive_partitioning=1)"
    else:
        read_path = str(staging_dir / f"{config.dest_table}.parquet").replace('\\', '/')
        read_statement = f"read_parquet('{read_path}')"

    create_sql = f"CREATE OR REPLACE TABLE {config.dest_table} AS SELECT * FROM {read_statement};"
    logger.info(f"Refreshing table '{config.dest_table}' in DuckDB from Parquet source.")
    conn.execute(create_sql)

def process_table(sql_engine: Engine, duckdb_conn: DuckDBPyConnection, config: TableConfig, etl_state: Dict[str, str]):
    """
    Điều phối toàn bộ quy trình ETL cho một bảng duy nhất:
    Extract -> Transform -> Load (to Parquet) -> Update DuckDB -> Update State
    """
    logger.info(f"Processing table: {config.source_table} -> {config.dest_table} "
                f"(Incremental: {config.incremental}, Partitioned: {bool(config.partition_cols)})")

    staging_dir = BASE_DATA_PATH / config.dest_table
    if not config.incremental and staging_dir.exists():
        shutil.rmtree(staging_dir)
        logger.info(f"Full-load: Removed old staging directory: {staging_dir}")

    staging_dir.mkdir(parents=True, exist_ok=True)
    last_timestamp = etl_state.get(config.dest_table, etl_settings.ETL_DEFAULT_TIMESTAMP)

    transformed_chunks = []
    max_timestamp_in_run: Optional[pd.Timestamp] = None

    for chunk in extract(sql_engine, config, last_timestamp):
        if chunk.empty:
            continue

        transformed_chunk = transform(chunk, config)
        if transformed_chunk.empty:
            continue

        transformed_chunks.append(transformed_chunk)

        if config.incremental and config.dest_timestamp_col:
            current_max = transformed_chunk[config.dest_timestamp_col].max()
            if max_timestamp_in_run is None or current_max > max_timestamp_in_run:
                max_timestamp_in_run = current_max

    if not transformed_chunks:
        logger.info(f"No new data found for table '{config.dest_table}'.")
        return

    final_df = pd.concat(transformed_chunks, ignore_index=True)
    total_rows = len(final_df)
    logger.info(f"Processed {total_rows} rows. Writing to Parquet staging area...")

    load_to_staging(final_df, config)
    update_duckdb_table(duckdb_conn, config)

    logger.info(f"Successfully staged and loaded {total_rows} rows for table '{config.dest_table}'.")

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
        logger.info(f"Successfully connected to SQL Server and DuckDB ('{duckdb_path_str}').")

        for table_name, config in etl_settings.TABLE_CONFIG.items():
            try:
                process_table(sql_engine, duckdb_conn, config, etl_state)

                # Lưu state ngay sau khi một bảng xử lý thành công
                save_etl_state(etl_state)
                logger.info(f"Successfully processed table '{config.source_table}'. State has been saved.")
            except Exception as e:
                logger.error(f"Failed to process table '{config.source_table}'. Error: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"A critical error occurred in the main ETL process: {e}", exc_info=True)
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
