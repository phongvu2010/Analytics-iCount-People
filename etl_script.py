import duckdb
import json
import os
import pandas as pd
import shutil

from datetime import datetime
from duckdb import DuckDBPyConnection
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Any, Dict, Iterator, Optional

from app.core import etl_settings
from app.utils.logger import setup_logger

# Tạo một instance logger duy nhất để import vào các module khác
logger = setup_logger('etl_app', 'ETL App')

def load_etl_state() -> Dict[str, str]:
    state_file = Path(etl_settings.STATE_FILE)
    if not state_file.exists():
        return {}

    try:
        with state_file.open('r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc file trạng thái '{state_file}'. Bắt đầu với trạng thái trống.")
        return {}

def save_etl_state(state: Dict[str, str]):
    state_file = Path(etl_settings.STATE_FILE)

    # Đảm bảo thư mục tồn tại trước khi ghi file
    state_file.parent.mkdir(parents=True, exist_ok=True)

    with state_file.open('w') as f:
        json.dump(state, f, indent=4)

def extract_data(sql_engine: Engine, config: Dict[str, Any], last_timestamp: str) -> Iterator[pd.DataFrame]:
    source_table = config['source_table']
    query_str = f"SELECT * FROM {source_table}"
    params = {}

    if config.get('incremental', True):
        timestamp_col = config['timestamp_col']
        query_str += f" WHERE {timestamp_col} > :last_ts ORDER BY {timestamp_col}"
        params = {"last_ts": last_timestamp}

    query = text(query_str)
    logger.info(f"Bắt đầu trích xuất dữ liệu từ {source_table} với high-water-mark > '{last_timestamp}'")

    # Sử dụng chunksize để tránh lỗi MemoryError với dữ liệu lớn
    return pd.read_sql(query, sql_engine, params=params, chunksize=etl_settings.ETL_CHUNK_SIZE)

def transform_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    # Trả về một DataFrame mới để tránh warning SettingWithCopyWarning
    df.rename(columns=config['rename_map'], inplace=True)

    # Chuyển đổi kiểu dữ liệu và tạo cột partition
    if 'partition_cols' in config:
        ts_col = config['dest_timestamp_col']

        # Chuyển đổi sang datetime, errors='coerce' sẽ biến các giá trị không hợp lệ thành NaT
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')

        # Bỏ qua các dòng có timestamp không hợp lệ sau khi chuyển đổi
        df.dropna(subset=[ts_col], inplace=True)

        df['year'] = df[ts_col].dt.year
        df['month'] = df[ts_col].dt.month

    return df

def save_chunk_to_parquet(df: pd.DataFrame, config: Dict[str, Any]):
    if df.empty: return

    base_path = Path(etl_settings.DUCKDB_PATH).parent / config['dest_table']
    partition_cols = config.get('partition_cols')

    df.to_parquet(
        path=base_path,
        engine='pyarrow',
        compression='snappy',
        partition_cols=partition_cols
    )

def create_table_from_parquet(conn: DuckDBPyConnection, config: Dict[str, Any]):
    dest_table = config['dest_table']
    base_path = Path(etl_settings.DUCKDB_PATH).parent / config['dest_table']
    is_partitioned = 'partition_cols' in config and config.get('partition_cols')

    read_statement: str
    if is_partitioned:
        # Đường dẫn cho dữ liệu partition, vd: 'data/fact_traffic/**/*.parquet'
        parquet_path = base_path / '**' / '*.parquet'
        read_statement = f"read_parquet('{str(parquet_path).replace('\\', '/')}', hive_partitioning=1)"
    else:
        # Đường dẫn cho file đơn lẻ, vd: 'data/dim_stores.parquet'
        parquet_path = base_path.with_suffix('.parquet')
        read_statement = f"read_parquet('{str(parquet_path).replace('\\', '/')}')"

    create_sql = f"CREATE OR REPLACE TABLE {dest_table} AS SELECT * FROM {read_statement};"
    logger.info(f"Tạo/Cập nhật bảng '{dest_table}' trong DuckDB từ nguồn Parquet.")
    conn.execute(create_sql)

def run_etl():
    etl_state = load_etl_state()
    sql_engine = None
    duckdb_conn = None

    logger.info("Bắt đầu quy trình ETL...")
    try:
        sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
        duckdb_path_str = str(Path(etl_settings.DUCKDB_PATH).resolve())
        duckdb_conn = duckdb.connect(duckdb_path_str, read_only=False)
        logger.info(f"Đã kết nối thành công tới SQL Server và DuckDB ('{duckdb_path_str}').")

        for table_key, config in etl_settings.TABLE_CONFIG.items():
            dest_table = config['dest_table']
            source_table = config['source_table']
            is_incremental = config.get('incremental', True)
            is_partitioned = 'partition_cols' in config and config.get('partition_cols')

            logger.info(f"Bắt đầu xử lý bảng: {source_table} -> {dest_table}")
            try:
                last_timestamp = etl_state.get(dest_table, etl_settings.ETL_DEFAULT_TIMESTAMP)
                df_chunks = extract_data(sql_engine, config, last_timestamp)
                total_rows = 0

                # Phân luồng xử lý cho bảng có partition và không có partition
                if is_partitioned:
                    # --- XỬ LÝ BẢNG CÓ PARTITION (INCREMENTAL) ---
                    parquet_base_path = Path(etl_settings.DUCKDB_PATH).parent / dest_table
                    parquet_base_path.mkdir(parents=True, exist_ok=True)

                    max_timestamp: Optional[pd.Timestamp] = None
                    for chunk_df in df_chunks:
                        if chunk_df.empty: continue

                        transformed_df = transform_data(chunk_df, config)

                        if transformed_df.empty: continue

                        save_chunk_to_parquet(transformed_df, config)
                        total_rows += len(transformed_df)

                        if is_incremental:
                            current_max = transformed_df[config['dest_timestamp_col']].max()
                            if max_timestamp is None or current_max > max_timestamp:
                                max_timestamp = current_max

                    if total_rows > 0 and is_incremental and max_timestamp:
                        etl_state[dest_table] = max_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                else:
                    # --- XỬ LÝ BẢNG KHÔNG CÓ PARTITION (FULL LOAD) ---
                    # Ghép tất cả các chunk thành một DataFrame lớn
                    all_data = pd.concat(
                        [transform_data(chunk, config) for chunk in df_chunks if not chunk.empty],
                        ignore_index=True
                    )
                    total_rows = len(all_data)

                    if total_rows > 0:
                        parquet_file_path = Path(etl_settings.DUCKDB_PATH).parent / dest_table
                        parquet_file_path = parquet_file_path.with_suffix('.parquet')

                        # Ghi toàn bộ dữ liệu ra một file duy nhất, ghi đè nếu đã tồn tại
                        all_data.to_parquet(parquet_file_path, engine='pyarrow', compression='snappy')

                # Cập nhật bảng trong DuckDB và lưu trạng thái
                if total_rows > 0:
                    create_table_from_parquet(duckdb_conn, config)
                    logger.info(f"Đã xử lý và ghi {total_rows} dòng cho bảng {dest_table}.")
                else:
                    logger.info(f"Không có dữ liệu mới cho bảng {dest_table}.")

                save_etl_state(etl_state)
                logger.info(f"Xử lý thành công cho bảng {dest_table}. Trạng thái được cập nhật.")
            except Exception as table_error:
                logger.error(f"Lỗi khi xử lý bảng {source_table}.", exc_info=True)
    except Exception as e:
        logger.error(f"ETL thất bại nghiêm trọng tại {run_etl.__name__}!", exc_info=True)
    finally:
        if sql_engine:
            sql_engine.dispose()
            logger.info("Đã giải phóng connection pool của SQLAlchemy.")

        if duckdb_conn:
            duckdb_conn.close()
            logger.info("Đã đóng kết nối DuckDB.")

        logger.info("Quy trình ETL kết thúc.")

if __name__ == '__main__':
    run_etl()
