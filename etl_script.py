import duckdb
import json
import os
import pandas as pd

from datetime import datetime
from duckdb import DuckDBPyConnection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Iterator, Dict, Any

from app.core import etl_settings
from app.utils.logger import setup_logger

# Tạo một instance logger duy nhất để import vào các module khác
logger = setup_logger('etl_app', 'ETL App')

def load_etl_state() -> Dict[str, str]:
    """Tải trạng thái ETL lần cuối từ file JSON."""
    if not os.path.exists(etl_settings.STATE_FILE):
        return {}

    try:
        with open(etl_settings.STATE_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc file trạng thái '{etl_settings.STATE_FILE}'. Bắt đầu với trạng thái trống.")
        return {}

def save_etl_state(state: Dict[str, str]):
    """Lưu trạng thái ETL mới vào file JSON."""
    with open(etl_settings.STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def extract_data(sql_engine: Engine, config: Dict[str, Any], last_timestamp: str) -> Iterator[pd.DataFrame]:
    """
    Trích xuất dữ liệu từ SQL Server theo từng chunk.
    Trả về một iterator của các DataFrame.
    """
    source_table = config['source_table']
    query = f"SELECT * FROM {source_table}"

    if config.get('incremental', True):
        timestamp_col = config['timestamp_col']
        query += f" WHERE {timestamp_col} > '{last_timestamp}' ORDER BY {timestamp_col};"
    else: # Full load
        query += ";"

    logger.info(f"Bắt đầu trích xuất dữ liệu từ {source_table} theo chunk...")

    # Sử dụng chunksize để tránh lỗi MemoryError với dữ liệu lớn
    return pd.read_sql(query, sql_engine, chunksize=10000)

def transform_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Áp dụng các bước transform cho DataFrame.
    """
    df.rename(columns=config['rename_map'], inplace=True)

    if 'partition_cols' in config:
        ts_col = config['dest_timestamp_col']
        df[ts_col] = pd.to_datetime(df[ts_col])
        df['year'] = df[ts_col].dt.year
        df['month'] = df[ts_col].dt.month

    return df

def load_data_to_duckdb(conn: DuckDBPyConnection, df: pd.DataFrame, config: Dict[str, Any], is_first_chunk: bool):
    """
    Ghi một DataFrame vào DuckDB.
    """
    dest_table = config['dest_table']
    is_incremental = config.get('incremental', True)

    if not is_incremental: # Full load
        # Ghi đè toàn bộ bảng với chunk đầu tiên, các chunk sau thì append
        if is_first_chunk:
            conn.execute(f"CREATE OR REPLACE TABLE {dest_table} AS SELECT * FROM df;")
        else:
            conn.execute(f"INSERT INTO {dest_table} SELECT * FROM df;")
    else: # Incremental load
        # Tạo bảng nếu chưa tồn tại (chỉ cần chạy cho chunk đầu tiên)
        if is_first_chunk:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {dest_table} AS SELECT * FROM df LIMIT 0;")

        # Chèn dữ liệu mới
        conn.execute(f"INSERT INTO {dest_table} SELECT * FROM df;")

def run_etl():
    """
    Hàm chính thực thi toàn bộ quy trình ETL:
    1. Kết nối tới các database.
    2. Tải trạng thái (lần cuối chạy).
    3. Trích xuất dữ liệu mới (Incremental).
    4. Transform dữ liệu (đổi tên cột, thêm cột partition).
    5. Load dữ liệu vào DuckDB với cấu trúc partition.
    6. Cập nhật trạng thái mới.
    """
    etl_state = load_etl_state()
    new_etl_state = etl_state.copy()
    sql_engine = None
    duckdb_conn = None

    logger.info("Bắt đầu quy trình ETL...")
    try:
        sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
        duckdb_conn = duckdb.connect(etl_settings.DUCKDB_PATH)
        logger.info(f"Đã kết nối thành công tới SQL Server và DuckDB ('{etl_settings.DUCKDB_PATH}').")

        for key, config in etl_settings.TABLE_CONFIG.items():
            dest_table = config['dest_table']
            source_table = config['source_table']
            logger.info(f"Bắt đầu xử lý bảng: {source_table} -> {dest_table}")

            last_timestamp = etl_state.get(dest_table, '1900-01-01 00:00:00')

            # --- EXTRACT (theo chunk) ---
            df_chunks = extract_data(sql_engine, config, last_timestamp)

            total_rows = 0
            max_timestamp = None
            is_first_chunk = True

            for chunk_df in df_chunks:
                if chunk_df.empty:
                    continue

                # --- TRANSFORM ---
                transformed_df = transform_data(chunk_df.copy(), config)

                # --- LOAD ---
                # Sử dụng cú pháp của DuckDB để ghi dữ liệu đã được partition
                # DuckDB sẽ tự tạo cấu trúc thư mục dạng hive (vd: year=2025/month=07/)
                load_data_to_duckdb(duckdb_conn, transformed_df, config, is_first_chunk)

                total_rows += len(transformed_df)
                is_first_chunk = False

                # Cập nhật high-water mark trong vòng lặp
                if config.get('incremental', True):
                    current_max = transformed_df[config['dest_timestamp_col']].max()
                    if max_timestamp is None or current_max > max_timestamp:
                        max_timestamp = current_max

            if total_rows > 0:
                logger.info(f"Load thành công {total_rows} dòng vào bảng {dest_table} trong DuckDB.")
                if config.get('incremental', True) and max_timestamp:
                    new_max_timestamp_str = max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    new_etl_state[dest_table] = new_max_timestamp_str
                    logger.info(f"Cập nhật high-water mark cho {dest_table} là: {new_max_timestamp_str}")
            else:
                logger.info(f"Không có dữ liệu mới cho bảng {dest_table}.")

        # ĐỀ XUẤT: Lưu trạng thái chỉ khi toàn bộ quá trình thành công
        save_etl_state(new_etl_state)
        logger.info("Lưu trạng thái ETL thành công.")
    except Exception as e:
        logger.error(f"ETL thất bại nghiêm trọng tại {run_etl.__name__}!", exc_info=True)
    finally:
        # --- CLEANUP ---
        if sql_engine:
            sql_engine.dispose()
            logger.info("Đã giải phóng connection pool của SQLAlchemy.")

        if duckdb_conn:
            duckdb_conn.close()
            logger.info("Đã đóng kết nối DuckDB.")

        logger.info("Quy trình ETL kết thúc.")

if __name__ == '__main__':
    run_etl()
