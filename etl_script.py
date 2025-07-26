import duckdb
import json
import os
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine

from app.core.config import settings, TABLE_CONFIG
from app.utils.logger import logger

# --- Định nghĩa các hằng số ---
DUCKDB_PATH = 'data/analytics.duckdb'
STATE_FILE = 'data/etl_state.json'

def load_etl_state():
    """Tải trạng thái ETL lần cuối từ file JSON."""
    if not os.path.exists(STATE_FILE):
        return {}

    with open(STATE_FILE, 'r') as f:
        return json.load(f)

def save_etl_state(state):
    """Lưu trạng thái ETL mới vào file JSON."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

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
    sql_engine = None # Khởi tạo để dùng trong finally

    logger.info("Bắt đầu quy trình ETL...")
    try:
        sql_engine = create_engine(settings.sqlalchemy_db_uri)
    #     duckdb_conn = duckdb.connect(DUCKDB_PATH)
    #     logger.info(f"Đã kết nối thành công tới SQL Server (qua SQLAlchemy) và DuckDB ('{DUCKDB_PATH}').")

    #     # Bật tính năng hive partitioning
    #     duckdb_conn.execute("SET enable_hive_partitioning = 1;")

    #     for key, config in TABLE_CONFIG.items():
    #         source_table = config['source_table']
    #         dest_table = config['dest_table']
    #         logger.info(f"Bắt đầu xử lý bảng: {source_table} -> {dest_table}")

    #         # --- EXTRACT ---
    #         query = f"SELECT * FROM {source_table}"
    #         is_incremental = config.get('incremental', True)

    #         if is_incremental:
    #             timestamp_col = config['timestamp_col']
    #             last_timestamp = etl_state.get(dest_table, '1900-01-01 00:00:00')
    #             query += f" WHERE {timestamp_col} > '{last_timestamp}' ORDER BY {timestamp_col};"
    #         else: # Full load
    #             query += ";"

    #         df = pd.read_sql(query, sql_engine)

    #         if df.empty:
    #             logger.info(f"Không có dữ liệu mới cho bảng {dest_table}.")
    #             continue

    #         logger.info(f"Trích xuất được {len(df)} dòng mới từ {source_table}.")

    #         # --- TRANSFORM ---
    #         df.rename(columns=config['rename_map'], inplace=True)

    #         if 'partition_cols' in config:
    #             ts_col = config['dest_timestamp_col']
    #             df[ts_col] = pd.to_datetime(df[ts_col])
    #             df['year'] = df[ts_col].dt.year
    #             df['month'] = df[ts_col].dt.month

    #         # --- LOAD ---
    #         # Sử dụng cú pháp của DuckDB để ghi dữ liệu đã được partition
    #         # DuckDB sẽ tự tạo cấu trúc thư mục dạng hive (vd: year=2025/month=07/)

    #         if is_incremental:
    #             # Ghi vào một bảng tạm rồi MERGE hoặc INSERT
    #             duckdb_conn.register('new_data_df', df)

    #             # Tạo bảng nếu chưa tồn tại
    #             duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS {dest_table} AS SELECT * FROM new_data_df LIMIT 0;")

    #             # Chèn dữ liệu mới
    #             duckdb_conn.execute(f"INSERT INTO {dest_table} SELECT * FROM new_data_df;")
    #             duckdb_conn.unregister('new_data_df')

    #             # Cập nhật high-water mark
    #             new_max_timestamp = df[config['dest_timestamp_col']].max().strftime('%Y-%m-%d %H:%M:%S')
    #             new_etl_state[dest_table] = new_max_timestamp
    #             logger.info(f"Cập nhật high-water mark cho {dest_table} là: {new_max_timestamp}")
    #         else: # Full load (cho bảng dim_stores)
    #             # Ghi đè toàn bộ bảng
    #             duckdb_conn.execute(f"CREATE OR REPLACE TABLE {dest_table} AS SELECT * FROM df;")
            
    #         logger.info(f"Load thành công {len(df)} dòng vào bảng {dest_table} trong DuckDB.")

    except Exception as e:
        # THAY ĐỔI: Sử dụng logger để ghi lỗi
        logger.error(f"ETL thất bại nghiêm trọng tại {run_etl.__name__}!", exc_info=True)
    finally:
        # --- CLEANUP & STATE SAVING ---
        if sql_engine:
            sql_engine.dispose()
            logger.info("Đã giải phóng connection pool của SQLAlchemy.")

        if 'duckdb_conn' in locals() and duckdb_conn:
            duckdb_conn.close()

        save_etl_state(new_etl_state)
        logger.info("Lưu trạng thái ETL và đóng kết nối. Quy trình kết thúc.")

if __name__ == '__main__':
    run_etl()
