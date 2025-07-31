# tools/run_etl.py
# Đây là script chính, đóng vai trò điều phối viên (orchestrator). Nó sẽ import và gọi các hàm từ app.etl
import duckdb
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys

from pathlib import Path
from sqlalchemy import create_engine
from typing import Optional

# Thêm thư mục gốc của dự án vào Python Path để có thể import `app`
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform, load # <-- Import các module đã tách

# --- Setup ---
setup_logging('logger.yaml')
logger = logging.getLogger(__name__)

def process_table(sql_engine, duckdb_conn, config: TableConfig, etl_state):
    """Điều phối toàn bộ quy trình ETL cho một bảng."""
    logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

    # 1. Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu cần)
    load.prepare_destination(config)

    # 2. Lấy high-water-mark từ lần chạy trước
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

    # 3. Trích xuất, biến đổi và tải vào Parquet theo từng chunk
    total_rows = 0
    max_ts_in_run: Optional[pd.Timestamp] = None
    writer: Optional[pq.ParquetWriter] = None

    try:
        # Đối với bảng không phân vùng, chúng ta cần một writer duy nhất
        if not config.partition_cols:
            output_file = Path(etl_settings.DATA_DIR) / config.dest_table / 'data.parquet'
            # Cần lấy schema từ chunk đầu tiên để khởi tạo writer
            first_chunk = next(extract.from_sql_server(sql_engine, config, last_timestamp), None)
            if first_chunk is not None:
                transformed_chunk = transform.run_transformations(first_chunk, config)
                arrow_table = pa.Table.from_pandas(transformed_chunk, preserve_index=False)
                writer = pq.ParquetWriter(str(output_file), arrow_table.schema)
                load.to_parquet(transformed_chunk, config, writer)

                total_rows += len(transformed_chunk)
                current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
                if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                    max_ts_in_run = current_max_ts

        # Xử lý các chunk còn lại
        for chunk in extract.from_sql_server(sql_engine, config, last_timestamp):
            if chunk.empty: continue

            transformed_chunk = transform.run_transformations(chunk, config)
            if transformed_chunk.empty: continue

            load.to_parquet(transformed_chunk, config, writer)

            total_rows += len(transformed_chunk)
            current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
            if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                max_ts_in_run = current_max_ts
    finally:
        if writer:
            writer.close()

    if total_rows == 0:
        logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
        return

    logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")

    # 4. Tải dữ liệu từ Parquet vào DuckDB
    load.refresh_duckdb_table(duckdb_conn, config)
    logger.info(f"Đã tải thành công {total_rows} dòng vào bảng DuckDB '{config.dest_table}'.")

    # 5. Cập nhật state nếu là incremental
    if config.incremental and max_ts_in_run:
        state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)

def main():
    """Hàm chính điều phối toàn bộ quy trình ETL."""
    etl_state = state.load_etl_state()
    sql_engine, duckdb_conn = None, None

    logger.info("Quy trình ETL bắt đầu...")
    try:
        sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
        duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
        logger.info(f"Kết nối thành công đến SQL Server và DuckDB ('{duckdb_path}').\n")

        for table_name, config in etl_settings.TABLE_CONFIG.items():
            try:
                process_table(sql_engine, duckdb_conn, config, etl_state)
                state.save_etl_state(etl_state)
                logger.info(f"Xử lý thành công bảng '{config.source_table}'. State đã được lưu.\n")
            except Exception as e:
                logger.error(f"Xử lý bảng '{config.source_table}' thất bại. Lỗi: {e}\n", exc_info=True)
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}\n", exc_info=True)
    finally:
        if sql_engine: sql_engine.dispose()
        if duckdb_conn: duckdb_conn.close()
        logger.info("Quy trình ETL kết thúc.\n")

if __name__ == '__main__':
    main()
