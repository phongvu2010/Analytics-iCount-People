# scripts/run_etl.py
# Script chính, đóng vai trò điều phối viên (orchestrator).
import duckdb
import logging
import pandas as pd
import sys

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Optional

sys.path.append(str(Path.cwd()))

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table

# --- Setup ---
setup_logging('app/logger.yaml') # Đường dẫn mới tới file logger
logger = logging.getLogger(__name__)

def process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
    """Điều phối toàn bộ quy trình ETL cho một bảng."""
    logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

    # 1. Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu cần)
    prepare_destination(config)

    # 2. Lấy high-water-mark từ lần chạy trước
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

    # 3. Trích xuất dữ liệu
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

    total_rows = 0
    max_ts_in_run: Optional[pd.Timestamp] = None

    try:
        with ParquetLoader(config) as loader:
            for chunk in data_iterator:
                # 4. Biến đổi dữ liệu
                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty:
                    continue

                # 5. Tải dữ liệu vào Parquet
                loader.write_chunk(transformed_chunk)

                # Cập nhật số liệu thống kê
                total_rows += len(transformed_chunk)
                current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
                if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                    max_ts_in_run = current_max_ts
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk và ghi Parquet cho bảng '{config.dest_table}': {e}")
        raise

    if total_rows == 0:
        logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
        return

    logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")

    # 6. Tải dữ liệu từ Parquet vào DuckDB
    refresh_duckdb_table(duckdb_conn, config)
    logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")

    # 7. Cập nhật state nếu là incremental
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
                continue
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}\n", exc_info=True)
    finally:
        if sql_engine: sql_engine.dispose()
        if duckdb_conn: duckdb_conn.close()
        logger.info("Quy trình ETL kết thúc.\n")

if __name__ == '__main__':
    main()




# def process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
#     """Điều phối toàn bộ quy trình ETL cho một bảng."""
#     logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

#     # 1. Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu cần)
#     prepare_destination(config)

#     # 2. Lấy high-water-mark từ lần chạy trước
#     last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

#     # 3. Trích xuất dữ liệu
#     data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

#     total_rows = 0
#     max_ts_in_run: Optional[pd.Timestamp] = None

#     # --- Thay đổi 2: Sử dụng ParquetLoader để đơn giản hóa hoàn toàn logic ghi file ---
#     # Toàn bộ logic phức tạp về writer, partition đã được đóng gói trong ParquetLoader
#     try:
#         with ParquetLoader(config) as loader:
#             for chunk in data_iterator:
#                 # 4. Biến đổi dữ liệu
#                 transformed_chunk = transform.run_transformations(chunk, config)
#                 if transformed_chunk.empty:
#                     continue

#                 # 5. Tải dữ liệu vào Parquet
#                 loader.write_chunk(transformed_chunk)

#                 # Cập nhật số liệu thống kê
#                 total_rows += len(transformed_chunk)
#                 current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
#                 if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
#                     max_ts_in_run = current_max_ts
#     except Exception as e:
#         logger.error(f"Lỗi trong quá trình xử lý chunk và ghi Parquet cho bảng '{config.dest_table}': {e}")
#         # Ném lại lỗi để quy trình xử lý bảng này dừng lại và được ghi nhận là thất bại
#         raise

#     if total_rows == 0:
#         logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
#         return

#     logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")

#     # 6. Tải dữ liệu từ Parquet vào DuckDB
#     refresh_duckdb_table(duckdb_conn, config)
#     logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")

#     # 7. Cập nhật state nếu là incremental
#     if config.incremental and max_ts_in_run:
#         state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)

# def main():
#     """Hàm chính điều phối toàn bộ quy trình ETL."""
#     etl_state = state.load_etl_state()
#     sql_engine, duckdb_conn = None, None

#     logger.info("Quy trình ETL bắt đầu...")
#     try:
#         sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
#         duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
#         duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
#         logger.info(f"Kết nối thành công đến SQL Server và DuckDB ('{duckdb_path}').\n")

#         for table_name, config in etl_settings.TABLE_CONFIG.items():
#             try:
#                 process_table(sql_engine, duckdb_conn, config, etl_state)
#                 # Chỉ lưu state khi một bảng được xử lý thành công
#                 state.save_etl_state(etl_state)
#                 logger.info(f"Xử lý thành công bảng '{config.source_table}'. State đã được lưu.\n")
#             except Exception as e:
#                 logger.error(f"Xử lý bảng '{config.source_table}' thất bại. Lỗi: {e}\n", exc_info=True)
#                 # Bỏ qua bảng bị lỗi và tiếp tục với bảng tiếp theo
#                 continue
#     except Exception as e:
#         logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}\n", exc_info=True)
#     finally:
#         if sql_engine: sql_engine.dispose()
#         if duckdb_conn: duckdb_conn.close()
#         logger.info("Quy trình ETL kết thúc.\n")




# def process_table(sql_engine, duckdb_conn, config: TableConfig, etl_state):
#     # ...
#     load.prepare_destination(config)
#     last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
#     data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)
    
#     total_rows = 0
#     max_ts_in_run: Optional[pd.Timestamp] = None
    
#     # --- Logic mới, gọn gàng hơn rất nhiều ---
#     logger.info("Bắt đầu ghi dữ liệu ra Parquet...")
#     with ParquetLoader(config) as loader:
#         for chunk in data_iterator:
#             transformed_chunk = transform.run_transformations(chunk, config)
#             if transformed_chunk.empty:
#                 continue

#             loader.write_chunk(transformed_chunk) # <-- Chỉ cần gọi một hàm duy nhất

#             total_rows += len(transformed_chunk)
#             current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
#             if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
#                 max_ts_in_run = current_max_ts

#     if total_rows == 0:
#         logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
#         return

#     logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")
#     # ... (gọi refresh_duckdb_table và các hàm khác như cũ)

# tools/run_etl.py

# def process_table(sql_engine, duckdb_conn, config: TableConfig, etl_state):
#     # ... (giữ nguyên phần trên) ...

#     total_rows = 0
#     max_ts_in_run: Optional[pd.Timestamp] = None
#     writer: Optional[pq.ParquetWriter] = None

#     # --- BẮT ĐẦU PHẦN SỬA LỖI ---
#     # 1. Tạo iterator một lần duy nhất
#     data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

#     try:
#         # Vòng lặp duy nhất xử lý tất cả các chunk
#         for chunk in data_iterator:
#             if chunk.empty: continue

#             transformed_chunk = transform.run_transformations(chunk, config)
#             if transformed_chunk.empty: continue

#             # Đối với bảng không phân vùng, khởi tạo writer một cách "lười biếng" (lazily) ở chunk đầu tiên
#             if not config.partition_cols and writer is None:
#                 output_file = Path(etl_settings.DATA_DIR) / config.dest_table / 'data.parquet'
#                 arrow_table = pa.Table.from_pandas(transformed_chunk, preserve_index=False)
#                 writer = pq.ParquetWriter(str(output_file), arrow_table.schema)

#             # Ghi chunk vào parquet
#             load.to_parquet(transformed_chunk, config, writer)

#             total_rows += len(transformed_chunk)
#             current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
#             if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
#                 max_ts_in_run = current_max_ts
#     finally:
#         if writer:
#             writer.close()
#     # --- KẾT THÚC PHẦN SỬA LỖI ---

#     # ... (giữ nguyên phần còn lại) ...


# import pyarrow as pa
# import pyarrow.parquet as pq

# # Thêm thư mục gốc của dự án vào Python Path để có thể import `app`
# sys.path.append(str(Path(__file__).resolve().parent.parent))

# from app.etl import state, extract, transform, load # <-- Import các module đã tách

# # # Đây là script chính, đóng vai trò điều phối viên (orchestrator). Nó sẽ import và gọi các hàm từ app.etl

# # def process_table(sql_engine, duckdb_conn, config: TableConfig, etl_state):
# #     """Điều phối toàn bộ quy trình ETL cho một bảng."""
# #     logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

# #     # 1. Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu cần)
# #     load.prepare_destination(config)

# #     # 2. Lấy high-water-mark từ lần chạy trước
# #     last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

# #     # 3. Trích xuất, biến đổi và tải vào Parquet theo từng chunk
# #     total_rows = 0
# #     max_ts_in_run: Optional[pd.Timestamp] = None
# #     writer: Optional[pq.ParquetWriter] = None

# #     try:
# #         # Đối với bảng không phân vùng, chúng ta cần một writer duy nhất
# #         if not config.partition_cols:
# #             output_file = Path(etl_settings.DATA_DIR) / config.dest_table / 'data.parquet'
# #             # Cần lấy schema từ chunk đầu tiên để khởi tạo writer
# #             first_chunk = next(extract.from_sql_server(sql_engine, config, last_timestamp), None)
# #             if first_chunk is not None:
# #                 transformed_chunk = transform.run_transformations(first_chunk, config)
# #                 arrow_table = pa.Table.from_pandas(transformed_chunk, preserve_index=False)
# #                 writer = pq.ParquetWriter(str(output_file), arrow_table.schema)
# #                 load.to_parquet(transformed_chunk, config, writer)

# #                 total_rows += len(transformed_chunk)
# #                 current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
# #                 if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
# #                     max_ts_in_run = current_max_ts

# #         # Xử lý các chunk còn lại
# #         for chunk in extract.from_sql_server(sql_engine, config, last_timestamp):
# #             if chunk.empty: continue

# #             transformed_chunk = transform.run_transformations(chunk, config)
# #             if transformed_chunk.empty: continue

# #             load.to_parquet(transformed_chunk, config, writer)

# #             total_rows += len(transformed_chunk)
# #             current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
# #             if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
# #                 max_ts_in_run = current_max_ts
# #     finally:
# #         if writer:
# #             writer.close()

# #     if total_rows == 0:
# #         logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
# #         return

# #     logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")

# #     # 4. Tải dữ liệu từ Parquet vào DuckDB
# #     load.refresh_duckdb_table(duckdb_conn, config)
# #     logger.info(f"Đã tải thành công {total_rows} dòng vào bảng DuckDB '{config.dest_table}'.")

# #     # 5. Cập nhật state nếu là incremental
# #     if config.incremental and max_ts_in_run:
# #         state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)

# # def main():
# #     """Hàm chính điều phối toàn bộ quy trình ETL."""
# #     etl_state = state.load_etl_state()
# #     sql_engine, duckdb_conn = None, None

# #     logger.info("Quy trình ETL bắt đầu...")
# #     try:
# #         sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
# #         duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
# #         duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
# #         logger.info(f"Kết nối thành công đến SQL Server và DuckDB ('{duckdb_path}').\n")

# #         for table_name, config in etl_settings.TABLE_CONFIG.items():
# #             try:
# #                 process_table(sql_engine, duckdb_conn, config, etl_state)
# #                 state.save_etl_state(etl_state)
# #                 logger.info(f"Xử lý thành công bảng '{config.source_table}'. State đã được lưu.\n")
# #             except Exception as e:
# #                 logger.error(f"Xử lý bảng '{config.source_table}' thất bại. Lỗi: {e}\n", exc_info=True)
# #     except Exception as e:
# #         logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}\n", exc_info=True)
# #     finally:
# #         if sql_engine: sql_engine.dispose()
# #         if duckdb_conn: duckdb_conn.close()
# #         logger.info("Quy trình ETL kết thúc.\n")
















# import pyarrow as pa
# import pyarrow.parquet as pq

# # Thêm thư mục gốc của dự án vào Python Path để có thể import `app`
# sys.path.append(str(Path(__file__).resolve().parent.parent))

# from app.etl import state, extract, transform, load # <-- Import các module đã tách



# def process_table(sql_engine, duckdb_conn, config: TableConfig, etl_state):
#     """Điều phối toàn bộ quy trình ETL cho một bảng."""
#     logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

#     # 1. Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu cần)
#     load.prepare_destination(config)

#     # 2. Lấy high-water-mark từ lần chạy trước
#     last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

#     # 3. Trích xuất, biến đổi và tải vào Parquet theo từng chunk
#     total_rows = 0
#     max_ts_in_run: Optional[pd.Timestamp] = None
#     writer: Optional[pq.ParquetWriter] = None

#     try:
#         # Đối với bảng không phân vùng, chúng ta cần một writer duy nhất
#         if not config.partition_cols:
#             output_file = Path(etl_settings.DATA_DIR) / config.dest_table / 'data.parquet'
#             # Cần lấy schema từ chunk đầu tiên để khởi tạo writer
#             first_chunk = next(extract.from_sql_server(sql_engine, config, last_timestamp), None)
#             if first_chunk is not None:
#                 transformed_chunk = transform.run_transformations(first_chunk, config)
#                 arrow_table = pa.Table.from_pandas(transformed_chunk, preserve_index=False)
#                 writer = pq.ParquetWriter(str(output_file), arrow_table.schema)
#                 load.to_parquet(transformed_chunk, config, writer)

#                 total_rows += len(transformed_chunk)
#                 current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
#                 if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
#                     max_ts_in_run = current_max_ts

#         # Xử lý các chunk còn lại
#         for chunk in extract.from_sql_server(sql_engine, config, last_timestamp):
#             if chunk.empty: continue

#             transformed_chunk = transform.run_transformations(chunk, config)
#             if transformed_chunk.empty: continue

#             load.to_parquet(transformed_chunk, config, writer)

#             total_rows += len(transformed_chunk)
#             current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
#             if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
#                 max_ts_in_run = current_max_ts
#     finally:
#         if writer:
#             writer.close()

#     if total_rows == 0:
#         logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
#         return

#     logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")

#     # 4. Tải dữ liệu từ Parquet vào DuckDB
#     load.refresh_duckdb_table(duckdb_conn, config)
#     logger.info(f"Đã tải thành công {total_rows} dòng vào bảng DuckDB '{config.dest_table}'.")

#     # 5. Cập nhật state nếu là incremental
#     if config.incremental and max_ts_in_run:
#         state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)

# def main():
#     """Hàm chính điều phối toàn bộ quy trình ETL."""
#     etl_state = state.load_etl_state()
#     sql_engine, duckdb_conn = None, None

#     logger.info("Quy trình ETL bắt đầu...")
#     try:
#         sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
#         duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
#         duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
#         logger.info(f"Kết nối thành công đến SQL Server và DuckDB ('{duckdb_path}').\n")

#         for table_name, config in etl_settings.TABLE_CONFIG.items():
#             try:
#                 process_table(sql_engine, duckdb_conn, config, etl_state)
#                 state.save_etl_state(etl_state)
#                 logger.info(f"Xử lý thành công bảng '{config.source_table}'. State đã được lưu.\n")
#             except Exception as e:
#                 logger.error(f"Xử lý bảng '{config.source_table}' thất bại. Lỗi: {e}\n", exc_info=True)
#     except Exception as e:
#         logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}\n", exc_info=True)
#     finally:
#         if sql_engine: sql_engine.dispose()
#         if duckdb_conn: duckdb_conn.close()
#         logger.info("Quy trình ETL kết thúc.\n")
