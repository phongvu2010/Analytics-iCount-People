import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import os

from duckdb import DuckDBPyConnection
from pathlib import Path
from typing import Optional

from app.core.config import etl_settings, TableConfig

logger = logging.getLogger(__name__)
BASE_DATA_PATH = Path(etl_settings.DATA_DIR)

def _validate_table_name(table_name: str):
    if not all(c.isalnum() or c == '_' for c in table_name):
        raise ValueError(f"Tên bảng không hợp lệ: '{table_name}'. Chỉ cho phép ký tự chữ, số và dấu gạch dưới.")

class ParquetLoader:
    def __init__(self, config: TableConfig):
        self.config = config
        self.dest_path = BASE_DATA_PATH / self.config.dest_table
        self.writer: Optional[pq.ParquetWriter] = None
        self.has_written_data = False 

    def __enter__(self):
        self.dest_path.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.writer:
            self.writer.close()
            logger.debug(f"ParquetWriter cho '{self.dest_path}' đã được đóng.")

        if exc_type is not None:
            logger.error(f"Xảy ra lỗi khi ghi Parquet cho '{self.config.dest_table}': {exc_val}")
            # Tùy chọn: Xóa các file/thư mục staging lỗi để đảm bảo lần chạy sau bắt đầu sạch
            # self.clean_staging_area() # Có thể gọi ở đây nếu muốn clean up ngay sau lỗi

    def write_chunk(self, df: pd.DataFrame):
        if df.empty: return

        try:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            if self.config.partition_cols:
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(self.dest_path),
                    partition_cols=self.config.partition_cols,
                    existing_data_behavior='overwrite_or_ignore'
                )
            else:
                if self.writer is None:
                    output_file = self.dest_path / 'data.parquet'
                    if not self.config.incremental and output_file.exists():
                        output_file.unlink()
                        logger.info(f"Full-load: Đã xóa file Parquet cũ: {output_file}")
                    
                    self.writer = pq.ParquetWriter(str(output_file), arrow_table.schema)
                    logger.debug(f"ParquetWriter cho '{output_file}' đã được tạo.")

                self.writer.write_table(arrow_table)
            
            self.has_written_data = True
        except pa.ArrowException as e:
            logger.error(f"Lỗi PyArrow khi ghi chunk cho bảng '{self.config.dest_table}': {e}")
            raise
        except Exception as e:
            logger.error(f"Lỗi không xác định khi ghi Parquet chunk cho bảng '{self.config.dest_table}': {e}")
            raise

    def clean_staging_area(self):
        if self.dest_path.exists():
            if self.dest_path.is_dir():
                try:
                    shutil.rmtree(self.dest_path)
                    logger.info(f"Đã xóa thư mục staging: {self.dest_path}")
                except OSError as e:
                    logger.error(f"Lỗi khi xóa thư mục staging '{self.dest_path}': {e}")
            elif self.dest_path.is_file():
                try:
                    self.dest_path.unlink()
                    logger.info(f"Đã xóa file staging: {self.dest_path}")
                except OSError as e:
                    logger.error(f"Lỗi khi xóa file staging '{self.dest_path}': {e}")

def prepare_destination(config: TableConfig):
    dest_path = BASE_DATA_PATH / config.dest_table
    if not config.incremental and dest_path.exists():
        if dest_path.is_dir():
            shutil.rmtree(dest_path)
            logger.info(f"Full-load: Đã xóa thư mục staging cũ: {dest_path}")
        else:
            dest_path.unlink()
            logger.info(f"Full-load: Đã xóa file staging cũ: {dest_path}")

    dest_path.mkdir(parents=True, exist_ok=True)

def refresh_duckdb_table(conn: DuckDBPyConnection, config: TableConfig, has_new_data_written: bool):
    dest_table = config.dest_table
    staging_table = f"{dest_table}_staging"

    _validate_table_name(dest_table)
    _validate_table_name(staging_table)

    staging_dir = BASE_DATA_PATH / dest_table

    if not has_new_data_written:
        logger.info(f"Không có dữ liệu mới được ghi vào thư mục staging '{staging_dir}'. Bỏ qua việc refresh DuckDB.")
        return

    if not staging_dir.exists() or not any(os.scandir(staging_dir)):
        logger.warning(f"Thư mục staging '{staging_dir}' không tồn tại hoặc rỗng dù đã ghi dữ liệu. Bỏ qua việc refresh DuckDB.")
        return

    logger.info(f"Bắt đầu tải dữ liệu vào bảng staging '{staging_table}'...")

    read_path: str
    read_options = ""
    if config.partition_cols:
        read_path = str(staging_dir / '**' / '*.parquet').replace('\\', '/')
        read_options = ", hive_partitioning=1"
        logger.info(f"Phát hiện bảng có partition. Đang đọc từ đường dẫn: {read_path}")
    else:
        single_file = staging_dir / 'data.parquet'
        if not single_file.exists():
            logger.warning(f"File parquet '{single_file}' không tìm thấy dù đã có dữ liệu. Bỏ qua.")
            return
        read_path = str(single_file).replace('\\', '/')
        logger.info(f"Phát hiện bảng không có partition. Đang đọc từ file: {read_path}")

    # Đảm bảo các cột trong Parquet khớp với kiểu dữ liệu mong muốn trong DuckDB
    # DuckDB thường tự động suy luận kiểu dữ liệu tốt từ Parquet,
    # nhưng nếu có vấn đề, có thể cần explicit casting trong SELECT * FROM read_parquet(...)
    
    try:
        create_staging_sql = f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM {read_statement};"
        conn.execute(create_staging_sql)
        logger.info(f"Tải dữ liệu vào bảng staging '{staging_table}' thành công.")
        logger.info(f"Bắt đầu hoán đổi bảng chính '{dest_table}' với bảng staging.")

        swap_sql = f"""
        BEGIN TRANSACTION;

        -- Xóa bảng backup cũ nếu nó còn tồn tại từ lần chạy trước
        DROP TABLE IF EXISTS {dest_table}_old;

        -- Đổi tên bảng chính hiện tại thành bảng backup.
        -- Dùng IF EXISTS để xử lý trường hợp ETL chạy lần đầu tiên (bảng chính chưa tồn tại)
        ALTER TABLE IF EXISTS {dest_table} RENAME TO {dest_table}_old;

        -- Đổi tên bảng staging thành bảng chính mới
        ALTER TABLE {staging_table} RENAME TO {dest_table};

        COMMIT;
        """
        conn.execute(swap_sql)
        logger.info(f"Hoán đổi bảng thành công. '{dest_table}' đã được cập nhật.")

        conn.execute(f"DROP TABLE IF EXISTS {dest_table}_old;")
        logger.info(f"Đã dọn dẹp bảng backup '{dest_table}_old'.")
    except Exception as e:
        logger.error(f"Lỗi khi refresh hoặc hoán đổi bảng DuckDB cho '{dest_table}': {e}", exc_info=True)
        conn.execute("ROLLBACK;")
        logger.warning(f"Đã ROLLBACK transaction do lỗi khi refresh/swap bảng '{dest_table}'.")
        raise
    finally:
        temp_loader = ParquetLoader(config)
        temp_loader.clean_staging_area()
