"""
Module này chịu trách nhiệm cho bước Load trong quy trình ETL.

Cụ thể, nó bao gồm:
- Ghi dữ liệu từ DataFrame vào các file Parquet (có hỗ trợ partitioning).
- Tải dữ liệu từ file Parquet vào DuckDB bằng kỹ thuật "atomic swap"
  để đảm bảo tính toàn vẹn và không có thời gian chết.
"""
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
    """ Kiểm tra tên bảng để phòng chống SQL Injection cơ bản. """
    if not all(c.isalnum() or c == "_" for c in table_name):
        raise ValueError(f"Tên bảng không hợp lệ: '{table_name}'. Chỉ cho phép ký tự chữ, số và dấu gạch dưới.")

class ParquetLoader:
    """
    Một class helper để ghi các chunk DataFrame vào file Parquet.

    Sử dụng như một context manager để đảm bảo tài nguyên được giải phóng đúng cách.

    Attributes:
        config (TableConfig): Cấu hình cho bảng đang được xử lý.
        dest_path (Path): Đường dẫn đến thư mục/file Parquet đích.
        writer (Optional[pq.ParquetWriter]): Đối tượng writer cho file Parquet.
        has_written_data (bool): Cờ để theo dõi xem có dữ liệu nào đã được ghi hay chưa.
    """
    def __init__(self, config: TableConfig):
        """ Khởi tạo ParquetLoader. """
        self.config = config
        self.dest_path = BASE_DATA_PATH / self.config.dest_table
        self.writer: Optional[pq.ParquetWriter] = None
        self.has_written_data = False

    def __enter__(self):
        """ Thiết lập context, tạo thư mục đích nếu cần. """
        self.dest_path.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Dọn dẹp context, đóng file writer nếu nó đang mở. """
        if self.writer:
            self.writer.close()
            logger.debug(f"ParquetWriter cho '{self.dest_path}' đã được đóng.")

        if exc_type is not None:
            logger.error(f"Xảy ra lỗi khi ghi Parquet cho '{self.config.dest_table}': {exc_val}")
            # Tùy chọn: Xóa các file/thư mục staging lỗi để đảm bảo lần chạy sau bắt đầu sạch
            # self.clean_staging_area() # Có thể gọi ở đây nếu muốn clean up ngay sau lỗi

    def write_chunk(self, df: pd.DataFrame):
        """
        Ghi một chunk DataFrame vào file/thư mục Parquet.

        Hỗ trợ cả ghi vào một file duy nhất (không partition) và ghi vào
        một cấu trúc thư mục (có partition).

        Args:
            df (pd.DataFrame): DataFrame chunk để ghi.
        """
        if df.empty: return

        try:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            if self.config.partition_cols:
                # Ghi dữ liệu vào dataset được partition
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(self.dest_path),
                    partition_cols=self.config.partition_cols,
                    existing_data_behavior="overwrite_or_ignore"
                )
            else:
                # Ghi dữ liệu vào một file Parquet duy nhất
                if self.writer is None:
                    output_file = self.dest_path / "data.parquet"
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
        """ Xóa thư mục/file Parquet staging. """
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
    """ Chuẩn bị thư mục đích. Nếu là full-load, xóa dữ liệu cũ. """
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
    """
    Tải dữ liệu từ Parquet vào DuckDB và thực hiện hoán đổi bảng (atomic swap).

    Quy trình:
    1. Tạo một bảng staging từ các file Parquet.
    2. Bắt đầu một TRANSACTION.
    3. Đổi tên bảng chính hiện tại thành bảng_old (nếu có).
    4. Đổi tên bảng staging thành bảng chính.
    5. COMMIT transaction. Nếu có lỗi, ROLLBACK.
    6. Xóa bảng_old.

    Args:
        conn: Đối tượng kết nối DuckDB.
        config: Cấu hình cho bảng đang được xử lý.
        has_new_data_written: Cờ cho biết có dữ liệu mới trong staging hay không.
    """
    dest_table = config.dest_table
    staging_table = f"{dest_table}_staging"

    _validate_table_name(dest_table)
    _validate_table_name(staging_table)

    staging_dir = BASE_DATA_PATH / dest_table

    if not has_new_data_written:
        logger.info(f"Không có dữ liệu mới được ghi vào thư mục staging '{staging_dir}'. Bỏ qua việc refresh DuckDB.")
        return

    success = False
    try:
        if not staging_dir.exists() or not any(os.scandir(staging_dir)):
            logger.warning(f"Thư mục staging '{staging_dir}' không tồn tại hoặc rỗng dù đã ghi dữ liệu. Bỏ qua.")
            return

        logger.info(f"Bắt đầu tải dữ liệu vào bảng staging '{staging_table}'...")

        # Đọc dữ liệu từ Parquet vào bảng staging
        read_path_str = str(staging_dir).replace("\\", "/")
        read_statement = f"read_parquet('{read_path_str}/*/*.parquet', hive_partitioning=1)" \
            if config.partition_cols else f"read_parquet('{read_path_str}/data.parquet')"

        create_staging_sql = f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM {read_statement};"
        conn.execute(create_staging_sql)
        logger.info(f"Tải dữ liệu vào bảng staging '{staging_table}' thành công.")

        logger.info(f"Bắt đầu hoán đổi bảng chính '{dest_table}' với bảng staging.")
        # Kỹ thuật "Atomic Swap" để đảm bảo không có downtime
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

        # Dọn dẹp bảng backup sau khi hoán đổi thành công
        conn.execute(f"DROP TABLE IF EXISTS {dest_table}_old;")
        logger.info(f"Đã dọn dẹp bảng backup '{dest_table}_old'.")

        success = True
    except Exception as e:
        logger.error(f"Lỗi khi refresh hoặc hoán đổi bảng DuckDB cho '{dest_table}': {e}", exc_info=True)
        conn.execute("ROLLBACK;")
        logger.warning(f"Đã ROLLBACK transaction do lỗi khi refresh/swap bảng '{dest_table}'.")
        raise
    finally:
        # Dọn dẹp thư mục staging tùy theo cấu hình
        if success or etl_settings.ETL_CLEANUP_ON_FAILURE:
            logger.info("Bắt đầu dọn dẹp thư mục staging...")
            temp_loader = ParquetLoader(config)
            temp_loader.clean_staging_area()
        else:
            logger.warning(
                f"Giữ lại dữ liệu trong thư mục staging '{staging_dir}' để gỡ lỗi "
                f"(ETL_CLEANUP_ON_FAILURE=false)."
            )
