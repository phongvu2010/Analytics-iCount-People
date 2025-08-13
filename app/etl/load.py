"""
Module này xử lý giai đoạn 'L' (Load) của pipeline ETL.

Chức năng chính:
1.  Ghi dữ liệu (đã biến đổi) vào các file Parquet (staging area).
2.  Tải dữ liệu từ các file Parquet vào DuckDB bằng kỹ thuật "atomic swap"
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

from ..core.config import settings, TableConfig

logger = logging.getLogger(__name__)
BASE_DATA_PATH = Path(settings.DATA_DIR)

def _validate_table_name(table_name: str):
    """ Kiểm tra tên bảng để tránh lỗi SQL Injection cơ bản. """
    if not all(c.isalnum() or c == '_' for c in table_name):
        raise ValueError(f"Tên bảng không hợp lệ: '{table_name}'.")

class ParquetLoader:
    """
    Một context manager để xử lý việc ghi dữ liệu vào file/dataset Parquet.
    Nó quản lý việc mở và đóng ParquetWriter, đặc biệt hữu ích khi
    ghi dữ liệu theo từng khối (chunk-by-chunk).
    """
    def __init__(self, config: TableConfig):
        self.config = config
        self.dest_path = BASE_DATA_PATH / self.config.dest_table
        self.writer: Optional[pq.ParquetWriter] = None
        self.has_written_data = False

    def __enter__(self):
        """ Khởi tạo môi trường, tạo thư mục nếu cần. """
        self.dest_path.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Dọn dẹp, đảm bảo writer được đóng lại. """
        if self.writer:
            self.writer.close()

        if exc_type is not None:
            logger.error(f"Lỗi khi ghi Parquet cho '{self.config.dest_table}': {exc_val}")

    def write_chunk(self, df: pd.DataFrame):
        """ Ghi một chunk DataFrame vào file/dataset Parquet. """
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
                    self.writer = pq.ParquetWriter(str(output_file), arrow_table.schema)
                self.writer.write_table(arrow_table)
            self.has_written_data = True
        except pa.ArrowException as e:
            logger.error(f"Lỗi PyArrow khi ghi chunk cho '{self.config.dest_table}': {e}")
            raise

    def clean_staging_area(self):
        """ Xóa thư mục/file Parquet sau khi đã tải xong vào DuckDB. """
        if not self.dest_path.exists(): return

        if self.dest_path.is_dir(): shutil.rmtree(self.dest_path)
        else: self.dest_path.unlink()
        logger.info(f"Đã xóa thư mục staging: {self.dest_path}")

def prepare_destination(config: TableConfig):
    """ Chuẩn bị thư mục staging, xóa dữ liệu cũ nếu là full-load. """
    dest_path = BASE_DATA_PATH / config.dest_table
    if not config.incremental and dest_path.exists():
        if dest_path.is_dir(): shutil.rmtree(dest_path)
        else: dest_path.unlink()
        logger.info(f"Full-load: Đã xóa staging cũ: {dest_path}")
    dest_path.mkdir(parents=True, exist_ok=True)

def refresh_duckdb_table(conn: DuckDBPyConnection, config: TableConfig, has_new_data_written: bool):
    """
    Tải dữ liệu từ Parquet vào DuckDB và thực hiện "atomic swap".

    Quy trình:
    1. Tải dữ liệu từ các file Parquet vào một bảng tạm (staging_table).
    2. Bắt đầu một TRANSACTION.
    3. Đổi tên bảng chính hiện tại thành bảng cũ (old_table).
    4. Đổi tên bảng tạm thành bảng chính.
    5. COMMIT transaction. Nếu thành công, dữ liệu mới sẽ được "publish".
    6. Xóa bảng cũ.
    7. Nếu có lỗi, ROLLBACK để quay về trạng thái ban đầu.
    """
    dest_table = config.dest_table
    staging_table = f"{dest_table}_staging"

    _validate_table_name(dest_table)
    _validate_table_name(staging_table)

    staging_dir = BASE_DATA_PATH / dest_table

    if not has_new_data_written:
        logger.info(f"Không có dữ liệu mới, bỏ qua refresh DuckDB cho '{dest_table}'.")
        return

    success = False
    try:
        if not staging_dir.exists() or not any(os.scandir(staging_dir)):
            logger.warning(f"Thư mục staging '{staging_dir}' rỗng. Bỏ qua.")
            return

        # 1. Tải dữ liệu vào bảng staging
        logger.info(f"Bắt đầu tải dữ liệu vào bảng staging '{staging_table}'...")
        if config.partition_cols:
            read_path = str(staging_dir / '**' / '*.parquet')
            hive_partitioning = True
        else:
            read_path = str(staging_dir / 'data.parquet')
            hive_partitioning = False

        create_staging_sql = f"""
            CREATE OR REPLACE TABLE {staging_table} AS
            SELECT * FROM read_parquet(?, hive_partitioning=?);
        """
        conn.execute(create_staging_sql, [read_path, hive_partitioning])
        logger.info(f"Tải dữ liệu vào bảng staging '{staging_table}' thành công.")

        # 2. Thực hiện hoán đổi nguyên tử (atomic swap)
        logger.info(f"Bắt đầu hoán đổi bảng chính '{dest_table}'...")
        swap_sql = f"""
        BEGIN TRANSACTION;

        -- Bước 1: Dọn dẹp bảng backup cũ (_old) nếu nó còn tồn tại
        -- từ một lần chạy trước bị lỗi giữa chừng, đảm bảo trạng thái sạch.
        DROP TABLE IF EXISTS {dest_table}_old;

        -- Bước 2: Đổi tên bảng chính hiện tại thành bảng backup.
        -- 'IF EXISTS' đảm bảo không có lỗi nếu đây là lần chạy đầu tiên
        -- và bảng chính chưa tồn tại.
        ALTER TABLE IF EXISTS {dest_table} RENAME TO {dest_table}_old;

        -- Bước 3: "Thăng cấp" bảng staging mới thành bảng chính.
        -- Đây là bước hoán đổi cốt lõi, diễn ra cực nhanh và an toàn trong transaction.
        ALTER TABLE {staging_table} RENAME TO {dest_table};

        COMMIT;
        """
        conn.execute(swap_sql)
        logger.info(f"Hoán đổi bảng '{dest_table}' thành công.")

        # 3. Dọn dẹp bảng backup
        conn.execute(f"DROP TABLE IF EXISTS {dest_table}_old;")
        logger.info(f"Đã dọn dẹp bảng backup '{dest_table}_old'.")
        success = True

    except Exception as e:
        logger.error(f"Lỗi khi refresh bảng DuckDB '{dest_table}': {e}", exc_info=True)
        conn.execute('ROLLBACK;') # Đảm bảo quay lại trạng thái an toàn
        logger.warning(f"Đã ROLLBACK transaction cho bảng '{dest_table}'.")
        raise

    finally:
        # 4. Dọn dẹp file Parquet
        if success or settings.ETL_CLEANUP_ON_FAILURE:
            logger.info('Bắt đầu dọn dẹp thư mục staging...')
            ParquetLoader(config).clean_staging_area()
        else:
            logger.warning(f"Giữ lại dữ liệu staging tại '{staging_dir}' để gỡ lỗi.")
