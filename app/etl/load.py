"""
Module xử lý giai đoạn 'L' (Load) của pipeline ETL.

Chức năng chính:
1. Ghi dữ liệu đã biến đổi vào các tệp Parquet trong một khu vực tạm
   (staging area). Parquet là một định dạng lưu trữ cột hiệu quả, tối ưu
   cho các truy vấn phân tích.
2. Tải dữ liệu từ Parquet vào DuckDB bằng kỹ thuật "atomic swap". Kỹ thuật
   này đảm bảo tính toàn vẹn và không gây gián đoạn (zero downtime) cho người
   dùng cuối đang truy vấn dữ liệu.
"""
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil

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
    Context manager để quản lý việc ghi dữ liệu vào tệp/dataset Parquet.

    Lớp này trừu tượng hóa logic phức tạp của việc ghi dữ liệu theo từng khối
    (chunk-by-chunk), đặc biệt hữu ích cho các bảng không phân vùng. Nó tự
    động xử lý việc mở và đóng `ParquetWriter` một cách an toàn.
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
        """ Dọn dẹp, đảm bảo writer được đóng lại an toàn khi thoát context. """
        if self.writer:
            self.writer.close()
        if exc_type is not None:
            logger.error(f"Lỗi khi ghi Parquet cho '{self.config.dest_table}': {exc_val}")

    def write_chunk(self, df: pd.DataFrame):
        """
        Ghi một chunk DataFrame vào tệp/dataset Parquet.

        - Đối với bảng có phân vùng, ghi trực tiếp vào dataset.
        - Đối với bảng không phân vùng, sử dụng một `ParquetWriter` duy nhất
          để ghi nối tiếp các chunk vào một file.
        """
        if df.empty:
            return

        try:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            if self.config.partition_cols:
                # Ghi dữ liệu phân vùng trực tiếp.
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(self.dest_path),
                    partition_cols=self.config.partition_cols,
                    existing_data_behavior='overwrite_or_ignore'
                )
            else:
                # Đối với bảng không phân vùng, sử dụng ParquetWriter.
                if self.writer is None:
                    output_file = self.dest_path / 'data.parquet'
                    # Nếu là full load, xóa file cũ trước khi ghi
                    if not self.config.incremental and output_file.exists():
                        output_file.unlink()
                    self.writer = pq.ParquetWriter(
                        str(output_file), arrow_table.schema
                    )
                self.writer.write_table(arrow_table)
            self.has_written_data = True
        except pa.ArrowException as e:
            logger.error(f"Lỗi PyArrow khi ghi chunk cho '{self.config.dest_table}': {e}")
            raise

    def clean_staging_area(self):
        """ Xóa thư mục/tệp Parquet sau khi đã tải xong vào DuckDB. """
        if not self.dest_path.exists():
            return

        try:
            if self.dest_path.is_dir():
                shutil.rmtree(self.dest_path)
            else:
                self.dest_path.unlink()
            logger.info(f"Đã xóa thư mục staging: {self.dest_path}")
        except OSError as e:
            logger.error(f"Lỗi khi xóa staging area '{self.dest_path}': {e}")


def prepare_destination(config: TableConfig):
    """ Chuẩn bị thư mục staging, xóa dữ liệu cũ nếu là full-load. """
    dest_path = BASE_DATA_PATH / config.dest_table
    if not config.incremental and dest_path.exists():
        logger.info(f"Full-load: Đã xóa staging cũ: {dest_path}")
        try:
            if dest_path.is_dir():
                shutil.rmtree(dest_path)
            else:
                dest_path.unlink()
        except OSError as e:
            logger.error(f"Lỗi khi dọn dẹp staging cũ '{dest_path}': {e}")
            raise
    dest_path.mkdir(parents=True, exist_ok=True)


def refresh_duckdb_table(conn: DuckDBPyConnection, config: TableConfig, has_new_data_written: bool):
    """
    Tải dữ liệu từ Parquet vào DuckDB và thực hiện "atomic swap".

    Quy trình này đảm bảo an toàn và không gián đoạn:
    1. Tải dữ liệu từ Parquet vào một bảng tạm (`_staging`).
    2. Bắt đầu một `TRANSACTION`.
    3. Đổi tên bảng chính hiện tại thành bảng cũ (`_old`).
    4. Đổi tên bảng tạm thành bảng chính.
    5. `COMMIT` transaction. Bước này diễn ra gần như tức thời.
    6. Xóa bảng cũ.
    7. Nếu có lỗi ở bất kỳ bước nào, `ROLLBACK` để quay về trạng thái ban đầu.
    """
    dest_table = config.dest_table
    staging_table = f'{dest_table}_staging'

    _validate_table_name(dest_table)
    _validate_table_name(staging_table)

    staging_dir = BASE_DATA_PATH / dest_table

    if not has_new_data_written:
        logger.info(f"Không có dữ liệu mới trong staging, bỏ qua refresh DuckDB cho '{dest_table}'.")
        return

    success = False
    try:
        if not staging_dir.exists() or not any(os.scandir(staging_dir)):
            logger.warning(f"Thư mục staging '{staging_dir}' rỗng. Bỏ qua.")
            return

        # 1. Tải dữ liệu vào bảng staging
        logger.info(f"Bắt đầu tải Parquet vào staging table '{staging_table}'...")
        read_path = str(staging_dir)
        # DuckDB có thể tự động phát hiện hive partitioning nếu đường dẫn là một thư mục.
        create_staging_sql = f"""
            CREATE OR REPLACE TABLE {staging_table} AS
            SELECT * FROM read_parquet('{read_path}/**', hive_partitioning=true);
        """
        conn.execute(create_staging_sql)
        logger.info(f"Tải vào staging table '{staging_table}' thành công.")

        # 2. Thực hiện hoán đổi nguyên tử (atomic swap)
        logger.info(f"Bắt đầu hoán đổi (atomic swap) cho bảng '{dest_table}'...")
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
        conn.execute(f'DROP TABLE IF EXISTS {dest_table}_old;')
        logger.info(f"Đã dọn dẹp bảng backup '{dest_table}_old'.")
        success = True

    except Exception as e:
        logger.error(f"Lỗi khi refresh bảng DuckDB '{dest_table}': {e}", exc_info=True)
        conn.execute('ROLLBACK;')
        logger.warning(f"Đã ROLLBACK transaction cho bảng '{dest_table}'.")
        raise

    finally:
        # 4. Dọn dẹp file Parquet
        if success or settings.ETL_CLEANUP_ON_FAILURE:
            logger.info('Bắt đầu dọn dẹp thư mục staging...')
            ParquetLoader(config).clean_staging_area()
        else:
            logger.warning(f"Giữ lại dữ liệu staging tại '{staging_dir}' để gỡ lỗi.")
