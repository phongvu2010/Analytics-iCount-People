import logging
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


class ParquetLoader:
    """
    Context manager để quản lý việc ghi dữ liệu vào tệp/dataset Parquet.

    Lớp này trừu tượng hóa logic phức tạp của việc ghi dữ liệu theo từng khối
    (chunk-by-chunk), tự động xử lý việc mở và đóng `ParquetWriter` an toàn.
    """
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
        if exc_type is not None:
            logger.error(
                f"Lỗi khi ghi Parquet cho '{self.config.dest_table}': {exc_val}"
            )

    def write_chunk(self, df: pd.DataFrame):
        if df.empty:
            return
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
                    self.writer = pq.ParquetWriter(
                        str(output_file), arrow_table.schema
                    )
                self.writer.write_table(arrow_table)
            self.has_written_data = True
        except pa.ArrowException as e:
            logger.error(
                f"Lỗi PyArrow khi ghi chunk cho '{self.config.dest_table}': {e}"
            )
            raise


def prepare_destination(config: TableConfig):
    dest_path = BASE_DATA_PATH / config.dest_table
    if not config.incremental and dest_path.exists():
        logger.info(f"Full-load: Đã xóa staging cũ: {dest_path}")
        try:
            shutil.rmtree(dest_path)
        except OSError as e:
            logger.error(f"Lỗi khi dọn dẹp staging cũ '{dest_path}': {e}")
            raise
    dest_path.mkdir(parents=True, exist_ok=True)


def refresh_duckdb_table(
    conn: DuckDBPyConnection, config: TableConfig, has_new_data: bool
):
    """
    Tải dữ liệu từ Parquet vào DuckDB và thực hiện "atomic swap".

    Quy trình này đảm bảo an toàn và không gián đoạn:
    1. Tải dữ liệu từ Parquet vào một bảng tạm (`_staging`).
    2. Bắt đầu một TRANSACTION.
    3. Đổi tên bảng chính hiện tại thành bảng cũ (`_old`).
    4. Đổi tên bảng tạm thành bảng chính.
    5. COMMIT transaction.
    6. Xóa bảng cũ.
    7. Chạy ANALYZE để cập nhật thống kê cho bảng mới.
    8. Nếu có lỗi, ROLLBACK để quay về trạng thái ban đầu.
    """
    if not has_new_data:
        logger.info(f"Bỏ qua refresh DuckDB cho '{config.dest_table}' do không có dữ liệu mới.")
        return

    dest_table = config.dest_table
    staging_table = f'{dest_table}_staging'
    backup_table = f'{dest_table}_old'
    staging_dir = str(BASE_DATA_PATH / dest_table)

    try:
        # 1. Tải Parquet vào bảng staging.
        logger.info(f"Bắt đầu tải Parquet vào staging table '{staging_table}'...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {staging_table} AS
            SELECT * FROM read_parquet('{staging_dir}/**', hive_partitioning=true);
        """)

        # 2. Thực hiện hoán đổi nguyên tử (atomic swap).
        logger.info(f"Bắt đầu hoán đổi (atomic swap) cho bảng '{dest_table}'...")
        conn.execute(f"""
            BEGIN TRANSACTION;

            -- Bước 1: Dọn dẹp bảng backup cũ (_old) nếu nó còn tồn tại
            -- từ một lần chạy trước bị lỗi giữa chừng, đảm bảo trạng thái sạch.
            DROP TABLE IF EXISTS {backup_table};

            -- Bước 2: Đổi tên bảng chính hiện tại thành bảng backup.
            -- 'IF EXISTS' đảm bảo không có lỗi nếu đây là lần chạy đầu tiên
            -- và bảng chính chưa tồn tại.
            ALTER TABLE IF EXISTS {dest_table} RENAME TO {backup_table};

            -- Bước 3: "Thăng cấp" bảng staging mới thành bảng chính.
            -- Đây là bước hoán đổi cốt lõi, diễn ra cực nhanh và an toàn trong transaction.
            ALTER TABLE {staging_table} RENAME TO {dest_table};

            COMMIT;
        """)
        logger.info(f"Hoán đổi bảng '{dest_table}' thành công.")

        # 3. Dọn dẹp bảng backup và staging area.
        conn.execute(f'DROP TABLE IF EXISTS {backup_table};')
        if not config.incremental and settings.ETL_CLEANUP_ON_FAILURE:
            shutil.rmtree(staging_dir)
            logger.info(f"Đã dọn dẹp staging area '{staging_dir}'.")
            
        # ================== THAY ĐỔI: THÊM LỆNH `ANALYZE` ==================
        # Sau khi nạp dữ liệu thành công, chạy ANALYZE để cập nhật
        # bảng thống kê cho bộ tối ưu hóa truy vấn của DuckDB.
        # Đây là bước quan trọng để đảm bảo hiệu năng truy vấn cao.
        logger.info(f"Đang cập nhật thống kê cho bảng '{dest_table}'...")
        conn.execute(f'ANALYZE {dest_table};')
        logger.info(f"✅ Cập nhật thống kê cho bảng '{dest_table}' thành công.")
        # =======================================================================

    except Exception as e:
        logger.error(
            f"Lỗi khi refresh bảng DuckDB '{dest_table}': {e}", exc_info=True
        )
        conn.execute('ROLLBACK;')
        logger.warning(f"Đã ROLLBACK transaction cho bảng '{dest_table}'.")
        raise
