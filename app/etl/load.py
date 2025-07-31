# app/etl/load.py
# Chịu trách nhiệm ghi dữ liệu vào Parquet và cập nhật bảng trong DuckDB.
import logging
import pandas as pd
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from duckdb import DuckDBPyConnection

from app.core.config import etl_settings, TableConfig

logger = logging.getLogger(__name__)
BASE_DATA_PATH = Path(etl_settings.DATA_DIR)

def prepare_destination(config: TableConfig):
    """Xóa dữ liệu cũ nếu là full-load."""
    dest_path = BASE_DATA_PATH / config.dest_table
    if not config.incremental and dest_path.exists():
        if dest_path.is_dir():
            shutil.rmtree(dest_path)
            logger.info(f"Full-load: Đã xóa thư mục staging cũ: {dest_path}")
        else:
            dest_path.unlink()
            logger.info(f"Full-load: Đã xóa file staging cũ: {dest_path}")

    dest_path.mkdir(parents=True, exist_ok=True)

def to_parquet(df: pd.DataFrame, config: TableConfig, writer: pq.ParquetWriter = None):
    """Ghi một DataFrame chunk vào file/dataset Parquet."""
    dest_path = BASE_DATA_PATH / config.dest_table
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    if config.partition_cols:
        pq.write_to_dataset(
            arrow_table,
            root_path=str(dest_path),
            partition_cols=config.partition_cols,
            existing_data_behavior='overwrite_or_ignore'
        )
    else:
        if writer is None:
            raise ValueError("ParquetWriter is required for non-partitioned tables.")
        writer.write_table(arrow_table)

def refresh_duckdb_table(conn: DuckDBPyConnection, config: TableConfig):
    """Tạo hoặc thay thế bảng trong DuckDB từ nguồn Parquet."""
    staging_dir = BASE_DATA_PATH / config.dest_table
    if not staging_dir.exists():
        logger.warning(f"Thư mục staging '{staging_dir}' không tồn tại. Bỏ qua việc refresh DuckDB.")
        return

    # Sử dụng glob pattern để đọc tất cả các file parquet trong thư mục
    read_path = str(staging_dir / '**' / '*.parquet').replace('\\', '/')

    is_partitioned = bool(config.partition_cols)
    hive_param = ", hive_partitioning=1" if is_partitioned else ""

    read_statement = f"read_parquet('{read_path}'{hive_param})"
    create_sql = f"CREATE OR REPLACE TABLE {config.dest_table} AS SELECT * FROM {read_statement};"

    logger.info(f"Đang refresh bảng '{config.dest_table}' trong DuckDB từ nguồn Parquet tại '{staging_dir}'.")
    conn.execute(create_sql)
    """Tạo hoặc thay thế bảng trong DuckDB từ nguồn Parquet."""
    staging_dir = BASE_DATA_PATH / config.dest_table
    if not staging_dir.exists():
        logger.warning(f"Thư mục staging '{staging_dir}' không tồn tại. Bỏ qua việc refresh DuckDB.")
        return

    # Sử dụng glob pattern để đọc tất cả các file parquet trong thư mục
    read_path = str(staging_dir / '**' / '*.parquet').replace('\\', '/')

    is_partitioned = bool(config.partition_cols)
    hive_param = ", hive_partitioning=1" if is_partitioned else ""

    read_statement = f"read_parquet('{read_path}'{hive_param})"
    create_sql = f"CREATE OR REPLACE TABLE {config.dest_table} AS SELECT * FROM {read_statement};"

    logger.info(f"Đang refresh bảng '{config.dest_table}' trong DuckDB từ nguồn Parquet tại '{staging_dir}'.")
    conn.execute(create_sql)
