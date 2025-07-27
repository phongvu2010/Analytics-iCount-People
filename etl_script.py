import duckdb
import json
import pandas as pd
import shutil

from duckdb import DuckDBPyConnection
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Any, Dict, Iterator, Optional, List

from app.core.config import etl_settings
from app.utils.logger import setup_logger

# Tạo một instance logger duy nhất
logger = setup_logger('etl_app', 'ETL App')


def load_etl_state() -> Dict[str, str]:
    """Tải trạng thái ETL cuối cùng từ file JSON."""
    state_file = Path(etl_settings.STATE_FILE)
    if not state_file.exists():
        return {}

    try:
        with state_file.open('r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc file trạng thái '{state_file}'. Bắt đầu với trạng thái trống.")
        return {}


def save_etl_state(state: Dict[str, str]):
    """Lưu trạng thái ETL hiện tại vào file JSON."""
    state_file = Path(etl_settings.STATE_FILE)
    # Đảm bảo thư mục tồn tại trước khi ghi file
    state_file.parent.mkdir(parents=True, exist_ok=True)

    with state_file.open('w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)


def extract_data(sql_engine: Engine, config: Dict[str, Any], last_timestamp: str) -> Iterator[pd.DataFrame]:
    """Trích xuất dữ liệu từ SQL Server theo từng chunk."""
    source_table = config['source_table']
    query_str = f"SELECT * FROM {source_table}"
    params = {}

    if config.get('incremental', True):
        timestamp_col = config['timestamp_col']
        query_str += f" WHERE {timestamp_col} > :last_ts ORDER BY {timestamp_col}"
        params = {"last_ts": last_timestamp}

    query = text(query_str)
    logger.info(f"Trích xuất dữ liệu từ '{source_table}' với high-water-mark > '{last_timestamp}'")

    # Sử dụng chunksize để tránh lỗi MemoryError với dữ liệu lớn
    return pd.read_sql(query, sql_engine, params=params, chunksize=etl_settings.ETL_CHUNK_SIZE)


def transform_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Áp dụng các bước transform cơ bản cho DataFrame."""
    transformed_df = df.rename(columns=config['rename_map'])

    if 'store_name' in transformed_df.columns:
        logger.info("Làm sạch cột 'store_name': loại bỏ khoảng trắng thừa.")

        # Đảm bảo cột là kiểu string trước khi dùng .str
        transformed_df['store_name'] = transformed_df['store_name'].astype(str).str.rstrip()

    # Xử lý partition cho các bảng có cấu hình
    if config.get('partition_cols'):
        ts_col = config['dest_timestamp_col']

        # Chuyển đổi sang datetime, errors='coerce' sẽ biến các giá trị không hợp lệ thành NaT
        transformed_df[ts_col] = pd.to_datetime(transformed_df[ts_col], errors='coerce')

        # Bỏ qua các dòng có timestamp không hợp lệ sau khi chuyển đổi
        transformed_df = transformed_df.dropna(subset=[ts_col])

        # Kiểm tra nếu dataframe còn dữ liệu sau khi dropna
        if not transformed_df.empty:
            transformed_df['year'] = transformed_df[ts_col].dt.year
            transformed_df['month'] = transformed_df[ts_col].dt.month

    return transformed_df


def create_table_from_parquet(conn: DuckDBPyConnection, config: Dict[str, Any]):
    """Tạo hoặc thay thế bảng trong DuckDB từ file/thư mục Parquet."""
    dest_table = config['dest_table']
    data_path = Path(etl_settings.DUCKDB_PATH).parent

    if config.get('partition_cols'):
        # DuckDB's hive_partitioning tự động phát hiện các partition
        parquet_glob_path = str(data_path / dest_table / '**' / '*.parquet').replace('\\', '/')

        # hive_partitioning=1 vẫn an toàn khi không có partition thực sự
        read_statement = f"read_parquet('{parquet_glob_path}', hive_partitioning=1)"
    else:
        parquet_file_path = str(data_path / f"{dest_table}.parquet").replace('\\', '/')
        read_statement = f"read_parquet('{parquet_file_path}')"

    create_sql = f"CREATE OR REPLACE TABLE {dest_table} AS SELECT * FROM {read_statement};"
    logger.info(f"Tạo/Cập nhật bảng '{dest_table}' trong DuckDB từ nguồn Parquet.")
    conn.execute(create_sql)


def process_table(sql_engine: Engine, duckdb_conn: DuckDBPyConnection, config: Dict[str, Any], etl_state: Dict[str, str]):
    """
    REFACTOR: Hàm xử lý chính được cấu trúc lại hoàn toàn cho rõ ràng và an toàn hơn.
    Quy trình:
    1. Xác định các tham số và đường dẫn.
    2. Dọn dẹp dữ liệu cũ nếu là full load.
    3. Extract & Transform theo từng chunk và thu thập vào một list.
    4. Nếu có dữ liệu, gộp các chunk lại và ghi ra Parquet (một lần duy nhất).
    5. Cập nhật bảng trong DuckDB và lưu lại trạng thái.
    """
    dest_table = config['dest_table']
    source_table = config['source_table']
    is_incremental = config.get('incremental', True)
    has_partitions = bool(config.get('partition_cols'))

    logger.info(f"Bắt đầu xử lý bảng: {source_table} -> {dest_table} (Incremental: {is_incremental}, Partitioned: {has_partitions})")

    data_path = Path(etl_settings.DUCKDB_PATH).parent

    # REFACTOR: Logic dọn dẹp được gom lại một chỗ, rõ ràng hơn.
    if not is_incremental: # Full load
        target_path = data_path / dest_table if has_partitions else data_path / f"{dest_table}.parquet"
        if target_path.is_dir() and has_partitions:
            shutil.rmtree(target_path)
            logger.info(f"Đã xóa thư mục Parquet cũ: {target_path}")

        elif target_path.is_file() and not has_partitions:
            target_path.unlink()
            logger.info(f"Đã xóa file Parquet cũ: {target_path}")

    # Đảm bảo thư mục tồn tại cho các bảng có partition
    if has_partitions:
        (data_path / dest_table).mkdir(parents=True, exist_ok=True)

    last_timestamp = etl_state.get(dest_table, etl_settings.ETL_DEFAULT_TIMESTAMP)
    df_chunks_iterator = extract_data(sql_engine, config, last_timestamp)

    transformed_chunks: List[pd.DataFrame] = []
    max_timestamp: Optional[pd.Timestamp] = None

    # REFACTOR: Gom tất cả các chunk đã transform vào list thay vì ghi file từng chunk.
    for chunk_df in df_chunks_iterator:
        if chunk_df.empty: continue

        transformed_df = transform_data(chunk_df, config)
        if transformed_df.empty: continue

        transformed_chunks.append(transformed_df)

        if is_incremental:
            current_max = transformed_df[config['dest_timestamp_col']].max()
            if max_timestamp is None or current_max > max_timestamp:
                max_timestamp = current_max

    if not transformed_chunks:
        logger.info(f"Không có dữ liệu mới cho bảng '{dest_table}'.")
        return

    # REFACTOR: Gộp các chunk và ghi ra file Parquet một lần duy nhất.
    # Điều này an toàn hơn và đơn giản hơn logic append/overwrite.
    final_df = pd.concat(transformed_chunks, ignore_index=True)
    total_rows = len(final_df)

    logger.info(f"Tổng hợp được {total_rows} dòng. Bắt đầu ghi ra file Parquet...")

    if has_partitions:
        final_df.to_parquet(
            path=data_path / dest_table,
            engine='pyarrow',
            compression='snappy',
            partition_cols=config.get('partition_cols')
        )
    else: # Bảng full load, ghi ra một file duy nhất
        single_parquet_file = data_path / f"{dest_table}.parquet"
        final_df.to_parquet(single_parquet_file, engine='pyarrow', compression='snappy', index=False)

    # Cập nhật bảng trong DuckDB và trạng thái ETL
    create_table_from_parquet(duckdb_conn, config)
    logger.info(f"Đã xử lý và ghi thành công {total_rows} dòng cho bảng '{dest_table}'.")

    if is_incremental and max_timestamp:
        # Sử dụng isoformat() để đảm bảo định dạng timestamp nhất quán
        etl_state[dest_table] = max_timestamp.isoformat(sep=' ')


def run_etl():
    """Hàm chính điều phối toàn bộ quy trình ETL."""
    etl_state = load_etl_state()
    sql_engine = None
    duckdb_conn = None

    logger.info("Bắt đầu quy trình ETL...")
    try:
        sql_engine = create_engine(etl_settings.sqlalchemy_db_uri)
        duckdb_path_str = str(Path(etl_settings.DUCKDB_PATH).resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path_str, read_only=False)
        logger.info(f"Đã kết nối thành công tới SQL Server và DuckDB ('{duckdb_path_str}').")

        for table_key, config in etl_settings.TABLE_CONFIG.items():
            try:
                process_table(sql_engine, duckdb_conn, config, etl_state)
                # REFACTOR: Chỉ lưu trạng thái sau khi một bảng được xử lý thành công.
                # Đây là một cách tiếp cận tốt để đảm bảo khả năng resume.
                save_etl_state(etl_state)
                logger.info(f"Xử lý thành công cho bảng '{config['source_table']}'. Trạng thái đã được cập nhật.")
            except Exception:
                logger.error(f"Lỗi khi xử lý bảng '{config['source_table']}'.", exc_info=True)

    except Exception:
        logger.error(f"ETL thất bại nghiêm trọng tại '{run_etl.__name__}'!", exc_info=True)

    finally:
        if sql_engine:
            sql_engine.dispose()
            logger.info("Đã giải phóng connection pool của SQLAlchemy.")

        if duckdb_conn:
            duckdb_conn.close()
            logger.info("Đã đóng kết nối DuckDB.")

        logger.info("Quy trình ETL kết thúc.\n")


if __name__ == '__main__':
    run_etl()
