"""
Giao diện dòng lệnh (Command-Line Interface - CLI) chính của ứng dụng.

File này sử dụng Typer để tạo ra các lệnh giúp tương tác với ứng dụng một
cách thân thiện, bao gồm:
- Chạy quy trình ETL hoàn chỉnh (`run-etl`).
- Khởi chạy máy chủ web FastAPI (`serve`).

Đây là điểm khởi đầu (entrypoint) cho các tác vụ vận hành và quản lý ứng dụng.
"""
import contextlib
import duckdb
import logging
import pandera.errors as pa_errors
import typer
import uvicorn

from duckdb import DuckDBPyConnection, Error as DuckdbError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterator
from typing_extensions import Annotated

from app.core.config import settings, TableConfig
from app.etl import extract, state, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table
from app.utils.logger import setup_logging

# Cấu hình logging ngay khi ứng dụng khởi chạy.
setup_logging('configs/logger.yaml')
logger = logging.getLogger(__name__)

# Khởi tạo Typer App để quản lý các câu lệnh CLI.
cli_app = typer.Typer()


@contextlib.contextmanager
def _get_database_connections() -> Iterator[tuple[Engine, DuckDBPyConnection]]:
    """
    Context manager để quản lý vòng đời của các kết nối database.

    Tự động thiết lập kết nối đến MS SQL Server và DuckDB khi bắt đầu
    và đảm bảo chúng được đóng lại một cách an toàn khi kết thúc,
    ngay cả khi có lỗi xảy ra.

    Yields:
        Một tuple chứa SQLAlchemy engine và kết nối DuckDB.

    Raises:
        SQLAlchemyError: Nếu không thể kết nối đến SQL Server.
        DuckDBError: Nếu không thể kết nối đến DuckDB.
    """
    sql_engine, duckdb_conn = None, None
    try:
        # 1. Kết nối SQL Server
        logger.info("Đang thiết lập kết nối tới MS SQL Server...")
        sql_engine = create_engine(settings.db.sqlalchemy_db_uri, pool_pre_ping=True)
        with sql_engine.connect() as connection:
            connection.execute(text('SELECT 1')) # Ping để kiểm tra kết nối
        logger.info("✅ Kết nối SQL Server thành công.")

        # 2. Kết nối DuckDB
        logger.info("Đang thiết lập kết nối tới DuckDB...")
        duckdb_path = str(settings.DUCKDB_PATH.resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
        logger.info(f"✅ Kết nối DuckDB ('{duckdb_path}') thành công.\n")

        yield sql_engine, duckdb_conn

    except SQLAlchemyError as e:
        logger.critical(f"❌ Lỗi nghiêm trọng khi kết nối SQL Server: {e}", exc_info=True)
        raise

    except DuckdbError as e:
        logger.critical(f"❌ Lỗi nghiêm trọng khi kết nối DuckDB: {e}", exc_info=True)
        raise

    finally:
        # 3. Đóng kết nối an toàn
        if sql_engine:
            sql_engine.dispose()
            logger.info("Kết nối SQL Server đã được đóng.")
        if duckdb_conn:
            duckdb_conn.close()
            logger.info("Kết nối DuckDB đã được đóng.")


def _process_table(
    sql_engine: Engine,
    duckdb_conn: DuckDBPyConnection,
    config: TableConfig,
    etl_state: dict
):
    """
    Thực hiện toàn bộ pipeline ETL cho một bảng duy nhất (E -> T -> L).

    Args:
        sql_engine: SQLAlchemy engine đã kết nối.
        duckdb_conn: Kết nối DuckDB đang hoạt động.
        config: Đối tượng cấu hình cho bảng đang xử lý.
        etl_state: Dictionary chứa trạng thái (high-water mark) của ETL.
    """
    logger.info(
        f"Bắt đầu xử lý bảng: '{config.source_table}' -> "
        f"'{config.dest_table}' (Incremental: {config.incremental})"
    )
    prepare_destination(config) # 1. Chuẩn bị thư mục staging
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp) # 2. Extract

    total_rows, max_ts_in_run = 0, None
    try:
        with ParquetLoader(config) as loader:
            for chunk in data_iterator:
                # 3. Transform & Validate
                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty:
                    continue

                # 4. Load vào staging (Parquet)
                loader.write_chunk(transformed_chunk)
                total_rows += len(transformed_chunk)

                # Cập nhật high-water mark cho lần chạy hiện tại
                current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
                if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                    max_ts_in_run = current_max_ts

        # 5. Load từ Parquet vào DuckDB
        if total_rows > 0:
            logger.info(f"Đã xử lý {total_rows} dòng. Bắt đầu tải vào DuckDB.")
            refresh_duckdb_table(duckdb_conn, config, loader.has_written_data)
            logger.info(f"Đã tải thành công dữ liệu vào DuckDB '{config.dest_table}'.")

            # 6. Cập nhật state nếu là incremental load
            if config.incremental and max_ts_in_run:
                state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)
        else:
            logger.info(f"Không tìm thấy dữ liệu mới cho '{config.dest_table}'.")

    except (SQLAlchemyError, DuckdbError, pa_errors.SchemaErrors) as e:
        logger.error(f"❌ Lỗi khi xử lý '{config.source_table}': {e}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"❌ Lỗi không xác định khi xử lý '{config.source_table}': {e}", exc_info=True)
        raise


@cli_app.command()
def run_etl():
    """
    Chạy quy trình ETL hoàn chỉnh từ SQL Server sang DuckDB.
    """
    logger.info("=" * 60)
    logger.info("🚀 BẮT ĐẦU QUY TRÌNH ETL")
    logger.info("=" * 60)

    succeeded, failed = [], []
    etl_state = state.load_etl_state()

    # Sắp xếp các bảng theo thứ tự xử lý đã định nghĩa trong config.
    tables_to_process = sorted(
        settings.TABLE_CONFIG.items(),
        key=lambda item: item[1].processing_order
    )

    try:
        with _get_database_connections() as (sql_engine, duckdb_conn):
            for table_name, config in tables_to_process:
                try:
                    _process_table(sql_engine, duckdb_conn, config, etl_state)
                    state.save_etl_state(etl_state) # Lưu state sau mỗi bảng thành công
                    succeeded.append(config.source_table)
                    logger.info(f"✅ Xử lý thành công '{config.source_table}'.\n")
                except Exception:
                    failed.append(config.source_table)
                    logger.error(f"❌ Xử lý '{config.source_table}' thất bại. " f"Chuyển sang bảng tiếp theo.\n")
                    continue

    except Exception as e:
        logger.critical(f"Quy trình ETL bị dừng do lỗi kết nối: {e}")

    finally:
        logger.info("=" * 60)
        logger.info("📊 TÓM TẮT KẾT QUẢ ETL")
        logger.info(f"Tổng số bảng: {len(settings.TABLE_CONFIG)}")
        logger.info(f"✅ Thành công: {len(succeeded)}")
        logger.info(f"❌ Thất bại: {len(failed)}")
        if failed:
            logger.warning(f"Danh sách bảng thất bại: {', '.join(failed)}")
        logger.info("=" * 60 + "\n")


@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server.")] = '127.0.0.1',
    port: Annotated[int, typer.Option(help="Port để chạy server.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại khi code thay đổi.")] = True,
):
    """
    Khởi chạy web server Uvicorn cho ứng dụng FastAPI.
    """
    logger.info(f"🚀 Khởi chạy FastAPI server tại http://{host}:{port}")
    uvicorn.run(
        'app.main:api_app',
        host=host,
        port=port,
        reload=reload,
        reload_dirs=['app', 'configs', 'template'] # Chỉ định rõ thư mục cần theo dõi
    )


if __name__ == '__main__':
    cli_app()
