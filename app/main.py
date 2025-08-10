import contextlib
import duckdb
import logging
import typer
import uvicorn

from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Iterator
from typing_extensions import Annotated

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table

setup_logging("configs/logger.yaml")
logger = logging.getLogger(__name__)

cli_app = typer.Typer()
api_app = FastAPI(title="iCount-People API", version="1.0.0")

# --- Context Manager để quản lý kết nối ---
@contextlib.contextmanager
def database_connections() -> Iterator[tuple[Engine, duckdb.DuckDBPyConnection]]:
    """
    Context manager để quản lý vòng đời của kết nối SQL Server và DuckDB.
    Đảm bảo kết nối được thiết lập và đóng một cách an toàn.
    """
    sql_engine, duckdb_conn = None, None
    try:
        # Kết nối SQL Server
        sql_engine = create_engine(etl_settings.db.sqlalchemy_db_uri, pool_pre_ping=True)
        with sql_engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Kết nối SQL Server thành công.")

        # Kết nối DuckDB
        duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
        logger.info(f"Kết nối DuckDB ('{duckdb_path}') thành công.")

        yield sql_engine, duckdb_conn

    except SQLAlchemyError as e:
        logger.critical(f"Lỗi nghiêm trọng khi kết nối SQL Server: {e}", exc_info=True)
        raise
    except duckdb.Error as e:
        logger.critical(f"Lỗi nghiêm trọng khi kết nối DuckDB: {e}", exc_info=True)
        raise
    finally:
        if sql_engine:
            sql_engine.dispose()
            logger.info("Đã đóng kết nối SQL Server.")
        if duckdb_conn:
            duckdb_conn.close()
            logger.info("Đã đóng kết nối DuckDB.")

# --- Các hàm xử lý ETL (giữ nguyên logic gốc) ---
def _process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
    logger.info(f"Bắt đầu xử lý bảng: '{config.source_table}' -> '{config.dest_table}' (Incremental: {config.incremental})")
    prepare_destination(config)
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

    total_rows, max_ts_in_run = 0, None
    try:
        with ParquetLoader(config) as loader:
            for chunk in data_iterator:
                if chunk.empty: continue
                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty: continue

                loader.write_chunk(transformed_chunk)
                total_rows += len(transformed_chunk)

                current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
                if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                    max_ts_in_run = current_max_ts
        
        if total_rows > 0:
            logger.info(f"Đã xử lý {total_rows} dòng. Bắt đầu tải vào DuckDB.")
            refresh_duckdb_table(duckdb_conn, config, loader.has_written_data)
            logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")
            if config.incremental and max_ts_in_run:
                state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)
        else:
            logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý cho bảng '{config.dest_table}': {e}", exc_info=True)
        raise # Ném lại lỗi để vòng lặp chính bắt và đánh dấu là failed

# --- Các lệnh CLI ---
@cli_app.command()
def run_etl():
    """Chạy quy trình ETL hoàn chỉnh từ SQL Server sang DuckDB."""
    logger.info("="*60)
    logger.info("BẮT ĐẦU QUY TRÌNH ETL")
    logger.info("="*60)

    succeeded_tables, failed_tables = [], []
    etl_state = state.load_etl_state()

    try:
        with database_connections() as (sql_engine, duckdb_conn):
            for table_name, config in etl_settings.TABLE_CONFIG.items():
                try:
                    _process_table(sql_engine, duckdb_conn, config, etl_state)
                    state.save_etl_state(etl_state) # Lưu trạng thái sau mỗi bảng thành công
                    succeeded_tables.append(config.source_table)
                    logger.info(f"✅ Xử lý thành công bảng '{config.source_table}'.\n")
                except Exception:
                    # Lỗi đã được log bên trong _process_table, chỉ cần đánh dấu thất bại
                    failed_tables.append(config.source_table)
                    logger.error(f"❌ Xử lý bảng '{config.source_table}' thất bại. Chuyển sang bảng tiếp theo.\n")
                    continue
    except Exception as e:
        logger.critical(f"Quy trình ETL bị dừng do lỗi kết nối hoặc lỗi nghiêm trọng khác: {e}")
    finally:
        # In ra bản tóm tắt kết quả
        logger.info("="*60)
        logger.info("TÓM TẮT KẾT QUẢ ETL")
        logger.info(f"Tổng số bảng cấu hình: {len(etl_settings.TABLE_CONFIG)}")
        logger.info(f"✅ Thành công: {len(succeeded_tables)}")
        logger.info(f"❌ Thất bại: {len(failed_tables)}")
        if failed_tables:
            logger.warning(f"Danh sách bảng thất bại: {', '.join(failed_tables)}")
        logger.info("="*60)

@api_app.get("/", include_in_schema=False)
def read_root():
    return {"message": "Chào mừng đến với iCount-People API"}

@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port để chạy server.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại server khi có thay đổi.")] = True
):
    """Khởi chạy web server Uvicorn cho ứng dụng FastAPI."""
    logger.info(f"🚀 Khởi chạy FastAPI server tại http://{host}:{port}")
    uvicorn.run("app.main:api_app", host=host, port=port, reload=reload)

if __name__ == "__main__":
    cli_app()
