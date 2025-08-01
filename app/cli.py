import logging
import typer
import duckdb
import uvicorn

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing_extensions import Annotated

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table

# Tạo một Typer app mới để quản lý các câu lệnh
cli_app = typer.Typer()

# --- LOGIC CỦA QUY TRÌNH ETL ---
# Hàm helper này chỉ được sử dụng bởi lệnh run_etl, nên nó thuộc về đây.
def _process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
    logger = logging.getLogger(__name__)
    logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

    prepare_destination(config)
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

    total_rows = 0
    max_ts_in_run = None

    try:
        with ParquetLoader(config) as loader:
            for chunk in data_iterator:
                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty:
                    continue
                loader.write_chunk(transformed_chunk)
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
    refresh_duckdb_table(duckdb_conn, config)
    logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")

    if config.incremental and max_ts_in_run:
        state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)

@cli_app.command()
def run_etl():
    """
    Chạy quy trình ETL để chuyển dữ liệu từ MSSQL sang DuckDB.
    """
    setup_logging('app/logger.yaml')
    logger = logging.getLogger(__name__)
    
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
                _process_table(sql_engine, duckdb_conn, config, etl_state)
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


# --- LỆNH ĐỂ CHẠY WEB SERVER ---
@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server, ví dụ '127.0.0.1'.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port để chạy server, ví dụ 8000.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại server khi có thay đổi code.")] = True,
):
    """
    Khởi chạy web server FastAPI với Uvicorn.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Khởi chạy FastAPI server tại http://{host}:{port}")
    # Sử dụng chuỗi "app.main:api_app" để Uvicorn biết file nào cần theo dõi khi reload
    uvicorn.run("app.main:api_app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    cli_app()
    