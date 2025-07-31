# app/main.py
# Để chạy quy trình ETL: python -m app.main run-etl
# Để chạy web server: python -m app.main serve
# Bạn cũng có thể thay đổi host và port: python -m app.main serve --host 0.0.0.0 --port 8888
# Để xem tất cả các lệnh có sẵn: python -m app.main --help
import logging
import typer
import duckdb
import uvicorn

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing_extensions import Annotated

# Import các module từ dự án của bạn
from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table

# --- Cài đặt ban đầu ---
# 1. Cấu hình logging cho toàn bộ ứng dụng
setup_logging('app/logger.yaml')
logger = logging.getLogger(__name__)

# 2. Khởi tạo các ứng dụng (CLI và Web)
# Typer app để quản lý các lệnh CLI
cli_app = typer.Typer()
# FastAPI app cho Giai đoạn 2
api_app = FastAPI(
    title="iCount-People API",
    description="API để phân tích dữ liệu lượng người ra vào.",
    version="1.0.0"
)

# --- LOGIC ETL (di chuyển từ run_etl.py) ---
def _process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
    """
    Điều phối toàn bộ quy trình ETL cho một bảng.
    (Hàm này được giữ nguyên từ run_etl.py)
    """
    logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

    # 1. Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu cần)
    prepare_destination(config)

    # 2. Lấy high-water-mark từ lần chạy trước
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

    # 3. Trích xuất dữ liệu
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

    # 6. Tải dữ liệu từ Parquet vào DuckDB
    refresh_duckdb_table(duckdb_conn, config)
    logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")

    # 7. Cập nhật state nếu là incremental
    if config.incremental and max_ts_in_run:
        state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)


@cli_app.command()
def run_etl():
    """
    Chạy quy trình ETL chính: Trích xuất dữ liệu từ SQL Server,
    biến đổi và tải vào DuckDB thông qua Parquet.
    """
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


# --- LOGIC WEB SERVER (cho Giai đoạn 2) ---
@api_app.get("/", summary="Endpoint chào mừng")
def read_root():
    """Endpoint gốc trả về một thông điệp chào mừng."""
    return {"message": "Chào mừng đến với iCount-People API"}

@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server, ví dụ '127.0.0.1'.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port để chạy server, ví dụ 8000.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại server khi có thay đổi code.")] = True,
):
    """
    Khởi chạy web server FastAPI để phục vụ API.
    """
    logger.info(f"Khởi chạy FastAPI server tại http://{host}:{port}")
    uvicorn.run("app.main:api_app", host=host, port=port, reload=reload)

# --- Entrypoint chính để chạy CLI ---
if __name__ == "__main__":
    cli_app()
