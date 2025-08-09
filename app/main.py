"""
Entrypoint chính cho ứng dụng, bao gồm cả giao diện dòng lệnh (CLI)
để chạy quy trình ETL và khởi chạy web server FastAPI.

Sử dụng Typer để quản lý các lệnh CLI:
- `run-etl`: Chạy quy trình ETL từ SQL Server sang DuckDB.
- `serve`: Khởi chạy web server FastAPI.
"""
import duckdb
import logging
import typer
import uvicorn

from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import Annotated

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table

# Cấu hình logging ngay từ đầu
setup_logging("configs/logger.yaml")
logger = logging.getLogger(__name__)

# Khởi tạo ứng dụng CLI
cli_app = typer.Typer()

def _process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
    """
    Xử lý toàn bộ quy trình ETL cho một bảng cụ thể.

    Bao gồm các bước: Extract (trích xuất), Transform (biến đổi), và Load (tải).
    Quy trình này xử lý dữ liệu theo từng khối (chunk) để tối ưu bộ nhớ.

    Args:
        sql_engine: Đối tượng engine của SQLAlchemy để kết nối SQL Server.
        duckdb_conn: Đối tượng kết nối DuckDB.
        config: Cấu hình (TableConfig) cho bảng đang được xử lý.
        etl_state: Dictionary chứa trạng thái high-water-mark của lần chạy ETL trước.

    Raises:
        Exception: Ném lại bất kỳ lỗi nào xảy ra trong quá trình xử lý để
                   hàm gọi chính có thể bắt và ghi nhận là thất bại.
    """
    logger.info(f"Bắt đầu xử lý bảng: '{config.source_table}' -> '{config.dest_table}' (Incremental: {config.incremental})")

    # 1. Chuẩn bị thư mục đích cho file Parquet
    prepare_destination(config)

    # 2. Trích xuất dữ liệu từ SQL Server
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)

    total_rows = 0
    max_ts_in_run, loader = None, None
    try:
        # 3. Biến đổi và Tải dữ liệu theo từng chunk
        with ParquetLoader(config) as loader:
            for chunk_idx, chunk in enumerate(data_iterator):
                logger.debug(f"Đang xử lý chunk {chunk_idx + 1} từ '{config.source_table}'. Kích thước: {len(chunk)} hàng.")
                if chunk.empty: continue

                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty: continue

                # Ghi chunk đã biến đổi vào file Parquet
                loader.write_chunk(transformed_chunk)
                total_rows += len(transformed_chunk)

                # Theo dõi timestamp lớn nhất trong phiên chạy
                current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
                if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                    max_ts_in_run = current_max_ts
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk và ghi Parquet cho bảng '{config.dest_table}': {e}", exc_info=True)
        raise

    if total_rows == 0:
        logger.info(f"Không tìm thấy dữ liệu mới cho bảng '{config.dest_table}'.")
        refresh_duckdb_table(duckdb_conn, config, loader.has_written_data)
        return

    # 4. Refresh bảng trong DuckDB và cập nhật trạng thái
    logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")
    refresh_duckdb_table(duckdb_conn, config, loader.has_written_data)
    logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")

    if config.incremental and max_ts_in_run:
        state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)
        logger.info(f"Đã cập nhật high-water mark cho '{config.dest_table}' thành: {max_ts_in_run.isoformat(sep=' ')}")

@cli_app.command()
def run_etl():
    """
    Chạy quy trình ETL hoàn chỉnh.

    Kết nối tới các cơ sở dữ liệu nguồn (SQL Server) và đích (DuckDB),
    sau đó lặp qua từng bảng được định nghĩa trong file cấu hình `tables.yaml`
    để thực hiện trích xuất, biến đổi và tải dữ liệu.
    """
    logger = logging.getLogger(__name__)

    # Khởi tạo danh sách theo dõi kết quả
    succeeded_tables = []
    failed_tables = []

    etl_state = state.load_etl_state()
    sql_engine, duckdb_conn = None, None

    logger.info("Quy trình ETL bắt đầu...")
    try:
        # Thiết lập kết nối
        sql_engine = create_engine(etl_settings.db.sqlalchemy_db_uri)
        with sql_engine.connect() as connection:
            connection.execute(text("SELECT 1"))

        logger.info("Kết nối SQL Server thành công.")

        duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
        logger.info(f"Kết nối DuckDB ('{duckdb_path}') thành công.")

        # Lặp qua các bảng và xử lý
        for table_name, config in etl_settings.TABLE_CONFIG.items():
            try:
                _process_table(sql_engine, duckdb_conn, config, etl_state)
                state.save_etl_state(etl_state)
                logger.info(f"Xử lý thành công bảng '{config.source_table}'.\n")
                succeeded_tables.append(config.source_table)
            except Exception as e:
                # Nếu một bảng lỗi, ghi log và tiếp tục với bảng tiếp theo
                logger.error(f"Xử lý bảng '{config.source_table}' thất bại. Lỗi: {e}\n", exc_info=False)
                failed_tables.append(config.source_table)
                continue
    except SQLAlchemyError as e:
        logger.critical(f"Lỗi nghiêm trọng khi kết nối SQL Server: {e}", exc_info=True)
    except duckdb.Error as e:
        logger.critical(f"Lỗi nghiêm trọng khi kết nối DuckDB: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}", exc_info=True)
    finally:
        # Luôn đảm bảo đóng kết nối và in tóm tắt
        if sql_engine:
            sql_engine.dispose()
            logger.info("Đã đóng kết nối SQL Server.")

        if duckdb_conn:
            duckdb_conn.close()
            logger.info("Đã đóng kết nối DuckDB.")

        # In ra bản tóm tắt kết quả
        logger.info("="*60)
        logger.info("TÓM TẮT KẾT QUẢ ETL")
        logger.info(f"Tổng số bảng cấu hình: {len(etl_settings.TABLE_CONFIG)}")
        logger.info(f"✅ Thành công: {len(succeeded_tables)}")
        logger.info(f"❌ Thất bại: {len(failed_tables)}")

        if failed_tables:
            logger.warning(f"Danh sách bảng thất bại: {', '.join(failed_tables)}")

        logger.info("="*60)
        logger.info("Quy trình ETL kết thúc.\n")

# Khởi tạo FastAPI ---
api_app = FastAPI(title="iCount-People API",
                  description="API để phân tích dữ liệu lượng người ra vào.",
                  version="1.0.0")

@api_app.get("/", summary="Endpoint chào mừng", include_in_schema=False)
def read_root():
    return {"message": "Chào mừng đến với iCount-People API"}

@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port để chạy server.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại server khi có thay đổi.")] = True
):
    """ Khởi chạy web server Uvicorn cho ứng dụng FastAPI. """
    logger = logging.getLogger(__name__)
    logger.info(f"Khởi chạy FastAPI server tại http://{host}:{port}")
    uvicorn.run("app.main:api_app", host=host, port=port, reload=reload)

if __name__ == "__main__":
    cli_app()
