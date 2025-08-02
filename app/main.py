import duckdb
import logging
import typer
import uvicorn

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing_extensions import Annotated

from app.core.config import etl_settings, TableConfig
from app.utils.logger import setup_logging
from app.etl import state, extract, transform
from app.etl.load import ParquetLoader, prepare_destination, refresh_duckdb_table

setup_logging('configs/logger.yaml')
logger = logging.getLogger(__name__)

cli_app = typer.Typer()

def _process_table(sql_engine: Engine, duckdb_conn: duckdb.DuckDBPyConnection, config: TableConfig, etl_state: dict):
    logger.info(f"Bắt đầu xử lý bảng: {config.source_table} -> {config.dest_table} (Incremental: {config.incremental})")

    # Bước 1: Chuẩn bị thư mục đích (xóa dữ liệu cũ nếu là full-load)
    prepare_destination(config)

    # Bước 2: Trích xuất dữ liệu
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)

    # Khởi tạo data_iterator bên ngoài try-except để có thể xử lý lỗi ngay lập tức
    data_iterator = None
    try:
        data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp)
    except SQLAlchemyError as e: # Bắt lỗi cụ thể từ SQLAlchemy khi extract
        logger.error(f"Lỗi khi trích xuất dữ liệu từ '{config.source_table}': {e}")
        # Không re-raise ở đây, nhưng chúng ta sẽ kiểm tra data_iterator sau
        # để biết có cần bỏ qua quá trình transform/load không.
        # Hoặc có thể raise ngay lập tức nếu muốn quy trình dừng lại.
        # Ở đây chọn log và tiếp tục (hoặc bỏ qua) nếu extract không thành công.
        logger.info(f"Bỏ qua xử lý bảng '{config.source_table}' do lỗi trích xuất.")
        return # Thoát sớm nếu không thể trích xuất

    total_rows = 0
    max_ts_in_run = None
    loader = None # Khởi tạo loader bên ngoài try block để đảm bảo nó được truy cập trong finally
    try:
        with ParquetLoader(config) as loader: # Context manager sẽ đảm bảo writer được đóng
            for chunk_idx, chunk in enumerate(data_iterator): # Đếm số chunk
                logger.debug(f"Đang xử lý chunk {chunk_idx + 1} từ '{config.source_table}'. Kích thước: {len(chunk)} hàng.")
                if chunk.empty:
                    logger.debug(f"Chunk {chunk_idx + 1} từ '{config.source_table}' rỗng, bỏ qua.")
                    continue

                transformed_chunk = transform.run_transformations(chunk, config)
                if transformed_chunk.empty:
                    logger.debug(f"Chunk {chunk_idx + 1} sau khi biến đổi rỗng, bỏ qua.")
                    continue

                loader.write_chunk(transformed_chunk)
                total_rows += len(transformed_chunk)

                current_max_ts = transform.get_max_timestamp(transformed_chunk, config)
                if current_max_ts and (max_ts_in_run is None or current_max_ts > max_ts_in_run):
                    max_ts_in_run = current_max_ts
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk và ghi Parquet cho bảng '{config.dest_table}': {e}", exc_info=True)
        # Bất kỳ lỗi nào trong quá trình ghi Parquet cũng cần được xử lý nghiêm túc.
        # Chúng ta sẽ không cập nhật trạng thái nếu có lỗi ở đây.
        return # Thoát sớm nếu có lỗi trong transform/load file

    if total_rows == 0:
        logger.info(f"Không tìm thấy dữ liệu mới hoặc không có hàng nào được xử lý cho bảng '{config.dest_table}'.")
        # Vẫn gọi refresh_duckdb_table để nó có thể kiểm tra và bỏ qua nếu không có dữ liệu
        # Tuy nhiên, hãy đảm bảo rằng loader.has_written_data là False trong trường hợp này
        # Kiểm tra loader có tồn tại và has_written_data
        has_data_written_to_staging = loader.has_written_data if loader else False
        refresh_duckdb_table(duckdb_conn, config, has_data_written_to_staging)
        return

    logger.info(f"Đã xử lý {total_rows} dòng. Hoàn tất ghi ra Parquet.")

    # Bước 3: Tải dữ liệu vào DuckDB và hoán đổi bảng
    # Truyền trạng thái ghi dữ liệu vào hàm refresh_duckdb_table
    has_data_written_to_staging = loader.has_written_data if loader else False
    try:
        refresh_duckdb_table(duckdb_conn, config, has_data_written_to_staging)
        logger.info(f"Đã tải thành công dữ liệu vào bảng DuckDB '{config.dest_table}'.")
    except Exception as e:
        logger.error(f"Lỗi khi refresh bảng DuckDB cho '{config.dest_table}': {e}", exc_info=True)
        # Nếu lỗi ở bước này, chúng ta không cập nhật timestamp để lần sau ETL chạy lại
        return # Thoát sớm

    # Bước 4: Cập nhật trạng thái high-water mark
    if config.incremental and max_ts_in_run:
        state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)
        logger.info(f"Đã cập nhật high-water mark cho '{config.dest_table}' thành: {max_ts_in_run.isoformat(sep=' ')}")

@cli_app.command()
def run_etl():
    # setup_logging đã được gọi ở ngoài cùng của main.py, không cần gọi lại ở đây.
    # Tuy nhiên, để đảm bảo nếu hàm này được gọi độc lập, có thể giữ lại hoặc cân nhắc.
    # setup_logging('configs/logger.yaml') 
    logger = logging.getLogger(__name__) # Lấy lại logger sau khi setup_logging

    etl_state = state.load_etl_state()
    sql_engine, duckdb_conn = None, None

    logger.info("Quy trình ETL bắt đầu...")
    try:
        sql_engine = create_engine(etl_settings.db.sqlalchemy_db_uri) # Sử dụng etl_settings.db.sqlalchemy_db_uri
        
        # Kiểm tra kết nối SQL Server
        with sql_engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Kết nối SQL Server thành công.")

        duckdb_path = str(etl_settings.DUCKDB_PATH.resolve())
        duckdb_conn = duckdb.connect(database=duckdb_path, read_only=False)
        logger.info(f"Kết nối DuckDB ('{duckdb_path}') thành công.\n")
        logger.info("Kết nối thành công đến SQL Server và DuckDB.")

        for table_name, config in etl_settings.TABLE_CONFIG.items():
            try:
                # Đảm bảo rằng config.timestamp_col tồn tại nếu config.incremental là True
                if config.incremental and not config.timestamp_col:
                    logger.error(
                        f"Lỗi cấu hình: Bảng '{config.source_table}' được đánh dấu là incremental "
                        "nhưng không có 'timestamp_col'. Bỏ qua bảng này."
                    )
                    continue

                _process_table(sql_engine, duckdb_conn, config, etl_state)
                # Lưu trạng thái sau mỗi bảng xử lý thành công để tránh mất tiến độ
                state.save_etl_state(etl_state) 
                logger.info(f"Xử lý thành công bảng '{config.source_table}'. State đã được lưu.\n")
            except Exception as e:
                # Lỗi đã được bắt và log chi tiết hơn trong _process_table.
                # Ở đây chỉ cần log lỗi chung cho bảng và tiếp tục các bảng khác.
                logger.error(f"Xử lý bảng '{config.source_table}' thất bại. Lỗi: {e}\n", exc_info=False) # exc_info=False để tránh log stack trace lặp lại
                continue
    except SQLAlchemyError as e:
        logger.critical(f"Lỗi nghiêm trọng khi kết nối SQL Server: {e}\n", exc_info=True)
    except duckdb.Error as e:
        logger.critical(f"Lỗi nghiêm trọng khi kết nối DuckDB: {e}\n", exc_info=True)
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng trong quy trình ETL chính: {e}\n", exc_info=True)
    finally:
        if sql_engine: 
            sql_engine.dispose()
            logger.info("Đã đóng kết nối SQL Server.")
        if duckdb_conn: 
            duckdb_conn.close()
            logger.info("Đã đóng kết nối DuckDB.")
        logger.info("Quy trình ETL kết thúc.\n")

api_app = FastAPI(
    title="iCount-People API",
    description="API để phân tích dữ liệu lượng người ra vào.",
    version="1.0.0"
)

@api_app.get("/", summary="Endpoint chào mừng")
def read_root():
    return {"message": "Chào mừng đến với iCount-People API"}

@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server, ví dụ '127.0.0.1'.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port để chạy server, ví dụ 8000.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại server khi có thay đổi code.")] = True,
):
    logger = logging.getLogger(__name__) # Lấy lại logger
    logger.info(f"Khởi chạy FastAPI server tại http://{host}:{port}")
    uvicorn.run("app.main:api_app", host=host, port=port, reload=reload)

if __name__ == "__main__":
    cli_app()
