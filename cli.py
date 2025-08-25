import contextlib
import duckdb
import logging
import pandera.errors as pa_errors
import requests
import typer
import uvicorn

from concurrent.futures import ThreadPoolExecutor, as_completed
from duckdb import DuckDBPyConnection, Error as DuckdbError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_fixed, before_sleep_log, retry_if_exception
from threading import Lock
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


# --- TÍCH HỢP CƠ CHẾ RETRY ---
# Decorator @retry sẽ bao bọc hàm _process_table.
# - stop_after_attempt(3): Thử lại tối đa 3 lần (1 lần đầu + 2 lần thử lại).
# - wait_fixed(15): Chờ 15 giây giữa mỗi lần thử lại.
# - before_sleep_log: Tự động ghi log cảnh báo trước mỗi lần thử lại.
# - retry_on_exception: Chỉ thử lại nếu gặp các lỗi liên quan đến DB/IO,
#   không thử lại với các lỗi logic dữ liệu như Pandera.
def is_retryable_exception(exception: BaseException) -> bool:
    return isinstance(exception, (SQLAlchemyError, DuckdbError, IOError))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(15),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    retry=retry_if_exception(is_retryable_exception)
)
def _process_table(
    sql_engine: Engine,
    duckdb_conn: DuckDBPyConnection,
    config: TableConfig,
    etl_state: dict,
    # ================== THAY ĐỔI 2: Thêm lock để đảm bảo an toàn luồng ==================
    state_lock: Lock
    # =================================================================================
):
    logger.info(
        f"Bắt đầu xử lý bảng: '{config.source_table}' -> "
        f"'{config.dest_table}' (Incremental: {config.incremental})"
    )
    prepare_destination(config) # 1. Chuẩn bị thư mục staging
    
    # Đọc high-water mark từ state. Không cần lock vì đây là hành động đọc.
    last_timestamp = state.get_last_timestamp(etl_state, config.dest_table)
    data_iterator = extract.from_sql_server(sql_engine, config, last_timestamp) # 2. Extract

    total_rows, max_ts_in_run = 0, None

    # Giữ khối try...except để bắt lỗi sau khi đã retry hết số lần
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
            logger.info(f"Đã xử lý {total_rows} dòng cho '{config.dest_table}'. Bắt đầu tải vào DuckDB.")
            refresh_duckdb_table(duckdb_conn, config, loader.has_written_data)
            logger.info(f"Đã tải thành công dữ liệu vào DuckDB '{config.dest_table}'.")

            # 6. Cập nhật state nếu là incremental load
            if config.incremental and max_ts_in_run:
                # ================== THAY ĐỔI 3: Sử dụng lock khi cập nhật state ==================
                with state_lock:
                    state.update_timestamp(etl_state, config.dest_table, max_ts_in_run)
                    state.save_etl_state(etl_state) # Lưu state ngay sau khi xử lý thành công
                # ===============================================================================
        else:
            logger.info(f"Không tìm thấy dữ liệu mới cho '{config.dest_table}'.")
        
        # Trả về tên bảng nếu thành công
        return config.source_table

    # Bắt các lỗi không thể retry (ví dụ: lỗi validation) hoặc lỗi sau khi đã retry hết
    except pa_errors.SchemaErrors as e:
        logger.error(f"❌ Lỗi validation dữ liệu không thể retry cho '{config.source_table}': {e}", exc_info=True)
        raise # Ném lại lỗi để vòng lặp chính bắt được

    except Exception as e:
        logger.error(f"❌ Đã xảy ra lỗi không thể phục hồi sau các lần thử lại cho '{config.source_table}': {e}", exc_info=True)
        raise


def _trigger_cache_clear(host: str, port: int):
    # Chỉ thực hiện nếu API token đã được cấu hình
    if not settings.INTERNAL_API_TOKEN:
        logger.warning("INTERNAL_API_TOKEN chưa được cấu hình, bỏ qua việc xóa cache.")
        return

    clear_cache_url = f"http://{host}:{port}/api/v1/admin/clear-cache"
    headers = {"X-Internal-Token": settings.INTERNAL_API_TOKEN}

    try:
        logger.info(f"Đang gửi yêu cầu xóa cache đến {clear_cache_url}...")
        response = requests.post(clear_cache_url, headers=headers, timeout=10)
        response.raise_for_status() # Ném lỗi nếu status code là 4xx hoặc 5xx
        if response.status_code == 204:
            logger.info("✅ Yêu cầu xóa cache được API server chấp nhận thành công.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Không thể xóa cache của API server: {e}")
        logger.warning("Lưu ý: Dữ liệu mới có thể mất tới 30 phút để hiển thị trên dashboard.")


@cli_app.command()
def run_etl(
    # Thêm tùy chọn để giới hạn số luồng, mặc định là 4
    max_workers: int = typer.Option(4, help="Số luồng tối đa để xử lý ETL song song."),
    # Thêm tùy chọn để kích hoạt/vô hiệu hóa việc xóa cache
    clear_cache: bool = typer.Option(True, help="Tự động xóa cache của API server sau khi ETL xong."),
    api_host: str = typer.Option("127.0.0.1", help="Host của API server đang chạy."),
    api_port: int = typer.Option(8000, help="Port của API server đang chạy."),
):
    logger.info("=" * 60)
    logger.info(f"🚀 BẮT ĐẦU QUY TRÌNH ETL (tối đa {max_workers} luồng)")
    logger.info("=" * 60)

    succeeded, failed = [], []
    etl_state = state.load_etl_state()

    # ================== THAY ĐỔI 4: Khởi tạo Lock và ThreadPoolExecutor ==================
    state_lock = Lock()
    
    # Sắp xếp các bảng theo thứ tự xử lý đã định nghĩa trong config.
    # Điều này vẫn quan trọng nếu có các bảng phụ thuộc (ví dụ: dim table phải chạy trước fact table)
    # ThreadPoolExecutor sẽ tôn trọng thứ tự submit task.
    tables_to_process = sorted(
        settings.TABLE_CONFIG.values(),
        key=lambda config: config.processing_order
    )

    try:
        with _get_database_connections() as (sql_engine, duckdb_conn):
            # Sử dụng ThreadPoolExecutor để chạy các tác vụ song song
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Tạo một future cho mỗi bảng cần xử lý
                future_to_table = {
                    executor.submit(_process_table, sql_engine, duckdb_conn, config, etl_state, state_lock): config
                    for config in tables_to_process
                }

                # Lặp qua các future khi chúng hoàn thành
                for future in as_completed(future_to_table):
                    config = future_to_table[future]
                    try:
                        # Lấy kết quả (tên bảng) nếu thành công
                        result = future.result()
                        succeeded.append(result)
                        logger.info(f"✅ Xử lý thành công '{config.source_table}'.\n")
                    except Exception:
                        # Exception ở đây nghĩa là hàm đã retry hết số lần mà vẫn lỗi
                        failed.append(config.source_table)
                        logger.error(
                            f"❌ Xử lý '{config.source_table}' thất bại sau tất cả các lần thử lại. "
                            f"Chi tiết lỗi đã được log ở trên.\n"
                        )
    # =====================================================================================

    except Exception as e:
        logger.critical(f"Quy trình ETL bị dừng do lỗi kết nối ban đầu: {e}")

    finally:
        # ================== THAY ĐỔI 3: Kích hoạt xóa cache sau khi ETL kết thúc ==================
        # Chỉ xóa cache nếu ETL chạy thành công ít nhất 1 bảng và người dùng cho phép
        if clear_cache and succeeded:
             _trigger_cache_clear(host=api_host, port=api_port)
        # ======================================================================================

        logger.info("=" * 60)
        logger.info("📊 TÓM TẮT KẾT QUẢ ETL")
        logger.info(f"Tổng số bảng: {len(tables_to_process)}")
        logger.info(f"✅ Thành công: {len(succeeded)}")
        logger.info(f"❌ Thất bại: {len(failed)}")
        if failed:
            logger.warning(f"Danh sách bảng thất bại: {', '.join(failed)}")
        logger.info("=" * 60 + "\n")


@cli_app.command()
def init_db():
    """
    Khởi tạo hoặc cập nhật các đối tượng database cần thiết, như VIEWs.

    Lệnh này nên được chạy một lần khi thiết lập dự án, hoặc mỗi khi có
    thay đổi về logic nghiệp vụ trong VIEW (ví dụ: thay đổi ngưỡng outlier).
    """
    logger.info("Bắt đầu khởi tạo/cập nhật các VIEWs trong DuckDB...")

    # Logic xử lý outlier được lấy từ config để VIEW luôn đồng bộ với cài đặt.
    scale = settings.OUTLIER_SCALE_RATIO
    then_logic_in = f'CAST(ROUND(a.visitors_in * {scale}, 0) AS INTEGER)' if scale > 0 else '1'
    then_logic_out = f'CAST(ROUND(a.visitors_out * {scale}, 0) AS INTEGER)' if scale > 0 else '1'
    
    # VIEW này sẽ chuẩn hóa dữ liệu, xử lý outlier và điều chỉnh "ngày làm việc".
    # Đây chính là logic từ hàm `_build_base_query` cũ trong services.py.
    create_view_sql = f"""
    CREATE OR REPLACE VIEW v_traffic_normalized AS
    SELECT
        CAST(a.recorded_at AS TIMESTAMP) as record_time,
        b.store_name,
        CASE
            WHEN a.visitors_in > {settings.OUTLIER_THRESHOLD} THEN {then_logic_in}
            ELSE a.visitors_in
        END as in_count,
        CASE
            WHEN a.visitors_out > {settings.OUTLIER_THRESHOLD} THEN {then_logic_out}
            ELSE a.visitors_out
        END as out_count,
        -- Dịch chuyển thời gian để ngày làm việc bắt đầu từ 00:00
        (CAST(a.recorded_at AS TIMESTAMP) - INTERVAL '{settings.WORKING_HOUR_START} hours') AS adjusted_time
    FROM fact_traffic AS a
    LEFT JOIN dim_stores AS b ON a.store_id = b.store_id;
    """
    
    try:
        # Kết nối trực tiếp đến file DuckDB để thực thi lệnh
        db_path = str(settings.DUCKDB_PATH.resolve())
        with duckdb.connect(database=db_path, read_only=False) as conn:
            conn.execute(create_view_sql)
        logger.info("✅ Đã tạo/cập nhật thành công VIEW 'v_traffic_normalized'.")
    except Exception as e:
        logger.error(f"❌ Lỗi khi khởi tạo VIEW: {e}", exc_info=True)
        raise typer.Exit(code=1)


@cli_app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host để chạy server.")] = '127.0.0.1',
    port: Annotated[int, typer.Option(help="Port để chạy server.")] = 8000,
    reload: Annotated[bool, typer.Option(help="Tự động tải lại khi code thay đổi.")] = True,
):
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
