from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, Any, Optional

# Định nghĩa một lớp cấu hình cơ bản cho SQL Server
class SqlServerSettings(BaseSettings):
    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    driver: str = "{ODBC Driver 17 for SQL Server}"

    model_config = SettingsConfigDict(
        env_prefix='SQL_SERVER_',   # Chỉ đọc các biến môi trường có tiền tố 'SQL_SERVER_'.
        env_file='.env',            # Tự động tải biến từ file .env.
        extra='ignore'              # Bỏ qua các biến môi trường không được định nghĩa trong lớp.
    )

# Định nghĩa lớp cấu hình cho toàn bộ ứng dụng ETL
class Settings(BaseSettings):
    # Cấu hình SQL Server
    sql_server: SqlServerSettings = SqlServerSettings()     # Nhúng cấu hình SQL Server vào đây

    # Cấu hình đường dẫn đầu ra
    data_dir: str = 'data'
    duckdb_file_name: str = 'analytics_data.duckdb'
    # output_parquet_file_name: str = 'traffic_analytics.parquet' # Không dùng trực tiếp nữa

    # Cấu hình Partition Parquet
    partition_parquet_by_year: bool = True # Bật/tắt partition theo năm
    # Nếu partition_parquet_by_year là True, output_parquet_base_path sẽ là thư mục gốc
    output_parquet_base_path: str = 'data/traffic_analytics'


    @property
    def duckdb_path(self) -> str:
        return f"{self.data_dir}/{self.duckdb_file_name}"

    # @property
    # def output_parquet_path(self) -> str: # Không còn cần thuộc tính này trực tiếp
    #     return f"{self.data_dir}/{self.output_parquet_file_name}"

    # Cấu hình tên bảng trong DuckDB sau khi biến đổi
    table_names: Dict[str, str] = {
        'stores': 'transformed_stores',
        'err_log': 'transformed_err_log',
        'num_crowd': 'transformed_num_crowd'
    }

    # Cấu hình cho Incremental Load
    incremental_config: Dict[str, Dict[str, str]] = {
        'num_crowd': {
            'table_name': 'num_crowd',          # Tên bảng gốc trong SQL Server
            'time_column': 'recordtime',        # Cột thời gian để theo dõi thay đổi
            'last_load_file': 'last_load_num_crowd.txt' # Tên file lưu trữ thời điểm tải cuối cùng
        },
        'err_log': {
            'table_name': 'ErrLog',
            'time_column': 'LogTime',
            'last_load_file': 'last_load_err_log.txt'
        }
    }

    # Định nghĩa các truy vấn SQL để trích xuất dữ liệu thô
    raw_sql_queries: Dict[str, str] = {
        'store': "SELECT tid, name FROM dbo.store",
        'err_log': "SELECT ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage FROM dbo.ErrLog",
        'num_crowd': "SELECT recordtime, in_num, out_num, position, storeid FROM dbo.num_crowd"
    }

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

# Khởi tạo settings để có thể import và sử dụng trực tiếp
settings = Settings()
