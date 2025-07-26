from pydantic import computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Any
from urllib import parse

class Settings(BaseSettings):
    """
    Loads configuration settings from a .env file.
    """
    # Load settings from a .env file
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'  # Bỏ qua các biến môi trường không được định nghĩa trong lớp.
    )

    # SQL Server settings
    DB_DRIVER: str
    DB_SERVER: str
    DB_DATABASE: str
    DB_USERNAME: str
    DB_PASSWORD: str

    # DuckDB settings
    DUCKDB_PATH: str = 'data/analytics.duckdb'
    DUCKDB_PARTITION_PATH: str = 'data/partitions'

    # ETL settings
    ETL_CHUNK_SIZE: int = 50000

    @computed_field
    @property
    def sqlalchemy_db_uri(self) -> str:
        """
        Generates the SQLAlchemy connection string for SQL Server.
        """
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.DB_USERNAME,
            password = parse.quote_plus(self.DB_PASSWORD),
            host = self.DB_SERVER,
            path = self.DB_DATABASE,
            query = f"driver={self.DB_DRIVER.replace(' ', '+')}"
        ))

    ETL_TABLES_CONFIG: List[Dict[str, Any]] = [
        {
            "name": "store",
            "type": "dimension",
            "source_query": "SELECT tid, name FROM store"
        }, {
            "name": "num_crowd",
            "type": "fact",
            "timestamp_column": "recordtime",
            "partition_cols": ["storeid", "year", "month"]
        }, {
            "name": "ErrLog",
            "type": "fact",
            "timestamp_column": "LogTime",
            "partition_cols": ["storeid", "year", "month"]
        }
    ]

# Create a single instance to be imported by other modules
settings = Settings()







# import os

# class Settings(BaseSettings):
#     DUCKDB_DATA_DIR: str

#     # Logging
#     LOG_FILE_PATH: str

# # Ensure log and data directories exist
# os.makedirs(os.path.dirname(settings.LOG_FILE_PATH), exist_ok=True)
# os.makedirs(settings.DUCKDB_DATA_DIR, exist_ok=True)






# # from pydantic import computed_field
# # from pydantic_core import MultiHostUrl

# from typing import Dict # Any, Optional

# # Định nghĩa một lớp cấu hình cơ bản cho SQL Server
# class SqlServerSettings(BaseSettings):
#     host: str
#     port: int = 1433
#     database: str
#     username: str
#     password: str
#     driver: str = 'ODBC Driver 17 for SQL Server'

#     # @computed_field
#     # @property
#     # def SQLALCHEMY_DATABASE_URI(self) -> str:
#     #     return str(MultiHostUrl.build(
#     #         scheme = 'mssql+pyodbc',
#     #         username = self.username,
#     #         password = parse.quote_plus(self.password),
#     #         host = self.host,
#     #         port = self.port,
#     #         path = self.database,
#     #         query = f"driver={self.driver.replace(' ', '+')}"
#     #     ))

#     model_config = SettingsConfigDict(
#         env_prefix='SQL_SERVER_',   # Chỉ đọc các biến môi trường có tiền tố 'SQL_SERVER_'.
#         env_file='.env',            # Tự động tải biến từ file .env.
#         extra='ignore'              # Bỏ qua các biến môi trường không được định nghĩa trong lớp.
#     )

# # Định nghĩa lớp cấu hình cho log
# class LoggingSettings(BaseSettings):
#     log_file: str = 'etl.log'
#     log_level: str = 'INFO'
#     log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     log_date_format: str = '%Y-%m-%d %H:%M:%S'

# # Định nghĩa lớp cấu hình cho toàn bộ ứng dụng ETL
# class EtlSettings(BaseSettings):
#     # Cấu hình SQL Server
#     sql_server: SqlServerSettings = SqlServerSettings()

#     # Cấu hình Log
#     logging: LoggingSettings = LoggingSettings()

#     # Cấu hình đường dẫn đầu ra
#     data_dir: str = 'data'
#     duckdb_file_name: str = 'analytics_data.duckdb'
#     # output_parquet_file_name: str = 'traffic_analytics.parquet'

#     # Cấu hình Partition Parquet
#     partition_parquet_by_year: bool = True  # Bật/tắt partition theo năm
#     # Nếu partition_parquet_by_year là True, output_parquet_base_path sẽ là thư mục gốc
#     output_parquet_base_path: str = 'data/traffic_analytics'

#     @property
#     def duckdb_path(self) -> str:
#         return f"{self.data_dir}/{self.duckdb_file_name}"

#     # @property
#     # def output_parquet_path(self) -> str:
#     #     return f"{self.data_dir}/{self.output_parquet_file_name}"

#     # Cấu hình tên bảng trong DuckDB sau khi biến đổi
#     table_names: Dict[str, str] = {
#         'stores': 'transformed_stores',
#         'err_log': 'transformed_err_log',
#         'num_crowd': 'transformed_num_crowd'
#     }

#     # Cấu hình cho Incremental Load
#     incremental_config: Dict[str, Dict[str, str]] = {
#         'num_crowd': {
#             'table_name': 'num_crowd',                      # Tên bảng gốc trong SQL Server
#             'time_column': 'recordtime',                    # Cột thời gian để theo dõi thay đổi
#             'last_load_file': 'last_load_num_crowd.txt'     # Tên file lưu trữ thời điểm tải cuối cùng
#         },
#         'err_log': {
#             'table_name': 'ErrLog',
#             'time_column': 'LogTime',
#             'last_load_file': 'last_load_err_log.txt'
#         }
#     }

#     # Định nghĩa các truy vấn SQL để trích xuất dữ liệu thô
#     raw_sql_queries: Dict[str, str] = {
#         'store': 'SELECT tid, name FROM dbo.store',
#         'err_log': 'SELECT ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage FROM dbo.ErrLog',
#         'num_crowd': 'SELECT recordtime, in_num, out_num, position, storeid FROM dbo.num_crowd'
#     }

# # Khởi tạo settings để có thể import và sử dụng trực tiếp
# settings = EtlSettings()
