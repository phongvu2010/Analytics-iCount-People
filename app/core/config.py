from pydantic import computed_field, Field
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib import parse

class EtlSettings(BaseSettings):
    """
    Class để đọc và validate các biến môi trường cho kết nối database.
    Tự động đọc từ file .env.
    """
    # Load settings from a .env file
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'  # Bỏ qua các biến môi trường không được định nghĩa trong lớp.
    )

    # Cấu hình SQL Server
    SQLSERVER_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    @computed_field
    @property
    def sqlalchemy_db_uri(self) -> str:
        """
        Tạo chuỗi kết nối SQLAlchemy cho SQL Server.
        Sử dụng quote_plus cho password để xử lý các ký tự đặc biệt.
        """
        return str(Url.build(
            scheme='mssql+pyodbc',
            username=self.SQLSERVER_UID,
            password=parse.quote_plus(self.SQLSERVER_PWD),
            host=self.SQLSERVER_SERVER,
            path=f"{self.SQLSERVER_DATABASE}",
            query=f"driver={self.SQLSERVER_DRIVER.replace(' ', '+')}"
        ))

    # Cấu hình DuckDB
    DUCKDB_PATH: str = 'data/analytics.duckdb'
    STATE_FILE: str = 'data/etl_state.json'

    ETL_CHUNK_SIZE: int = Field(default=10000, description="Số dòng xử lý mỗi chunk để tối ưu bộ nhớ")
    ETL_DEFAULT_TIMESTAMP: str = Field(default='1900-01-01 00:00:00', description="Timestamp bắt đầu nếu chưa có trạng thái")

    # Cấu hình chi tiết cho từng bảng ETL
    TABLE_CONFIG: dict = {
        'store': {
            'source_table': 'dbo.store',
            'dest_table': 'dim_stores',
            'incremental': False,  # Bảng dimension nhỏ, chạy full load mỗi lần
            'partition_cols': [], 
            'rename_map': {
                'tid': 'store_id',
                'name': 'store_name'
            }
        },
        'num_crowd': {
            'source_table': 'dbo.num_crowd',
            'dest_table': 'fact_traffic',
            'timestamp_col': 'recordtime',
            'dest_timestamp_col': 'recorded_at',
            'partition_cols': ['year', 'month'],
            'rename_map': {
                'recordtime': 'recorded_at',
                'in_num': 'visitors_in',
                'out_num': 'visitors_out',
                'position': 'device_position',
                'storeid': 'store_id'
            }
        },
        'ErrLog': {
            'source_table': 'dbo.ErrLog',
            'dest_table': 'fact_errors',
            'timestamp_col': 'LogTime',
            'dest_timestamp_col': 'logged_at',
            'partition_cols': ['year', 'month'],
            'rename_map': {
                'ID': 'log_id',
                'storeid': 'store_id',
                'DeviceCode': 'device_code',
                'LogTime': 'logged_at',
                'Errorcode': 'error_code',
                'ErrorMessage': 'error_message'
            }
        }
    }

# Tạo một instance của settings để import và sử dụng trong các file khác
etl_settings = EtlSettings()
