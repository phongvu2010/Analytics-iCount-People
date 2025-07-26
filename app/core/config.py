from pydantic import computed_field
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib import parse

class DatabaseSettings(BaseSettings):
    """
    Class để đọc và validate các biến môi trường cho kết nối database.
    Tự động đọc từ file .env.
    """
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    SQLSERVER_DRIVER: str = '{ODBC Driver 17 for SQL Server}'
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    # Thêm computed_field để tạo chuỗi kết nối SQLAlchemy
    @computed_field
    @property
    def sqlalchemy_db_uri(self) -> str:
        """
        Tạo chuỗi kết nối SQLAlchemy cho SQL Server.
        Sử dụng quote_plus cho password để xử lý các ký tự đặc biệt.
        """
        # Lưu ý: path cần có dấu / ở đầu
        return str(Url.build(
            scheme='mssql+pyodbc',
            username=self.SQLSERVER_UID,
            password=parse.quote_plus(self.SQLSERVER_PWD),
            host=self.SQLSERVER_SERVER,
            path=f"/{self.SQLSERVER_DATABASE}",
            query=f"driver={self.SQLSERVER_DRIVER.replace(' ', '+')}"
        ))

    # def get_sql_server_connection():
    #     """Tạo và trả về connection tới SQL Server."""
    #     # THAY ĐỔI: Sử dụng object `settings` đã được validate bởi Pydantic
    #     conn_str = (
    #         f"DRIVER={settings.SQL_SERVER_DRIVER};"
    #         f"SERVER={settings.SQL_SERVER_SERVER};"
    #         f"DATABASE={settings.SQL_SERVER_DATABASE};"
    #         f"UID={settings.SQL_SERVER_UID};"
    #         f"PWD={settings.SQL_SERVER_PWD};"
    #         "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    #     )
    #     return pyodbc.connect(conn_str)

# Tạo một instance của settings để import và sử dụng trong các file khác
settings = DatabaseSettings()

# Cấu hình chi tiết cho từng bảng ETL
TABLE_CONFIG = {
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
        'dest_table': 'log_errors',
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
    },
    'store': {
        'source_table': 'dbo.store',
        'dest_table': 'dim_stores',
        'incremental': False,  # Bảng dimension nhỏ, chạy full load mỗi lần
        'rename_map': {
            'tid': 'store_id',
            'name': 'store_name'
        }
    }
}
