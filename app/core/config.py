from pathlib import Path
from pydantic import BaseModel, computed_field, Field
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, List, Optional
from urllib import parse

# --- Định nghĩa cấu trúc cho mỗi table config ---
class TableConfig(BaseModel):
    """
    Cấu trúc cho cấu hình xử lý của một bảng.
    Cung cấp validation và gợi ý code tốt hơn.
    """
    # Các trường này luôn bắt buộc
    source_table: str
    dest_table: str
    incremental: bool = True
    rename_map: Dict[str, str] = {}
    partition_cols: List[str] = []

    # Dùng Optional vì các bảng full-load không cần các cột này
    timestamp_col: Optional[str] = None
    dest_timestamp_col: Optional[str] = None

class EtlSettings(BaseSettings):
    """
    Class để đọc và validate các biến môi trường và cấu hình ETL.
    Tự động đọc từ file .env.
    """
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
    DATA_DIR: Path = Field(
        default=Path('data'),
        description='Thư mục gốc chứa dữ liệu.'
    )

    @property
    def DUCKDB_PATH(self) -> Path:
        """Đường dẫn đến file DuckDB, dựa trên DATA_DIR."""
        return self.DATA_DIR / 'analytics.duckdb'

    @property
    def STATE_FILE(self) -> Path:
        """Đường dẫn đến file trạng thái ETL, dựa trên DATA_DIR."""
        return self.DATA_DIR / 'etl_state.json'

    # Cấu hình chung cho ETL
    ETL_CHUNK_SIZE: int = Field(
        default=100000,
        description='Số dòng xử lý mỗi chunk để tối ưu bộ nhớ.'
    )

    ETL_DEFAULT_TIMESTAMP: str = Field(
        default='1900-01-01 00:00:00',
        description='Timestamp bắt đầu nếu chưa có trạng thái.'
    )

    # Sử dụng TableConfig để Pydantic tự động parse và validate
    TABLE_CONFIG: Dict[str, TableConfig] = {
        'store': {
            'source_table': 'dbo.store',
            'dest_table': 'dim_stores',
            'incremental': False,  # Bảng dimension nhỏ, chạy full load mỗi lần
            'rename_map': { 'tid': 'store_id', 'name': 'store_name' }
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
