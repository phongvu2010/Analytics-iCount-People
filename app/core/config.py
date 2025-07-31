import yaml

from pathlib import Path
from pydantic import BaseModel, computed_field, Field, TypeAdapter
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, List, Optional
from urllib import parse

# Định nghĩa cấu trúc cho mỗi table config
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
    DATA_DIR: Path = 'data'

    @property
    def DUCKDB_PATH(self) -> Path:
        """Đường dẫn đến file DuckDB, dựa trên DATA_DIR."""
        return self.DATA_DIR / 'analytics.duckdb'

    @property
    def STATE_FILE(self) -> Path:
        """Đường dẫn đến file trạng thái ETL, dựa trên DATA_DIR."""
        return self.DATA_DIR / 'etl_state.json'

    # Cấu hình chung cho ETL
    ETL_CHUNK_SIZE: int = 100000
    ETL_DEFAULT_TIMESTAMP: str = '1900-01-01 00:00:00'

    TABLE_CONFIG_PATH: Path = 'app/tables.yaml'

    # Sử dụng TableConfig để Pydantic tự động parse và validate
    @computed_field
    @property
    def TABLE_CONFIG(self) -> Dict[str, TableConfig]:
        try:
            with self.TABLE_CONFIG_PATH.open('r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)

            adapter = TypeAdapter(Dict[str, TableConfig])
            return adapter.validate_python(raw_config)
        except FileNotFoundError:
            raise ValueError(f"Không tìm thấy file cấu hình bảng tại: {self.TABLE_CONFIG_PATH}")
        except Exception as e:
            raise ValueError(f"Lỗi khi tải hoặc xác thực cấu hình bảng: {e}")

# Tạo một instance của settings để import và sử dụng trong các file khác
etl_settings = EtlSettings()
