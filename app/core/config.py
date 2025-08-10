import yaml

from pathlib import Path
from pydantic import BaseModel, Field, model_validator, TypeAdapter, ValidationError
from pydantic_core import Url
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, List, Literal, Optional
from urllib import parse

class CleaningRule(BaseModel):
    column: str
    action: Literal['strip']

class TableConfig(BaseModel):
    source_table: str
    dest_table: str
    incremental: bool = True
    rename_map: Dict[str, str] = {}
    partition_cols: List[str] = []
    cleaning_rules: List[CleaningRule] = Field(default_factory=list)

    timestamp_col: Optional[str] = None

    @model_validator(mode='after')
    def validate_incremental_config(self) -> 'TableConfig':
        if self.incremental and not self.timestamp_col:
            raise ValueError(
                f"Cấu hình cho bảng '{self.source_table}': Cần cung cấp 'timestamp_col' khi 'incremental' là True."
            )
        return self

    @property
    def final_timestamp_col(self) -> Optional[str]:
        if not self.timestamp_col: return None
        return self.rename_map.get(self.timestamp_col, self.timestamp_col)

class DatabaseSettings(BaseModel):
    SQLSERVER_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    @property
    def sqlalchemy_db_uri(self) -> str:
        # URL-encode the password to handle special characters
        encoded_pwd = parse.quote_plus(self.SQLSERVER_PWD)
        driver_for_query = self.SQLSERVER_DRIVER.replace(' ', '+')

        # Trả về chuỗi kết nối SQLAlchemy cho SQL Server
        # Sử dụng pyodbc driver
        return (
            f"mssql+pyodbc://{self.SQLSERVER_UID}:{encoded_pwd}@"
            f"{self.SQLSERVER_SERVER}/{self.SQLSERVER_DATABASE}?"
            f"driver={driver_for_query}"
        )

class EtlSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore' # Bỏ qua các biến môi trường không được định nghĩa trong model
    )

    # 1. Đưa các trường cấu hình DB lên cấp cao nhất của EtlSettings
    #    để pydantic-settings có thể đọc trực tiếp từ file .env
    SQLSERVER_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    # 2. Định nghĩa trường 'db' là Optional, chúng ta sẽ tạo nó ngay sau đây
    db: Optional[DatabaseSettings] = None

    # 3. Sử dụng model_validator để xây dựng đối tượng 'db' sau khi đã đọc các biến
    @model_validator(mode='after')
    def assemble_db_settings(self) -> 'EtlSettings':
        # Sau khi các biến SQLSERVER_* đã được load,
        # chúng ta dùng chúng để tạo đối tượng DatabaseSettings
        if not self.db:
            self.db = DatabaseSettings(
                SQLSERVER_DRIVER=self.SQLSERVER_DRIVER,
                SQLSERVER_SERVER=self.SQLSERVER_SERVER,
                SQLSERVER_DATABASE=self.SQLSERVER_DATABASE,
                SQLSERVER_UID=self.SQLSERVER_UID,
                SQLSERVER_PWD=self.SQLSERVER_PWD
            )
        return self

    DATA_DIR: Path = Path('data')
    ETL_CHUNK_SIZE: int = 100000
    ETL_DEFAULT_TIMESTAMP: str = '1900-01-01 00:00:00'
    ETL_CLEANUP_ON_FAILURE: bool = True
    TABLE_CONFIG_PATH: Path = Path('configs/tables.yaml')
    TABLE_CONFIG: Dict[str, TableConfig] = {}

    @property
    def DUCKDB_PATH(self) -> Path:
        return self.DATA_DIR / 'analytics.duckdb'

    @property
    def STATE_FILE(self) -> Path:
        return self.DATA_DIR / 'etl_state.json'

    @model_validator(mode='after')
    def load_and_validate_table_config(self) -> 'EtlSettings':
        if self.TABLE_CONFIG: return self

        try:
            with self.TABLE_CONFIG_PATH.open('r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                raise ValueError(f"File cấu hình bảng '{self.TABLE_CONFIG_PATH}' rỗng hoặc không hợp lệ.")

            adapter = TypeAdapter(Dict[str, TableConfig])
            self.TABLE_CONFIG = adapter.validate_python(raw_config)
        except FileNotFoundError:
            raise ValueError(f"Lỗi: Không tìm thấy file cấu hình bảng tại: {self.TABLE_CONFIG_PATH}.")
        except yaml.YAMLError as e:
            raise ValueError(f"Lỗi phân tích cú pháp file YAML cấu hình bảng '{self.TABLE_CONFIG_PATH}': {e}.")
        except ValidationError as e:
            raise ValueError(f"Lỗi xác thực cấu hình bảng từ file '{self.TABLE_CONFIG_PATH}':\n{e}.")
        except Exception as e:
            raise ValueError(f"Lỗi không xác định khi tải hoặc xác thực cấu hình bảng: {e}")

        return self

etl_settings = EtlSettings()
