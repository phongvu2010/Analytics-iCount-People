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
        return str(Url.build(
            scheme='mssql+pyodbc',
            username=self.SQLSERVER_UID,
            password=parse.quote_plus(self.SQLSERVER_PWD),
            host=self.SQLSERVER_SERVER,
            path=f"{self.SQLSERVER_DATABASE}",
            query=f"driver={self.SQLSERVER_DRIVER.replace(' ', '+')}"
        ))

class EtlSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)

    DATA_DIR: Path = 'data'

    @property
    def DUCKDB_PATH(self) -> Path:
        return self.DATA_DIR / 'analytics.duckdb'

    @property
    def STATE_FILE(self) -> Path:
        return self.DATA_DIR / 'etl_state.json'

    ETL_CHUNK_SIZE: int = 100000
    ETL_DEFAULT_TIMESTAMP: str = '1900-01-01 00:00:00'
    ETL_CLEANUP_ON_FAILURE: bool = True
    TABLE_CONFIG_PATH: Path = 'configs/tables.yaml'
    TABLE_CONFIG: Dict[str, TableConfig] = {}

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
            raise ValueError(f"Lỗi: Không tìm thấy file cấu hình bảng tại: {self.TABLE_CONFIG_PATH}. "
                             "Hãy đảm bảo đường dẫn chính xác và file tồn tại.")
        except yaml.YAMLError as e:
            raise ValueError(f"Lỗi phân tích cú pháp file YAML cấu hình bảng '{self.TABLE_CONFIG_PATH}': {e}. "
                             "Kiểm tra định dạng YAML.")
        except ValidationError as e:
            raise ValueError(f"Lỗi xác thực cấu hình bảng từ file '{self.TABLE_CONFIG_PATH}':\n{e}. "
                             "Kiểm tra cấu trúc file tables.yaml.")
        except Exception as e:
            raise ValueError(f"Lỗi không xác định khi tải hoặc xác thực cấu hình bảng: {e}")

        return self

etl_settings = EtlSettings()
