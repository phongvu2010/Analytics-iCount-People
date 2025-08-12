"""
Định nghĩa tất cả các model cấu hình cho dự án bằng Pydantic.

Module này sử dụng pydantic-settings để đọc cấu hình từ file .env
và file tables.yaml, cung cấp một đối tượng `etl_settings` duy nhất,
đã được xác thực và có kiểu dữ liệu rõ ràng cho toàn bộ ứng dụng.
"""
import yaml

from pathlib import Path
from pydantic import AnyUrl, BeforeValidator, BaseModel, Field, model_validator, TypeAdapter, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated, Any, Dict, List, Literal, Optional
from urllib import parse

def parse_cors(v: Any) -> List[str] | str:
    """ Helper để phân tích chuỗi CORS từ biến môi trường thành danh sách. """
    if isinstance(v, str) and not v.startswith('['):
        return [i.strip() for i in v.split(', ')]

    if isinstance(v, list | str):
        return v

    raise ValueError(v)

class CleaningRule(BaseModel):
    """ Định nghĩa một quy tắc làm sạch dữ liệu cho một cột. """
    column: str                 # Tên cột gốc trong source_table
    action: Literal['strip']    # Hành động làm sạch (hiện chỉ hỗ trợ 'strip')

class TableConfig(BaseModel):
    """ Cấu hình chi tiết cho việc xử lý một bảng. """
    source_table: str                       # Tên bảng nguồn trong MS SQL
    dest_table: str                         # Tên bảng đích trong DuckDB
    incremental: bool = True                # True: chạy incremental, False: full-load
    description: Optional[str] = None       # Mô tả mục đích của bảng
    processing_order: int = 99              # Thứ tự xử lý để đảm bảo dependency
    rename_map: Dict[str, str] = {}         # Mapping đổi tên cột
    partition_cols: List[str] = []          # Danh sách các cột để partition trong Parquet
    cleaning_rules: List[CleaningRule] = Field(default_factory=list) # Danh sách quy tắc làm sạch
    timestamp_col: Optional[str] = None     # Tên cột timestamp cho incremental load

    @model_validator(mode='after')
    def validate_incremental_config(self) -> 'TableConfig':
        """ Xác thực rằng timestamp_col phải được cung cấp khi chạy incremental. """
        if self.incremental and not self.timestamp_col:
            raise ValueError(
                f"Cấu hình cho '{self.source_table}': 'timestamp_col' là bắt buộc khi 'incremental' là True."
            )
        return self

    @property
    def final_timestamp_col(self) -> Optional[str]:
        """ Lấy tên cột timestamp cuối cùng (sau khi đã đổi tên). """
        if not self.timestamp_col:
            return None
        return self.rename_map.get(self.timestamp_col, self.timestamp_col)

class DatabaseSettings(BaseModel):
    """ Cấu hình kết nối tới MS SQL Server. """
    SQLSERVER_DRIVER: str
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    @property
    def sqlalchemy_db_uri(self) -> str:
        """ Tạo chuỗi kết nối SQLAlchemy từ các biến cấu hình. """
        encoded_pwd = parse.quote_plus(self.SQLSERVER_PWD)
        driver_for_query = self.SQLSERVER_DRIVER.replace(' ', '+')

        return (
            f"mssql+pyodbc://{self.SQLSERVER_UID}:{encoded_pwd}@"
            f"{self.SQLSERVER_SERVER}/{self.SQLSERVER_DATABASE}?"
            f"driver={driver_for_query}"
        )

class EtlSettings(BaseSettings):
    """ Model cấu hình chính, tổng hợp tất cả các thiết lập. """
    model_config = SettingsConfigDict(
        env_file='.env',
        case_sensitive=True,
        env_file_encoding='utf-8',
        extra='ignore' # Bỏ qua các biến môi trường không có trong model
    )

    # Cấu hình chung
    PROJECT_NAME: str = 'Analytics iCount People API'
    DESCRIPTION: str = 'API cung cấp dữ liệu phân tích lượt ra vào cửa hàng.'

    BACKEND_CORS_ORIGINS: Annotated[
        List[AnyUrl], BeforeValidator(parse_cors)
    ] = []

    # --- Database Credentials (đọc từ .env) ---
    SQLSERVER_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    # --- Các thiết lập chung cho ETL ---
    DATA_DIR: Path = Path('data')
    ETL_CHUNK_SIZE: int = 100000
    ETL_DEFAULT_TIMESTAMP: str = '1900-01-01 00:00:00'
    ETL_CLEANUP_ON_FAILURE: bool = True
    TABLE_CONFIG_PATH: Path = Path('configs/tables.yaml')

    # --- Các thuộc tính được tính toán ---
    db: Optional[DatabaseSettings] = None
    TABLE_CONFIG: Dict[str, TableConfig] = {}

    @property
    def DUCKDB_PATH(self) -> Path:
        """ Đường dẫn đến file database DuckDB. """
        return self.DATA_DIR / 'analytics.duckdb'

    @property
    def STATE_FILE(self) -> Path:
        """ Đường dẫn đến file JSON lưu trạng thái ETL. """
        return self.DATA_DIR / 'etl_state.json'

    # --- Validators ---
    @model_validator(mode='after')
    def assemble_settings(self) -> 'EtlSettings':
        """
        Validator chạy sau khi các biến môi trường đã được load.
        Nó thực hiện hai việc:
        1. Tạo đối tượng `DatabaseSettings` từ các biến SQLSERVER_*.
        2. Tải và xác thực cấu hình bảng từ file `tables.yaml`.
        """
        # 1. Tạo đối tượng db
        if not self.db:
            self.db = DatabaseSettings(
                SQLSERVER_DRIVER=self.SQLSERVER_DRIVER,
                SQLSERVER_SERVER=self.SQLSERVER_SERVER,
                SQLSERVER_DATABASE=self.SQLSERVER_DATABASE,
                SQLSERVER_UID=self.SQLSERVER_UID,
                SQLSERVER_PWD=self.SQLSERVER_PWD
            )

        # 2. Tải cấu hình bảng
        if self.TABLE_CONFIG: return self

        try:
            with self.TABLE_CONFIG_PATH.open('r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                raise ValueError(f"File cấu hình '{self.TABLE_CONFIG_PATH}' rỗng.")

            # Dùng TypeAdapter để xác thực một dictionary phức tạp
            adapter = TypeAdapter(Dict[str, TableConfig])
            self.TABLE_CONFIG = adapter.validate_python(raw_config)
        except FileNotFoundError:
            raise ValueError(f"Lỗi: Không tìm thấy file cấu hình bảng tại: {self.TABLE_CONFIG_PATH}.")
        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(f"Lỗi cú pháp hoặc nội dung file cấu hình bảng '{self.TABLE_CONFIG_PATH}':\n{e}.")

        return self

# Khởi tạo một đối tượng settings duy nhất để sử dụng trong toàn bộ ứng dụng
etl_settings = EtlSettings()
