"""
Định nghĩa các Pydantic model cho việc quản lý cấu hình của ứng dụng.

Module này sử dụng `pydantic-settings` để đọc và xác thực cấu hình từ nhiều nguồn
khác nhau (ví dụ: file .env, biến môi trường), cung cấp một đối tượng `settings`
duy nhất, an toàn về kiểu dữ liệu cho toàn bộ ứng dụng.
"""
import yaml

from pathlib import Path
from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    Field,
    model_validator,
    TypeAdapter,
    ValidationError,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated, Any, Dict, List, Literal, Optional
from urllib import parse


def parse_cors(v: Any) -> List[str] | str:
    """
    Hàm helper để chuyển đổi chuỗi CORS từ biến môi trường thành danh sách.

    Args:
        v: Giá trị từ biến môi trường, có thể là chuỗi hoặc danh sách.

    Returns:
        Danh sách các origin được cho phép.
    """
    if isinstance(v, str) and not v.startswith('['):
        return [i.strip() for i in v.split(',')]
    if isinstance(v, (list, str)):
        return v
    raise ValueError(v)


class CleaningRule(BaseModel):
    """
    Định nghĩa một quy tắc làm sạch dữ liệu cho một cột cụ thể.
    """
    column: str                 # Tên cột gốc trong bảng nguồn.
    action: Literal['strip']    # Hành động làm sạch (hiện tại chỉ hỗ trợ 'strip').


class DatabaseSettings(BaseModel):
    """
    Cấu hình kết nối đến cơ sở dữ liệu MS SQL Server.
    """
    SQLSERVER_DRIVER: str
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    @property
    def sqlalchemy_db_uri(self) -> str:
        """
        Tạo chuỗi kết nối SQLAlchemy an toàn từ các biến cấu hình.

        Returns:
            Chuỗi kết nối tương thích với SQLAlchemy cho MS SQL Server qua pyodbc.
        """
        encoded_pwd = parse.quote_plus(self.SQLSERVER_PWD)
        driver_for_query = self.SQLSERVER_DRIVER.replace(' ', '+')

        return (
            f'mssql+pyodbc://{self.SQLSERVER_UID}:{encoded_pwd}@'
            f'{self.SQLSERVER_SERVER}/{self.SQLSERVER_DATABASE}?'
            f'driver={driver_for_query}'
        )


class TableConfig(BaseModel):
    """
    Cấu hình chi tiết cho việc xử lý ETL của một bảng.
    """
    source_table: str                       # Tên bảng nguồn trong MS SQL.
    dest_table: str                         # Tên bảng đích trong DuckDB (sau khi biến đổi).
    incremental: bool = True                # True: chạy incremental load, False: full-load.
    description: Optional[str] = None       # Mô tả mục đích và vai trò của bảng.
    processing_order: int = 99              # Thứ tự xử lý, số nhỏ hơn chạy trước.
    rename_map: Dict[str, str] = {}         # Ánh xạ đổi tên cột từ nguồn sang đích.
    partition_cols: List[str] = []          # Danh sách cột dùng để phân vùng (partition).
    cleaning_rules: List[CleaningRule] = Field(default_factory=list)
    timestamp_col: Optional[str] = None     # Tên cột timestamp cho incremental load.

    @model_validator(mode='after')
    def validate_incremental_config(self) -> 'TableConfig':
        """
        Xác thực rằng `timestamp_col` phải được cung cấp khi `incremental` là True.
        """
        if self.incremental and not self.timestamp_col:
            raise ValueError(
                f"Cấu hình cho '{self.source_table}': 'timestamp_col' là bắt buộc "
                f"khi 'incremental' là True."
            )
        return self

    @property
    def final_timestamp_col(self) -> Optional[str]:
        """
        Lấy tên cột timestamp cuối cùng (sau khi đã được đổi tên).
        """
        if not self.timestamp_col:
            return None
        return self.rename_map.get(self.timestamp_col, self.timestamp_col)


class Settings(BaseSettings):
    """
    Model cấu hình chính, tổng hợp tất cả các thiết lập cho ứng dụng.
    """
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'                      # Bỏ qua các biến môi trường không được định nghĩa.
    )

    # --- Cấu hình chung ---
    PROJECT_NAME: str = 'Analytics iCount People API'
    DESCRIPTION: str = 'API cung cấp dữ liệu phân tích lượt ra vào cửa hàng.'
    BACKEND_CORS_ORIGINS: Annotated[
        List[AnyUrl], BeforeValidator(parse_cors)
    ] = []

    # --- Cấu hình nghiệp vụ ---
    OUTLIER_THRESHOLD: int = 100            # Ngưỡng để xác định giá trị ngoại lệ.
    OUTLIER_SCALE_RATIO: float = 0.00001    # Tỷ lệ để điều chỉnh giá trị ngoại lệ.
    WORKING_HOUR_START: int = 9             # Giờ bắt đầu ngày làm việc (09:00).
    WORKING_HOUR_END: int = 2               # Giờ kết thúc ngày làm việc (02:00 sáng hôm sau).

    # --- Cấu hình kết nối Database (đọc từ .env) ---
    SQLSERVER_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    SQLSERVER_SERVER: str
    SQLSERVER_DATABASE: str
    SQLSERVER_UID: str
    SQLSERVER_PWD: str

    # --- Cấu hình ETL ---
    DATA_DIR: Path = Path('data')
    ETL_CHUNK_SIZE: int = 100000            # Số dòng xử lý mỗi lần đọc từ SQL Server.
    ETL_DEFAULT_TIMESTAMP: str = '1900-01-01 00:00:00'
    ETL_CLEANUP_ON_FAILURE: bool = True     # Xóa file tạm nếu ETL thất bại.
    TABLE_CONFIG_PATH: Path = Path('configs/tables.yaml')

    # --- Thuộc tính được tính toán và tải động ---
    db: Optional[DatabaseSettings] = None
    TABLE_CONFIG: Dict[str, TableConfig] = {}

    @property
    def DUCKDB_PATH(self) -> Path:
        """
        Đường dẫn đầy đủ đến tệp cơ sở dữ liệu DuckDB.
        """
        return self.DATA_DIR / 'analytics.duckdb'

    @property
    def STATE_FILE(self) -> Path:
        """
        Đường dẫn đầy đủ đến tệp JSON lưu trạng thái ETL.
        """
        return self.DATA_DIR / 'etl_state.json'

    @model_validator(mode='after')
    def assemble_settings(self) -> 'Settings':
        """
        Validator để tự động tạo các đối tượng cấu hình phụ sau khi load .env.
        - Tạo đối tượng `DatabaseSettings`.
        - Tải và xác thực cấu hình bảng từ file YAML.
        """
        # 1. Tạo đối tượng `db`
        if not self.db:
            self.db = DatabaseSettings(
                SQLSERVER_DRIVER=self.SQLSERVER_DRIVER,
                SQLSERVER_SERVER=self.SQLSERVER_SERVER,
                SQLSERVER_DATABASE=self.SQLSERVER_DATABASE,
                SQLSERVER_UID=self.SQLSERVER_UID,
                SQLSERVER_PWD=self.SQLSERVER_PWD
            )

        # 2. Tải cấu hình bảng từ file YAML
        if self.TABLE_CONFIG:
            return self

        try:
            with self.TABLE_CONFIG_PATH.open('r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                raise ValueError(f"File cấu hình '{self.TABLE_CONFIG_PATH}' rỗng.")

            adapter = TypeAdapter(Dict[str, TableConfig])
            self.TABLE_CONFIG = adapter.validate_python(raw_config)

        except FileNotFoundError:
            raise ValueError(
                f"Lỗi: Không tìm thấy file cấu hình bảng tại: "
                f"{self.TABLE_CONFIG_PATH}."
            )

        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(
                f"Lỗi cú pháp hoặc nội dung file cấu hình bảng "
                f"'{self.TABLE_CONFIG_PATH}':\n{e}."
            )

        return self


# Khởi tạo một đối tượng settings duy nhất để sử dụng trong toàn bộ ứng dụng.
settings = Settings()
