"""
Định nghĩa các Pydantic model để quản lý cấu hình của ứng dụng.

Module này sử dụng `pydantic-settings` để đọc và xác thực cấu hình từ
nhiều nguồn khác nhau (ví dụ: file .env, biến môi trường). Nó cung cấp một
đối tượng `settings` duy nhất, an toàn về kiểu dữ liệu, giúp việc truy cập
cấu hình trong toàn bộ ứng dụng trở nên nhất quán và dễ đoán.
"""
import yaml

from pathlib import Path
from pydantic import AnyUrl, BaseModel, BeforeValidator, Field, TypeAdapter, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated, Any, Dict, List, Literal, Optional
from urllib import parse


def parse_cors(v: Any) -> List[str] | str:
    """
    Hàm helper để chuyển đổi chuỗi CORS từ biến môi trường thành danh sách.

    Ví dụ, một chuỗi "http://localhost, http://127.0.0.1" sẽ được chuyển đổi
    thành `['http://localhost', 'http://127.0.0.1']`.

    Args:
        v: Giá trị từ biến môi trường, có thể là chuỗi hoặc danh sách.

    Returns:
        Danh sách các origin được phép hoặc giá trị gốc nếu đã là danh sách.

    Raises:
        ValueError: Nếu giá trị đầu vào không phải là chuỗi hoặc danh sách.
    """
    if isinstance(v, str) and not v.startswith('['):
        return [i.strip() for i in v.split(',')]
    if isinstance(v, (list, str)):
        return v
    raise ValueError(v)


class CleaningRule(BaseModel):
    """
    Định nghĩa một quy tắc làm sạch dữ liệu cho một cột cụ thể.

    Attributes:
        column: Tên cột gốc trong bảng nguồn.
        action: Hành động làm sạch (hiện tại chỉ hỗ trợ 'strip' để loại
                bỏ khoảng trắng thừa).
    """
    column: str
    action: Literal['strip']


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

        Phương thức này tự động mã hóa mật khẩu và định dạng driver để đảm bảo
        chuỗi kết nối hợp lệ, tránh các lỗi liên quan đến ký tự đặc biệt.

        Returns:
            Chuỗi kết nối tương thích với SQLAlchemy cho MS SQL Server
            sử dụng driver `pyodbc`.
        """
        # Mã hóa mật khẩu để xử lý các ký tự đặc biệt
        encoded_pwd = parse.quote_plus(self.SQLSERVER_PWD)
        # Thay thế khoảng trắng trong tên driver bằng dấu '+' cho URL
        driver_for_query = self.SQLSERVER_DRIVER.replace(' ', '+')

        return (
            f"mssql+pyodbc://{self.SQLSERVER_UID}:{encoded_pwd}"
            f"@{self.SQLSERVER_SERVER}/{self.SQLSERVER_DATABASE}"
            f"?driver={driver_for_query}"
        )


class TableConfig(BaseModel):
    """
    Cấu hình chi tiết cho việc xử lý ETL của một bảng.

    Mỗi instance của lớp này đại diện cho một pipeline ETL nhỏ cho một bảng
    duy nhất, từ trích xuất, biến đổi đến tải dữ liệu.
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
        Xác thực `timestamp_col` phải được cung cấp khi `incremental` là True.
        """
        if self.incremental and not self.timestamp_col:
            raise ValueError(
                f"Cấu hình cho '{self.source_table}': 'timestamp_col' là "
                f"bắt buộc khi 'incremental' là True."
            )
        return self

    @property
    def final_timestamp_col(self) -> Optional[str]:
        """
        Lấy tên cột timestamp cuối cùng (sau khi đã được đổi tên).

        Hàm này giúp pipeline ETL biết được tên cột timestamp trong DataFrame
        đã được biến đổi để thực hiện các thao tác liên quan đến incremental load.
        """
        if not self.timestamp_col:
            return None
        return self.rename_map.get(self.timestamp_col, self.timestamp_col)


class Settings(BaseSettings):
    """
    Model cấu hình chính, tổng hợp tất cả các thiết lập cho ứng dụng.

    Lớp này sử dụng `BaseSettings` để tự động đọc các biến từ file `.env`
    và môi trường, sau đó xác thực và gán chúng vào các thuộc tính đã định nghĩa.
    """
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'                      # Bỏ qua các biến môi trường không được định nghĩa
    )

    # --- Cấu hình chung của ứng dụng ---
    PROJECT_NAME: str = 'Analytics iCount People API'
    DESCRIPTION: str = 'API cung cấp dữ liệu phân tích lượt ra vào cửa hàng.'
    BACKEND_CORS_ORIGINS: Annotated[
        List[AnyUrl], BeforeValidator(parse_cors)
    ] = []

    # --- Cấu hình nghiệp vụ ---
    OUTLIER_THRESHOLD: int = 100            # Ngưỡng để xác định giá trị ngoại lai.
    OUTLIER_SCALE_RATIO: float = 0.00001    # Tỷ lệ để điều chỉnh giá trị ngoại lai.
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
    TIME_OFFSETS_PATH: Path = Path('configs/time_offsets.yaml')

    # --- Thuộc tính được tính toán và tải động ---
    db: Optional[DatabaseSettings] = None
    TABLE_CONFIG: Dict[str, TableConfig] = {}
    TIME_OFFSETS: Dict[str, Dict[int, int]] = {}

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
        """
        # 1. Tạo đối tượng `db` để nhóm các thông tin kết nối
        if not self.db:
            self.db = DatabaseSettings(
                SQLSERVER_DRIVER=self.SQLSERVER_DRIVER,
                SQLSERVER_SERVER=self.SQLSERVER_SERVER,
                SQLSERVER_DATABASE=self.SQLSERVER_DATABASE,
                SQLSERVER_UID=self.SQLSERVER_UID,
                SQLSERVER_PWD=self.SQLSERVER_PWD
            )

        # 2. Tải cấu hình bảng từ file YAML
        self._load_table_config()

        # 3. Tải cấu hình chênh lệch thời gian
        self._load_time_offsets()

        return self

    def _load_table_config(self):
        """
        Tải và xác thực cấu hình bảng từ file YAML.
        """
        if self.TABLE_CONFIG:
            return

        try:
            with self.TABLE_CONFIG_PATH.open('r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)

            if not raw_config:
                raise ValueError(
                    f"File cấu hình '{self.TABLE_CONFIG_PATH}' rỗng."
                )

            # Sử dụng TypeAdapter để xác thực toàn bộ cấu trúc dict
            adapter = TypeAdapter(Dict[str, TableConfig])
            self.TABLE_CONFIG = adapter.validate_python(raw_config)

        except FileNotFoundError:
            raise ValueError(
                f"Lỗi: Không tìm thấy file cấu hình tại: {self.TABLE_CONFIG_PATH}."
            )

        except (yaml.YAMLError, ValidationError) as e:
            raise ValueError(
                f"Lỗi cú pháp trong file '{self.TABLE_CONFIG_PATH}':\n{e}"
            )

    def _load_time_offsets(self):
        """
        Tải và xác thực cấu hình chênh lệch thời gian từ file YAML.
        """
        if self.TIME_OFFSETS:
            return

        try:
            with self.TIME_OFFSETS_PATH.open('r', encoding='utf-8') as f:
                raw_offsets = yaml.safe_load(f)

            if not raw_offsets:
                raise ValueError(
                    f"File cấu hình '{self.TIME_OFFSETS_PATH}' rỗng."
                )
            self.TIME_OFFSETS = raw_offsets

        except FileNotFoundError:
            raise ValueError(
                f"Lỗi: Không tìm thấy file tại: {self.TIME_OFFSETS_PATH}."
            )

        except yaml.YAMLError as e:
            raise ValueError(
                f"Lỗi cú pháp trong file '{self.TIME_OFFSETS_PATH}':\n{e}"
            )


# Khởi tạo một instance duy nhất để sử dụng trong toàn bộ ứng dụng.
settings = Settings()
