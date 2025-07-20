from pydantic import AnyUrl, BeforeValidator, computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated, Any, List
from urllib import parse

def parse_cors(v: Any) -> List[str] | str:
    """Helper để phân tích chuỗi CORS từ biến môi trường thành danh sách."""
    if isinstance(v, str) and not v.startswith('['):
        return [i.strip() for i in v.split(',')]
    if isinstance(v, list | str):
        return v
    raise ValueError(v)

class Settings(BaseSettings):
    """Lớp quản lý tập trung cấu hình cho toàn bộ ứng dụng.

    Các thuộc tính được tự động đọc và xác thực từ tệp `.env`.
    """
    model_config = SettingsConfigDict(
        env_file = '.env',
        case_sensitive = True,
        env_file_encoding = 'utf-8'
    )

    # Cấu hình chung
    PROJECT_NAME: str
    DESCRIPTION: str

    BACKEND_CORS_ORIGINS: Annotated[
        List[AnyUrl], BeforeValidator(parse_cors)
    ] = []

    # Đường dẫn dữ liệu
    DATA_PATH: str = 'data'

    @property
    def CROWD_COUNTS_PATH(self) -> str:
        """Đường dẫn tới các tệp parquet chứa dữ liệu đếm người."""
        # Dấu `*` cho phép DuckDB tự động đọc tất cả các tệp trong thư mục con.
        return f'{self.DATA_PATH}/crowd_counts/*/*.parquet'

    @property
    def ERROR_LOGS_PATH(self) -> str:
        """Đường dẫn tới các tệp parquet chứa dữ liệu log lỗi."""
        return f'{self.DATA_PATH}/error_logs/*/*.parquet'

    # Cấu hình xử lý dữ liệu ngoại lệ (outlier)
    OUTLIER_THRESHOLD: int = 100
    OUTLIER_SCALE_RATIO: float = 0.00001

    # Định nghĩa "ngày làm việc" (có thể qua đêm)
    WORKING_HOUR_START: int = 9   # 09:00
    WORKING_HOUR_END: int = 2     # 02:00 sáng hôm sau

    # Cấu hình kết nối Database MSSQL (dành cho ETL)
    DB_HOST: str
    DB_PORT: int = 1433
    DB_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    DB_NAME: str
    DB_USER: str
    DB_PASS: str

    @computed_field
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """Tự động tạo chuỗi kết nối SQLAlchemy cho MSSQL.

        Sử dụng pyodbc làm driver và mã hóa password để đảm bảo an toàn.
        """
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.DB_USER,
            password = parse.quote_plus(self.DB_PASS),
            host = self.DB_HOST,
            port = self.DB_PORT,
            path = self.DB_NAME,
            query = f'driver={self.DB_DRIVER.replace(" ", "+")}'
        ))

# Tạo một instance Settings duy nhất để sử dụng trong toàn bộ ứng dụng.
settings = Settings()
