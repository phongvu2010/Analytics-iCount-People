from pydantic import AnyUrl, BeforeValidator, computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings
from typing import Annotated, Any
from urllib import parse

class Settings(BaseSettings):
    """
    Class để quản lý toàn bộ cấu hình của ứng dụng.
    Các thuộc tính được tự động đọc từ file .env.
    """
    # Cấu hình chung của ứng dụng
    PROJECT_NAME: str
    DESCRIPTION: str

    def parse_cors(v: Any) -> list[str] | str:
        """ Helper function to parse CORS origins from a string. """
        if isinstance(v, str) and not v.startswith('['):
            return [i.strip() for i in v.split(',')]
        elif isinstance(v, list | str):
            return v
        raise ValueError(v)

    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = []

    # Cấu hình cho Database MSSQL
    DB_HOST: str
    DB_PORT: int = 1433             # Cổng mặc định của MSSQL
    DB_DRIVER: str = 'SQL Server'   # 'ODBC Driver 17 for SQL Server'
    DB_NAME: str
    DB_USER: str
    DB_PASS: str

    @computed_field # type: ignore[misc]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """
        Tự động tạo chuỗi kết nối SQLAlchemy từ các biến môi trường.
        Sử dụng pyodbc làm driver kết nối với MSSQL.
        """
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.DB_USER,
            password = parse.quote_plus(self.DB_PASS),
            host = self.DB_HOST,
            port = self.DB_PORT,
            path = self.DB_NAME,
            query = f'driver={self.DB_DRIVER.replace(' ', '+')}'
        ))

    class Config:
        case_sensitive = True
        env_file = '.env'
        env_file_encoding = 'utf-8'
