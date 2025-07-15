from pydantic import computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from urllib import parse

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file = '.env',
        case_sensitive = True,
        env_file_encoding = 'utf-8'
    )

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

# Tạo một instance của Settings để sử dụng trong toàn bộ ứng dụng
settings = Settings()   # type: ignore
