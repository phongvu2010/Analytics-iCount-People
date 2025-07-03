from pydantic import computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings
from urllib import parse


class Settings(BaseSettings):
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    # --- Cấu hình cho MSSQL ---
    MSSQL_HOST: str
    MSSQL_PORT: int = 1433  # Cổng mặc định của MSSQL
    MSSQL_USER: str
    MSSQL_PASSWORD: str
    MSSQL_DB: str
    # MSSQL_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    MSSQL_DRIVER: str = 'SQL Server'

    @computed_field  # type: ignore[misc]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.MSSQL_USER,
            password = parse.quote_plus(self.MSSQL_PASSWORD),
            host = self.MSSQL_HOST,
            port = self.MSSQL_PORT,
            path = self.MSSQL_DB,
            query = f'driver={self.MSSQL_DRIVER}'
        ))

    class Config:
        env_file = '.env'

settings = Settings()
