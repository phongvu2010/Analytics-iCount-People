from pydantic import AnyUrl, BeforeValidator, computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings
from typing import Annotated, Any
from urllib import parse

class Settings(BaseSettings):
    PROJECT_NAME: str
    DESCRIPTION: str

    def parse_cors(v: Any) -> list[str] | str:
        if isinstance(v, str) and not v.startswith('['):
            return [i.strip() for i in v.split(',')]
        elif isinstance(v, list | str):
            return v

        raise ValueError(v)

    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = []

    # SECRET_KEY: str
    # ALGORITHM: str
    # ACCESS_TOKEN_EXPIRE_MINUTES: int

    # --- Cấu hình cho DB ---
    DB_HOST: str
    DB_PORT: int = 1433  # Cổng mặc định của MSSQL
    DB_DRIVER: str = 'SQL Server'
    DB_NAME: str
    DB_USER: str
    DB_PASS: str

    @computed_field  # type: ignore[misc]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.DB_USER,
            password = parse.quote_plus(self.DB_PASS),
            host = self.DB_HOST,
            port = self.DB_PORT,
            path = self.DB_NAME,
            query = f'driver={self.DB_DRIVER}'
        ))

    class Config:
        case_sensitive = True
        env_file = '.env'
        env_file_encoding = 'utf-8'

settings = Settings()  # type: ignore
