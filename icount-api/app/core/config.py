from pydantic import AnyUrl, BeforeValidator, computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings
from typing import Annotated, Any
from urllib import parse

class Settings(BaseSettings):
    API_VERSION: str = '/api/v1'
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

    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    # --- Cấu hình cho DB ---
    DATA_HOST: str
    DATA_PORT: int = 1433  # Cổng mặc định của MSSQL
    DATA_USER: str
    DATA_PASS: str
    DATA_DB: str
    DATA_DRIVER: str = 'SQL Server'

    @computed_field  # type: ignore[misc]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.DATA_USER,
            password = parse.quote_plus(self.DATA_PASS),
            host = self.DATA_HOST,
            port = self.DATA_PORT,
            path = self.DATA_DB,
            query = f'driver={self.DATA_DRIVER}'
        ))

    class Config:
        env_file = '.env'

settings = Settings()  # type: ignore
