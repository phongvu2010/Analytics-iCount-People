from pydantic import AnyUrl, BeforeValidator, computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings
from typing import Annotated, Any
from urllib import parse


def parse_cors(v: Any) -> list[str] | str:
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)

class Settings(BaseSettings):
    API_VERSION: str = "/api/v1"
    PROJECT_NAME: str
    DESCRIPTION: str

    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = []

    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    # --- Cấu hình cho MSSQL ---
    MSSQL_HOST: str
    MSSQL_PORT: int = 1433  # Cổng mặc định của MSSQL
    MSSQL_USER: str
    MSSQL_PASS: str
    MSSQL_DB: str
    # MSSQL_DRIVER: str = "ODBC Driver 17 for SQL Server"
    MSSQL_DRIVER: str = "SQL Server"

    @computed_field  # type: ignore[misc]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return str(MultiHostUrl.build(
            scheme = "mssql+pyodbc",
            username = self.MSSQL_USER,
            password = parse.quote_plus(self.MSSQL_PASS),
            host = self.MSSQL_HOST,
            port = self.MSSQL_PORT,
            path = self.MSSQL_DB,
            query = f"driver={self.MSSQL_DRIVER}"
        ))

    class Config:
        env_file = "../.env"

settings = Settings()  # type: ignore
