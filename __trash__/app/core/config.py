from pydantic import computed_field
from pydantic_core import MultiHostUrl


from urllib import parse

class Settings(BaseSettings):
    """Lớp quản lý tập trung cấu hình cho toàn bộ ứng dụng.

    Các thuộc tính được tự động đọc và xác thực từ tệp `.env`.
    """

    # Cấu hình kết nối Database MSSQL (dành cho ETL)
    MSSQL_DB_HOST: str
    MSSQL_DB_PORT: int = 1433
    MSSQL_DB_DRIVER: str = 'ODBC Driver 17 for SQL Server'
    MSSQL_DB_NAME: str
    MSSQL_DB_USER: str
    MSSQL_DB_PASS: str

    @computed_field
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """Tự động tạo chuỗi kết nối SQLAlchemy cho MSSQL.

        Sử dụng pyodbc làm driver và mã hóa password để đảm bảo an toàn.
        """
        return str(MultiHostUrl.build(
            scheme = 'mssql+pyodbc',
            username = self.MSSQL_DB_USER,
            password = parse.quote_plus(self.MSSQL_DB_PASS),
            host = self.MSSQL_DB_HOST,
            port = self.MSSQL_DB_PORT,
            path = self.MSSQL_DB_NAME,
            query = f'driver={self.MSSQL_DB_DRIVER.replace(" ", "+")}'
        ))

# Tạo một instance Settings duy nhất để sử dụng trong toàn bộ ứng dụng.
settings = Settings()
