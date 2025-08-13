from pydantic import computed_field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings

from urllib import parse

class Settings(BaseSettings):
    """Lớp quản lý tập trung cấu hình cho toàn bộ ứng dụng.

    Các thuộc tính được tự động đọc và xác thực từ tệp `.env`.
    """
    # Đường dẫn dữ liệu
    DATA_PATH: str = 'data'

    # @property
    # def CROWD_COUNTS_PATH(self) -> str:
    #     """ Đường dẫn tới các tệp parquet chứa dữ liệu đếm người. """
    #     # Dấu `*` cho phép DuckDB tự động đọc tất cả các tệp trong thư mục con.
    #     return f'{self.DATA_PATH}/crowd_counts/*/*.parquet'

    # @property
    # def ERROR_LOGS_PATH(self) -> str:
    #     """ Đường dẫn tới các tệp parquet chứa dữ liệu log lỗi. """
    #     return f'{self.DATA_PATH}/error_logs/*/*.parquet'

    # Cấu hình xử lý dữ liệu ngoại lệ (outlier)
    OUTLIER_THRESHOLD: int = 100
    OUTLIER_SCALE_RATIO: float = 0.00001

    # Định nghĩa "ngày làm việc" (có thể qua đêm)
    WORKING_HOUR_START: int = 9   # 09:00
    WORKING_HOUR_END: int = 2     # 02:00 sáng hôm sau

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
