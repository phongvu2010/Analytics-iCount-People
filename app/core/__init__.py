from .config import Settings
from .data_handler import query_parquet_as_dataframe

# Tạo một instance của Settings để sử dụng trong toàn bộ ứng dụng
settings = Settings()   # type: ignore
