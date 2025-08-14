import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_duckdb_connection():
    """
    Tạo và trả về một kết nối DuckDB in-memory.

    Sử dụng kết nối ':memory:' để đạt hiệu năng cao nhất cho các tác vụ đọc.
    Mỗi lời gọi sẽ tạo một kết nối mới để đảm bảo thread-safety khi
    chạy các truy vấn song song.
    """
    return duckdb.connect(database=':memory:', read_only=False)

def query_parquet_as_dataframe(query: str, params: list = None) -> pd.DataFrame:
    """
    Thực thi một câu lệnh SQL trên các tệp Parquet bằng DuckDB.

    Hàm này mở một kết nối DuckDB, thực thi truy vấn, và đóng kết nối
    để giải phóng tài nguyên.

    Args:
        query: Câu lệnh SQL để thực thi.
        params: Danh sách các tham số để truyền vào câu lệnh SQL một cách an toàn.

    Returns:
        Một DataFrame chứa kết quả, hoặc DataFrame rỗng nếu có lỗi.
    """
    con = get_duckdb_connection()
    try:
        # Thực thi câu lệnh và trả về kết quả dưới dạng Pandas DataFrame
        return con.execute(query, parameters=params).df()

    except Exception as e:
        logger.error(f"Lỗi khi thực thi query với DuckDB: {e}\nQuery: {query}\nParams: {params}")
        return pd.DataFrame()

    finally:
        # Đảm bảo kết nối luôn được đóng sau khi sử dụng.
        con.close()
