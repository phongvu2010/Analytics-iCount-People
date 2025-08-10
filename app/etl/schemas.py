"""
Module này định nghĩa các schema dữ liệu sử dụng Pandera.

Mỗi schema tương ứng với một bảng đích trong DuckDB, đảm bảo rằng dữ liệu
được nạp vào luôn tuân thủ đúng định dạng, kiểu dữ liệu và các ràng buộc logic.
"""
import pandera.pandas as pa

from pandera.typing import Series, DateTime, String, Int

# Định nghĩa các schema cho từng bảng đích (dest_table)
class DimStoresSchema(pa.DataFrameModel):
    """ Schema xác thực cho bảng dim_stores. """
    store_id: Series[Int] = pa.Field(unique=True, nullable=False)
    store_name: Series[String] = pa.Field(nullable=False)

    class Config:
        strict = True  # Đảm bảo không có cột nào thừa
        coerce = True  # Tự động ép kiểu dữ liệu nếu hợp lệ

class FactTrafficSchema(pa.DataFrameModel):
    """ Schema xác thực cho bảng fact_traffic. """
    recorded_at: Series[DateTime] = pa.Field(nullable=False)
    # ge=0: Ràng buộc giá trị phải lớn hơn hoặc bằng 0
    visitors_in: Series[Int] = pa.Field(ge=0, default=0)
    visitors_out: Series[Int] = pa.Field(ge=0, default=0)
    device_position: Series[String] = pa.Field(nullable=True)
    store_id: Series[Int] = pa.Field(nullable=False)
    year: Series[Int]
    month: Series[Int]

    class Config:
        strict = True
        coerce = True

class FactErrorsSchema(pa.DataFrameModel):
    """ Schema xác thực cho bảng fact_errors. """
    log_id: Series[Int] = pa.Field(unique=True, nullable=False)
    store_id: Series[Int] = pa.Field(nullable=False)
    device_code: Series[Int] = pa.Field(nullable=True)
    logged_at: Series[DateTime] = pa.Field(nullable=False)
    error_code: Series[Int] = pa.Field(nullable=True)
    error_message: Series[String] = pa.Field(nullable=True)
    year: Series[Int]
    month: Series[Int]

    class Config:
        strict = True
        coerce = True

# Dictionary để dễ dàng truy cập schema từ tên bảng
table_schemas = {
    "dim_stores": DimStoresSchema,
    "fact_traffic": FactTrafficSchema,
    "fact_errors": FactErrorsSchema,
}
