# app/etl/transform.py
# Tập trung vào việc làm sạch và biến đổi dữ liệu.
import pandas as pd
from typing import Optional
from app.core.config import TableConfig

def _rename_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Đổi tên các cột dựa trên rename_map trong config."""
    if config.rename_map:
        df = df.rename(columns=config.rename_map)
    return df

def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Áp dụng các bước làm sạch dữ liệu cụ thể."""
    # Các quy tắc làm sạch có thể được thêm vào đây trong tương lai
    if 'store_name' in df.columns:
        df['store_name'] = df['store_name'].astype(str).str.rstrip()
    return df

def _handle_timestamps_and_partitions(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chuyển đổi kiểu dữ liệu cho cột timestamp và tạo các cột phân vùng (partition).
    """
    # Lấy tên cột timestamp sau khi đã được đổi tên
    ts_col_renamed = config.rename_map.get(config.timestamp_col)

    if ts_col_renamed and ts_col_renamed in df.columns:
        # Chuyển đổi sang datetime, các giá trị lỗi sẽ trở thành NaT (Not a Time)
        df[ts_col_renamed] = pd.to_datetime(df[ts_col_renamed], errors='coerce')

        # Xóa các dòng có timestamp không hợp lệ
        df.dropna(subset=[ts_col_renamed], inplace=True)

        # Tạo các cột phân vùng nếu cần và dataframe không rỗng
        if not df.empty and config.partition_cols:
            df['year'] = df[ts_col_renamed].dt.year
            df['month'] = df[ts_col_renamed].dt.month

    return df

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Điều phối và áp dụng tuần tự các bước transform cho DataFrame.
    Đây là một pipeline biến đổi dữ liệu.
    """
    df_transformed = (df.pipe(_rename_columns, config)
                        .pipe(_clean_data)
                        .pipe(_handle_timestamps_and_partitions, config))

    return df_transformed

def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    """Lấy timestamp lớn nhất từ một chunk dữ liệu."""
    if not config.incremental:
        return None

    # Đơn giản hóa logic: .get() sẽ trả về None nếu key không tồn tại
    ts_col_renamed = config.rename_map.get(config.timestamp_col)

    if ts_col_renamed and ts_col_renamed in df.columns:
        # Đảm bảo cột là kiểu datetime trước khi lấy max
        if pd.api.types.is_datetime64_any_dtype(df[ts_col_renamed]):
            return df[ts_col_renamed].max()

    return None







# # app/etl/transform.py
# # Tập trung vào việc làm sạch và biến đổi dữ liệu.
# import pandas as pd

# from typing import Optional

# from app.core.config import TableConfig

# def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
#     """Áp dụng các bước transform cơ bản cho DataFrame."""
#     df_transformed = df.rename(columns=config.rename_map)

#     # Xử lý các trường hợp cụ thể
#     if 'store_name' in df_transformed.columns:
#         df_transformed['store_name'] = df_transformed['store_name'].astype(str).str.rstrip()

#     # Xử lý cột timestamp và tạo cột partition
#     ts_col_renamed = config.rename_map.get(config.timestamp_col) if config.timestamp_col else None

#     if ts_col_renamed and ts_col_renamed in df_transformed.columns:
#         df_transformed[ts_col_renamed] = pd.to_datetime(df_transformed[ts_col_renamed], errors='coerce')
#         df_transformed.dropna(subset=[ts_col_renamed], inplace=True)

#         if not df_transformed.empty and config.partition_cols:
#             df_transformed['year'] = df_transformed[ts_col_renamed].dt.year
#             df_transformed['month'] = df_transformed[ts_col_renamed].dt.month

#     return df_transformed

# def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
#     """Lấy timestamp lớn nhất từ một chunk dữ liệu."""
#     if not config.incremental:
#         return None

#     ts_col_renamed = config.rename_map.get(config.timestamp_col)
#     if ts_col_renamed and ts_col_renamed in df.columns:
#         return df[ts_col_renamed].max()

#     return None
