# app/etl/transform.py
# Tập trung vào việc làm sạch và biến đổi dữ liệu.
import logging
import pandas as pd

from typing import Optional

from app.core.config import TableConfig

logger = logging.getLogger(__name__)

def _rename_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Đổi tên các cột dựa trên rename_map trong config."""
    if config.rename_map:
        df = df.rename(columns=config.rename_map)
    return df

def _clean_data(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Applies cleaning rules defined in the configuration to the DataFrame.
    """
    if not config.cleaning_rules:
        return df

    for rule in config.cleaning_rules:
        if rule.column not in df.columns:
            logger.warning(f"Cột '{rule.column}' trong cleaning_rules không tồn tại. Bỏ qua.")
            continue

        try:
            if rule.action == 'rstrip':
                # Đảm bảo cột là kiểu string trước khi áp dụng hàm rstrip
                df[rule.column] = df[rule.column].astype(str).str.rstrip()
            # Bạn có thể dễ dàng thêm các hành động khác ở đây trong tương lai
            # elif rule.action == 'lowercase':
            #     df[rule.column] = df[rule.column].astype(str).str.lower()
            else:
                logger.warning(f"Hành động làm sạch '{rule.action}' chưa được hỗ trợ. Bỏ qua.")
        except Exception as e:
            logger.error(f"Lỗi khi áp dụng quy tắc làm sạch cho cột '{rule.column}': {e}")

    return df

def _handle_timestamps_and_partitions(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chuyển đổi kiểu dữ liệu cho cột timestamp và tạo các cột phân vùng (partition).
    """
    # # Lấy tên cột timestamp sau khi đã được đổi tên
    # ts_col_renamed = config.rename_map.get(config.timestamp_col)

    # <-- TỐI ƯU: Sử dụng thuộc tính đã được tạo sẵn từ config
    ts_col = config.final_timestamp_col

    if ts_col and ts_col in df.columns:
        # Chuyển đổi sang datetime, các giá trị lỗi sẽ trở thành NaT (Not a Time)
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')

        # Xóa các dòng có timestamp không hợp lệ
        df.dropna(subset=[ts_col], inplace=True)

        # Tạo các cột phân vùng nếu cần và dataframe không rỗng
        if not df.empty and config.partition_cols:
            df['year'] = df[ts_col].dt.year
            df['month'] = df[ts_col].dt.month

    return df

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Điều phối và áp dụng tuần tự các bước transform cho DataFrame.
    Đây là một pipeline biến đổi dữ liệu.
    """
    df_transformed = (df.pipe(_rename_columns, config)
                        .pipe(_clean_data, config)  # Truyền config vào hàm _clean_data
                        .pipe(_handle_timestamps_and_partitions, config))

    return df_transformed

def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    """Lấy timestamp lớn nhất từ một chunk dữ liệu."""
    if not config.incremental:
        return None

    # # Đơn giản hóa logic: .get() sẽ trả về None nếu key không tồn tại
    # ts_col = config.rename_map.get(config.timestamp_col)

    # <-- TỐI ƯU: Sử dụng thuộc tính đã được tạo sẵn từ config, loại bỏ logic lặp lại
    ts_col = config.final_timestamp_col

    if ts_col and ts_col in df.columns:
        # Đảm bảo cột là kiểu datetime trước khi lấy max
        if pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            return df[ts_col].max()

    return None







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
