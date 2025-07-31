# app/etl/transform.py
# Tập trung vào việc làm sạch và biến đổi dữ liệu.
import pandas as pd

from typing import Optional

from app.core.config import TableConfig

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """Áp dụng các bước transform cơ bản cho DataFrame."""
    df_transformed = df.rename(columns=config.rename_map)

    # Xử lý các trường hợp cụ thể
    if 'store_name' in df_transformed.columns:
        df_transformed['store_name'] = df_transformed['store_name'].astype(str).str.rstrip()

    # Xử lý cột timestamp và tạo cột partition
    ts_col_renamed = config.rename_map.get(config.timestamp_col) if config.timestamp_col else None

    if ts_col_renamed and ts_col_renamed in df_transformed.columns:
        df_transformed[ts_col_renamed] = pd.to_datetime(df_transformed[ts_col_renamed], errors='coerce')
        df_transformed.dropna(subset=[ts_col_renamed], inplace=True)

        if not df_transformed.empty and config.partition_cols:
            df_transformed['year'] = df_transformed[ts_col_renamed].dt.year
            df_transformed['month'] = df_transformed[ts_col_renamed].dt.month

    return df_transformed

def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    """Lấy timestamp lớn nhất từ một chunk dữ liệu."""
    if not config.incremental:
        return None

    ts_col_renamed = config.rename_map.get(config.timestamp_col)
    if ts_col_renamed and ts_col_renamed in df.columns:
        return df[ts_col_renamed].max()

    return None
