import logging
import pandas as pd

from typing import List, Optional

from app.core.config import TableConfig

logger = logging.getLogger(__name__)

def _rename_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    if config.rename_map:
        df = df.rename(columns=config.rename_map)
    return df

def _apply_strip(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()

# def _apply_lowercase(series: pd.Series) -> pd.Series:
#     return series.astype(str).str.lower()

CLEANING_ACTIONS = {
    'strip': _apply_strip,
    # 'lowercase': _apply_lowercase,
}

def _clean_data(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    if not config.cleaning_rules: return df

    for rule in config.cleaning_rules:
        col_to_clean = config.rename_map.get(rule.column, rule.column)
        if col_to_clean not in df.columns:
            logger.warning(f"Cột '{col_to_clean}' trong cleaning_rules không tồn tại. Bỏ qua.")
            continue

        action_func = CLEANING_ACTIONS.get(rule.action)
        if action_func and pd.api.types.is_object_dtype(df[col_to_clean]):
            df[col_to_clean] = action_func(df[col_to_clean])
        elif not action_func:
            logger.warning(f"Hành động làm sạch '{rule.action}' chưa được hỗ trợ.")
    return df

def _process_numeric_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Hàm helper để xử lý các cột dạng số.
    - Chuyển đổi các giá trị âm thành 0.
    - Chuyển đổi sang kiểu số nguyên, điền giá trị lỗi/trống bằng 0.
    """
    for col in columns:
        if col in df.columns:
            # Chỉ xử lý nếu cột tồn tại
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            neg_count = (df[col] < 0).sum()
            if neg_count > 0:
                df[col] = df[col].apply(lambda x: max(0, x) if pd.notna(x) else x)
                logger.info(f"Đã chuyển đổi {neg_count} giá trị âm thành 0 cho cột '{col}'.")
            
            # Điền NA/NaT và chuyển đổi sang kiểu Int64 để hỗ trợ giá trị null
            df[col] = df[col].fillna(0).astype(int)
    return df

def _handle_timestamps_and_partitions(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    ts_col = config.final_timestamp_col
    if not (ts_col and ts_col in df.columns):
        return df

    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
    
    num_nat = df[ts_col].isna().sum()
    if num_nat > 0:
        logger.warning(f"Đã tìm thấy {num_nat} giá trị timestamp không hợp lệ trong cột '{ts_col}'. Các hàng này sẽ bị loại bỏ.")
        df.dropna(subset=[ts_col], inplace=True)

    if not df.empty and config.partition_cols:
        if 'year' in config.partition_cols:
            df['year'] = df[ts_col].dt.year
        if 'month' in config.partition_cols:
            df['month'] = df[ts_col].dt.month
    return df

def _ensure_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Đảm bảo các cột có kiểu dữ liệu phù hợp trước khi ghi."""
    for col in df.columns:
        # Chuyển đổi cột object sang string, thay thế các chuỗi rỗng bằng None
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).replace({'None': None, 'NaT': None, 'nan': None})
        # Chuyển đổi các cột ID sang Int64 để cho phép giá trị null (an toàn hơn)
        elif 'id' in col or 'code' in col:
             df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    return df

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chạy toàn bộ quy trình biến đổi dữ liệu trên một DataFrame.
    Sử dụng phương thức .pipe() để chuỗi các hàm lại với nhau một cách rõ ràng.
    """
    # Lấy ra tên cột cuối cùng sau khi đã rename
    visitors_in_col = config.rename_map.get('in_num', 'in_num')
    visitors_out_col = config.rename_map.get('out_num', 'out_num')

    df_transformed = (
        df.pipe(_rename_columns, config)
          .pipe(_clean_data, config)
          .pipe(_process_numeric_columns, [visitors_in_col, visitors_out_col])
          .pipe(_handle_timestamps_and_partitions, config)
          .pipe(_ensure_data_types)
    )

    return df_transformed

def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    if not config.incremental: return None

    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        max_ts = df[ts_col].max()
        return max_ts if pd.notna(max_ts) else None

    return None
