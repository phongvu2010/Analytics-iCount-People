import logging
import pandas as pd

from typing import Optional

from app.core.config import TableConfig, CleaningRule

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
            logger.warning(f"Cột '{col_to_clean}' trong cleaning_rules không tồn tại trong DataFrame sau khi rename. Bỏ qua quy tắc này.")
            continue

        action_func = CLEANING_ACTIONS.get(rule.action)
        if action_func:
            try:
                if pd.api.types.is_object_dtype(df[col_to_clean]):
                    df[col_to_clean] = action_func(df[col_to_clean])
                else:
                    logger.warning(f"Bỏ qua quy tắc '{rule.action}' cho cột '{col_to_clean}' vì nó không phải kiểu chuỗi.")
            except Exception as e:
                logger.error(f"Lỗi khi áp dụng quy tắc '{rule.action}' cho cột '{col_to_clean}': {e}")
        else:
            logger.warning(f"Hành động làm sạch '{rule.action}' chưa được hỗ trợ. Bỏ qua.")

    return df

def _handle_numeric_data(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    in_num_col = config.rename_map.get('in_num', 'in_num')
    out_num_col = config.rename_map.get('out_num', 'out_num')

    if in_num_col in df.columns:
        if pd.api.types.is_numeric_dtype(df[in_num_col]):
            original_neg_count = (df[in_num_col] < 0).sum()
            if original_neg_count > 0:
                df[in_num_col] = df[in_num_col].apply(lambda x: max(0, x))
                logger.info(f"Đã chuyển đổi {original_neg_count} giá trị âm thành 0 cho cột '{in_num_col}'.")
            df[in_num_col] = pd.to_numeric(df[in_num_col], errors='coerce').fillna(0).astype(int)
        else:
            logger.warning(f"Cột '{in_num_col}' không phải kiểu số. Bỏ qua xử lý số âm và chuyển đổi kiểu.")
            df[in_num_col] = pd.to_numeric(df[in_num_col], errors='coerce').fillna(0).astype(int)

    if out_num_col in df.columns:
        if pd.api.types.is_numeric_dtype(df[out_num_col]):
            original_neg_count = (df[out_num_col] < 0).sum()
            if original_neg_count > 0:
                df[out_num_col] = df[out_num_col].apply(lambda x: max(0, x))
                logger.info(f"Đã chuyển đổi {original_neg_count} giá trị âm thành 0 cho cột '{out_num_col}'.")
            df[out_num_col] = pd.to_numeric(df[out_num_col], errors='coerce').fillna(0).astype(int)
        else:
            logger.warning(f"Cột '{out_num_col}' không phải kiểu số. Bỏ qua xử lý số âm và chuyển đổi kiểu.")
            df[out_num_col] = pd.to_numeric(df[out_num_col], errors='coerce').fillna(0).astype(int)
    return df

def _handle_timestamps_and_partitions(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')

        num_nat = df[ts_col].isna().sum()
        if num_nat > 0:
            logger.warning(f"Đã chuyển đổi {num_nat} giá trị không hợp lệ thành NaT trong cột '{ts_col}'.")

        original_rows = len(df)
        df.dropna(subset=[ts_col], inplace=True)
        if len(df) < original_rows:
            logger.info(f"Đã loại bỏ {original_rows - len(df)} hàng do giá trị NaT trong cột '{ts_col}'.")

        if not df.empty and config.partition_cols:
            if 'year' in config.partition_cols and 'year' not in df.columns:
                df['year'] = df[ts_col].dt.year
            if 'month' in config.partition_cols and 'month' not in df.columns:
                df['month'] = df[ts_col].dt.month

            if 'year' in df.columns:
                df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(-1).astype(int)
            if 'month' in df.columns:
                df['month'] = pd.to_numeric(df['month'], errors='coerce').fillna(-1).astype(int)
    return df

def _ensure_data_types(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    id_cols = {
        'storeid': 'store_id',
        'ID': 'log_id',
        'DeviceCode': 'device_code',
        'Errorcode': 'error_code'
    }

    for source_col, dest_col in id_cols.items():
        final_col_name = config.rename_map.get(source_col, dest_col)
        if final_col_name in df.columns:
            df[final_col_name] = pd.to_numeric(df[final_col_name], errors='coerce').astype('Int64')

    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).replace({'None': None, 'NaT': None, 'nan': None})

    return df

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    df_transformed = (df.pipe(_rename_columns, config)
                        .pipe(_clean_data, config)
                        .pipe(_handle_numeric_data, config)
                        .pipe(_handle_timestamps_and_partitions, config)
                        .pipe(_ensure_data_types, config))

    return df_transformed

def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    if not config.incremental: return None

    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            return df[ts_col].max()

    return None
