"""
Module này xử lý giai đoạn 'T' (Transform) của pipeline ETL.

Nó nhận vào một DataFrame thô từ bước Extract và thực hiện một chuỗi
các thao tác biến đổi dữ liệu như: đổi tên cột, làm sạch dữ liệu,
xử lý kiểu dữ liệu, và cuối cùng là xác thực dữ liệu với Pandera
để đảm bảo chất lượng trước khi tải.
"""
import logging
import pandas as pd
import pandera.pandas as pa

from pathlib import Path
from typing import List, Optional

from .schemas import table_schemas
from ..core.config import TableConfig, etl_settings

logger = logging.getLogger(__name__)

def _select_final_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chọn và sắp xếp các cột cuối cùng của DataFrame để khớp với schema.
    Điều này đảm bảo không có cột thừa nào được đưa vào bước xác thực.
    """
    schema = table_schemas.get(config.dest_table)
    if not schema: return df

    # Lấy danh sách các cột được định nghĩa trong schema
    schema_columns = list(schema.to_schema().columns.keys())

    # Giữ lại các cột tồn tại trong cả DataFrame và schema
    final_columns = [col for col in schema_columns if col in df.columns]

    # Trả về DataFrame chỉ với các cột đã được chọn lọc
    return df[final_columns]

def _save_rejected_data(df: pd.DataFrame, config: TableConfig):
    """ Lưu các hàng dữ liệu bị từ chối vào một file Parquet riêng. """
    rejected_path = etl_settings.DATA_DIR / 'rejected' / config.dest_table
    rejected_path.mkdir(parents=True, exist_ok=True)

    # Tạo tên file duy nhất dựa trên timestamp hiện tại
    timestamp_str = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')
    file_path = rejected_path / f"rejected_{timestamp_str}.parquet"

    try:
        df.to_parquet(file_path, index=False)
        logger.warning(f"Đã lưu {len(df)} hàng dữ liệu không hợp lệ vào: {file_path}")
    except Exception as e:
        logger.error(f"Không thể lưu dữ liệu không hợp lệ: {e}")

def _validate_with_pandera(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Xác thực DataFrame với schema Pandera tương ứng. """
    schema = table_schemas.get(config.dest_table)
    if not schema:
        logger.warning(f"Không tìm thấy schema Pandera cho bảng '{config.dest_table}'. Bỏ qua xác thực.")
        return df

    try:
        logger.debug(f"Bắt đầu xác thực schema cho bảng '{config.dest_table}'.")
        # lazy=True để thu thập tất cả lỗi thay vì dừng lại ở lỗi đầu tiên
        validated_df = schema.validate(df, lazy=True)
        logger.debug(f"Xác thực schema cho '{config.dest_table}' thành công.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        logger.error(f"Xác thực dữ liệu cho bảng '{config.dest_table}' thất bại!")
        logger.error(f"Chi tiết lỗi schema:\n{err.failure_cases.to_string()}")

        # Triển khai "Dead-Letter Queue"
        # Lưu các hàng lỗi vào một file riêng để phân tích sau.
        if not err.failure_cases.empty:
            _save_rejected_data(err.failure_cases, config)

        # Ném lại lỗi để quy trình ETL cho bảng này dừng lại và được ghi nhận là FAILED
        raise

def _rename_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Đổi tên các cột dựa trên `rename_map` trong cấu hình. """
    if config.rename_map:
        df = df.rename(columns=config.rename_map)
    return df

def _apply_strip(series: pd.Series) -> pd.Series:
    """ Loại bỏ khoảng trắng thừa ở đầu và cuối chuỗi. """
    return series.astype(str).str.strip()

# def _apply_lowercase(series: pd.Series) -> pd.Series:
#     return series.astype(str).str.lower()

CLEANING_ACTIONS = {
    "strip": _apply_strip,
    # "lowercase": _apply_lowercase,
}

def _clean_data(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Áp dụng các quy tắc làm sạch dữ liệu từ cấu hình. """
    if not config.cleaning_rules: return df

    for rule in config.cleaning_rules:
        col_to_clean = config.rename_map.get(rule.column, rule.column)
        if col_to_clean not in df.columns: continue

        action_func = CLEANING_ACTIONS.get(rule.action)
        if action_func and pd.api.types.is_object_dtype(df[col_to_clean]):
            df[col_to_clean] = action_func(df[col_to_clean])

    return df

def _process_numeric_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """ Chuyển đổi các cột số, xử lý giá trị âm và giá trị rỗng. """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            neg_count = (df[col] < 0).sum()
            if neg_count > 0:
                df[col] = df[col].apply(lambda x: max(0, x) if pd.notna(x) else x)
            df[col] = df[col].fillna(0).astype(int)

    return df

def _handle_timestamps_and_partitions(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Chuyển đổi cột timestamp và tạo các cột partition (năm, tháng). """
    ts_col = config.final_timestamp_col
    if not (ts_col and ts_col in df.columns): return df

    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
    num_nat = df[ts_col].isna().sum()
    if num_nat > 0:
        logger.warning(f"Tìm thấy {num_nat} timestamp không hợp lệ trong '{ts_col}', các hàng này sẽ bị loại bỏ.")
        df.dropna(subset=[ts_col], inplace=True)

    if not df.empty and config.partition_cols:
        if 'year' in config.partition_cols: df['year'] = df[ts_col].dt.year
        if 'month' in config.partition_cols: df['month'] = df[ts_col].dt.month

    return df

def _ensure_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """ Đảm bảo các cột có kiểu dữ liệu phù hợp trước khi xác thực và tải. """
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).replace({'None': None, 'NaT': None, 'nan': None})
        elif 'id' in col or 'code' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chạy toàn bộ quy trình biến đổi dữ liệu trên một DataFrame.
    Sử dụng phương thức .pipe() của Pandas để chuỗi các hàm lại với nhau,
    giúp code dễ đọc và bảo trì.
    """
    visitors_in_col = config.rename_map.get('in_num', 'in_num')
    visitors_out_col = config.rename_map.get('out_num', 'out_num')

    df_transformed = (
        df.pipe(_rename_columns, config)
          .pipe(_clean_data, config)
          .pipe(_process_numeric_columns, [visitors_in_col, visitors_out_col])
          .pipe(_handle_timestamps_and_partitions, config)
          .pipe(_ensure_data_types)
          .pipe(_select_final_columns, config)
          .pipe(_validate_with_pandera, config)
    )
    return df_transformed

def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    """ Lấy giá trị timestamp lớn nhất từ một chunk để cập nhật high-water mark. """
    if not config.incremental: return None

    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        max_ts = df[ts_col].max()
        return max_ts if pd.notna(max_ts) else None

    return None
