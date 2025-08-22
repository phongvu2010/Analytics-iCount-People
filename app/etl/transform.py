"""
Module xử lý giai đoạn 'T' (Transform) của pipeline ETL.

Nhận vào một DataFrame thô từ bước Extract và thực hiện một chuỗi các thao tác
biến đổi dữ liệu, bao gồm: điều chỉnh chênh lệch thời gian, đổi tên cột,
làm sạch chuỗi, xử lý kiểu dữ liệu, tạo cột partition, và cuối cùng là xác
thực với Pandera để đảm bảo chất lượng dữ liệu trước khi nạp.
"""
import logging
import pandas as pd
import pandera.errors as pa_errors

from typing import Optional

from .schemas import table_schemas
from ..core.config import settings, TableConfig

logger = logging.getLogger(__name__)

# --- Private Helper Functions ---

def _apply_time_offsets(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Áp dụng điều chỉnh chênh lệch thời gian cho cột timestamp.
    """
    if not config.timestamp_col:
        return df

    table_name_key = config.source_table.split('.')[-1]
    offsets_config = settings.TIME_OFFSETS.get(table_name_key)

    if not offsets_config:
        return df

    store_id_col = 'storeid'
    ts_col = config.timestamp_col

    if store_id_col not in df.columns or ts_col not in df.columns:
        logger.warning(f"Bỏ qua điều chỉnh time offset cho '{table_name_key}' do thiếu cột.")
        return df

    # Vector hóa việc điều chỉnh để tăng hiệu suất
    offsets = df[store_id_col].map(offsets_config).fillna(0)
    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce') - pd.to_timedelta(offsets, unit='m')

    logger.info(f"Đã áp dụng điều chỉnh chênh lệch thời gian cho '{table_name_key}'.")
    return df

def _rename_and_clean(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Đổi tên cột và áp dụng các quy tắc làm sạch cơ bản.
    """
    # Đổi tên cột
    if config.rename_map:
        df = df.rename(columns=config.rename_map)

    # Làm sạch dữ liệu
    for rule in config.cleaning_rules:
        col_to_clean = config.rename_map.get(rule.column, rule.column)
        if rule.action == 'strip' and col_to_clean in df.columns:
            if pd.api.types.is_object_dtype(df[col_to_clean]):
                df[col_to_clean] = df[col_to_clean].str.strip()
    return df

def _handle_data_types(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chuẩn hóa kiểu dữ liệu cho các cột số, timestamp và partition.
    """
    # Xử lý cột số
    numeric_cols = [
        config.rename_map.get('in_num'),
        config.rename_map.get('out_num')
    ]
    for col in filter(None, numeric_cols):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            df[col] = df[col].apply(lambda x: max(0, x))

    # Xử lý cột timestamp và tạo partition
    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')

        # THAY ĐỔI: Gán lại kết quả thay vì dùng inplace=True
        df = df.dropna(subset=[ts_col])

        if not df.empty:
            # Các thao tác thêm cột giờ đây an toàn hơn
            if 'year' in config.partition_cols:
                df['year'] = df[ts_col].dt.year
            if 'month' in config.partition_cols:
                df['month'] = df[ts_col].dt.month

    return df

def _select_and_validate(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Chọn các cột cuối cùng và xác thực bằng Pandera.
    """
    schema = table_schemas.get(config.dest_table)
    if not schema:
        logger.warning(f"Không tìm thấy schema cho '{config.dest_table}'. Bỏ qua xác thực.")
        return df

    # Chọn các cột có trong schema trước khi xác thực
    schema_cols = list(schema.to_schema().columns.keys())
    final_cols = [col for col in schema_cols if col in df.columns]
    df_subset = df[final_cols]

    # Xác thực
    try:
        # Trả về DataFrame đã được xác thực (có thể đã được ép kiểu)
        return schema.validate(df_subset, lazy=True)
    except pa_errors.SchemaErrors as err:
        logger.error(
            f"Xác thực dữ liệu cho '{config.dest_table}' thất bại!\n"
            f"{err.failure_cases.to_string()}"
        )
        # Lưu dữ liệu lỗi để phân tích sau (Dead-Letter Queue)
        rejected_path = settings.DATA_DIR / 'rejected' / config.dest_table
        rejected_path.mkdir(parents=True, exist_ok=True)
        timestamp_str = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        err.failure_cases.to_parquet(
            rejected_path / f'rejected_{timestamp_str}.parquet'
        )
        raise

# --- Public Orchestrator Function ---

def run_transformations(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Điều phối toàn bộ quy trình biến đổi dữ liệu trên một DataFrame.

    Sử dụng phương thức `.pipe()` của Pandas để chuỗi các hàm lại với nhau,
    giúp mã nguồn dễ đọc, dễ hiểu và dễ dàng thay đổi thứ tự hoặc thêm/bớt
    các bước biến đổi.

    Args:
        df: DataFrame đầu vào từ bước Extract.
        config: Cấu hình cho bảng đang được xử lý.

    Returns:
        DataFrame đã được biến đổi, làm sạch và xác thực.
    """
    if df.empty:
        return df

    return (
        df.pipe(_apply_time_offsets, config)
          .pipe(_rename_and_clean, config)
          .pipe(_handle_data_types, config)
          .pipe(_select_and_validate, config)
    )


def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    """
    Lấy giá trị timestamp lớn nhất từ một chunk đã biến đổi.

    Giá trị này sẽ được dùng để cập nhật "high-water mark" cho lần ETL tiếp theo.

    Args:
        df: DataFrame đã được biến đổi.
        config: Cấu hình của bảng.

    Returns:
        Timestamp lớn nhất hoặc None nếu không áp dụng.
    """
    if not config.incremental:
        return None

    ts_col = config.final_timestamp_col
    if ts_col and ts_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        max_ts = df[ts_col].max()
        return max_ts if pd.notna(max_ts) else None

    return None
