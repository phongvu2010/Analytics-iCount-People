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

from typing import List, Optional

from .schemas import table_schemas
from ..core.config import settings, TableConfig

logger = logging.getLogger(__name__)


def _apply_time_offsets(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Áp dụng điều chỉnh chênh lệch thời gian cho cột timestamp một cách hiệu quả.

    Hàm này sử dụng phương pháp vector hóa của Pandas để tăng hiệu suất:
    1. Lấy map chênh lệch thời gian cho bảng hiện tại từ settings.
    2. Tạo một Series chứa giá trị offset (đã chuyển đổi thành Timedelta) cho
       mỗi dòng bằng cách map `storeid` với cấu hình.
    3. Trừ trực tiếp Series Timedelta này khỏi cột timestamp.
    """
    if not config.timestamp_col:
        return df

    # Lấy tên bảng không có schema (ví dụ: 'num_crowd' thay vì 'dbo.num_crowd')
    table_name_key = config.source_table.split('.')[-1]
    time_offsets_for_table = settings.TIME_OFFSETS.get(table_name_key)

    if not time_offsets_for_table:
        logger.debug(f"Không có cấu hình chênh lệch thời gian cho bảng '{table_name_key}'. Bỏ qua.")
        return df

    # Sử dụng tên cột gốc từ SQL Server để map offset
    store_id_col_source = 'storeid'
    ts_col_source = config.timestamp_col

    if store_id_col_source not in df.columns or ts_col_source not in df.columns:
        logger.warning(
            f"Bỏ qua điều chỉnh thời gian cho '{table_name_key}' do thiếu cột nguồn "
            f"'{store_id_col_source}' hoặc '{ts_col_source}' trong DataFrame thô."
        )
        return df

    # 1. Tạo một Series chứa giá trị offset cho mỗi dòng.
    #    fillna(0) để các store_id không có trong config sẽ không bị điều chỉnh.
    offsets_in_minutes = df[store_id_col_source].map(time_offsets_for_table).fillna(0)

    # 2. Chuyển đổi Series offset (phút) thành Timedelta.
    timedelta_offsets = pd.to_timedelta(offsets_in_minutes, unit='m')

    # 3. Đảm bảo cột timestamp là kiểu datetime.
    df[ts_col_source] = pd.to_datetime(df[ts_col_source], errors='coerce')

    # 4. Thực hiện phép trừ vector hóa để điều chỉnh thời gian.
    #    Lưu ý: Dấu trừ (-) vì logic là "thời gian đúng = thời gian sai - chênh lệch"
    #    Ví dụ: đồng hồ chạy nhanh 55 phút (offset: 55), cần trừ đi 55 phút.
    #            đồng hồ chạy chậm 105 phút (offset: -105), cần trừ đi -105 phút (tức là cộng thêm).
    df[ts_col_source] = df[ts_col_source] - timedelta_offsets

    logger.info(f"Đã áp dụng điều chỉnh chênh lệch thời gian cho bảng '{table_name_key}'.")
    return df


def _select_final_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Chọn và sắp xếp các cột cuối cùng để khớp với schema đích. """
    schema = table_schemas.get(config.dest_table)
    if not schema:
        return df

    # Lấy danh sách các cột được định nghĩa trong schema
    schema_columns = list(schema.to_schema().columns.keys())
    # Giữ lại các cột tồn tại trong cả DataFrame và schema
    final_columns = [col for col in schema_columns if col in df.columns]
    return df[final_columns]


def _save_rejected_data(df: pd.DataFrame, config: TableConfig):
    """ Lưu các hàng dữ liệu bị từ chối vào một file Parquet riêng. """
    rejected_path = settings.DATA_DIR / 'rejected' / config.dest_table
    rejected_path.mkdir(parents=True, exist_ok=True)

    # Tạo tên file duy nhất dựa trên timestamp hiện tại
    timestamp_str = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')
    file_path = rejected_path / f'rejected_{timestamp_str}.parquet'

    try:
        df.to_parquet(file_path, index=False)
        logger.warning(f"Đã lưu {len(df)} hàng dữ liệu không hợp lệ vào: {file_path}")
    except Exception as e:
        logger.error(f"Không thể lưu dữ liệu không hợp lệ: {e}")


def _validate_with_pandera(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """
    Xác thực DataFrame với schema Pandera tương ứng.

    Nếu xác thực thất bại, các hàng lỗi sẽ được lưu lại và một exception
    sẽ được ném ra để dừng quá trình xử lý của bảng hiện tại.
    """
    schema = table_schemas.get(config.dest_table)
    if not schema:
        logger.warning(f"Không tìm thấy schema Pandera cho '{config.dest_table}'. "
                       f"Bỏ qua xác thực.")
        return df

    try:
        logger.debug(f"Bắt đầu xác thực schema cho '{config.dest_table}'.")
        # lazy=True để thu thập tất cả lỗi thay vì dừng ở lỗi đầu tiên.
        validated_df = schema.validate(df, lazy=True)
        logger.debug(f"Xác thực schema cho '{config.dest_table}' thành công.")
        return validated_df
    except pa_errors.SchemaErrors as err:
        logger.error(f"Xác thực dữ liệu cho '{config.dest_table}' thất bại!")
        logger.error(f"Chi tiết lỗi:\n{err.failure_cases.to_string()}")

        # Triển khai "Dead-Letter Queue"
        # Lưu các hàng lỗi vào một file riêng để phân tích sau.
        if not err.failure_cases.empty:
            _save_rejected_data(err.failure_cases, config)

        raise  # Ném lại lỗi để pipeline chính xử lý.


def _rename_columns(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Đổi tên các cột dựa trên `rename_map` trong cấu hình. """
    if config.rename_map:
        return df.rename(columns=config.rename_map)
    return df


def _apply_strip(series: pd.Series) -> pd.Series:
    """ Helper function để loại bỏ khoảng trắng thừa ở đầu và cuối chuỗi. """
    return series.str.strip()


CLEANING_ACTIONS = {
    'strip': _apply_strip
}


def _clean_data(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Áp dụng các quy tắc làm sạch dữ liệu từ cấu hình. """
    if not config.cleaning_rules:
        return df

    for rule in config.cleaning_rules:
        # Lấy tên cột đích đã được đổi tên để áp dụng quy tắc.
        col_to_clean = config.rename_map.get(rule.column, rule.column)

        if col_to_clean in df.columns and pd.api.types.is_object_dtype(df[col_to_clean]):
            action_func = CLEANING_ACTIONS.get(rule.action)
            if action_func:
                df[col_to_clean] = action_func(df[col_to_clean])

    return df


def _process_numeric_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """ Chuyển đổi các cột số, xử lý giá trị âm và rỗng. """
    for col in columns:
        if col in df.columns:
            # `coerce` sẽ chuyển các giá trị không hợp lệ thành NaT.
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Chuyển các giá trị âm thành 0.
            if (df[col] < 0).any():
                df[col] = df[col].apply(lambda x: max(0, x) if pd.notna(x) else x)
            # Điền giá trị rỗng bằng 0 và chuyển sang kiểu integer.
            df[col] = df[col].fillna(0).astype(int)

    return df


def _handle_timestamps_and_partitions(df: pd.DataFrame, config: TableConfig) -> pd.DataFrame:
    """ Chuyển đổi cột timestamp và tạo các cột partition (năm, tháng). """
    ts_col = config.final_timestamp_col
    if not (ts_col and ts_col in df.columns):
        return df

    # Bước này chỉ đảm bảo kiểu dữ liệu, vì giá trị đã được chuyển đổi ở các bước trước
    df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')

    # Loại bỏ các hàng có timestamp không hợp lệ.
    if df[ts_col].isna().any():
        num_nat = df[ts_col].isna().sum()
        logger.warning(f"Tìm thấy {num_nat} timestamp không hợp lệ trong cột '{ts_col}'. "
                       f"Các hàng này sẽ bị loại bỏ.")
        df.dropna(subset=[ts_col], inplace=True)

    # Tạo cột partition nếu được cấu hình.
    if not df.empty and config.partition_cols:
        if 'year' in config.partition_cols:
            df['year'] = df[ts_col].dt.year
        if 'month' in config.partition_cols:
            df['month'] = df[ts_col].dt.month

    return df


def _ensure_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """ Đảm bảo các cột có kiểu dữ liệu phù hợp trước khi xác thực. """
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            # Chuẩn hóa các giá trị rỗng trong cột chuỗi.
            df[col] = df[col].astype(str).replace(
                {'None': None, 'NaT': None, 'nan': None, '<NA>': None}
            )
        elif 'id' in col or 'code' in col:
            # Chuyển đổi các cột ID/code sang kiểu số nguyên có thể rỗng.
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df


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
    # Lấy tên cột đích từ config
    visitors_in_col = config.rename_map.get('in_num', 'in_num')
    visitors_out_col = config.rename_map.get('out_num', 'out_num')

    return (
        df.pipe(_apply_time_offsets, config)
        .pipe(_rename_columns, config)
        .pipe(_clean_data, config)
        .pipe(_process_numeric_columns, [visitors_in_col, visitors_out_col])
        .pipe(_handle_timestamps_and_partitions, config)
        .pipe(_ensure_data_types)
        .pipe(_select_final_columns, config)
        .pipe(_validate_with_pandera, config)
    )


def get_max_timestamp(df: pd.DataFrame, config: TableConfig) -> Optional[pd.Timestamp]:
    """
    Lấy giá trị timestamp lớn nhất từ một chunk.

    Giá trị này sẽ được dùng để cập nhật "high-water mark" cho lần chạy ETL tiếp theo.

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
