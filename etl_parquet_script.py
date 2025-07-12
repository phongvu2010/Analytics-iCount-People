# Phân vùng kiểu Hive (Hive-style Partitioning)
# .venv\Scripts\python.exe etl_parquet_script.py --dest_dir=data --full_load
import argparse
import logging
import os
import pandas as pd
import shutil
from datetime import date
from sqlalchemy import create_engine, exc

from app.core.config import settings
from app.core.logging_utils import setup_logging

def extract_from_mssql(table_name: str, full_load: bool):
    """
    Kết nối tới MSSQL và trích xuất dữ liệu.
    """
    logging.info(f'    -> Bắt đầu trích xuất dữ liệu từ bảng: `dbo.{table_name}`')

    incremental_tables = {'num_crowd': 'recordtime', 'ErrLog': 'LogTime'}
    query = f'SELECT * FROM dbo.{table_name}'

    # Nếu không phải full_load, chỉ lấy dữ liệu năm hiện tại cho các bảng lớn
    if table_name in incremental_tables and not full_load:
        date_column = incremental_tables[table_name]
        current_year = date.today().year
        query += f' WHERE YEAR({date_column}) = {current_year}'
        logging.info(f'    -> Chế độ Tăng trưởng: Chỉ lấy dữ liệu năm `{current_year}`.')
    else:
        logging.info('    -> Chế độ Tải toàn bộ (Full Load).')

    try:
        engine_url = settings.SQLALCHEMY_DATABASE_URI
        engine = create_engine(engine_url, echo=False)
        df = pd.read_sql(query, engine)
        logging.info(f'    -> Trích xuất thành công {len(df)} dòng dữ liệu.\n')
        return df
    except exc.SQLAlchemyError as e:
        logging.error(f'   -> Lỗi SQLAlchemy khi kết nối hoặc truy vấn MSSQL: {e}\n')
        return None
    except Exception as e:
        logging.error(f'   -> Lỗi không xác định trong quá trình trích xuất: {e}\n')
        return None

def transform_and_join(stores_df: pd.DataFrame, fact_df: pd.DataFrame, table_type: str):
    """
    Thực hiện biến đổi, hợp nhất dữ liệu và thêm cột 'year' để phân vùng.
    """
    if fact_df is None or stores_df is None:
        return None
    logging.info(f'    -> Bắt đầu biến đổi và hợp nhất cho: `{table_type}`')

    try:
        stores_renamed_df = stores_df.rename(columns={'tid': 'store_id', 'name': 'store_name'})
        stores_subset_df = stores_renamed_df[['store_id', 'store_name']].copy()
        stores_subset_df['store_name'] = stores_subset_df['store_name'].str.strip().astype('string')

        date_column_name = ''
        if table_type == 'error_logs':
            fact_df = fact_df.rename(columns={
                'ID': 'id', 'storeid': 'store_id', 'LogTime': 'log_time',
                'Errorcode': 'error_code', 'ErrorMessage': 'error_message'
            })
            date_column_name = 'log_time'

        elif table_type == 'crowd_counts':
            fact_df = fact_df.rename(columns={
                'recordtime': 'record_time', 'in_num': 'in_count',
                'out_num': 'out_count', 'storeid': 'store_id'
            })
            date_column_name = 'record_time'

        fact_df[date_column_name] = pd.to_datetime(fact_df[date_column_name], errors='coerce')
        fact_df['year'] = fact_df[date_column_name].dt.year

        merged_df = pd.merge(fact_df, stores_subset_df, on='store_id', how='left')

        final_cols = []
        if table_type == 'error_logs':
            final_cols = ['id', 'store_name', 'log_time', 'error_code', 'error_message', 'year']

        elif table_type == 'crowd_counts':
            final_cols = ['record_time', 'store_name', 'in_count', 'out_count', 'year']

        final_df = merged_df[final_cols].dropna(subset=['year'])
        final_df['year'] = final_df['year'].astype(int)

        logging.info(f'    -> Biến đổi và hợp nhất thành công cho bảng: `{table_type}`.\n')
        return final_df
    except Exception as e:
        logging.error(f'   -> Lỗi trong quá trình biến đổi và hợp nhất cho bảng `{table_type}`: {e}\n')
        return None

def load_to_partitioned_parquet(df: pd.DataFrame, table_name: str, base_path: str, full_load: bool):
    """
    Lưu DataFrame vào các file Parquet được phân vùng theo năm.
    """
    if df is None or df.empty:
        logging.warning(f' -> Bỏ qua bước nạp dữ liệu cho bảng `{table_name}` do không có dữ liệu.')
        return

    full_path = os.path.join(base_path, table_name)
    logging.info(f'    -> Bắt đầu nạp dữ liệu vào thư mục Parquet phân vùng: `{full_path}`...')

    # Nếu là full load và thư mục đã tồn tại, xóa đi để làm mới hoàn toàn
    if full_load and os.path.exists(full_path):
        logging.warning(f' -> Chế độ Full Load: Xóa thư mục cũ `{full_path}` để làm mới.')
        shutil.rmtree(full_path)

    try:
        # Ghi dữ liệu, phân vùng theo cột 'year'
        # existing_data_behavior='delete_matching' sẽ xóa các phân vùng đang được ghi đè
        df.to_parquet(
            full_path,
            engine = 'pyarrow',
            partition_cols = ['year'],
            existing_data_behavior = 'delete_matching'
        )
        logging.info(f'    -> Nạp thành công dữ liệu Parquet cho bảng `{table_name}`.\n')
    except Exception as e:
        logging.error(f'   -> Lỗi khi nạp dữ liệu Parquet cho bảng `{table_name}`: {e}\n')

def main():
    """
    Hàm điều phối chính, chạy toàn bộ quy trình ETL.
    """
    setup_logging('etl_parquet')

    parser = argparse.ArgumentParser(description='Chạy ETL từ MSSQL và lưu ra các file Parquet được phân vùng.')
    parser.add_argument('--dest_dir', default = 'data', help = 'Thư mục gốc để lưu dữ liệu Parquet.')
    parser.add_argument('--full_load', action = 'store_true', help = 'Chạy chế độ full load, tải lại toàn bộ dữ liệu lịch sử.')
    args = parser.parse_args()

    os.makedirs(args.dest_dir, exist_ok = True)

    run_mode = 'TẢI TOÀN BỘ (FULL LOAD)' if args.full_load else 'TĂNG TRƯỞNG (INCREMENTAL)'

    logging.info('=======================================================================')
    logging.info(f'--- BẮT ĐẦU TỔNG THỂ TÁC VỤ ETL (CHẾ ĐỘ: {run_mode}) ---')
    logging.info(f'    Dữ liệu sẽ được lưu tại thư mục: `{args.dest_dir}`\n')

    # --- 1. EXTRACT ---
    stores_df = extract_from_mssql('store', args.full_load) 
    errlog_df = extract_from_mssql('ErrLog', args.full_load)
    num_crowd_df = extract_from_mssql('num_crowd', args.full_load)

    # --- 2. TRANSFORM & JOIN ---
    final_error_logs = transform_and_join(stores_df, errlog_df, 'error_logs')
    final_crowd_counts = transform_and_join(stores_df, num_crowd_df, 'crowd_counts')

    # --- 3. LOAD ---
    load_to_partitioned_parquet(final_error_logs, 'error_logs', args.dest_dir, args.full_load)
    load_to_partitioned_parquet(final_crowd_counts, 'crowd_counts', args.dest_dir, args.full_load)

    logging.info('--- KẾT THÚC TỔNG THỂ TÁC VỤ ETL ---\n\n')

if __name__ == '__main__':
    main()






# 1. Đếm tổng số lượt khách vào/ra trong năm 2025:
# SELECT 
#     SUM(in_count) AS total_in, 
#     SUM(out_count) AS total_out 
# FROM 'data/crowd_counts/';

# 2. Tìm 10 cửa hàng có nhiều lỗi nhất trong năm 2025:
# SELECT 
#     store_name, 
#     COUNT(*) AS error_count
# FROM 'data/error_logs/'
# GROUP BY store_name
# ORDER BY error_count DESC
# LIMIT 10;
