# DuckDB, và Task Scheduler, dùng argparse để truyền tham số trong file .env vào script
# .venv\Scripts\python.exe etl_script.py --dest_db=data.duckdb
import argparse
import duckdb
import logging
import os
import pandas as pd
from datetime import date
from sqlalchemy import exc

from app.core.db import engine
from app.utils.logger import setup_logging

def extract_from_mssql(table_name: str, is_first_run: bool):
    """
    Kết nối tới MSSQL và trích xuất dữ liệu.
    Nếu không phải lần chạy đầu, sẽ chỉ lấy dữ liệu năm hiện tại cho các bảng lớn.
    """
    logging.info(f'    -> Bắt đầu trích xuất dữ liệu từ bảng: `dbo.{table_name}`')

    # Các bảng lớn và cột ngày tháng tương ứng để lọc
    incremental_tables = {'num_crowd': 'recordtime', 'ErrLog': 'LogTime'}
    query = f'SELECT * FROM dbo.{table_name}'

    # Nếu không phải lần chạy đầu và là bảng lớn, chỉ lấy dữ liệu năm hiện tại
    if not is_first_run and table_name in incremental_tables:
        date_column = incremental_tables[table_name]
        current_year = date.today().year
        query += f' WHERE YEAR({date_column}) = {current_year}'
        logging.info(f'    -> Chế độ tăng trưởng: Chỉ lấy dữ liệu năm `{current_year}`.')
    else:
        logging.info('    -> Chế độ tải toàn bộ (Full Load).')

    try:
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
    Thực hiện biến đổi và hợp nhất dữ liệu.
    """
    if fact_df is None or stores_df is None:
        return None

    logging.info(f'    -> Bắt đầu biến đổi và hợp nhất cho bảng: `{table_type}`')
    try:
        # 1. Chuẩn hóa tên cột trong bảng stores
        stores_renamed_df = stores_df.rename(columns={'tid': 'store_id', 'name': 'store_name'})
        stores_subset_df = stores_renamed_df[['store_id', 'store_name']].copy()
        stores_subset_df['store_name'] = stores_subset_df['store_name'].str.strip().astype('string')

        # 2. Biến đổi bảng dữ liệu chính (fact table)
        if table_type == 'error_logs':
            fact_df = fact_df.rename(columns={
                'ID': 'id', 'storeid': 'store_id', 'LogTime': 'log_time',
                'Errorcode': 'error_code', 'ErrorMessage': 'error_message'
            })
            fact_df['log_time'] = pd.to_datetime(fact_df['log_time'], errors='coerce')

        elif table_type == 'crowd_counts':
            fact_df = fact_df.rename(columns={
                'recordtime': 'record_time', 'in_num': 'in_count',
                'out_num': 'out_count', 'storeid': 'store_id'
            })
            fact_df['record_time'] = pd.to_datetime(fact_df['record_time'], errors='coerce')

        # 3. Hợp nhất (join) để thêm store_name
        merged_df = pd.merge(fact_df, stores_subset_df, on='store_id', how='left')

        # 4. Sắp xếp và chọn các cột cuối cùng
        if table_type == 'error_logs':
            final_cols = ['id', 'store_name', 'log_time', 'error_code', 'error_message']
            final_df = merged_df[final_cols]

        elif table_type == 'crowd_counts':
            final_cols = ['record_time', 'store_name', 'in_count', 'out_count']
            final_df = merged_df[final_cols]

        else:
            final_df = merged_df

        logging.info(f'    -> Biến đổi và hợp nhất thành công cho bảng: `{table_type}`.\n')
        return final_df
    except Exception as e:
        logging.error(f'   -> Lỗi trong quá trình biến đổi và hợp nhất cho bảng `{table_type}`: {e}\n')
        return None

def load_to_duckdb(df: pd.DataFrame, table_name: str, duckdb_path: str, is_first_run: bool):
    """
    Nạp dữ liệu vào DuckDB. Dùng chế độ phù hợp: tạo mới hoặc xóa/chèn.
    """
    if df is None:
        logging.warning(f' -> Bỏ qua bước nạp dữ liệu cho bảng `{table_name}` do không có dữ liệu.')
        return

    date_columns = {'crowd_counts': 'record_time', 'error_logs': 'log_time'}

    try:
        with duckdb.connect(database = duckdb_path, read_only = False) as con:
            if is_first_run:
                logging.info(f'    -> Chế độ [CREATE OR REPLACE] cho bảng `{table_name}`...')
                con.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df')

            else:
                logging.info(f'    -> Chế độ [DELETE/INSERT] cho bảng: `{table_name}`...')
                date_column = date_columns[table_name]
                current_year = date.today().year

                delete_query = f'DELETE FROM {table_name} WHERE YEAR({date_column}) = {current_year}'
                deleted_rows = con.execute(delete_query).fetchone()[0]
                logging.info(f'    -> Đã xóa {deleted_rows} dòng của năm {current_year} khỏi bảng `{table_name}`.')

                con.execute(f'INSERT INTO {table_name} SELECT * FROM df')
                logging.info(f'    -> Đã chèn {len(df)} dòng mới của năm {current_year} vào bảng `{table_name}`.\n')
    except Exception as e:
        logging.error(f'   -> Lỗi khi nạp dữ liệu vào DuckDB cho bảng `{table_name}`: {e}\n')

def main():
    """
    Hàm điều phối chính, chạy toàn bộ quy trình ETL.
    """
    setup_logging('etl')

    parser = argparse.ArgumentParser(description='Chạy ETL từ MSSQL sang một file DuckDB đã được tối ưu.')
    parser.add_argument('--dest_db', default = 'data.duckdb', help = 'Đường dẫn tới file DuckDB đích.')
    args = parser.parse_args()

    is_first_run = not os.path.exists(args.dest_db)
    run_mode = 'LẦN ĐẦU (FULL LOAD)' if is_first_run else 'TĂNG TRƯỞNG (INCREMENTAL)'

    logging.info('=======================================================================')
    logging.info(f'--- BẮT ĐẦU TỔNG THỂ TÁC VỤ ETL (CHẾ ĐỘ: {run_mode}) ---')
    logging.info(f'    -> File DuckDB đích: `{args.dest_db}`\n')

    # --- 1. EXTRACT ---
    stores_df = extract_from_mssql('store', True) # Bảng store luôn tải toàn bộ
    errlog_df = extract_from_mssql('ErrLog', is_first_run)
    num_crowd_df = extract_from_mssql('num_crowd', is_first_run)

    # --- 2. TRANSFORM & JOIN ---
    final_error_logs = transform_and_join(stores_df, errlog_df, 'error_logs')
    final_crowd_counts = transform_and_join(stores_df, num_crowd_df, 'crowd_counts')

    # --- 3. LOAD ---
    load_to_duckdb(final_error_logs, 'error_logs', args.dest_db, is_first_run)
    load_to_duckdb(final_crowd_counts, 'crowd_counts', args.dest_db, is_first_run)

    logging.info('--- KẾT THÚC TỔNG THỂ TÁC VỤ ETL ---\n\n')

if __name__ == '__main__':
    main()
