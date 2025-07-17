# Phân vùng kiểu Hive (Hive-style Partitioning)
# .venv\Scripts\python.exe etl_master_script.py --output_format=parquet --destination=data --full_load
# Data DuckDB
# .venv\Scripts\python.exe etl_master_script.py --output_format=duckdb --destination=data.duckdb --full_load
import argparse
import duckdb
import logging
import os
import pandas as pd
import shutil

from datetime import date
from sqlalchemy import exc

from app.core.database import engine
from app.utils.logger import setup_logging

def extract_from_mssql(table_name: str, full_load: bool):
    """
    Kết nối tới MSSQL và trích xuất dữ liệu.
    """
    logging.info(f'    -> Bắt đầu trích xuất dữ liệu từ bảng: `dbo.{table_name}`')

    # Các bảng lớn và cột ngày tháng tương ứng để lọc
    incremental_tables = {'num_crowd': 'recordtime', 'ErrLog': 'LogTime'}
    query = f'SELECT * FROM dbo.{table_name}'

    # [Note] Chiến lược Tăng trưởng (Incremental Strategy):
    # Để đơn giản, chế độ tăng trưởng sẽ tải lại toàn bộ dữ liệu của năm hiện tại.
    # Điều này đảm bảo dữ liệu trong năm luôn được cập nhật và giúp script có tính
    # idempotent (chạy lại nhiều lần không gây ra lỗi dữ liệu).
    # Một giải pháp nâng cao hơn có thể dùng "high-water mark" (lưu lại timestamp
    # cuối cùng) để chỉ tải những dòng thực sự mới.
    if table_name in incremental_tables and not full_load:
        date_column = incremental_tables[table_name]
        current_year = date.today().year
        query += f' WHERE YEAR({date_column}) = {current_year}'
        logging.info(f'    -> Chế độ Tăng trưởng: Chỉ lấy dữ liệu năm `{current_year}`.')
    else:
        logging.info('    -> Chế độ Tải toàn bộ (Full Load).')

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

def transform_and_join(stores_df: pd.DataFrame, fact_df: pd.DataFrame, table_type: str, output_format: str):
    """
    Thực hiện biến đổi, hợp nhất dữ liệu và thêm cột 'year' nếu cần.
    [UPDATE] Cải thiện xử lý lỗi để bắt các lỗi cụ thể hơn.
    """
    if fact_df is None or stores_df is None:
        return None

    logging.info(f'    -> Bắt đầu biến đổi và hợp nhất cho bảng: `{table_type}`')
    try:
        # 1. Chuẩn hóa tên cột trong bảng stores
        stores_df.rename(columns={'tid': 'store_id', 'name': 'store_name'}, inplace=True)
        stores_df = stores_df[['store_id', 'store_name']].copy()
        stores_df['store_name'] = stores_df['store_name'].str.strip().astype('string')

        # 2. Biến đổi bảng dữ liệu chính (fact table)
        date_column_name = ''
        if table_type == 'error_logs':
            fact_df = fact_df.rename(columns={
                'ID': 'id', 'storeid': 'store_id', 'LogTime': 'log_time',
                'Errorcode': 'error_code', 'ErrorMessage': 'error_message'
            })
            fact_df['error_message'] = fact_df['error_message'].str.strip().astype('string')
            date_column_name = 'log_time'
        elif table_type == 'crowd_counts':
            fact_df = fact_df.rename(columns={
                'recordtime': 'record_time', 'in_num': 'in_count',
                'out_num': 'out_count', 'storeid': 'store_id'
            })
            date_column_name = 'record_time'

        fact_df[date_column_name] = pd.to_datetime(fact_df[date_column_name], errors='coerce')
        fact_df.dropna(subset=[date_column_name], inplace=True) # Loại bỏ các dòng có ngày tháng không hợp lệ

        # 3. Hợp nhất (join) để thêm store_name
        merged_df = pd.merge(fact_df, stores_df, on='store_id', how='left')
        merged_df.dropna(subset=['store_name'], inplace=True)

        # 4. Sắp xếp và chọn các cột cuối cùng
        final_cols = []
        if table_type == 'error_logs':
            final_cols = ['id', 'store_name', 'log_time', 'error_code', 'error_message']
        elif table_type == 'crowd_counts':
            final_cols = ['record_time', 'store_name', 'in_count', 'out_count']

        # Chỉ thêm cột 'year' nếu định dạng đầu ra là parquet để phân vùng
        if output_format == 'parquet':
            merged_df['year'] = merged_df[date_column_name].dt.year
            final_cols.append('year')
            final_df = merged_df[final_cols].dropna(subset=['year'])
            final_df['year'] = final_df['year'].astype(int)
        else:
            final_df = merged_df[final_cols]

        logging.info(f'    -> Biến đổi và hợp nhất thành công cho bảng: `{table_type}`.\n')
        return final_df

    # [UPDATE] Bắt các lỗi cụ thể hơn để dễ dàng gỡ rối
    except KeyError as e:
        logging.error(f'   -> Lỗi không tìm thấy cột (KeyError) trong quá trình biến đổi cho bảng `{table_type}`. Vui lòng kiểm tra lại tên cột trong DB. Lỗi: {e}\n')
        return None
    except AttributeError as e:
        logging.error(f'   -> Lỗi thuộc tính (AttributeError), có thể do kiểu dữ liệu không đúng (ví dụ: áp dụng .str cho cột không phải chuỗi). Bảng `{table_type}`. Lỗi: {e}\n')
        return None
    except Exception as e:
        logging.error(f'   -> Lỗi không xác định trong quá trình biến đổi và hợp nhất cho bảng `{table_type}`: {e}\n')
        return None

def load_to_duckdb(df: pd.DataFrame, table_name: str, duckdb_path: str, is_first_run: bool):
    """
    Nạp dữ liệu vào DuckDB. Dùng chế độ phù hợp: tạo mới hoặc xóa/chèn.
    """
    if df is None or df.empty:
        logging.warning(f' -> Bỏ qua bước nạp dữ liệu cho bảng `{table_name}` do không có dữ liệu.')
        return

    date_columns = {'crowd_counts': 'record_time', 'error_logs': 'log_time'}
    try:
        with duckdb.connect(database=duckdb_path, read_only=False) as con:
            if is_first_run:
                # Lần đầu chạy hoặc khi full_load, tạo mới hoặc thay thế hoàn toàn bảng
                logging.info(f'    -> Chế độ [CREATE OR REPLACE] cho bảng `{table_name}`...')
                con.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df')
            else:
                # [Note] Chế độ tăng trưởng cho DuckDB:
                # 1. Xóa dữ liệu của năm hiện tại trong bảng đích.
                # 2. Chèn dữ liệu mới của năm hiện tại vừa được trích xuất.
                # Cách này đảm bảo dữ liệu cho năm hiện tại luôn được làm mới.
                logging.info(f'    -> Chế độ [DELETE/INSERT] cho bảng: `{table_name}`...')
                date_column = date_columns[table_name]
                current_year = date.today().year

                con.execute(f'DELETE FROM {table_name} WHERE YEAR({date_column}) = {current_year}')
                con.execute(f'INSERT INTO {table_name} SELECT * FROM df')
        logging.info(f'    -> Nạp thành công dữ liệu vào DuckDB cho bảng `{table_name}`.\n')
    except Exception as e:
        logging.error(f'   -> Lỗi khi nạp dữ liệu vào DuckDB cho bảng `{table_name}`: {e}\n')

def load_to_partitioned_parquet(df: pd.DataFrame, table_name: str, base_path: str, full_load: bool):
    """
    Lưu DataFrame vào các file Parquet được phân vùng theo năm.
    """
    if df is None or df.empty:
        logging.warning(f' -> Bỏ qua bước nạp dữ liệu cho bảng `{table_name}` do không có dữ liệu.')
        return

    full_path = os.path.join(base_path, table_name)
    logging.info(f'    -> Bắt đầu nạp dữ liệu vào thư mục Parquet phân vùng: `{full_path}`...')

    if full_load and os.path.exists(full_path):
        logging.warning(f' -> Chế độ Full Load: Xóa thư mục cũ `{full_path}` để làm mới.')
        shutil.rmtree(full_path)

    try:
        # [Note] Ghi dữ liệu, phân vùng theo cột 'year'.
        # existing_data_behavior='delete_matching' sẽ tự động xóa các phân vùng (năm)
        # đang được ghi đè. Điều này phù hợp hoàn hảo với chiến lược tải lại
        # toàn bộ dữ liệu của năm hiện tại trong chế độ tăng trưởng.
        df.to_parquet(full_path, engine='pyarrow', partition_cols=['year'], existing_data_behavior='delete_matching')
        logging.info(f'    -> Nạp thành công dữ liệu Parquet cho bảng `{table_name}`.\n')
    except Exception as e:
        logging.error(f'   -> Lỗi khi nạp dữ liệu Parquet cho bảng `{table_name}`: {e}\n')

def main():
    """
    Hàm điều phối chính, chạy toàn bộ quy trình ETL.
    """
    setup_logging('etl_master')
    parser = argparse.ArgumentParser(description='Chạy ETL từ MSSQL và lưu ra định dạng được chỉ định.')
    parser.add_argument('--output_format', required=True, choices=['duckdb', 'parquet'], help='Định dạng đầu ra: duckdb hoặc parquet.')
    parser.add_argument('--destination', required=True, help='Đường dẫn tới file .duckdb hoặc thư mục gốc cho Parquet.')
    parser.add_argument('--full_load', action='store_true', help='Chạy chế độ full load, tải lại toàn bộ dữ liệu lịch sử.')
    args = parser.parse_args()

    run_mode = 'TẢI TOÀN BỘ (FULL LOAD)' if args.full_load else 'TĂNG TRƯỞNG (INCREMENTAL)'
    logging.info('==================================================================================')
    logging.info(f'--- BẮT ĐẦU TÁC VỤ ETL (ĐỊNH DẠNG: {args.output_format.upper()}, CHẾ ĐỘ: {run_mode}) ---')
    logging.info(f'    -> Đích đến: `{args.destination}`\n')

    # --- 1. EXTRACT ---
    stores_df = extract_from_mssql('store', args.full_load) 
    errlog_df = extract_from_mssql('ErrLog', args.full_load)
    num_crowd_df = extract_from_mssql('num_crowd', args.full_load)

    # --- 2. TRANSFORM & JOIN ---
    final_error_logs = transform_and_join(stores_df, errlog_df, 'error_logs', args.output_format)
    final_crowd_counts = transform_and_join(stores_df, num_crowd_df, 'crowd_counts', args.output_format)

    # --- 3. LOAD ---
    if args.output_format == 'duckdb':
        is_first_run = not os.path.exists(args.destination)
        if args.full_load and not is_first_run:
            logging.warning(f' -> Chế độ Full Load: Sẽ thay thế hoàn toàn file `{args.destination}`.')
            is_first_run = True # Coi như lần đầu để CREATE OR REPLACE
        load_to_duckdb(final_error_logs, 'error_logs', args.destination, is_first_run)
        load_to_duckdb(final_crowd_counts, 'crowd_counts', args.destination, is_first_run)
    elif args.output_format == 'parquet':
        os.makedirs(args.destination, exist_ok=True)
        load_to_partitioned_parquet(final_error_logs, 'error_logs', args.destination, args.full_load)
        load_to_partitioned_parquet(final_crowd_counts, 'crowd_counts', args.destination, args.full_load)

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
