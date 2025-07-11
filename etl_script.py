# DuckDB, và Task Scheduler, dùng argparse để truyền tham số trong file .env vào script
# .venv\Scripts\python.exe etl_script.py data.duckdb
import argparse
import duckdb
import logging
import numpy as np
import os
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, exc

from app.core.config import settings

def setup_logging():
    """
    Thiết lập hệ thống logging để ghi ra cả console và file.
    Tự động tạo thư mục 'logs' nếu chưa tồn tại.
    """
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)  # Đảm bảo thư mục logs tồn tại

    date_str = date.today().strftime('%Y-%m-%d')
    log_file = os.path.join(log_dir, f'etl_{date_str}.log')

    # Cấu hình logging, thêm encoding='utf-8' để hỗ trợ ghi file log có dấu tiếng Việt
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(levelname)s - %(message)s',
        handlers = [
            logging.FileHandler(log_file, encoding = 'utf-8'),
            logging.StreamHandler()
        ]
    )

def extract_from_mssql(table_name: str, is_first_run: bool):
    """
    Kết nối tới MSSQL và trích xuất dữ liệu.
    Nếu không phải lần chạy đầu, sẽ chỉ lấy dữ liệu năm hiện tại cho các bảng lớn.
    """
    logging.info(f'->  Bắt đầu trích xuất dữ liệu từ bảng: `dbo.{table_name}`')

    # Các bảng lớn và cột ngày tháng tương ứng để lọc
    incremental_tables = {'num_crowd': 'recordtime', 'ErrLog': 'LogTime'}
    query = f'SELECT * FROM dbo.{table_name}'

    # Nếu không phải lần chạy đầu và là bảng lớn, chỉ lấy dữ liệu năm hiện tại
    if not is_first_run and table_name in incremental_tables:
        date_column = incremental_tables[table_name]
        current_year = date.today().year
        query += f' WHERE YEAR({date_column}) = {current_year}'
        logging.info(f'->  Chế độ tăng trưởng: Chỉ lấy dữ liệu năm {current_year}.')
    else:
        logging.info('->  Chế độ tải toàn bộ (Full Load).')

    try:
        engine_url = settings.SQLALCHEMY_DATABASE_URI
        engine = create_engine(engine_url, echo=False)
        df = pd.read_sql(query, engine)

        logging.info(f'->  Trích xuất thành công {len(df)} dòng dữ liệu.')
        return df
    except exc.SQLAlchemyError as e:
        logging.error(f'->  Lỗi SQLAlchemy khi kết nối hoặc truy vấn MSSQL: {e}')
        return None
    except Exception as e:
        logging.error(f'->  Lỗi không xác định trong quá trình trích xuất: {e}')
        return None

def transform_data(df: pd.DataFrame, table_name: str):
    """
    Thực hiện các bước biến đổi trên DataFrame dựa trên tên bảng.
    """
    if df is None:
        return None

    logging.info(f'->  Bắt đầu quá trình biến đổi cho bảng: {table_name}')
    try:
        cols_str_type = df.select_dtypes(include='object').columns
        for col_name in cols_str_type:
            df[col_name] = df[col_name].str.strip().astype('string')
        df = df.replace([None, ''], np.nan)

        if table_name == 'store':
            df['isbranch'] = (df['isbranch'] == 'yes')
            df['lastEditDate'] = pd.to_datetime(df['lastEditDate'], errors='coerce')

        elif table_name == 'ErrLog':
            df['LogTime'] = pd.to_datetime(df['LogTime'], errors='coerce')

        elif table_name == 'num_crowd':
            df['recordtime'] = pd.to_datetime(df['recordtime'], errors='coerce')

        elif table_name == 'Status':
            for i in range(1, 9):
                df[f'RC{i}'] = df[f'RC{i}'].astype(bool)
            df['DcTime'] = pd.to_datetime(df['DcTime'], errors='coerce')
            df['T'] = pd.to_datetime(df['T'], errors='coerce')

        logging.info(f'->  Biến đổi dữ liệu thành công cho bảng: `{table_name}`.')
        return df
    except Exception as e:
        logging.error(f'->  Lỗi trong quá trình biến đổi dữ liệu cho bảng `{table_name}`: {e}')
        return None

def load_to_duckdb(df: pd.DataFrame, table_name: str, duckdb_path: str, is_first_run: bool):
    """
    Nạp dữ liệu vào DuckDB. Dùng chế độ phù hợp: tạo mới hoặc xóa/chèn.
    """
    if df is None:
        logging.warning(f'->  Bỏ qua bước nạp dữ liệu cho bảng `{table_name}` do không có dữ liệu.')
        return

    incremental_tables = {'num_crowd': 'recordtime', 'ErrLog': 'LogTime'}
    try:
        with duckdb.connect(database = duckdb_path, read_only = False) as con:
            # Nếu là lần chạy đầu hoặc không phải bảng lớn, làm mới hoàn toàn
            if is_first_run or table_name not in incremental_tables:
                logging.info(f'->  Chế độ [CREATE OR REPLACE] cho bảng `{table_name}`...')
                con.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df')
                logging.info(f'->  Nạp thành công dữ liệu vào bảng `{table_name}`.')

            # Nếu là lần chạy sau và là bảng lớn, thực hiện xóa và chèn
            else:
                logging.info(f'->  Chế độ [DELETE/INSERT] cho bảng `{table_name}`...')
                date_column = incremental_tables[table_name]
                current_year = date.today().year

                # 1. Xóa dữ liệu năm hiện tại
                delete_query = f'DELETE FROM {table_name} WHERE YEAR({date_column}) = {current_year}'
                deleted_rows = con.execute(delete_query).fetchone()[0]
                logging.info(f'->  Đã xóa {deleted_rows} dòng dữ liệu của năm {current_year} khỏi bảng `{table_name}`.')

                # 2. Chèn dữ liệu mới của năm hiện tại
                con.execute(f'INSERT INTO {table_name} SELECT * FROM df')
                logging.info(f'->  Đã chèn {len(df)} dòng dữ liệu mới của năm {current_year} vào bảng `{table_name}`.')
    except Exception as e:
        logging.error(f'->  Lỗi khi nạp dữ liệu vào DuckDB cho bảng {table_name}: {e}')

def main():
    """
    Hàm điều phối chính, chạy toàn bộ quy trình ETL cho danh sách các bảng được chỉ định.
    """
    setup_logging()

    default_tables = ['store', 'ErrLog', 'num_crowd', 'Status']

    parser = argparse.ArgumentParser(description='Chạy ETL từ MSSQL sang một file DuckDB được chỉ định.')
    parser.add_argument('--tables', nargs = '+', default = default_tables,
                        help = f'Danh sách các bảng nguồn cần xử lý. Mặc định: {default_tables}')
    parser.add_argument('--dest_db', default='data.duckdb',
                        help = 'Đường dẫn tới file DuckDB đích.')
    args = parser.parse_args()

    # Kiểm tra xem có phải lần chạy đầu tiên không (dựa vào sự tồn tại của file DB)
    is_first_run = not os.path.exists(args.dest_db)
    run_mode = 'LẦN ĐẦU (FULL LOAD)' if is_first_run else 'TĂNG TRƯỞNG (INCREMENTAL)'

    logging.info(f'--- BẮT ĐẦU TỔNG THỂ TÁC VỤ ETL (CHẾ ĐỘ: {run_mode}) ---')
    logging.info(f'->  Các bảng sẽ được xử lý: {args.tables}')
    logging.info(f'->  File DuckDB đích: {args.dest_db}')

    for table_name in args.tables:
        logging.info(f'========== Bắt đầu xử lý bảng: {table_name} ==========')
        source_df = extract_from_mssql(table_name, is_first_run)
        transformed_df = transform_data(source_df, table_name)
        load_to_duckdb(transformed_df, table_name, args.dest_db, is_first_run)
        logging.info(f'========== Hoàn thành xử lý bảng: {table_name} ==========')

    logging.info('--- KẾT THÚC TỔNG THỂ TÁC VỤ ETL ---')

if __name__ == '__main__':
    main()
