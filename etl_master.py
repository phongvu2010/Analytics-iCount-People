import pyodbc
import pandas as pd
import duckdb
import os
import datetime
import logging
from typing import Optional

# Import đối tượng settings đã khởi tạo
from config import settings
# Import hàm setup_logging từ file mới
from utils.logger_config import setup_logging

# --- Cấu hình Logger ---
logger = setup_logging(
    log_file=settings.logging.log_file,
    log_level=settings.logging.log_level,
    log_format=settings.logging.log_format,
    log_date_format=settings.logging.log_date_format,
    log_dir=settings.data_dir
)

# --- Các hàm hỗ trợ Incremental Load ---
def get_last_load_time(file_name: str) -> Optional[datetime.datetime]:
    """Đọc thời điểm tải cuối cùng từ file trong thư mục data."""
    file_path = os.path.join(settings.data_dir, file_name)
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                last_time_str = f.read().strip()
                if last_time_str:
                    return datetime.datetime.fromisoformat(last_time_str)
                else:
                    logger.warning(f"Last load time file {file_path} is empty. Starting from scratch.")
                    return None
            except ValueError as e:
                logger.error(f"Could not parse last load time from {file_path}: {e}. Starting from scratch.")
                return None
    logger.info(f"Last load time file {file_path} not found. This is likely the first run for this table.")
    return None

def update_last_load_time(file_name: str, current_time: datetime.datetime):
    """Cập nhật thời điểm tải cuối cùng vào file trong thư mục data."""
    file_path = os.path.join(settings.data_dir, file_name)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as f:
        f.write(current_time.isoformat())
    logger.info(f"Updated last load time in {file_path} to {current_time.isoformat()}.")

# --- Hàm kết nối và trích xuất dữ liệu ---
def connect_sql_server():
    """Tạo kết nối tới SQL Server."""
    sql_config = settings.sql_server
    conn_str = (
        f"DRIVER={sql_config.driver};"
        f"SERVER={sql_config.host};"
        f"DATABASE={sql_config.database};"
        f"UID={sql_config.username};"
        f"PWD={sql_config.password}"
    )
    logger.info(f"Attempting to connect to SQL Server: {sql_config.host}/{sql_config.database}")
    return pyodbc.connect(conn_str)

def extract_data_from_sql_server(sql_conn):
    """Trích xuất dữ liệu từ các bảng SQL Server, có hỗ trợ incremental load."""
    logger.info("Starting data extraction from SQL Server.")
    
    data = {}
    for table_name, query_template in settings.raw_sql_queries.items():
        current_query = query_template
        
        if table_name in settings.incremental_config:
            config = settings.incremental_config[table_name]
            last_load_time = get_last_load_time(config['last_load_file'])
            
            if last_load_time:
                last_load_time_str = last_load_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] 
                current_query += f" WHERE {config['time_column']} > '{last_load_time_str}'"
                logger.info(f"  Fetching incremental data for {table_name} since {last_load_time_str} using query: {current_query}")
            else:
                logger.info(f"  No previous load time found for {table_name}. Fetching all data using query: {current_query}")
        else:
            logger.info(f"  Fetching full data for {table_name} using query: {current_query}")

        df = pd.read_sql(current_query, sql_conn)
        data[table_name] = df
        logger.info(f"    Loaded {len(df)} rows for {table_name}.")
    
    logger.info("Data extraction completed.")
    return data

# --- Hàm biến đổi dữ liệu ---
def transform_data(raw_data):
    """Biến đổi dữ liệu."""
    logger.info("Starting data transformation.")
    
    stores_df = raw_data['store']
    err_log_df = raw_data['err_log']
    num_crowd_df = raw_data['num_crowd']

    logger.info("  Transforming num_crowd data...")
    num_crowd_df['recordtime'] = pd.to_datetime(num_crowd_df['recordtime'])
    num_crowd_df = pd.merge(num_crowd_df, stores_df, left_on='storeid', right_on='tid', how='left')
    num_crowd_df.rename(columns={'name': 'store_name'}, inplace=True)
    if 'tid' in num_crowd_df.columns:
        num_crowd_df.drop(columns=['tid'], inplace=True)
    num_crowd_df['total_traffic'] = num_crowd_df['in_num'] + num_crowd_df['out_num']
    num_crowd_df['traffic_date'] = num_crowd_df['recordtime'].dt.date
    num_crowd_df['traffic_hour'] = num_crowd_df['recordtime'].dt.hour
    
    # THÊM CỘT NĂM ĐỂ PARTITION
    num_crowd_df['traffic_year'] = num_crowd_df['recordtime'].dt.year
    logger.info(f"  Added 'traffic_year' column to num_crowd data.")

    logger.info(f"  Transformed {len(num_crowd_df)} rows for num_crowd.")

    logger.info("  Transforming err_log data...")
    err_log_df['LogTime'] = pd.to_datetime(err_log_df['LogTime'])
    err_log_df = pd.merge(err_log_df, stores_df, left_on='storeid', right_on='tid', how='left')
    err_log_df.rename(columns={'name': 'store_name'}, inplace=True)
    if 'tid' in err_log_df.columns:
        err_log_df.drop(columns=['tid'], inplace=True)
    logger.info(f"  Transformed {len(err_log_df)} rows for err_log.")

    logger.info("Data transformation complete.")
    return {
        settings.table_names['num_crowd']: num_crowd_df,
        settings.table_names['err_log']: err_log_df,
        settings.table_names['stores']: stores_df
    }

# --- Hàm tải dữ liệu vào DuckDB và xuất Parquet ---
def load_data_to_duckdb(transformed_data):
    """Tải dữ liệu đã biến đổi vào DuckDB và xuất ra Parquet."""
    logger.info(f"Starting data load to DuckDB at {settings.duckdb_path}.")
    
    con = duckdb.connect(database=settings.duckdb_path, read_only=False)
    
    for table_alias, df in transformed_data.items():
        logger.info(f"  Processing table '{table_alias}' in DuckDB.")
        
        original_table_name = next((k for k, v in settings.table_names.items() if v == table_alias), None)

        if original_table_name and original_table_name in settings.incremental_config:
            logger.info(f"    Appending incremental data to {table_alias}...")
            if not df.empty:
                con.execute(f"INSERT INTO {table_alias} SELECT * FROM df")
                logger.info(f"    Appended {len(df)} rows to {table_alias}.")
            else:
                logger.info(f"    No new data to append for {table_alias}.")
        else:
            logger.info(f"    Replacing existing data for {table_alias}...")
            if not df.empty:
                con.execute(f"CREATE OR REPLACE TABLE {table_alias} AS SELECT * FROM df")
                logger.info(f"    Loaded {len(df)} rows into {table_alias}.")
            else:
                logger.warning(f"    No data to load for {table_alias}. Table might become empty or remain unchanged if it exists.")

    # --- XUẤT PARTITIONED PARQUET ---
    # Chỉ áp dụng partitioning cho bảng num_crowd
    if settings.partition_parquet_by_year and settings.table_names['num_crowd'] in transformed_data:
        num_crowd_table_name = settings.table_names['num_crowd']
        logger.info(f"  Exporting '{num_crowd_table_name}' to partitioned Parquet by year at {settings.output_parquet_base_path}...")
        
        try:
            # Tạo thư mục gốc cho partitioned data nếu chưa có
            os.makedirs(settings.output_parquet_base_path, exist_ok=True)

            # DuckDB có khả năng xuất dữ liệu đã partitioned trực tiếp
            # Sử dụng CONCAT để tạo đường dẫn với thư mục con 'year=YYYY'
            # OVERWRITE_OR_IGNORE TRUE là quan trọng để ghi đè các file nếu chạy lại cùng dữ liệu
            # Mặc định, DuckDB khi COPY TO một thư mục sẽ tạo nhiều file Parquet nhỏ bên trong.
            # Với PARTITION_BY, nó sẽ tạo cấu trúc thư mục.
            con.execute(f"""
                COPY {num_crowd_table_name} 
                TO '{settings.output_parquet_base_path}' 
                (FORMAT PARQUET, CODEC ZSTD, OVERWRITE_OR_IGNORE TRUE, PARTITION_BY (traffic_year));
            """)
            logger.info(f"    Data for '{num_crowd_table_name}' exported to partitioned Parquet.")
        except duckdb.CatalogException as e:
            logger.error(f"    Failed to export partitioned Parquet. Table '{num_crowd_table_name}' might not exist or be empty in DuckDB. Error: {e}")
        except Exception as e:
            logger.error(f"    An unexpected error occurred during partitioned Parquet export: {e}")
    else:
        # Nếu không partition, xuất ra một file Parquet duy nhất như trước (hoặc bỏ qua nếu không cần)
        # Trong trường hợp này, chúng ta sẽ không xuất nếu không bật partitioning để tránh ghi đè
        logger.info("  Partitioning by year is disabled or num_crowd table not found. Skipping partitioned Parquet export.")


    con.close()
    logger.info("DuckDB operations complete.")

def main():
    current_run_time = datetime.datetime.now()
    
    logger.info("ETL process started.")
    try:
        os.makedirs(settings.data_dir, exist_ok=True)

        sql_conn = connect_sql_server()
        raw_data = extract_data_from_sql_server(sql_conn)
        sql_conn.close()
        logger.info("SQL Server connection closed.")
        
        transformed_data = transform_data(raw_data)
        load_data_to_duckdb(transformed_data)
        
        for table_name, config in settings.incremental_config.items():
            if table_name in raw_data and not raw_data[table_name].empty:
                update_last_load_time(config['last_load_file'], current_run_time)

        logger.info("ETL process completed successfully!")
        logger.info(f"Analytics data available in DuckDB at {settings.duckdb_path}.")
        if settings.partition_parquet_by_year:
            logger.info(f"Partitioned Parquet data for traffic is available at {settings.output_parquet_base_path}.")
        else:
            logger.info(f"Parquet data (non-partitioned) would be at {settings.output_parquet_base_path} if enabled.") # Cập nhật thông báo

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.critical(f"SQL Server connection or query error ({sqlstate}): {ex}")
        logger.critical("ETL process failed due to SQL Server error.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during ETL process: {e}", exc_info=True)
        logger.critical("ETL process failed.")

if __name__ == "__main__":
    main()
