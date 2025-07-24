import duckdb
import logging
import pandas as pd

from duckdb import DuckDBPyConnection
from pathlib import Path
from sqlalchemy import create_engine

from app.core.config import settings
from app.utils.logger_config import setup_logging

# --- CONSTANTS ---
ETL_METADATA_TABLE = "etl_metadata"

def get_last_processed_timestamp(duckdb_con: DuckDBPyConnection, table_name: str) -> pd.Timestamp:
    """
    Retrieves the last processed timestamp for a given table from the metadata table.
    Returns a very old date if the table has not been processed before.
    """
    try:
        result = duckdb_con.execute(
            f"SELECT last_timestamp FROM {ETL_METADATA_TABLE} WHERE table_name = ?", [table_name]
        ).fetchone()
        if result:
            logging.info(f"Found last processed timestamp for '{table_name}': {result[0]}")
            return pd.to_datetime(result[0])
    except duckdb.CatalogException:
        logging.warning(f"Metadata table '{ETL_METADATA_TABLE}' not found. Will create it.")
    except Exception as e:
        logging.error(f"Error getting last timestamp for {table_name}: {e}")

    # Default to a very old date for the first run
    return pd.to_datetime("1900-01-01")

def update_last_processed_timestamp(duckdb_con: DuckDBPyConnection, table_name: str, new_timestamp: pd.Timestamp):
    """
    Updates or inserts the last processed timestamp for a given table.
    """
    logging.info(f"Updating last processed timestamp for '{table_name}' to {new_timestamp}")
    duckdb_con.execute(f"""
        CREATE TABLE IF NOT EXISTS {ETL_METADATA_TABLE} (
            table_name VARCHAR,
            last_timestamp TIMESTAMP,
            PRIMARY KEY (table_name)
        );
    """)
    duckdb_con.execute(f"""
        INSERT INTO {ETL_METADATA_TABLE} (table_name, last_timestamp)
        VALUES (?, ?)
        ON CONFLICT (table_name) DO UPDATE
        SET last_timestamp = EXCLUDED.last_timestamp;
    """, [table_name, new_timestamp])

def process_dimension_table(sql_engine, duckdb_con: DuckDBPyConnection):
    """
    Full refresh for the 'store' dimension table.
    It's small, so a full reload is efficient and ensures data consistency.
    """
    logging.info("Processing dimension table: store")
    try:
        df_store = pd.read_sql("SELECT tid, name FROM store", sql_engine)
        # Create or replace the table in DuckDB
        duckdb_con.execute("CREATE OR REPLACE TABLE store AS SELECT * FROM df_store")
        logging.info(f"Successfully loaded {len(df_store)} records into 'store' table.")
    except Exception as e:
        logging.error(f"Failed to process 'store' table: {e}")
        raise

def process_incremental_fact_table(
    sql_engine, duckdb_con: DuckDBPyConnection, source_table_name: str, timestamp_column: str
):
    """
    Processes a fact table incrementally, fetching only new data since the last run.
    Data is written to partitioned Parquet files for optimal analytical performance.
    """
    logging.info(f"Starting incremental processing for '{source_table_name}'...")

    last_ts = get_last_processed_timestamp(duckdb_con, source_table_name)
    max_ts_in_batch = last_ts
    total_rows_processed = 0

    # Base directory for this table's partitions
    partition_base_path = Path(settings.DUCKDB_PARTITION_PATH) / source_table_name
    partition_base_path.mkdir(parents=True, exist_ok=True)

    sql_query = f"SELECT * FROM {source_table_name} WHERE {timestamp_column} > ? ORDER BY {timestamp_column}"

    try:
        # Process data in chunks to manage memory usage
        for chunk_df in pd.read_sql_query(sql_query, sql_engine, params=(last_ts,), chunksize=settings.ETL_CHUNK_SIZE):
            if chunk_df.empty:
                logging.info(f"No new data found for '{source_table_name}'.")
                break

            logging.info(f"Processing a chunk of {len(chunk_df)} rows for '{source_table_name}'...")

            # --- Transformations ---
            # Ensure timestamp column is in datetime format
            chunk_df[timestamp_column] = pd.to_datetime(chunk_df[timestamp_column])

            # Add partition columns
            chunk_df['year'] = chunk_df[timestamp_column].dt.year
            chunk_df['month'] = chunk_df[timestamp_column].dt.month

            # Write to partitioned Parquet files
            chunk_df.to_parquet(
                path=partition_base_path,
                engine='pyarrow',
                partition_cols=['storeid', 'year', 'month']
            )

            # Update max timestamp and row count
            batch_max = chunk_df[timestamp_column].max()
            if batch_max > max_ts_in_batch:
                max_ts_in_batch = batch_max
            total_rows_processed += len(chunk_df)

        if total_rows_processed > 0:
            logging.info(f"Finished processing {total_rows_processed} new rows for '{source_table_name}'.")
            # Register partitioned dataset as a view in DuckDB for easy querying
            duckdb_con.execute(f"""
                CREATE OR REPLACE VIEW {source_table_name}_view AS 
                SELECT * FROM read_parquet('{str(partition_base_path)}/**/*.parquet', hive_partitioning=1);
            """)
            logging.info(f"Created/updated view '{source_table_name}_view' in DuckDB.")

            # Update metadata with the latest timestamp from this run
            update_last_processed_timestamp(duckdb_con, source_table_name, max_ts_in_batch)
        else:
            logging.info(f"No new records to process for '{source_table_name}'.")
    except Exception as e:
        logging.error(f"An error occurred during incremental processing of '{source_table_name}': {e}")
        raise

def main():
    """Main ETL execution function."""
    setup_logging('etl_process.log')
    logging.info("=============================================")
    logging.info("Starting ETL process...")

    duckdb_con = None
    try:
        # Create parent directory for DuckDB if it doesn't exist
        Path(settings.DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

        # --- Connections ---
        logging.info(f"Connecting to SQL Server: {settings.DB_SERVER}/{settings.DB_DATABASE}")
        sql_engine = create_engine(settings.sqlalchemy_db_uri)

        logging.info(f"Connecting to DuckDB at: {settings.DUCKDB_PATH}")
        duckdb_con = duckdb.connect(database=settings.DUCKDB_PATH, read_only=False)

        # --- ETL Steps ---
        for table_config in settings.ETL_TABLES_CONFIG:
            table_name = table_config["name"]
            logging.info(f"--- Processing table: {table_name} ---")
            if table_config["type"] == "dimension":
                # 1. Process dimension table (full refresh)
                process_dimension_table(sql_engine, duckdb_con, table_config)
            elif table_config["type"] == "fact":
                process_incremental_fact_table(sql_engine, duckdb_con, table_config)

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}", exc_info=True)
    finally:
        if duckdb_con:
            duckdb_con.close()
            logging.info("DuckDB connection closed.")
        logging.info("=============================================\n")

if __name__ == "__main__":
    main()








# import pyodbc


# # 1. ================== LOGGER SETUP ==================
# def setup_logger():
#     """Configures the logger for the ETL process."""
#     logger = logging.getLogger(__name__)
#     logger.setLevel(logging.INFO)

#     # Prevent duplicate handlers
#     if logger.hasHandlers():
#         logger.handlers.clear()

#     # Console handler
#     c_handler = logging.StreamHandler()
#     c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     c_handler.setFormatter(c_format)
#     logger.addHandler(c_handler)

#     # File handler
#     f_handler = logging.FileHandler(settings.LOG_FILE_PATH)
#     f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     f_handler.setFormatter(f_format)
#     logger.addHandler(f_handler)

#     return logger

# logger = setup_logger()

# # 2. ================== EXTRACT ==================
# def extract_from_sql_server(table_name: str) -> pd.DataFrame:
#     """Extracts data from a specified table in SQL Server."""
#     logger.info(f"Extracting data from table: {table_name}")
#     try:
#         conn_str = (
#             f"DRIVER={settings.DB_DRIVER};"
#             f"SERVER={settings.DB_SERVER};"
#             f"DATABASE={settings.DB_DATABASE};"
#             f"UID={settings.DB_USER};"
#             f"PWD={settings.DB_PASSWORD};"
#             "MARS_Connection=yes;"
#         )
#         with pyodbc.connect(conn_str) as cnxn:
#             query = f"SELECT * FROM dbo.{table_name}"
#             df = pd.read_sql(query, cnxn)
#             logger.info(f"Successfully extracted {len(df)} rows from {table_name}.")
#             return df
#     except Exception as e:
#         logger.error(f"Failed to extract from {table_name}. Error: {e}")
#         raise

# # 3. ================== TRANSFORM ==================
# def transform_data(df: pd.DataFrame, time_column: str = None) -> pd.DataFrame:
#     """
#     Transforms data for loading, adding partitioning columns if a time_column is provided.
#     """
#     if df.empty:
#         return df

#     if time_column and time_column in df.columns:
#         logger.info(f"Adding 'year' and 'month' columns from '{time_column}'.")
#         # Ensure the column is datetime
#         df[time_column] = pd.to_datetime(df[time_column])
#         df['year'] = df[time_column].dt.year
#         df['month'] = df[time_column].dt.month
    
#     # Other transformations can be added here
#     # e.g., df['column'] = df['column'].astype('new_type')

#     return df

# # 4. ================== LOAD ==================
# def load_to_duckdb_partitioned(
#     df: pd.DataFrame, 
#     table_name: str, 
#     partition_cols: list = None
# ):
#     """
#     Loads a DataFrame into DuckDB by saving it as a partitioned Parquet file.
#     """
#     if df.empty:
#         logger.warning(f"DataFrame for table '{table_name}' is empty. Skipping load.")
#         return

#     output_dir = f"{settings.DUCKDB_DATA_DIR}/{table_name}"
#     logger.info(f"Loading data for '{table_name}' into partitioned Parquet at: {output_dir}")

#     try:
#         with duckdb.connect(database=settings.DUCKDB_PATH, read_only=False) as con:
#             # Register DataFrame as a temporary view to use DuckDB's SQL power
#             con.register('temp_df', df)

#             # Build the COPY statement
#             copy_sql = f"COPY (SELECT * FROM temp_df) TO '{output_dir}' "
#             if partition_cols:
#                 copy_sql += f"(FORMAT PARQUET, PARTITION_BY ({', '.join(partition_cols)}), OVERWRITE_OR_IGNORE 1);"
#             else:
#                 copy_sql += "(FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);"
            
#             con.execute(copy_sql)
            
#             # Create a view in DuckDB to easily query the partitioned data
#             logger.info(f"Creating or replacing view 'v_{table_name}' in DuckDB.")
#             con.execute(f"CREATE OR REPLACE VIEW v_{table_name} AS SELECT * FROM read_parquet('{output_dir}/**/*.parquet');")
            
#         logger.info(f"Successfully loaded data for '{table_name}'.")

#     except Exception as e:
#         logger.error(f"Failed to load data for {table_name}. Error: {e}")
#         raise

# # 5. ================== MAIN ETL PIPELINE ==================
# def run_etl():
#     """Main function to run the full ETL pipeline."""
#     logger.info("====== Starting ETL Process ======")

#     tables_to_process = {
#         'store': {'time_col': None, 'partition_cols': None},
#         'ErrLog': {'time_col': 'LogTime', 'partition_cols': ['year', 'month']},
#         'num_crowd': {'time_col': 'recordtime', 'partition_cols': ['year', 'month']}
#     }

#     for table, config in tables_to_process.items():
#         try:
#             # EXTRACT
#             df_raw = extract_from_sql_server(table)

#             # TRANSFORM
#             df_transformed = transform_data(df_raw, time_column=config['time_col'])

#             # LOAD
#             load_to_duckdb_partitioned(
#                 df_transformed, 
#                 table_name=table, 
#                 partition_cols=config['partition_cols']
#             )
#         except Exception as e:
#             logger.critical(f"ETL failed for table '{table}'. Halting process for this table. Error: {e}")
#             continue # Continue to the next table

#     logger.info("====== ETL Process Finished Successfully ======")


# if __name__ == "__main__":
#     run_etl()









# import os

# from datetime import datetime

# from sqlalchemy.exc import SQLAlchemyError
# from typing import Optional

# # --- Cấu hình Logger ---
# logger = setup_logging(
#     log_file=settings.logging.log_file,
#     log_level=settings.logging.log_level,
#     log_format=settings.logging.log_format,
#     log_date_format=settings.logging.log_date_format,
#     log_dir=settings.data_dir
# )

# # --- Các hàm hỗ trợ Incremental Load (Giữ nguyên) ---
# def get_last_load_time(file_name: str) -> Optional[datetime]:
#     """Đọc thời điểm tải cuối cùng từ file trong thư mục data."""
#     file_path = os.path.join(settings.data_dir, file_name)
#     if os.path.exists(file_path):
#         with open(file_path, 'r') as f:
#             try:
#                 last_time_str = f.read().strip()
#                 if last_time_str:
#                     return datetime.fromisoformat(last_time_str)
#                 else:
#                     logger.warning(f"Last load time file {file_path} is empty. Starting from scratch.")
#                     return None
#             except ValueError as e:
#                 logger.error(f"Could not parse last load time from {file_path}: {e}. Starting from scratch.")
#                 return None
#     logger.info(f"Last load time file {file_path} not found. This is likely the first run for this table.")
#     return None

# def update_last_load_time(file_name: str, current_time: datetime):
#     """Cập nhật thời điểm tải cuối cùng vào file trong thư mục data."""
#     file_path = os.path.join(settings.data_dir, file_name)
#     os.makedirs(os.path.dirname(file_path), exist_ok=True)
#     with open(file_path, 'w') as f:
#         f.write(current_time.isoformat())
#     logger.info(f"Updated last load time in {file_path} to {current_time.isoformat()}.")

# # --- Hàm kết nối (Sử dụng SQLAlchemy) ---
# def connect_sql_server():
#     """Tạo SQLAlchemy Engine để kết nối tới SQL Server."""
#     sql_config = settings.sql_server
#     try:
#         engine = create_engine(sql_config.SQLALCHEMY_DATABASE_URI)
#         # Thử kết nối để xác minh
#         with engine.connect() as connection:
#             logger.info(f"Successfully connected to SQL Server: {sql_config.host}/{sql_config.database}")
#         return engine
#     except SQLAlchemyError as e:
#         logger.critical(f"SQLAlchemy connection error: {e}")
#         raise # Ném lại lỗi để dừng quá trình ETL

# # --- Hàm trích xuất dữ liệu (Sử dụng SQLAlchemy Engine với Pandas) ---
# def extract_data_from_sql_server(sql_engine): # Tham số đã thay đổi thành sql_engine
#     """Trích xuất dữ liệu từ các bảng SQL Server, có hỗ trợ incremental load."""
#     logger.info("Starting data extraction from SQL Server using SQLAlchemy.")
    
#     data = {}
#     for table_name, query_template in settings.raw_sql_queries.items():
#         current_query = query_template
        
#         if table_name in settings.incremental_config:
#             config = settings.incremental_config[table_name]
#             last_load_time = get_last_load_time(config['last_load_file'])
            
#             if last_load_time:
#                 last_load_time_str = last_load_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] 
#                 current_query += f" WHERE {config['time_column']} > '{last_load_time_str}'"
#                 logger.info(f"  Fetching incremental data for {table_name} since {last_load_time_str} using query: {current_query}")
#             else:
#                 logger.info(f"  No previous load time found for {table_name}. Fetching all data using query: {current_query}")
#         else:
#             logger.info(f"  Fetching full data for {table_name} using query: {current_query}")

#         # Thay thế pd.read_sql(query, sql_conn) bằng pd.read_sql(query, sql_engine)
#         df = pd.read_sql(current_query, sql_engine) 
#         data[table_name] = df
#         logger.info(f"    Loaded {len(df)} rows for {table_name}.")
    
#     logger.info("Data extraction completed.")
#     return data

# # --- Hàm biến đổi dữ liệu (Giữ nguyên) ---
# def transform_data(raw_data):
#     """Biến đổi dữ liệu."""
#     logger.info("Starting data transformation.")
    
#     stores_df = raw_data['store']
#     err_log_df = raw_data['err_log']
#     num_crowd_df = raw_data['num_crowd']

#     logger.info("  Transforming num_crowd data...")
#     num_crowd_df['recordtime'] = pd.to_datetime(num_crowd_df['recordtime'])
#     num_crowd_df = pd.merge(num_crowd_df, stores_df, left_on='storeid', right_on='tid', how='left')
#     num_crowd_df.rename(columns={'name': 'store_name'}, inplace=True)
#     if 'tid' in num_crowd_df.columns:
#         num_crowd_df.drop(columns=['tid'], inplace=True)
#     num_crowd_df['total_traffic'] = num_crowd_df['in_num'] + num_crowd_df['out_num']
#     num_crowd_df['traffic_date'] = num_crowd_df['recordtime'].dt.date
#     num_crowd_df['traffic_hour'] = num_crowd_df['recordtime'].dt.hour
    
#     num_crowd_df['traffic_year'] = num_crowd_df['recordtime'].dt.year
#     logger.info(f"  Added 'traffic_year' column to num_crowd data.")

#     logger.info(f"  Transformed {len(num_crowd_df)} rows for num_crowd.")

#     logger.info("  Transforming err_log data...")
#     err_log_df['LogTime'] = pd.to_datetime(err_log_df['LogTime'])
#     err_log_df = pd.merge(err_log_df, stores_df, left_on='storeid', right_on='tid', how='left')
#     err_log_df.rename(columns={'name': 'store_name'}, inplace=True)
#     if 'tid' in err_log_df.columns:
#         err_log_df.drop(columns=['tid'], inplace=True)
#     logger.info(f"  Transformed {len(err_log_df)} rows for err_log.")

#     logger.info("Data transformation complete.")
#     return {
#         settings.table_names['num_crowd']: num_crowd_df,
#         settings.table_names['err_log']: err_log_df,
#         settings.table_names['stores']: stores_df
#     }

# # --- Hàm tải dữ liệu vào DuckDB và xuất Parquet (Giữ nguyên) ---
# def load_data_to_duckdb(transformed_data):
#     """Tải dữ liệu đã biến đổi vào DuckDB và xuất ra Parquet."""
#     logger.info(f"Starting data load to DuckDB at {settings.duckdb_path}.")
    
#     con = duckdb.connect(database=settings.duckdb_path, read_only=False)
    
#     for table_alias, df in transformed_data.items():
#         logger.info(f"  Processing table '{table_alias}' in DuckDB.")
        
#         original_table_name = next((k for k, v in settings.table_names.items() if v == table_alias), None)

#         if original_table_name and original_table_name in settings.incremental_config:
#             logger.info(f"    Appending incremental data to {table_alias}...")
#             if not df.empty:
#                 con.execute(f"INSERT INTO {table_alias} SELECT * FROM df")
#                 logger.info(f"    Appended {len(df)} rows to {table_alias}.")
#             else:
#                 logger.info(f"    No new data to append for {table_alias}.")
#         else:
#             logger.info(f"    Replacing existing data for {table_alias}...")
#             if not df.empty:
#                 con.execute(f"CREATE OR REPLACE TABLE {table_alias} AS SELECT * FROM df")
#                 logger.info(f"    Loaded {len(df)} rows into {table_alias}.")
#             else:
#                 logger.warning(f"    No data to load for {table_alias}. Table might become empty or remain unchanged if it exists.")

#     # --- XUẤT PARTITIONED PARQUET HOẶC NON-PARTITIONED ---
#     num_crowd_table_name = settings.table_names['num_crowd']
#     num_crowd_df_to_export = transformed_data.get(num_crowd_table_name)

#     if num_crowd_df_to_export is None or num_crowd_df_to_export.empty:
#         logger.info(f"  No data available for '{num_crowd_table_name}'. Skipping Parquet export.")
#     else:
#         if settings.partition_parquet_by_year:
#             logger.info(f"  Exporting '{num_crowd_table_name}' to partitioned Parquet by year at {settings.output_parquet_base_path}...")
#             try:
#                 os.makedirs(settings.output_parquet_base_path, exist_ok=True)
#                 con.execute(f"""
#                     COPY {num_crowd_table_name} 
#                     TO '{settings.output_parquet_base_path}' 
#                     (FORMAT PARQUET, CODEC ZSTD, OVERWRITE_OR_IGNORE TRUE, PARTITION_BY (traffic_year));
#                 """)
#                 logger.info(f"    Data for '{num_crowd_table_name}' exported to partitioned Parquet successfully.")
#             except duckdb.CatalogException as e:
#                 logger.error(f"    Failed to export partitioned Parquet. Error: {e}")
#             except Exception as e:
#                 logger.error(f"    An unexpected error occurred during partitioned Parquet export: {e}")
#         else:
#             output_single_parquet_path = f"{settings.data_dir}/traffic_analytics_all_years.parquet"
#             logger.info(f"  Exporting '{num_crowd_table_name}' to single Parquet file at {output_single_parquet_path} (partitioning disabled).")
#             try:
#                 con.execute(f"COPY {num_crowd_table_name} TO '{output_single_parquet_path}' (FORMAT PARQUET, CODEC ZSTD, OVERWRITE_OR_IGNORE TRUE)")
#                 logger.info(f"    Data for '{num_crowd_table_name}' exported to single Parquet file successfully.")
#             except duckdb.CatalogException as e:
#                 logger.error(f"    Failed to export single Parquet. Error: {e}")
#             except Exception as e:
#                 logger.error(f"    An unexpected error occurred during single Parquet export: {e}")

#     con.close()
#     logger.info("DuckDB operations complete.")

# def main():
#     current_run_time = datetime.now()
    
#     logger.info("ETL process started.")
#     # Đảm bảo thư mục 'data' tồn tại trước khi làm bất cứ điều gì
#     os.makedirs(settings.data_dir, exist_ok=True)

#     sql_engine = None # Khởi tạo biến engine để đảm bảo nó tồn tại
#     try:
#         sql_engine = connect_sql_server() # connect_sql_server giờ trả về một SQLAlchemy Engine
#         raw_data = extract_data_from_sql_server(sql_engine) # Truyền engine vào extract_data_from_sql_server
        
#         # Không cần đóng engine ở đây, SQLAlchemy tự quản lý pool và kết nối
#         logger.info("SQL Server data extraction completed.")
        
#         transformed_data = transform_data(raw_data)
#         load_data_to_duckdb(transformed_data)
        
#         for table_name, config in settings.incremental_config.items():
#             if table_name in raw_data and not raw_data[table_name].empty:
#                 update_last_load_time(config['last_load_file'], current_run_time)

#         logger.info("ETL process completed successfully!")
#         logger.info(f"Analytics data available in DuckDB at {settings.duckdb_path}.")
#         if settings.partition_parquet_by_year:
#             logger.info(f"Partitioned Parquet data for traffic is available at {settings.output_parquet_base_path}.")
#         else:
#             logger.info(f"Non-partitioned Parquet data for traffic is available at {settings.data_dir}/traffic_analytics_all_years.parquet.")

#     except SQLAlchemyError as ex: # Bắt lỗi SQLAlchemy cụ thể
#         logger.critical(f"SQLAlchemy or SQL Server error: {ex}", exc_info=True)
#         logger.critical("ETL process failed due to database error.")
#     except Exception as e:
#         logger.critical(f"An unexpected error occurred during ETL process: {e}", exc_info=True)
#         logger.critical("ETL process failed.")
