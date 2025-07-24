import logging
import os
import sys

def setup_logging(log_file: str, log_dir: str='logs'):
    """
    Configures logging to output to both console and a file.
    """
    log_file_path = os.path.join(log_dir, log_file)
    os.makedirs(log_dir, exist_ok=True) # Đảm bảo thư mục log tồn tại

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(sys.stdout)
        ]
    )




# # from typing import Optional

# # Định nghĩa một logger cấp module để có thể gọi từ bên ngoài
# _logger_initialized = False     # Biến cờ để đảm bảo logger chỉ được cấu hình một lần

# def setup_logging(
#     log_file: str,
#     log_level: str='INFO',
#     log_format: str='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     log_date_format: str='%Y-%m-%d %H:%M:%S',
#     log_dir: str='logs'
# ) -> logging.Logger:
#     """
#     Cấu hình hệ thống ghi log và trả về đối tượng logger.

#     Args:
#         log_file (str): Tên file log.
#         log_level (str): Mức độ log tối thiểu (DEBUG, INFO, WARNING, ERROR, CRITICAL).
#         log_format (str): Định dạng chuỗi cho mỗi dòng log.
#         log_date_format (str): Định dạng thời gian trong log.
#         log_dir (str): Thư mục chứa file log.

#     Returns:
#         logging.Logger: Đối tượng logger đã được cấu hình.
#     """
#     global _logger_initialized # Sử dụng biến global

#     # Chỉ cấu hình logger một lần
#     if _logger_initialized:
#         return logging.getLogger(__name__.split('.')[-1]) # Trả về logger đã có

#     # Lấy logger gốc, thường dùng tên của module chính (ví dụ: 'etl_app')
#     # hoặc một tên chung cho tất cả các log từ ứng dụng này.
#     logger = logging.getLogger('ETL_App_Logger') # Đặt tên logger cụ thể
#     logger.setLevel(log_level.upper())

#     # Clear handlers hiện có để tránh trùng lặp khi hàm được gọi nhiều lần (ví dụ trong testing)
#     if logger.hasHandlers():
#         logger.handlers.clear()

#     # Tạo FileHandler để ghi log vào file
#     file_handler = logging.FileHandler(log_file_path)
#     file_handler.setLevel(log_level.upper())

#     # Tạo StreamHandler để ghi log ra console
#     console_handler = logging.StreamHandler()
#     console_handler.setLevel(logging.INFO) # Console thường chỉ cần INFO trở lên

#     # Định dạng log
#     formatter = logging.Formatter(log_format, datefmt=log_date_format)
#     file_handler.setFormatter(formatter)
#     console_handler.setFormatter(formatter)

#     # Thêm handlers vào logger
#     logger.addHandler(file_handler)
#     logger.addHandler(console_handler)

#     _logger_initialized = True # Đánh dấu logger đã được cấu hình

#     return logger
