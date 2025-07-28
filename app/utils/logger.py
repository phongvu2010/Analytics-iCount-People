import logging
import os

from logging.handlers import TimedRotatingFileHandler

LOG_DIR = 'logs'

def get_logger(logger_name: str, log_file: str):
    """
    Thiết lập và trả về một logger chuyên nghiệp.

    - Ghi log ra cả console và file.
    - Tự động xoay vòng file log vào mỗi nửa đêm (giữ lại 7 file cũ).
    - Ngăn chặn việc thêm handler trùng lặp để tránh log bị lặp lại.

    Args:
        logger_name (str): Tên của logger (vd: 'ETL').
        log_file (str): Tên file log (không có extension, vd: 'etl_app').

    Returns:
        logging.Logger: Một instance của logger đã được cấu hình.
    """
    # Tạo thư mục logs nếu chưa tồn tại
    os.makedirs(LOG_DIR, exist_ok=True)

    # 1. Lấy logger và đặt level
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # 2. Ngăn chặn việc thêm handler nếu logger đã được cấu hình
    # Điều này rất quan trọng để tránh log bị lặp lại.
    if logger.hasHandlers():
        return logger

    # 3. Tạo Formatter
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 4. Cấu hình Console Handler (để hiển thị log trên màn hình)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # 5. Cấu hình File Handler (để ghi log vào file)
    # Sử dụng TimedRotatingFileHandler để tự động xoay vòng file log
    #    when='midnight': Xoay vòng vào mỗi nửa đêm
    #    backupCount=7: Giữ lại 7 file log cũ nhất (etl_app.log.2025-07-26, ...)
    file_handler = TimedRotatingFileHandler(
        os.path.join(LOG_DIR, log_file + '.log'),
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    file_handler.setFormatter(log_format)

    # 6. Thêm các handler vào logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
