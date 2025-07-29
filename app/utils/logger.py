import logging
import os
import sys

from logging.handlers import TimedRotatingFileHandler
from typing import Optional

def get_logger(
    logger_name: str,
    level: int = logging.INFO,
    console_enabled: bool = True,
    log_file: Optional[str] = 'app.log',
    log_dir: str = 'logs',
    when: str = 'midnight',
    interval: int = 1,
    backup_count: int = 14,
    log_format_str: Optional[str] = None
) -> logging.Logger:
    """
    Thiết lập và trả về một logger chuyên nghiệp, linh hoạt và an toàn.

    Args:
        logger_name (str): Tên của logger.
        level (int, optional): Cấp độ log tối thiểu. Mặc định là logging.INFO.
        console_enabled (bool, optional): Bật/tắt ghi log ra console. Mặc định là True.
        log_dir (str, optional): Thư mục để lưu file log. Mặc định là 'logs'.
        log_file (str | None, optional): Tên file log. Nếu là None, sẽ không ghi log ra file.
        backup_count (int, optional): Số lượng file backup để giữ lại. Mặc định là 14.
        log_format_str (str | None, optional): Chuỗi định dạng log. Nếu None, dùng định dạng mặc định.

    Returns:
        logging.Logger: Một instance của logger đã được cấu hình.
    """
    # Lấy logger và đặt level
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Set propagate to False để tránh log bị truyền lên logger gốc (root logger)
    # Điều này giúp kiểm soát output tốt hơn trong các ứng dụng phức tạp.
    logger.propagate = False

    # Ngăn chặn việc thêm handler nếu logger đã được cấu hình
    if logger.hasHandlers():
        return logger

    # Tạo Formatter chung, cho phép override từ bên ngoài
    if log_format_str is None:
        log_format_str = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'

    log_format = logging.Formatter(log_format_str, datefmt='%Y-%m-%d %H:%M:%S')

    # Cấu hình Console Handler (nếu được bật hiển thị log ra màn hình)
    if console_enabled:
        # Ghi các log từ INFO trở xuống ra sys.stdout (luồng ra chuẩn)
        console_out_handler = logging.StreamHandler(sys.stdout)
        console_out_handler.setFormatter(log_format)

        # Chỉ xử lý log có level thấp hơn WARNING
        console_out_handler.addFilter(lambda record: record.levelno < logging.WARNING)
        logger.addHandler(console_out_handler)

        # Ghi các log từ WARNING trở lên ra sys.stderr (luồng lỗi chuẩn)
        console_err_handler = logging.StreamHandler(sys.stderr)
        console_err_handler.setFormatter(log_format)

        # Chỉ xử lý log có level từ WARNING trở lên
        console_err_handler.setLevel(logging.WARNING)
        logger.addHandler(console_err_handler)

    # Cấu hình File Handler (nếu được bật và có tên file)
    if log_file:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            os.path.join(log_dir, log_file),
            when=when,          # Sử dụng tham số
            interval=interval,  # Sử dụng tham số
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)

    return logger
