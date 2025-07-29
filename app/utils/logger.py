import logging
import os
import sys

from logging.handlers import TimedRotatingFileHandler

# Sử dụng một class Filter chuyên dụng.
class InfoLevelFilter(logging.Filter):
    """
    Filter này chỉ cho phép các record log có leveldưới WARNING (tức là DEBUG và INFO) đi qua.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.WARNING

def get_logger(logger_name: str, log_file: str, level: int = logging.INFO, log_dir: str = 'logs') -> logging.Logger:
    """
    Thiết lập và trả về một logger chuyên nghiệp, linh hoạt.

    Args:
        logger_name (str): Tên của logger (vd: 'ETL_Process').
        log_file (str): Tên file log (không có extension, vd: 'etl_app').
        level (int, optional): Cấp độ log tối thiểu. Mặc định là logging.INFO.
        log_dir (str, optional): Thư mục để lưu file log. Mặc định là 'logs'.

    Returns:
        logging.Logger: Một instance của logger đã được cấu hình.
    """
    # Tạo thư mục logs nếu chưa tồn tại
    os.makedirs(log_dir, exist_ok=True)

    # 1. Lấy logger và đặt level
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # 2. Set propagate to False để tránh log bị truyền lên logger gốc (root logger)
    # Điều này giúp kiểm soát output tốt hơn trong các ứng dụng phức tạp.
    logger.propagate = False

    # 3. Ngăn chặn việc thêm handler nếu logger đã được cấu hình
    if logger.hasHandlers():
        return logger

    # 4. Tạo Formatter chung
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 5. Cấu hình Console Handler (hiển thị log ra màn hình)
    # Ghi các log từ INFO trở xuống ra sys.stdout (luồng ra chuẩn)
    console_out_handler = logging.StreamHandler(sys.stdout)
    console_out_handler.setFormatter(log_format)

    # Chỉ xử lý log có level thấp hơn WARNING
    console_out_handler.addFilter(InfoLevelFilter())

    # Ghi các log từ WARNING trở lên ra sys.stderr (luồng lỗi chuẩn)
    console_err_handler = logging.StreamHandler(sys.stderr)
    console_err_handler.setFormatter(log_format)

    # Chỉ xử lý log có level từ WARNING trở lên
    console_err_handler.setLevel(logging.WARNING)

    # 6. Cấu hình File Handler (ghi log vào file, xoay vòng theo ngày)
    file_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, log_file + '.log'),
        when='midnight',
        interval=1,
        backupCount=14, # Giữ lại log của 14 ngày gần nhất
        encoding='utf-8'
    )
    file_handler.setFormatter(log_format)

    # 7. Thêm các handler vào logger
    logger.addHandler(console_out_handler)
    logger.addHandler(console_err_handler)
    logger.addHandler(file_handler)

    return logger
