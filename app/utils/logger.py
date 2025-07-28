import logging
import os
import sys

from logging.handlers import TimedRotatingFileHandler

# Đọc cấu hình thư mục log từ biến môi trường, nếu không có thì dùng 'logs'
# Đây là một pattern rất phổ biến để tăng tính linh hoạt cho ứng dụng
DEFAULT_LOG_DIR = os.getenv('LOG_DIR', 'logs')

def get_logger(
    logger_name: str,
    log_file: str,
    level: int = logging.INFO,
    log_dir: str = DEFAULT_LOG_DIR
) -> logging.Logger:
    """
    Thiết lập và trả về một logger chuyên nghiệp, linh hoạt.

    Điểm cải tiến so với phiên bản gốc:
    - Cấp độ log (level) và thư mục log (log_dir) có thể được tùy chỉnh khi gọi hàm.
    - Thư mục log mặc định có thể được cấu hình qua biến môi trường LOG_DIR.
    - Thêm handler cho cả stdout và stderr để phân luồng log thông thường và log lỗi.

    Args:
        logger_name (str): Tên của logger (vd: 'ETL_Process').
        log_file (str): Tên file log (không có extension, vd: 'etl_app').
        level (int, optional): Cấp độ log tối thiểu. Mặc định là logging.INFO.
        log_dir (str, optional): Thư mục để lưu file log. Mặc định là giá trị của
                                 biến môi trường LOG_DIR hoặc 'logs'.

    Returns:
        logging.Logger: Một instance của logger đã được cấu hình.
    """
    # Tạo thư mục logs nếu chưa tồn tại
    os.makedirs(log_dir, exist_ok=True)

    # 1. Lấy logger và đặt level
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # 2. Ngăn chặn việc thêm handler nếu logger đã được cấu hình
    if logger.hasHandlers():
        return logger

    # 3. Tạo Formatter chung
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 4. Cấu hình Console Handler (hiển thị log ra màn hình)
    # Ghi các log từ INFO trở xuống ra sys.stdout (luồng ra chuẩn)
    console_out_handler = logging.StreamHandler(sys.stdout)
    console_out_handler.setFormatter(log_format)
    # Chỉ xử lý log có level thấp hơn WARNING
    console_out_handler.addFilter(lambda record: record.levelno < logging.WARNING)

    # Ghi các log từ WARNING trở lên ra sys.stderr (luồng lỗi chuẩn)
    console_err_handler = logging.StreamHandler(sys.stderr)
    console_err_handler.setFormatter(log_format)

    # Chỉ xử lý log có level từ WARNING trở lên
    console_err_handler.setLevel(logging.WARNING)

    # 5. Cấu hình File Handler (ghi log vào file)
    file_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, log_file + '.log'),
        when='midnight',
        interval=1,
        backupCount=14,  # Tăng số lượng backup lên 14 ngày
        encoding='utf-8'
    )
    file_handler.setFormatter(log_format)

    # 6. Thêm các handler vào logger
    logger.addHandler(console_out_handler)
    logger.addHandler(console_err_handler)
    logger.addHandler(file_handler)

    # 7. Set propagate to False để tránh log bị truyền lên logger gốc (root logger)
    # Điều này giúp kiểm soát output tốt hơn trong các ứng dụng phức tạp.
    logger.propagate = False

    return logger
