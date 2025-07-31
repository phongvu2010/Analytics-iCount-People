import logging
import logging.config
import yaml
import os

from pathlib import Path
from typing import Union

class LevelFilter(logging.Filter):
    """
    Lọc các bản ghi log có level THẤP HƠN level được chỉ định.

    Ví dụ, nếu level được set là WARNING, filter này sẽ chỉ cho phép
    các bản ghi DEBUG và INFO đi qua.

    Attributes:
        level (int): Giá trị số của logging level để so sánh.
    """
    def __init__(self, level: Union[str, int], **kwargs):
        """
        Khởi tạo LevelFilter.

        Args:
            level (Union[str, int]): Tên level (ví dụ: 'WARNING') hoặc giá trị số của level (ví dụ: 30).
        """
        super().__init__(**kwargs)
        if isinstance(level, str):
            # Chuyển đổi tên level dạng chuỗi (không phân biệt hoa thường)
            # thành giá trị số tương ứng.
            self.level = logging.getLevelNamesMapping()[level.upper()]
        else:
            self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Thực hiện lọc bản ghi log.

        Args:
            record (logging.LogRecord): Đối tượng bản ghi log cần kiểm tra.

        Returns:
            bool: True nếu level của bản ghi thấp hơn level của filter, ngược lại là False.
        """
        return record.levelno < self.level

# Lấy đường dẫn của thư mục chứa file logger.py này
CURRENT_DIR = Path(__file__).parent
# Tạo đường dẫn mặc định đến file logging.yaml trong cùng thư mục
DEFAULT_CONFIG_PATH = CURRENT_DIR / 'logger.yaml'

def setup_logging(
    config_path: Union[str, Path] = DEFAULT_CONFIG_PATH,
    default_level: int = logging.INFO
) -> None:
    """
    Cấu hình hệ thống logging từ file YAML.

    Hàm này sẽ đọc file cấu hình YAML, thiết lập các thư mục cần thiết,
    và áp dụng cấu hình cho module logging của Python. Nếu file cấu hình
    không tồn tại hoặc có lỗi, nó sẽ chuyển sang sử dụng cấu hình logging
    cơ bản (basicConfig) để đảm bảo ứng dụng vẫn có thể ghi log.

    Args:
        config_path (Union[str, Path]): Đường dẫn đến file logging.yaml.
        default_level (int): Log level mặc định sẽ được sử dụng nếu cấu hình từ file thất bại.
    """
    # Đảm bảo đường dẫn là đối tượng Path để xử lý nhất quán.
    config_path = Path(config_path)

    # Tạo thư mục 'logs' để chứa file log nếu chưa tồn tại.
    # Đây là một thực hành tốt để tránh lỗi khi handler cố gắng ghi file.
    log_dir = Path('logs')
    log_dir.mkdir(parents=True, exist_ok=True)

    if not config_path.is_file():
        logging.basicConfig(level=default_level)
        logging.error(f"Không tìm thấy file cấu hình tại '{config_path}'. Sử dụng cấu hình cơ bản.")
        return

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)

        if config_dict:
            # Lấy log level từ biến môi trường để ghi đè.
            # Điều này rất hữu ích trong môi trường production/staging/dev.
            log_level_from_env = os.environ.get('LOG_LEVEL')
            if log_level_from_env and 'root' in config_dict:
                print(f"Phát hiện biến môi trường LOG_LEVEL='{log_level_from_env}'. Ghi đè cấu hình.")
                config_dict['root']['level'] = log_level_from_env.upper()

            # Dùng dictConfig để áp dụng toàn bộ cấu hình từ file YAML.
            # Đây là trái tim của phương pháp này.
            logging.config.dictConfig(config_dict)
            logging.info(f"Hệ thống logging đã được cấu hình thành công từ file: {config_path}")
        else:
            # Xử lý trường hợp file YAML tồn tại nhưng trống hoặc không hợp lệ.
            raise ValueError("File YAML rỗng hoặc không hợp lệ.")
    except Exception as e:
        # Nếu có bất kỳ lỗi nào xảy ra trong quá trình đọc và áp dụng file YAML
        # (ví dụ: cú pháp YAML sai, giá trị không hợp lệ),
        # hệ thống sẽ chuyển về cấu hình cơ bản để không bị sập.
        logging.basicConfig(level=default_level)
        logging.exception(f"Lỗi khi cấu hình logging từ file YAML: {e}")



# def run_exam():
#     """Hàm chính của ứng dụng để minh họa việc sử dụng logger."""
#     logger = logging.getLogger(__name__)

#     logger.debug("Đây là một thông điệp debug.")
#     logger.info("Ứng dụng đang khởi chạy...")
#     logger.warning("Cảnh báo: Một API sắp hết hạn trong 3 ngày.")
#     logger.error("Đã xảy ra lỗi khi xử lý request X.")
#     logger.critical("Hệ thống gặp lỗi nghiêm trọng! Không thể kết nối tới database.")

# if __name__ == "__main__":
#     setup_logging()
#     run_exam()


# Template logging.yaml
# ```
# version: 1
# disable_existing_loggers: false

# filters:
#   below_warning:
#     # Cú pháp '()' này chỉ định class sẽ được khởi tạo.
#     (): app.utils.logger.LevelFilter # Giả sử file tên là logger.py
#     level: WARNING

# formatters:
#   default:
#     format: "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s] - %(message)s"
#     datefmt: "%Y-%m-%d %H:%M:%S"

#   json:
#     # Cần cài đặt thư viện: pip install python-json-logger
#     class: pythonjsonlogger.jsonlogger.JsonFormatter
#     format: "%(asctime)s %(name)s %(levelname)s %(filename)s %(lineno)d %(message)s"

# handlers:
#   stdout:
#     class: logging.StreamHandler
#     level: DEBUG
#     formatter: default
#     stream: ext://sys.stdout
#     filters: [below_warning] # Áp dụng filter để chỉ log INFO, DEBUG

#   stderr:
#     class: logging.StreamHandler
#     level: WARNING # Handler này chỉ xử lý từ WARNING trở lên
#     formatter: default
#     stream: ext://sys.stderr

#   file:
#     class: logging.handlers.TimedRotatingFileHandler
#     level: INFO
#     formatter: default
#     filename: logs/app.log
#     when: midnight
#     interval: 1
#     backupCount: 14
#     encoding: utf-8

#   file_json:
#     class: logging.handlers.TimedRotatingFileHandler
#     level: INFO
#     formatter: json # Sử dụng formatter JSON
#     filename: logs/app.json.log # Ghi ra file log JSON riêng biệt
#     when: midnight
#     interval: 1
#     backupCount: 14
#     encoding: utf-8

# # Cấu hình cho root logger, áp dụng cho toàn bộ ứng dụng
# root:
#   level: INFO # Mặc dù handler có level riêng, level ở root là ngưỡng cuối cùng
#   handlers: [stdout, stderr, file, file_json]
# ```
