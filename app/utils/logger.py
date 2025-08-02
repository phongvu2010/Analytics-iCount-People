import logging
import logging.config
import os
import yaml

from pathlib import Path
from typing import Union

class MaxLevelFilter(logging.Filter):
    def __init__(self, level: Union[str, int], **kwargs):
        super().__init__(**kwargs)
        if isinstance(level, str):
            self.level = logging.getLevelNamesMapping()[level.upper()]
        else:
            self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.level

CURRENT_DIR = Path(__file__).parent
DEFAULT_CONFIG_PATH = CURRENT_DIR.parent / 'configs' / 'logger.yaml'

def setup_logging(
    config_path: Union[str, Path] = DEFAULT_CONFIG_PATH,
    default_level: int = logging.INFO
) -> None:
    config_path = Path(config_path)

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
            log_level_from_env = os.environ.get('LOG_LEVEL')
            if log_level_from_env and 'root' in config_dict:
                config_dict['root']['level'] = log_level_from_env.upper()

            logging.config.dictConfig(config_dict)
            logging.info(f"Hệ thống logging đã được cấu hình thành công từ file: {config_path}")

            if log_level_from_env and 'root' in config_dict:
                logging.info(f"Log level được ghi đè thành '{log_level_from_env.upper()}' bởi biến môi trường LOG_LEVEL.")
        else:
            raise ValueError("File YAML rỗng hoặc không hợp lệ.")
    except Exception as e:
        logging.basicConfig(level=default_level)
        logging.exception(f"Lỗi khi cấu hình logging từ file YAML: {e}")
