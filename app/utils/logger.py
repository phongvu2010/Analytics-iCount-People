"""
Module tiện ích để thiết lập hệ thống logging cho toàn bộ ứng dụng.

Module này cung cấp chức năng đọc cấu hình từ một file YAML, cho phép thiết lập
linh hoạt các handlers (ví dụ: stdout, stderr, file), formatters và log levels.
Nó cũng hỗ trợ ghi đè log level thông qua biến môi trường, giúp việc gỡ lỗi
trong các môi trường khác nhau trở nên dễ dàng hơn.
"""
import logging
import logging.config
import os
import yaml

from pathlib import Path
from typing import Union


class MaxLevelFilter(logging.Filter):
    """
    Filter để chỉ cho phép các log record có level DƯỚI hoặc BẰNG một mức cho trước.

    Đây là một lớp tiện ích thường được sử dụng trong cấu hình logging để tách biệt
    các luồng output. Ví dụ, handler cho `stdout` có thể sử dụng filter này để
    chỉ hiển thị các log INFO và DEBUG, trong khi handler cho `stderr` sẽ hiển thị
    các log từ WARNING trở lên.
    """
    def __init__(self, level: Union[str, int], **kwargs):
        """
        Khởi tạo filter với một level tối đa.

        Args:
            level: Mức log tối đa được phép đi qua filter. Có thể là một chuỗi
                   (ví dụ: 'WARNING') hoặc một số nguyên (ví dụ: logging.WARNING).
        """
        super().__init__(**kwargs)
        if isinstance(level, str):
            # Chuyển đổi tên level dạng chuỗi (không phân biệt hoa thường)
            # thành giá trị số nguyên tương ứng.
            self.level = logging.getLevelNamesMapping()[level.upper()]
        else:
            self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Kiểm tra xem một log record có nên được xử lý hay không.

        Args:
            record: Đối tượng LogRecord đang được đánh giá.

        Returns:
            True nếu level của record nhỏ hơn hoặc bằng level của filter,
            ngược lại là False.
        """
        return record.levelno <= self.level


def setup_logging(config_path: Union[str, Path] = 'configs/logger.yaml', default_level: int = logging.INFO ) -> None:
    """
    Thiết lập cấu hình logging cho toàn bộ ứng dụng từ một file YAML.

    Hàm này sẽ đọc file cấu hình, đảm bảo thư mục log tồn tại, và áp dụng
    cấu hình. Nếu có lỗi xảy ra hoặc không tìm thấy file, nó sẽ quay về
    sử dụng cấu hình logging cơ bản.

    Args:
        config_path: Đường dẫn đến file YAML cấu hình logging. Mặc định là
                     'configs/logger.yaml'.
        default_level: Log level mặc định sẽ được sử dụng nếu file cấu hình
                       không hợp lệ hoặc không tìm thấy.
    """
    config_path = Path(config_path)

    # Đảm bảo thư mục 'logs' tồn tại để các file handler có thể ghi file.
    log_dir = Path('logs')
    log_dir.mkdir(parents=True, exist_ok=True)

    if not config_path.is_file():
        logging.basicConfig(level=default_level)
        logging.warning(f"Không tìm thấy file cấu hình tại '{config_path}'. Sử dụng cấu hình logging cơ bản.")
        return

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)

        if not config_dict:
            raise ValueError('File YAML rỗng hoặc không hợp lệ.')

        # Cho phép ghi đè log level bằng biến môi trường `LOG_LEVEL`.
        # Điều này rất hữu ích khi cần gỡ lỗi trên môi trường production
        # mà không cần thay đổi code hay file config.
        log_level_from_env = os.environ.get('LOG_LEVEL')
        if log_level_from_env and 'root' in config_dict:
            config_dict['root']['level'] = log_level_from_env.upper()
            logging.info(f"Log level được ghi đè thành '{log_level_from_env.upper()}' bởi biến môi trường LOG_LEVEL.")

        logging.config.dictConfig(config_dict)

    except Exception as e:
        # Trong trường hợp có bất kỳ lỗi nào khi xử lý file config,
        # quay về cấu hình cơ bản để đảm bảo ứng dụng vẫn có thể log lỗi.
        logging.basicConfig(level=default_level)
        logging.exception(f"Lỗi khi cấu hình logging từ file YAML: {e}")
