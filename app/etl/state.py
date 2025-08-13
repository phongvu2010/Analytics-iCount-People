"""
Module này quản lý trạng thái của pipeline ETL.

Nó chịu trách nhiệm đọc và ghi "high-water mark" (thường là timestamp
lớn nhất đã xử lý) cho mỗi bảng vào một file JSON. Điều này cho phép
pipeline "ghi nhớ" đã xử lý đến đâu để trong lần chạy tiếp theo, nó chỉ
cần lấy các bản ghi mới hơn (incremental load).
"""
import json
import logging
import pandas as pd

from pathlib import Path
from typing import Dict

from ..core.config import settings

logger = logging.getLogger(__name__)
STATE_FILE = Path(settings.STATE_FILE)

def load_etl_state() -> Dict[str, str]:
    """ Tải trạng thái ETL từ file JSON. """
    if not STATE_FILE.exists(): return {}

    try:
        with STATE_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc file state '{STATE_FILE}'. Bắt đầu lại từ đầu.")
        return {}

def save_etl_state(state: Dict[str, str]):
    """ Lưu trạng thái ETL hiện tại vào file JSON. """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open('w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

    logger.debug(f"Trạng thái ETL đã được lưu vào {STATE_FILE}")

def get_last_timestamp(state: Dict[str, str], table_name: str) -> str:
    """ Lấy high-water mark của một bảng, hoặc trả về giá trị mặc định nếu chưa có. """
    return state.get(table_name, settings.ETL_DEFAULT_TIMESTAMP)

def update_timestamp(state: Dict[str, str], table_name: str, new_timestamp: pd.Timestamp):
    """ Cập nhật high-water mark cho một bảng. """
    # Chỉ cập nhật nếu timestamp mới là một giá trị hợp lệ
    if pd.notna(new_timestamp):
        # Dùng isoformat để đảm bảo định dạng chuỗi nhất quán
        state[table_name] = new_timestamp.isoformat(sep=' ')
        logger.debug(f"Đã cập nhật timestamp cho '{table_name}': {state[table_name]}")
    else:
        logger.warning(
            f"Không thể cập nhật timestamp cho '{table_name}' vì timestamp mới không hợp lệ. "
            "Trạng thái high-water mark sẽ không thay đổi."
        )
