import json
import logging
import pandas as pd

from pathlib import Path
from typing import Dict

from app.core.config import etl_settings

logger = logging.getLogger(__name__)
STATE_FILE = Path(etl_settings.STATE_FILE)

def load_etl_state() -> Dict[str, str]:
    if not STATE_FILE.exists(): return {}

    try:
        with STATE_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc file state '{STATE_FILE}'. Bắt đầu lại từ đầu.\n")
        return {}

def save_etl_state(state: Dict[str, str]):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open('w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

    logger.debug(f"Trạng thái ETL đã được lưu vào {STATE_FILE}")

def get_last_timestamp(state: Dict[str, str], table_name: str) -> str:
    return state.get(table_name, etl_settings.ETL_DEFAULT_TIMESTAMP)

def update_timestamp(state: Dict[str, str], table_name: str, new_timestamp: pd.Timestamp):
    if pd.notna(new_timestamp):
        state[table_name] = new_timestamp.isoformat(sep=' ')
        logger.debug(f"Đã cập nhật timestamp cho '{table_name}': {state[table_name]}")
    else:
        logger.warning(
            f"Không thể cập nhật timestamp cho bảng '{table_name}' vì 'new_timestamp' là NaT "
            "(có thể do không có dữ liệu mới hoặc tất cả timestamp đều không hợp lệ). "
            "Trạng thái high-water mark sẽ không thay đổi."
        )
