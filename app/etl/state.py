# app/etl/state.py
# File này sẽ quản lý việc đọc/ghi trạng thái của pipeline (lần cuối chạy tới đâu).
import json
import logging
import pandas as pd

from pathlib import Path
from typing import Dict #, Optional

from app.core.config import etl_settings #, TableConfig

logger = logging.getLogger(__name__)
STATE_FILE = Path(etl_settings.STATE_FILE)

def load_etl_state() -> Dict[str, str]:
    """Tải trạng thái ETL cuối cùng từ file JSON."""
    if not STATE_FILE.exists():
        return {}

    try:
        with STATE_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Không thể đọc file state '{STATE_FILE}'. Bắt đầu lại từ đầu.\n")
        return {}

def save_etl_state(state: Dict[str, str]):
    """Lưu trạng thái ETL hiện tại vào file JSON."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open('w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

    logger.debug(f"Trạng thái ETL đã được lưu vào {STATE_FILE}")

def get_last_timestamp(state: Dict[str, str], table_name: str) -> str:
    """Lấy high-water-mark (timestamp cuối) cho một bảng."""
    return state.get(table_name, etl_settings.ETL_DEFAULT_TIMESTAMP)

def update_timestamp(state: Dict[str, str], table_name: str, new_timestamp: pd.Timestamp):
    """Cập nhật high-water-mark cho một bảng."""
    if pd.notna(new_timestamp):
        state[table_name] = new_timestamp.isoformat(sep=' ')
