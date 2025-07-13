from fastapi import APIRouter
from typing import List

# from app.schemas.errors import ErrorLog
# from app.services import analytics_service

router = APIRouter()

@router.get("/", response_model=List[ErrorLog])
def read_error_logs():
    """
    Lấy danh sách các log lỗi từ hệ thống.
    """
    df = analytics_service.get_error_logs()
    if df.empty:
        return []
    # Đổi tên cột để khớp với Pydantic model
    df.rename(columns={'log_time': 'log_time', 'error_message': 'error_message'}, inplace=True)
    return df.to_dict(orient='records')
