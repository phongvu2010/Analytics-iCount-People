# from fastapi import APIRouter
# from typing import List

# from ...schemas import ErrorLog
# from ...services import analytics_service

# router = APIRouter()

# @router.get('/', response_model = List[ErrorLog])
# def read_error_logs():
#     """
#     Lấy danh sách các log lỗi từ hệ thống.
#     """
#     df = analytics_service.get_error_logs()
#     if df.empty:
#         return []

#     return df.to_dict(orient = 'records')
