from datetime import datetime
from pydantic import BaseModel
from typing import Optional # , List

# --- Pydantic Models (dùng cho API request/response) ---
class StoreSchema(BaseModel):
    tid: int
    name: str
    class Config:
        from_attributes = True

class ErrLogSchema(BaseModel):
    store_name: Optional[str] = "N/A"
    log_time: datetime
    error_code: str
    error_message: str
    class Config:
        from_attributes = True















# # ======== Token Schemas ========
# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# # ======== Crowd Data Schemas ========
# class NumCrowd(BaseModel):
#     recordtime: datetime
#     in_num: int
#     out_num: int
#     class Config:
#         from_attributes = True

# # ======== Aggregated Data Schema ========
# class AggregatedCrowdData(BaseModel):
#     period: str # Sẽ là ngày, tuần, hoặc tháng
#     in_num: int
#     out_num: int
