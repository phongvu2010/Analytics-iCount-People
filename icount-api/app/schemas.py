from pydantic import BaseModel
# from datetime import datetime
# from typing import Optional, List

# # ======== Token Schemas ========
# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# ======== Store Schemas ========
class Store(BaseModel):
    id: int
    name: str
    class Config:
        from_attributes = True

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

# # ======== Error Log Schemas ========
# class ErrLog(BaseModel):
#     LogTime: datetime
#     Errorcode: str
#     ErrorMessage: str
#     class Config:
#         from_attributes = True
