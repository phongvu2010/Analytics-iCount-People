# # === FILENAME: schemas.py ===
# # Module định nghĩa các schema Pydantic để validate và serialize dữ liệu.


# # Pydantic schemas (validate dữ liệu)


# # =======================================
# # Schemas cho Token (Xác thực)
# # =======================================
# class Token(BaseModel):
#     access_token: str
#     token_type: str

# class TokenData(BaseModel):
#     username: Optional[str] = None

# # =======================================
# # Schemas cho Store
# # =======================================
# # Schema cơ bản cho Store
# class StoreBase(BaseModel):
#     name: str

# # Schema dùng để trả về thông tin Store từ API
# class Store(StoreBase):
#     tid: int

#     class Config:
#         orm_mode = True # Cho phép Pydantic đọc dữ liệu từ ORM model

# # =======================================
# # Schemas cho NumCrowd
# # =======================================
# class NumCrowdBase(BaseModel):
#     recordtime: datetime
#     in_num: int
#     out_num: int
#     position: Optional[str] = None
#     storeid: int

# class NumCrowd(NumCrowdBase):
#     class Config:
#         orm_mode = True

# # =======================================
# # Schemas cho ErrLog
# # =======================================
# class ErrLogBase(BaseModel):
#     storeid: int
#     LogTime: datetime
#     DeviceCode: Optional[int] = None
#     Errorcode: Optional[int] = None
#     ErrorMessage: Optional[str] = None

# class ErrLog(ErrLogBase):
#     ID: int

#     class Config:
#         orm_mode = True









# # # --- Pydantic Models (dùng cho API request/response) ---
# # class StoreSchema(BaseModel):
# #     tid: int
# #     name: str

# #     class Config:
# #         from_attributes = True

# # class ErrLogSchema(BaseModel):
# #     store_name: Optional[str] = 'N/A'
# #     log_time: datetime
# #     # error_code: str
# #     error_message: str

# #     class Config:
# #         from_attributes = True










# Pydantic schemas (validate dữ liệu)

# =======================================
# Schemas cho Token (Xác thực)
# =======================================
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

# Schema cơ bản cho Store
class StoreBase(BaseModel):
    name: str

# Schema dùng để trả về thông tin Store từ API
class Store(StoreBase):
    tid: int

    class Config:
        orm_mode = True # Cho phép Pydantic đọc dữ liệu từ ORM model

# =======================================
# Schemas cho NumCrowd
# =======================================
class NumCrowdBase(BaseModel):
    recordtime: datetime
    in_num: int
    out_num: int
    position: Optional[str] = None
    storeid: int

class NumCrowd(NumCrowdBase):
    class Config:
        orm_mode = True

class ErrLogBase(BaseModel):
    storeid: int
    LogTime: datetime
    DeviceCode: Optional[int] = None
    Errorcode: Optional[int] = None
    ErrorMessage: Optional[str] = None

class ErrLog(ErrLogBase):
    ID: int

    class Config:
        orm_mode = True












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




from datetime import datetime
from pydantic import BaseModel
# from typing import List, Optional

# =======================================
# Schemas cho Store
# =======================================
class Store(BaseModel):
    tid: int
    name: str
    class Config:
        orm_mode = True

# =======================================
# Schemas cho ErrLog
# =======================================
class ErrLog(BaseModel):
    ID: int
    storeid: int
    LogTime: datetime
    ErrorMessage: str
    class Config:
        orm_mode = True
