# # === FILENAME: schemas.py ===
# # Module định nghĩa các schema Pydantic để validate và serialize dữ liệu.

# from pydantic import BaseModel
# from datetime import datetime
# from typing import List, Optional

# class Store(BaseModel):
#     tid: int
#     name: str
#     class Config:
#         orm_mode = True

# class ErrLog(BaseModel):
#     ID: int
#     storeid: int
#     LogTime: datetime
#     ErrorMessage: str
#     class Config:
#         orm_mode = True











# # Pydantic schemas (validate dữ liệu)

# from datetime import datetime
# from pydantic import BaseModel
# from typing import Optional, List

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
