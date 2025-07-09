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
