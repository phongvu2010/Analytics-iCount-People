from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class Store(BaseModel):
    tid: int
    name: str
    class Config:
        orm_mode = True

class ErrLog(BaseModel):
    ID: int
    storeid: int
    LogTime: datetime
    ErrorMessage: str
    class Config:
        orm_mode = True
