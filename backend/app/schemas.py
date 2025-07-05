from pydantic import BaseModel

# --- Pydantic Models (dùng cho API request/response) ---
class StoreSchema(BaseModel):
    tid: int
    name: str

    class Config:
        from_attributes = True

# class ErrLogSchema(BaseModel):
#     store_name: Optional[str] = "N/A"
#     log_time: datetime
#     error_message: str

#     class Config:
#         orm_mode = True
