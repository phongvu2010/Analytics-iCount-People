"""
Pydantic schemas cho đối tượng Store.
"""
from pydantic import BaseModel, Field

class Store(BaseModel):
    """ Schema cho dữ liệu của một cửa hàng trả về qua API. """
    store_id: int = Field(..., description="ID định danh duy nhất của cửa hàng.")
    store_name: str = Field(..., description="Tên của cửa hàng.")

    # Pydantic V2 dùng orm_mode, Pydantic V1 dùng `class Config: orm_mode=True`
    # Điều này cho phép Pydantic đọc dữ liệu từ các đối tượng ORM/DB
    # và chuyển đổi thành schema.
    model_config = {
        "from_attributes": True
    }
