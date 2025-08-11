"""
API router cho các endpoint liên quan đến Stores (cửa hàng).
"""
import duckdb

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

from ..dependencies import get_db_connection
from ...schemas.store import Store

# Khởi tạo một router mới
router = APIRouter(
    prefix='/stores',  # Tiền tố cho tất cả các endpoint trong router này
    tags=['Stores'],   # Gom nhóm các endpoint trong tài liệu API (Swagger UI)
)

@router.get('/', response_model=List[Store], summary='Lấy danh sách tất cả cửa hàng')
def get_all_stores(db: duckdb.DuckDBPyConnection = Depends(get_db_connection)):
    """
    API endpoint để truy vấn và trả về danh sách tất cả các cửa hàng
    có trong hệ thống.
    """
    try:
        stores_df = db.execute("SELECT store_id, store_name FROM dim_stores ORDER BY store_name").fetchdf()
        # Chuyển đổi DataFrame thành list of dictionaries để Pydantic có thể xử lý
        return stores_df.to_dict('records')
    except duckdb.Error as e:
        # Nếu bảng không tồn tại hoặc có lỗi truy vấn, trả về lỗi 500
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi truy vấn database: {e}"
        )

@router.get('/{store_id}', response_model=Store, summary='Lấy thông tin một cửa hàng theo ID')
def get_store_by_id(
    store_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db_connection)
):
    """
    API endpoint để lấy thông tin chi tiết của một cửa hàng dựa trên ID.
    """
    try:
        query = "SELECT store_id, store_name FROM dim_stores WHERE store_id = ?"
        result = db.execute(query, [store_id]).fetchone()

        if result is None:
            # Nếu không tìm thấy cửa hàng, trả về lỗi 404
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Không tìm thấy cửa hàng với ID: {store_id}"
            )

        # `fetchone()` trả về một tuple, cần chuyển thành dictionary
        return {"store_id": result[0], "store_name": result[1]}
    except duckdb.Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi truy vấn database: {e}"
        )
