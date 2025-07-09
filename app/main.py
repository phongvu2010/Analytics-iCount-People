# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .routers import router as api_router
from .core.db import get_db_connection

app = FastAPI(
    title="iCount People Dashboard",
    description="API để theo dõi và thống kê lượng người ra vào trung tâm thương mại.",
    version="1.0.0"
)

# Mount thư mục static để phục vụ các file CSS, JS, images
app.mount("/static", StaticFiles(directory="app/statics"), name="static")

# Include router từ file routers.py
app.include_router(api_router)

@app.on_event("startup")
async def startup_event():
    """
    Kiểm tra kết nối database khi ứng dụng khởi động.
    """
    print("Khởi động ứng dụng...")
    conn = get_db_connection()
    if conn:
        print("Kết nối CSDL thành công.")
        conn.close()
    else:
        print("!!! LỖI: Không thể kết nối tới CSDL. Vui lòng kiểm tra file .env và kết nối mạng.")

@app.get("/health")
def health_check():
    """Endpoint kiểm tra sức khoẻ của ứng dụng"""
    return {"status": "ok"}
