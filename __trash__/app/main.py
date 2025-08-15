# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
import logging

# from .api.routers import stores as stores_router
from .utils.logger import setup_logging

# --- Application Events ---
@api_app.on_event('startup')
async def startup_event():
    """Thiết lập logging khi ứng dụng khởi động."""
    setup_logging('FastAPI')
    logging.info('Application startup complete.')

@api_app.on_event('shutdown')
async def shutdown_event():
    """Ghi log khi ứng dụng tắt."""
    logging.info('Application shutdown.')
