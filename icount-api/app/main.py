# Windows: .venv\Scripts\uvicorn.exe app.main:app --host 0.0.0.0 --port 8000 --reload
# Unix: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI
# from .api.routers import data, logs, auth # Sẽ uncomment sau khi tạo router

app = FastAPI(
    title="iCount - People Counter API",
    description="API for analyzing people traffic in stores.",
    version="1.0.0"
)

# app.include_router(auth.router, prefix="/api/v1", tags=["Authentication"])
# app.include_router(data.router, prefix="/api/v1", tags=["Crowd Data"])
# app.include_router(logs.router, prefix="/api/v1", tags=["Error Logs"])

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Welcome to iCount API!"}
