from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.services import analytics_service

# --- Khởi tạo ---
router = APIRouter()
templates = Jinja2Templates(directory='app/templates')

# --- API Endpoints ---
@router.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    """
    Render trang dashboard chính.
    """
    # Lấy danh sách cửa hàng để hiển thị trong bộ lọc
    stores = analytics_service.get_distinct_stores()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stores": stores
    })

@router.get("/traffic-data")
async def get_traffic_data_api(
    period: str = Query("day", enum=["day", "week", "month", "year"]),
    store: str = Query("all")
):
    """
    API endpoint để cung cấp dữ liệu thống kê cho frontend.
    """
    df = analytics_service.get_traffic_data(period=period, store_name=store)
    
    # Chuyển đổi DataFrame thành định dạng JSON mà Chart.js có thể đọc
    if df.empty:
        return JSONResponse(content={"labels": [], "in_data": [], "out_data": []})
        
    chart_data = {
        "labels": df['period'].tolist(),
        "in_data": df['total_in'].tolist(),
        "out_data": df['total_out'].tolist(),
    }
    return JSONResponse(content=chart_data)
