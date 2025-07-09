from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from .core.db import fetch_data_as_dataframe
from .schemas import StatsResponse, ErrorLog

# Khởi tạo router và templates
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

def smooth_data(df: pd.DataFrame, column: str = 'in_num', std_dev_threshold: float = 2.5) -> pd.DataFrame:
    """
    Hàm xử lý dữ liệu bất thường (lọc nhiễu).
    Loại bỏ các giá trị vượt quá ngưỡng độ lệch chuẩn (standard deviation).
    Đây là một phương pháp phân tích dữ liệu phổ biến để làm "mượt" dữ liệu.
    """
    if df.empty or column not in df.columns:
        return df

    # Chuyển đổi cột sang kiểu số, ép lỗi thành NaN
    df[column] = pd.to_numeric(df[column], errors='coerce')
    df.dropna(subset=[column], inplace=True) # Xóa các hàng có giá trị NaN trong cột này
    
    if df.empty:
        return df

    mean = df[column].mean()
    std_dev = df[column].std()
    
    # Giữ lại các dòng có giá trị trong khoảng (mean - N*std_dev, mean + N*std_dev)
    # Tham số std_dev_threshold có thể được điều chỉnh để tăng/giảm độ "nhạy" của bộ lọc
    df_filtered = df[np.abs(df[column] - mean) <= std_dev * std_dev_threshold]
    
    return df_filtered

@router.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Endpoint chính, render trang dashboard.
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})

@router.get("/api/stats", response_model=StatsResponse)
async def get_stats(
    period: str = Query("day", enum=["day", "week", "month", "year"]),
    smooth_threshold: Optional[float] = Query(2.5, ge=1.0, le=5.0)
):
    """
    API endpoint để lấy dữ liệu thống kê lượng người vào (in_num).
    - period: Khoảng thời gian thống kê ('day', 'week', 'month', 'year').
    - smooth_threshold: Ngưỡng lọc nhiễu, tính bằng số lần độ lệch chuẩn.
    """
    today = datetime.now()
    
    # Xác định khoảng thời gian bắt đầu và kết thúc
    if period == "day":
        start_date = today.strftime('%Y-%m-%d 00:00:00')
        date_format_sql = "%H:00" # Nhóm theo giờ
        date_format_pandas = "%H:00"
    elif period == "week":
        start_date = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d 00:00:00')
        date_format_sql = "%Y-%m-%d" # Nhóm theo ngày
        date_format_pandas = "%a, %d %b" # Thứ, ngày tháng
    elif period == "month":
        start_date = today.strftime('%Y-%m-01 00:00:00')
        date_format_sql = "%Y-%m-%d" # Nhóm theo ngày
        date_format_pandas = "%d/%m"
    else: # year
        start_date = today.strftime('%Y-01-01 00:00:00')
        date_format_sql = "%Y-%m" # Nhóm theo tháng
        date_format_pandas = "Thg %m, %Y"

    # Câu lệnh SQL để lấy dữ liệu thô
    # Sử dụng CONVERT để định dạng ngày tháng phù hợp với MSSQL
    # DATEPART cho tuần, tháng, năm
    if period == "day":
        group_by_clause = "DATEPART(hour, recordtime)"
        label_clause = "CONVERT(varchar(2), DATEPART(hour, recordtime)) + ':00'"
    elif period == "week":
        group_by_clause = "CONVERT(date, recordtime)"
        label_clause = "CONVERT(varchar, CONVERT(date, recordtime), 103)" # dd/MM/yyyy
    elif period == "month":
        group_by_clause = "CONVERT(date, recordtime)"
        label_clause = "CONVERT(varchar, CONVERT(date, recordtime), 103)"
    else: # year
        group_by_clause = "CONVERT(varchar(7), recordtime, 120)" # YYYY-MM
        label_clause = "CONVERT(varchar(7), recordtime, 120)"

    query = f"""
        SELECT
            {label_clause} as label,
            SUM(in_num) as value
        FROM dbo.num_crowd
        WHERE recordtime >= ?
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause};
    """
    
    df = fetch_data_as_dataframe(query, params=(start_date,))

    # Xử lý lọc nhiễu nếu có dữ liệu
    if not df.empty:
        # Chúng ta cần xử lý nhiễu trên dữ liệu gốc trước khi group by,
        # nhưng để đơn giản, ta sẽ làm mượt trên kết quả đã group.
        # Một cách tiếp cận tốt hơn là lấy dữ liệu thô, làm mượt rồi group trong pandas.
        # Ở đây, ta sẽ làm mượt kết quả SUM
        df = smooth_data(df, column='value', std_dev_threshold=smooth_threshold)

    data_list = df.to_dict(orient='records')

    return StatsResponse(period=period, data=data_list)


@router.get("/api/errors", response_model=list[ErrorLog])
async def get_error_logs(limit: int = Query(5, ge=1, le=50)):
    """
    API endpoint để lấy các log lỗi mới nhất.
    """
    query = f"""
        SELECT TOP (?) ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage
        FROM dbo.ErrLog
        ORDER BY LogTime DESC;
    """
    df = fetch_data_as_dataframe(query, params=(limit,))
    
    # Chuyển đổi DataFrame thành list các dictionary để Pydantic validate
    if df.empty:
        return []
    
    return df.to_dict(orient='records')
