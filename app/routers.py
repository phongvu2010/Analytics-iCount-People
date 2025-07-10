import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional

# Thay đổi import để sử dụng SQLAlchemy session
from .core.db import get_db, execute_query_as_dataframe
from .schemas import StatsResponse, ErrorLog

# Khởi tạo router và templates
router = APIRouter()
templates = Jinja2Templates(directory='app/templates')

def smooth_data(df: pd.DataFrame, column: str = 'value', std_dev_threshold: float = 2.5) -> pd.DataFrame:
    """
    Hàm xử lý dữ liệu bất thường (lọc nhiễu).
    Loại bỏ các giá trị vượt quá ngưỡng độ lệch chuẩn (standard deviation).
    """
    if df.empty or column not in df.columns:
        return df

    df[column] = pd.to_numeric(df[column], errors='coerce')
    df.dropna(subset=[column], inplace=True)

    if df.empty:
        return df

    mean = df[column].mean()
    std_dev = df[column].std()

    df_filtered = df[np.abs(df[column] - mean) <= std_dev * std_dev_threshold]

    return df_filtered

@router.get('/', response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Endpoint chính, render trang dashboard.
    """
    return templates.TemplateResponse('dashboard.html', {'request': request})

@router.get('/api/stats', response_model=StatsResponse)
async def get_stats(
    period: str = Query('day', enum=['day', 'week', 'month', 'year']),
    smooth_threshold: Optional[float] = Query(2.5, ge=1.0, le=5.0),
    db: Session = Depends(get_db) # <-- Dependency Injection của SQLAlchemy session
):
    """
    API endpoint để lấy dữ liệu thống kê lượng người vào (in_num).
    """
    today = datetime.now()

    if period == 'day':
        start_date = today.strftime('%Y-%m-%d 00:00:00')
        group_by_clause = 'DATEPART(hour, recordtime)'
        label_clause = "CONVERT(varchar(2), DATEPART(hour, recordtime)) + ':00'"
    elif period == 'week':
        start_date = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d 00:00:00')
        group_by_clause = 'CONVERT(date, recordtime)'
        label_clause = 'CONVERT(varchar, CONVERT(date, recordtime), 103)' # dd/MM/yyyy
    elif period == 'month':
        start_date = today.strftime('%Y-%m-01 00:00:00')
        group_by_clause = 'CONVERT(date, recordtime)'
        label_clause = 'CONVERT(varchar, CONVERT(date, recordtime), 103)'
    else: # year
        start_date = today.strftime('%Y-01-01 00:00:00')
        group_by_clause = 'CONVERT(varchar(7), recordtime, 120)' # YYYY-MM
        label_clause = 'CONVERT(varchar(7), recordtime, 120)'

    # Câu lệnh SQL không đổi, placeholder '?' vẫn dùng được với pyodbc
    query = f"""
        SELECT
            {label_clause} as label,
            SUM(in_num) as value
        FROM dbo.num_crowd
        WHERE recordtime >= ?
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause};
    """

    # Sử dụng hàm helper mới với session từ dependency
    df = execute_query_as_dataframe(query, db, params=(start_date,))

    if not df.empty:
        df = smooth_data(df, column='value', std_dev_threshold=smooth_threshold)

    data_list = df.to_dict(orient='records')

    return StatsResponse(period=period, data=data_list)

@router.get('/api/errors', response_model=list[ErrorLog])
async def get_error_logs(
    limit: int = Query(5, ge=1, le=50),
    db: Session = Depends(get_db) # <-- Dependency Injection của SQLAlchemy session
):
    """
    API endpoint để lấy các log lỗi mới nhất.
    """
    query = f"""
        SELECT TOP (?) ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage
        FROM dbo.ErrLog
        ORDER BY LogTime DESC;
    """
    # Sử dụng hàm helper mới
    df = execute_query_as_dataframe(query, db, params=(limit,))

    if df.empty:
        return []

    return df.to_dict(orient='records')
