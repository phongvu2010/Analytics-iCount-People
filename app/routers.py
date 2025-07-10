import numpy as np
import pandas as pd
import io
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Request, Query, Depends, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional, List

from .core.db import get_db, execute_query_as_dataframe
from .schemas import StatsResponse, ErrorLog, Store, TableDataRow, StatDataPoint, DonutChartDataPoint

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

    if len(df) < 2:
        return df

    mean = df[column].mean()
    std_dev = df[column].std()

    # Tránh trường hợp std_dev = 0
    if std_dev == 0:
        return df

    df_filtered = df[np.abs(df[column] - mean) <= std_dev * std_dev_threshold].copy()

    return df_filtered

@router.get('/', response_class=HTMLResponse)
async def read_root(request: Request, db: Session = Depends(get_db)):
    """
    Endpoint chính, render trang dashboard.
    Truy vấn sẵn danh sách cửa hàng để truyền cho template.
    """
    stores_query = "SELECT tid, name FROM dbo.store ORDER BY name;"
    stores_df = execute_query_as_dataframe(stores_query, db)
    stores = stores_df.to_dict(orient='records')

    # Mặc định lấy 20 lỗi mới nhất để hiển thị ban đầu
    errors_query = """
        SELECT TOP 20 e.ID, e.storeid, s.name as store_name, e.DeviceCode, e.LogTime, e.Errorcode, e.ErrorMessage
        FROM dbo.ErrLog e
        LEFT JOIN dbo.store s ON e.storeid = s.tid
        ORDER BY e.LogTime DESC;
    """
    errors_df = execute_query_as_dataframe(errors_query, db)
    # Chuyển đổi datetime để tương thích JSON
    errors_df['LogTime'] = errors_df['LogTime'].dt.isoformat()
    error_logs = errors_df.to_dict(orient='records')

    return templates.TemplateResponse('dashboard.html', {
        'request': request,
        'stores': stores,
        'error_logs': error_logs
    })

@router.get('/api/stores', response_model=List[Store])
async def get_stores(db: Session = Depends(get_db)):
    """
    API endpoint để lấy danh sách tất cả các cửa hàng.
    """
    query = "SELECT tid, name FROM dbo.store ORDER BY name;"
    df = execute_query_as_dataframe(query, db)
    if df.empty:
        return []
    return df.to_dict(orient='records')

@router.get('/api/stats', response_model=StatsResponse)
async def get_stats(
    period: str = Query('year', enum=['day', 'week', 'month', 'year']),
    filter_date: date = Query(default_factory=date.today),
    store_id: Optional[int] = Query(None, description="Lọc theo ID cửa hàng cụ thể. Mặc định là tất cả."),
    smooth_threshold: Optional[float] = Query(2.5, ge=1.0, le=5.0),
    db: Session = Depends(get_db)
):
    """
    API endpoint chính để lấy dữ liệu thống kê, đã được nâng cấp toàn diện.
    """
    # 1. Xác định khoảng thời gian (start_date, end_date) và cách nhóm dữ liệu
    params = []
    if period == 'day':
        start_date = datetime.combine(filter_date, datetime.min.time())
        end_date = start_date + timedelta(days=1)
        group_by_clause = 'DATEPART(hour, recordtime)'
        label_clause = "FORMAT(recordtime, 'HH:00')"
        order_by_clause = 'DATEPART(hour, recordtime)'
    elif period == 'week':
        start_of_week = filter_date - timedelta(days=filter_date.weekday())
        start_date = datetime.combine(start_of_week, datetime.min.time())
        end_date = start_date + timedelta(days=7)
        group_by_clause = 'CONVERT(date, recordtime)'
        label_clause = "FORMAT(CONVERT(date, recordtime), 'dd/MM')"
        order_by_clause = 'CONVERT(date, recordtime)'
    elif period == 'month':
        start_date = datetime(filter_date.year, filter_date.month, 1)
        # Tìm ngày cuối tháng
        next_month = start_date.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day) + timedelta(days=1)
        group_by_clause = 'CONVERT(date, recordtime)'
        label_clause = "FORMAT(CONVERT(date, recordtime), 'dd/MM')"
        order_by_clause = 'CONVERT(date, recordtime)'
    else: # year
        start_date = datetime(filter_date.year, 1, 1)
        end_date = datetime(filter_date.year + 1, 1, 1)
        group_by_clause = "FORMAT(recordtime, 'yyyy-MM')"
        label_clause = "FORMAT(recordtime, 'yyyy-MM')"
        order_by_clause = "FORMAT(recordtime, 'yyyy-MM')"

    params.extend([start_date, end_date])

    # 2. Xây dựng câu SQL động
    where_clauses = ["recordtime >= ? AND recordtime < ?"]
    if store_id is not None:
        where_clauses.append("storeid = ?")
        params.append(store_id)

    where_sql = " AND ".join(where_clauses)

    # --- Query cho Line Chart và Table ---
    line_chart_query = f"""
        SELECT
            {label_clause} as label,
            SUM(CAST(in_num AS INT)) as value
        FROM dbo.num_crowd
        WHERE {where_sql}
        GROUP BY {group_by_clause}, {label_clause}
        ORDER BY {order_by_clause};
    """
    df_stats = execute_query_as_dataframe(line_chart_query, db, params=tuple(params))

    # 3. Xử lý dữ liệu
    if not df_stats.empty:
        df_stats = smooth_data(df_stats, column='value', std_dev_threshold=smooth_threshold)
        # Tính toán % thay đổi
        df_stats['percentage_change'] = df_stats['value'].pct_change().fillna(0) * 100
        df_stats['percentage_change'] = df_stats['percentage_change'].round(2)
    
    line_chart_data = [StatDataPoint(**row) for row in df_stats[['label', 'value']].to_dict(orient='records')]
    table_data = [TableDataRow(**row) for row in df_stats.to_dict(orient='records')]

    # --- Query cho Donut Chart (luôn lấy theo store) ---
    donut_where_clauses = ["recordtime >= ? AND recordtime < ?"]
    donut_params = [start_date, end_date] # Bắt đầu lại params cho query này
    # Nếu store_id được chọn, donut chart sẽ chỉ có 1 lát cắt. Nếu không, nó sẽ so sánh tất cả.
    # Yêu cầu là so sánh các cửa, nên ta sẽ bỏ qua store_id ở đây.
    
    donut_query = f"""
        SELECT
            s.name as store_name,
            SUM(CAST(nc.in_num AS INT)) as value
        FROM dbo.num_crowd nc
        JOIN dbo.store s ON nc.storeid = s.tid
        WHERE nc.recordtime >= ? AND nc.recordtime < ?
        GROUP BY s.name
        HAVING SUM(CAST(nc.in_num AS INT)) > 0 -- Chỉ lấy các cửa có lượt vào
        ORDER BY value DESC;
    """
    df_donut = execute_query_as_dataframe(donut_query, db, params=tuple(donut_params))
    donut_chart_data = [DonutChartDataPoint(**row) for row in df_donut.to_dict(orient='records')]

    return StatsResponse(
        line_chart_data=line_chart_data,
        table_data=table_data,
        donut_chart_data=donut_chart_data
    )

@router.get('/api/errors', response_model=List[ErrorLog])
async def get_error_logs(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    API endpoint để lấy các log lỗi mới nhất, kèm theo tên cửa hàng.
    """
    query = """
        SELECT TOP (?) e.ID, e.storeid, s.name as store_name, e.DeviceCode, e.LogTime, e.Errorcode, e.ErrorMessage
        FROM dbo.ErrLog e
        LEFT JOIN dbo.store s ON e.storeid = s.tid
        ORDER BY e.LogTime DESC;
    """
    df = execute_query_as_dataframe(query, db, params=(limit,))

    if df.empty:
        return []

    return df.to_dict(orient='records')

@router.get("/api/export-data")
async def export_data(
    period: str = Query('year', enum=['day', 'week', 'month', 'year']),
    filter_date: date = Query(default_factory=date.today),
    store_id: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    """
    API để xuất dữ liệu chi tiết ra file Excel.
    """
    # (Logic xác định ngày tháng tương tự như /api/stats)
    # ... [Copy logic xác định start_date, end_date từ /api/stats] ...
    if period == 'day':
        start_date = datetime.combine(filter_date, datetime.min.time())
        end_date = start_date + timedelta(days=1)
    elif period == 'week':
        start_of_week = filter_date - timedelta(days=filter_date.weekday())
        start_date = datetime.combine(start_of_week, datetime.min.time())
        end_date = start_date + timedelta(days=7)
    elif period == 'month':
        start_date = datetime(filter_date.year, filter_date.month, 1)
        next_month = start_date.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day) + timedelta(days=1)
    else: # year
        start_date = datetime(filter_date.year, 1, 1)
        end_date = datetime(filter_date.year + 1, 1, 1)

    params = [start_date, end_date]
    where_clauses = ["nc.recordtime >= ? AND nc.recordtime < ?"]
    if store_id is not None:
        where_clauses.append("nc.storeid = ?")
        params.append(store_id)
    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT
            nc.recordtime,
            s.name as store_name,
            nc.in_num,
            nc.out_num,
            nc.position
        FROM dbo.num_crowd nc
        LEFT JOIN dbo.store s ON nc.storeid = s.tid
        WHERE {where_sql}
        ORDER BY nc.recordtime ASC;
    """
    df = execute_query_as_dataframe(query, db, params=tuple(params))

    if df.empty:
        raise HTTPException(status_code=404, detail="Không có dữ liệu để xuất cho lựa chọn này.")

    # Tạo file Excel trong bộ nhớ
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    output.seek(0)

    filename = f"export_data_{period}_{filter_date.strftime('%Y%m%d')}.xlsx"
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }

    return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')






# import numpy as np
# import pandas as pd
# from datetime import datetime, timedelta
# from fastapi import APIRouter, Request, Query, Depends
# from fastapi.responses import HTMLResponse
# from fastapi.templating import Jinja2Templates
# from sqlalchemy.orm import Session
# from typing import Optional

# # Thay đổi import để sử dụng SQLAlchemy session
# from .core.db import get_db, execute_query_as_dataframe
# from .schemas import StatsResponse, ErrorLog

# # Khởi tạo router và templates
# router = APIRouter()
# templates = Jinja2Templates(directory='app/templates')

# def smooth_data(df: pd.DataFrame, column: str = 'value', std_dev_threshold: float = 2.5) -> pd.DataFrame:
#     """
#     Hàm xử lý dữ liệu bất thường (lọc nhiễu).
#     Loại bỏ các giá trị vượt quá ngưỡng độ lệch chuẩn (standard deviation).
#     """
#     if df.empty or column not in df.columns:
#         return df

#     df[column] = pd.to_numeric(df[column], errors='coerce')
#     df.dropna(subset=[column], inplace=True)

#     if df.empty:
#         return df

#     mean = df[column].mean()
#     std_dev = df[column].std()

#     df_filtered = df[np.abs(df[column] - mean) <= std_dev * std_dev_threshold]

#     return df_filtered

# @router.get('/', response_class=HTMLResponse)
# async def read_root(request: Request):
#     """
#     Endpoint chính, render trang dashboard.
#     """
#     return templates.TemplateResponse('dashboard.html', {'request': request})

# @router.get('/api/stats', response_model=StatsResponse)
# async def get_stats(
#     period: str = Query('day', enum=['day', 'week', 'month', 'year']),
#     smooth_threshold: Optional[float] = Query(2.5, ge=1.0, le=5.0),
#     db: Session = Depends(get_db) # <-- Dependency Injection của SQLAlchemy session
# ):
#     """
#     API endpoint để lấy dữ liệu thống kê lượng người vào (in_num).
#     """
#     today = datetime.now()

#     if period == 'day':
#         start_date = today.strftime('%Y-%m-%d 00:00:00')
#         group_by_clause = 'DATEPART(hour, recordtime)'
#         label_clause = "CONVERT(varchar(2), DATEPART(hour, recordtime)) + ':00'"
#     elif period == 'week':
#         start_date = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d 00:00:00')
#         group_by_clause = 'CONVERT(date, recordtime)'
#         label_clause = 'CONVERT(varchar, CONVERT(date, recordtime), 103)' # dd/MM/yyyy
#     elif period == 'month':
#         start_date = today.strftime('%Y-%m-01 00:00:00')
#         group_by_clause = 'CONVERT(date, recordtime)'
#         label_clause = 'CONVERT(varchar, CONVERT(date, recordtime), 103)'
#     else: # year
#         start_date = today.strftime('%Y-01-01 00:00:00')
#         group_by_clause = 'CONVERT(varchar(7), recordtime, 120)' # YYYY-MM
#         label_clause = 'CONVERT(varchar(7), recordtime, 120)'

#     # Câu lệnh SQL không đổi, placeholder '?' vẫn dùng được với pyodbc
#     query = f"""
#         SELECT
#             {label_clause} as label,
#             SUM(in_num) as value
#         FROM dbo.num_crowd
#         WHERE recordtime >= ?
#         GROUP BY {group_by_clause}
#         ORDER BY {group_by_clause};
#     """

#     # Sử dụng hàm helper mới với session từ dependency
#     df = execute_query_as_dataframe(query, db, params=(start_date,))

#     if not df.empty:
#         df = smooth_data(df, column='value', std_dev_threshold=smooth_threshold)

#     data_list = df.to_dict(orient='records')

#     return StatsResponse(period=period, data=data_list)

# @router.get('/api/errors', response_model=list[ErrorLog])
# async def get_error_logs(
#     limit: int = Query(5, ge=1, le=50),
#     db: Session = Depends(get_db) # <-- Dependency Injection của SQLAlchemy session
# ):
#     """
#     API endpoint để lấy các log lỗi mới nhất.
#     """
#     query = f"""
#         SELECT TOP (?) ID, storeid, DeviceCode, LogTime, Errorcode, ErrorMessage
#         FROM dbo.ErrLog
#         ORDER BY LogTime DESC;
#     """
#     # Sử dụng hàm helper mới
#     df = execute_query_as_dataframe(query, db, params=(limit,))

#     if df.empty:
#         return []

#     return df.to_dict(orient='records')
