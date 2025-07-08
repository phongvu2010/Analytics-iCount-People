import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, Dash
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# =============================================================================
# PHẦN 1: TẠO DỮ LIỆU GIẢ LẬP (MOCK DATA)
# Trong thực tế, bạn sẽ thay thế phần này bằng cách kết nối và truy vấn CSDL
# =============================================================================
def create_mock_data():
    """
    Hàm này tạo ra các DataFrame giả lập dựa trên cấu trúc CSDL bạn cung cấp.
    """
    # --- Bảng dbo.store ---
    stores_data = {
        'tid': [1, 2, 3],
        'name': ['Vincom Center Landmark 81', 'AEON Mall Tân Phú Celadon', 'Gigamall Thủ Đức'],
        'city': ['Hồ Chí Minh', 'Hồ Chí Minh', 'Hồ Chí Minh'],
        'code': ['VCL81', 'AMTPC', 'GMTD'],
        'cameranum': [50, 80, 60]
    }
    df_store = pd.DataFrame(stores_data)

    # --- Bảng dbo.num_crowd ---
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 2) # Dữ liệu trong 2 năm
    date_range = pd.date_range(start=start_date, end=end_date, freq='15min') # Dữ liệu 15 phút/lần

    crowd_data = []
    for store_id in df_store['tid']:
        # Tạo dữ liệu ngẫu nhiên với xu hướng theo ngày và giờ
        base_traffic = (store_id * 10)
        for record_time in date_range:
            # Lưu lượng cao hơn vào cuối tuần và buổi chiều/tối
            weekend_multiplier = 1.8 if record_time.dayofweek >= 5 else 1
            time_multiplier = 1 + (record_time.hour - 8) / 16 if 8 <= record_time.hour <= 22 else 0.5
            
            in_num = int(np.random.randint(5, 20) * time_multiplier * weekend_multiplier + base_traffic)
            out_num = int(in_num * np.random.uniform(0.8, 1.1))
            
            crowd_data.append({
                'recordtime': record_time,
                'in_num': in_num,
                'out_num': out_num,
                'storeid': store_id
            })
    df_crowd = pd.DataFrame(crowd_data)
    df_crowd['recordtime'] = pd.to_datetime(df_crowd['recordtime'])

    # --- Bảng dbo.ErrLog ---
    err_log_data = []
    for _ in range(50): # Tạo 50 lỗi ngẫu nhiên
        store_id = np.random.choice(df_store['tid'])
        log_time = start_date + timedelta(seconds=np.random.randint(0, int((end_date - start_date).total_seconds())))
        error_code = np.random.choice([101, 202, 303, 404])
        error_messages = {
            101: "Camera disconnected",
            202: "Network timeout",
            303: "Power failure",
            404: "Data transmission error"
        }
        err_log_data.append({
            'ID': _ + 1,
            'storeid': store_id,
            'DeviceCode': np.random.randint(1, 10),
            'LogTime': log_time,
            'Errorcode': error_code,
            'ErrorMessage': error_messages[error_code]
        })
    df_err_log = pd.DataFrame(err_log_data)
    df_err_log['LogTime'] = pd.to_datetime(df_err_log['LogTime'])

    return df_store, df_crowd, df_err_log

# Tải dữ liệu
df_store, df_crowd, df_err_log = create_mock_data()

# Xử lý trước dữ liệu để tính toán hiệu quả hơn
# Gộp tên cửa hàng vào bảng dữ liệu chính
df_crowd_merged = pd.merge(df_crowd, df_store[['tid', 'name']], left_on='storeid', right_on='tid')
# Đặt recordtime làm chỉ mục để dễ dàng resample
df_crowd_merged.set_index('recordtime', inplace=True)


# =============================================================================
# PHẦN 2: XÂY DỰNG ỨNG DỤNG DASHBOARD
# =============================================================================

# Chọn một theme đẹp từ dash-bootstrap-components, ví dụ: VAPOR, CYBORG, DARKLY
app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Traffic Analysis Dashboard"

# --- Định nghĩa Layout của ứng dụng ---
app.layout = dbc.Container(fluid=True, children=[
    # Dòng tiêu đề
    dbc.Row([
        dbc.Col(html.H1("Dashboard Phân Tích Lưu Lượng Khách Hàng", className="text-center text-primary, mb-4"), width=12)
    ]),

    # Dòng chứa các bộ lọc và thông báo lỗi
    dbc.Row([
        # Cột chứa bộ lọc
        dbc.Col(width=4, children=[
            dbc.Card([
                dbc.CardHeader("Bảng Điều Khiển"),
                dbc.CardBody([
                    html.H5("Chọn Trung Tâm Thương Mại:", className="card-title"),
                    dcc.Dropdown(
                        id='store-selector',
                        options=[{'label': name, 'value': tid} for tid, name in df_store[['tid', 'name']].values],
                        value=df_store['tid'].iloc[0], # Giá trị mặc định
                        clearable=False
                    ),
                    html.Br(),
                    html.H5("Thống Kê Theo:", className="card-title"),
                    dcc.RadioItems(
                        id='agg-selector',
                        options=[
                            {'label': 'Ngày', 'value': 'D'},
                            {'label': 'Tuần', 'value': 'W'},
                            {'label': 'Tháng', 'value': 'M'},
                            {'label': 'Năm', 'value': 'Y'},
                        ],
                        value='D', # Giá trị mặc định
                        labelStyle={'display': 'inline-block', 'margin-right': '15px'},
                        inputStyle={"margin-right": "5px"}
                    ),
                ])
            ])
        ]),
        # Cột chứa thông báo lỗi
        dbc.Col(width=8, children=[
            dbc.Card([
                dbc.CardHeader("Thông Báo Lỗi Hệ Thống (Error Logs)"),
                dbc.CardBody([
                    html.Div(id='error-log-display', style={'maxHeight': '200px', 'overflowY': 'auto'})
                ])
            ])
        ])
    ], className="mb-4"),

    # Dòng chứa các thẻ KPI
    dbc.Row(id='kpi-cards', className="mb-4"),

    # Dòng chứa biểu đồ chính
    dbc.Row([
        dbc.Col(dcc.Graph(id='traffic-chart'), width=12)
    ])
])

# --- Định nghĩa Callback để cập nhật Dashboard ---
@app.callback(
    [Output('traffic-chart', 'figure'),
     Output('kpi-cards', 'children'),
     Output('error-log-display', 'children')],
    [Input('store-selector', 'value'),
     Input('agg-selector', 'value')]
)
def update_dashboard(selected_store_id, agg_period):
    # 1. Lọc dữ liệu theo cửa hàng được chọn
    dff_crowd = df_crowd_merged[df_crowd_merged['storeid'] == selected_store_id]
    dff_err = df_err_log[df_err_log['storeid'] == selected_store_id].sort_values('LogTime', ascending=False)

    # 2. Tổng hợp dữ liệu theo chu kỳ thời gian (Ngày, Tuần, Tháng, Năm)
    dff_agg = dff_crowd['in_num'].resample(agg_period).sum().reset_index()
    dff_agg.columns = ['Thời gian', 'Lượt vào']
    
    period_map = {'D': 'Ngày', 'W': 'Tuần', 'M': 'Tháng', 'Y': 'Năm'}
    chart_title = f"Tổng Lượt Khách Vào Theo {period_map[agg_period]} tại {df_store.loc[df_store['tid'] == selected_store_id, 'name'].iloc[0]}"

    # 3. Tạo biểu đồ đường
    fig = px.line(
        dff_agg, 
        x='Thời gian', 
        y='Lượt vào', 
        title=chart_title,
        template='plotly_dark' # Sử dụng template tối màu cho hợp với theme
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis_title="Thời gian",
        yaxis_title="Số lượt khách vào"
    )
    fig.update_traces(mode='lines+markers')

    # 4. Tính toán các chỉ số KPI
    total_visitors = dff_crowd['in_num'].sum()
    
    # Tính trung bình theo đơn vị đã chọn
    avg_visitors_period = dff_agg['Lượt vào'].mean() if not dff_agg.empty else 0

    # Tìm ngày/tuần/tháng/năm cao điểm
    if not dff_agg.empty:
        peak_period_row = dff_agg.loc[dff_agg['Lượt vào'].idxmax()]
        peak_time = peak_period_row['Thời gian']
        peak_value = peak_period_row['Lượt vào']

        if agg_period == 'D':
            peak_time_str = peak_time.strftime('%d-%m-%Y')
        elif agg_period == 'W':
            peak_time_str = f"Tuần bắt đầu {peak_time.strftime('%d-%m-%Y')}"
        elif agg_period == 'M':
            peak_time_str = peak_time.strftime('%B %Y')
        else: # Year
            peak_time_str = peak_time.strftime('%Y')
    else:
        peak_value = 0
        peak_time_str = "N/A"


    # Tạo các thẻ KPI
    kpi_cards = [
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{total_visitors:,.0f}", className="card-title"),
            html.P("Tổng Lượt Khách", className="card-text")
        ]), color="primary", inverse=True), width=4),
        
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{avg_visitors_period:,.0f}", className="card-title"),
            html.P(f"Trung Bình Lượt Khách / {period_map[agg_period]}", className="card-text")
        ]), color="success", inverse=True), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{peak_value:,.0f} ({peak_time_str})", className="card-title"),
            html.P(f"{period_map[agg_period]} Cao Điểm", className="card-text")
        ]), color="info", inverse=True), width=4),
    ]

    # 5. Tạo danh sách thông báo lỗi
    if dff_err.empty:
        error_logs = [html.P("✓ Không có lỗi nào được ghi nhận.", className="text-success")]
    else:
        error_logs = []
        for _, row in dff_err.head(10).iterrows(): # Hiển thị 10 lỗi gần nhất
            error_logs.append(
                html.P(f"[{row['LogTime'].strftime('%Y-%m-%d %H:%M')}] - {row['ErrorMessage']} (Code: {row['Errorcode']})", className="text-danger small")
            )

    return fig, kpi_cards, error_logs

# =============================================================================
# PHẦN 3: CHẠY ỨNG DỤNG
# =============================================================================
if __name__ == '__main__':
    app.run(debug=True)




# import dash
# import dash_bootstrap_components as dbc
# from dash import dcc, html, Input, Output
# import plotly.express as px
# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta

# # =============================================================================
# # PHẦN 1: TẠO DỮ LIỆU GIẢ LẬP (MOCK DATA)
# # Trong thực tế, bạn sẽ thay thế phần này bằng cách kết nối và truy vấn CSDL
# # =============================================================================
# def create_mock_data():
#     """
#     Hàm này tạo ra các DataFrame giả lập dựa trên cấu trúc CSDL bạn cung cấp.
#     """
#     # --- Bảng dbo.store ---
#     stores_data = {
#         'tid': [1, 2, 3],
#         'name': ['Vincom Center Landmark 81', 'AEON Mall Tân Phú Celadon', 'Gigamall Thủ Đức'],
#         'city': ['Hồ Chí Minh', 'Hồ Chí Minh', 'Hồ Chí Minh'],
#         'code': ['VCL81', 'AMTPC', 'GMTD'],
#         'cameranum': [50, 80, 60]
#     }
#     df_store = pd.DataFrame(stores_data)

#     # --- Bảng dbo.num_crowd ---
#     end_date = datetime.now()
#     start_date = end_date - timedelta(days=365 * 2) # Dữ liệu trong 2 năm
#     date_range = pd.date_range(start=start_date, end=end_date, freq='15min') # Dữ liệu 15 phút/lần

#     crowd_data = []
#     for store_id in df_store['tid']:
#         # Tạo dữ liệu ngẫu nhiên với xu hướng theo ngày và giờ
#         base_traffic = (store_id * 10)
#         for record_time in date_range:
#             # Lưu lượng cao hơn vào cuối tuần và buổi chiều/tối
#             weekend_multiplier = 1.8 if record_time.dayofweek >= 5 else 1
#             time_multiplier = 1 + (record_time.hour - 8) / 16 if 8 <= record_time.hour <= 22 else 0.5
            
#             in_num = int(np.random.randint(5, 20) * time_multiplier * weekend_multiplier + base_traffic)
#             out_num = int(in_num * np.random.uniform(0.8, 1.1))
            
#             crowd_data.append({
#                 'recordtime': record_time,
#                 'in_num': in_num,
#                 'out_num': out_num,
#                 'storeid': store_id
#             })
#     df_crowd = pd.DataFrame(crowd_data)
#     df_crowd['recordtime'] = pd.to_datetime(df_crowd['recordtime'])

#     # --- Bảng dbo.ErrLog ---
#     err_log_data = []
#     for _ in range(50): # Tạo 50 lỗi ngẫu nhiên
#         store_id = np.random.choice(df_store['tid'])
#         log_time = start_date + timedelta(seconds=np.random.randint(0, int((end_date - start_date).total_seconds())))
#         error_code = np.random.choice([101, 202, 303, 404])
#         error_messages = {
#             101: "Camera disconnected",
#             202: "Network timeout",
#             303: "Power failure",
#             404: "Data transmission error"
#         }
#         err_log_data.append({
#             'ID': _ + 1,
#             'storeid': store_id,
#             'DeviceCode': np.random.randint(1, 10),
#             'LogTime': log_time,
#             'Errorcode': error_code,
#             'ErrorMessage': error_messages[error_code]
#         })
#     df_err_log = pd.DataFrame(err_log_data)
#     df_err_log['LogTime'] = pd.to_datetime(df_err_log['LogTime'])

#     return df_store, df_crowd, df_err_log

# # Tải dữ liệu
# df_store, df_crowd, df_err_log = create_mock_data()

# # Xử lý trước dữ liệu để tính toán hiệu quả hơn
# # Gộp tên cửa hàng vào bảng dữ liệu chính
# df_crowd_merged = pd.merge(df_crowd, df_store[['tid', 'name']], left_on='storeid', right_on='tid')
# # Đặt recordtime làm chỉ mục để dễ dàng resample
# df_crowd_merged.set_index('recordtime', inplace=True)


# # =============================================================================
# # PHẦN 2: XÂY DỰNG ỨNG DỤNG DASHBOARD
# # =============================================================================

# # Chọn một theme đẹp từ dash-bootstrap-components, ví dụ: VAPOR, CYBORG, DARKLY
# app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
# app.title = "Traffic Analysis Dashboard"

# # --- Định nghĩa Layout của ứng dụng ---
# app.layout = dbc.Container(fluid=True, children=[
#     # Dòng tiêu đề
#     dbc.Row([
#         dbc.Col(html.H1("Dashboard Phân Tích Lưu Lượng Khách Hàng", className="text-center text-primary, mb-4"), width=12)
#     ]),

#     # Dòng chứa các bộ lọc và thông báo lỗi
#     dbc.Row([
#         # Cột chứa bộ lọc
#         dbc.Col(width=4, children=[
#             dbc.Card([
#                 dbc.CardHeader("Bảng Điều Khiển"),
#                 dbc.CardBody([
#                     html.H5("Chọn Trung Tâm Thương Mại:", className="card-title"),
#                     dcc.Dropdown(
#                         id='store-selector',
#                         options=[{'label': name, 'value': tid} for tid, name in df_store[['tid', 'name']].values],
#                         value=df_store['tid'].iloc[0], # Giá trị mặc định
#                         clearable=False
#                     ),
#                     html.Br(),
#                     html.H5("Thống Kê Theo:", className="card-title"),
#                     dcc.RadioItems(
#                         id='agg-selector',
#                         options=[
#                             {'label': 'Ngày', 'value': 'D'},
#                             {'label': 'Tuần', 'value': 'W'},
#                             {'label': 'Tháng', 'value': 'M'},
#                             {'label': 'Năm', 'value': 'Y'},
#                         ],
#                         value='D', # Giá trị mặc định
#                         labelStyle={'display': 'inline-block', 'margin-right': '15px'},
#                         inputStyle={"margin-right": "5px"}
#                     ),
#                 ])
#             ])
#         ]),
#         # Cột chứa thông báo lỗi
#         dbc.Col(width=8, children=[
#             dbc.Card([
#                 dbc.CardHeader("Thông Báo Lỗi Hệ Thống (Error Logs)"),
#                 dbc.CardBody([
#                     html.Div(id='error-log-display', style={'maxHeight': '200px', 'overflowY': 'auto'})
#                 ])
#             ])
#         ])
#     ], className="mb-4"),

#     # Dòng chứa các thẻ KPI
#     dbc.Row(id='kpi-cards', className="mb-4"),

#     # Dòng chứa biểu đồ chính
#     dbc.Row([
#         dbc.Col(dcc.Graph(id='traffic-chart'), width=12)
#     ])
# ])

# # --- Định nghĩa Callback để cập nhật Dashboard ---
# @app.callback(
#     [Output('traffic-chart', 'figure'),
#      Output('kpi-cards', 'children'),
#      Output('error-log-display', 'children')],
#     [Input('store-selector', 'value'),
#      Input('agg-selector', 'value')]
# )
# def update_dashboard(selected_store_id, agg_period):
#     # 1. Lọc dữ liệu theo cửa hàng được chọn
#     dff_crowd = df_crowd_merged[df_crowd_merged['storeid'] == selected_store_id]
#     dff_err = df_err_log[df_err_log['storeid'] == selected_store_id].sort_values('LogTime', ascending=False)

#     # 2. Tổng hợp dữ liệu theo chu kỳ thời gian (Ngày, Tuần, Tháng, Năm)
#     dff_agg = dff_crowd['in_num'].resample(agg_period).sum().reset_index()
#     dff_agg.columns = ['Thời gian', 'Lượt vào']
    
#     period_map = {'D': 'Ngày', 'W': 'Tuần', 'M': 'Tháng', 'Y': 'Năm'}
#     chart_title = f"Tổng Lượt Khách Vào Theo {period_map[agg_period]} tại {df_store.loc[df_store['tid'] == selected_store_id, 'name'].iloc[0]}"

#     # 3. Tạo biểu đồ đường
#     fig = px.line(
#         dff_agg, 
#         x='Thời gian', 
#         y='Lượt vào', 
#         title=chart_title,
#         template='plotly_dark' # Sử dụng template tối màu cho hợp với theme
#     )
#     fig.update_layout(
#         margin=dict(l=20, r=20, t=50, b=20),
#         paper_bgcolor='rgba(0,0,0,0)',
#         plot_bgcolor='rgba(0,0,0,0)',
#         xaxis_title="Thời gian",
#         yaxis_title="Số lượt khách vào"
#     )
#     fig.update_traces(mode='lines+markers')

#     # 4. Tính toán các chỉ số KPI
#     total_visitors = dff_crowd['in_num'].sum()
    
#     # Tính trung bình theo đơn vị đã chọn
#     avg_visitors_period = dff_agg['Lượt vào'].mean()

#     # Tìm ngày/tuần/tháng/năm cao điểm
#     peak_period_row = dff_agg.loc[dff_agg['Lượt vào'].idxmax()]
#     peak_time = peak_period_row['Thời gian']
#     peak_value = peak_period_row['Lượt vào']

#     if agg_period == 'D':
#         peak_time_str = peak_time.strftime('%d-%m-%Y')
#     elif agg_period == 'W':
#         peak_time_str = f"Tuần bắt đầu {peak_time.strftime('%d-%m-%Y')}"
#     elif agg_period == 'M':
#         peak_time_str = peak_time.strftime('%B %Y')
#     else: # Year
#         peak_time_str = peak_time.strftime('%Y')

#     # Tạo các thẻ KPI
#     kpi_cards = [
#         dbc.Col(dbc.Card(dbc.CardBody([
#             html.H4(f"{total_visitors:,.0f}", className="card-title"),
#             html.P("Tổng Lượt Khách", className="card-text")
#         ]), color="primary", inverse=True), width=4),
        
#         dbc.Col(dbc.Card(dbc.CardBody([
#             html.H4(f"{avg_visitors_period:,.0f}", className="card-title"),
#             html.P(f"Trung Bình Lượt Khách / {period_map[agg_period]}", className="card-text")
#         ]), color="success", inverse=True), width=4),

#         dbc.Col(dbc.Card(dbc.CardBody([
#             html.H4(f"{peak_value:,.0f} ({peak_time_str})", className="card-title"),
#             html.P(f"{period_map[agg_period]} Cao Điểm", className="card-text")
#         ]), color="info", inverse=True), width=4),
#     ]

#     # 5. Tạo danh sách thông báo lỗi
#     if dff_err.empty:
#         error_logs = [html.P("✓ Không có lỗi nào được ghi nhận.", className="text-success")]
#     else:
#         error_logs = []
#         for _, row in dff_err.head(10).iterrows(): # Hiển thị 10 lỗi gần nhất
#             error_logs.append(
#                 html.P(f"[{row['LogTime'].strftime('%Y-%m-%d %H:%M')}] - {row['ErrorMessage']} (Code: {row['Errorcode']})", className="text-danger small")
#             )

#     return fig, kpi_cards, error_logs

# # =============================================================================
# # PHẦN 3: CHẠY ỨNG DỤNG
# # =============================================================================
# if __name__ == '__main__':
#     app.run_server(debug=True)
