# Ví dụ trong app/services/analysis.py
import pandas as pd

def aggregate_crowd_data(data, period: str = 'daily'):
    if not data:
        return []

    # Tạo DataFrame từ dữ liệu truy vấn được
    df = pd.DataFrame(data, columns=['recordtime', 'in_num', 'out_num'])
    df['recordtime'] = pd.to_datetime(df['recordtime'])
    df.set_index('recordtime', inplace=True)

    # Resample (nhóm lại) dữ liệu theo chu kỳ và tính tổng
    # 'D' -> Daily, 'W' -> Weekly, 'M' -> Monthly
    rule = 'D'
    if period == 'weekly':
        rule = 'W-MON' # Tuần bắt đầu từ thứ 2
    elif period == 'monthly':
        rule = 'M'

    # Nhóm theo chu kỳ và tính tổng số lượng vào/ra
    agg_df = df.resample(rule).sum()

    # Chuyển đổi kết quả về dạng JSON để trả về cho API
    agg_df.reset_index(inplace=True)
    agg_df['recordtime'] = agg_df['recordtime'].dt.strftime('%Y-%m-%d') # Format ngày tháng

    return agg_df.to_dict(orient='records')
