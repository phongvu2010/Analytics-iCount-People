import pandas as pd
from typing import List, Dict

def aggregate_crowd_data(data: List[Dict], period: str = 'daily') -> List[Dict]:
    """ Nhóm dữ liệu đếm người theo ngày, tuần, tháng. """
    if not data:
        return []

    df = pd.DataFrame([d.__dict__ for d in data])
    if df.empty:
        return []

    # Tạo DataFrame từ dữ liệu truy vấn được
    df = pd.DataFrame(data, columns=['recordtime', 'in_num', 'out_num'])
    df['recordtime'] = pd.to_datetime(df['recordtime'])
    df.set_index('recordtime', inplace = True)

    rule_map = {'daily': 'D', 'weekly': 'W-MON', 'monthly': 'M'}
    rule = rule_map.get(period, 'D')

    # Nhóm theo chu kỳ và tính tổng số lượng vào/ra
    agg_df = df[['in_num', 'out_num']].resample(rule).sum()

    # Chuyển đổi kết quả về dạng JSON để trả về cho API
    # Đổi tên cột `recordtime` thành `period` để dễ hiểu hơn ở frontend
    agg_df.reset_index(inplace = True)
    agg_df.rename(columns = {'recordtime': 'period'}, inplace = True)
    agg_df['period'] = agg_df['period'].dt.strftime('%Y-%m-%d')

    return agg_df.to_dict(orient = 'records')
