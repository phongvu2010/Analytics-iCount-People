# import database as db
# import pandas as pd

# from database import engine, getSession
# from models import Store, Camera, NumCrowd, ErrLog, Status, Setting#, User

# results = db.getSession().query(Setting).first() #.all() #.filter(m.Store.id>2)
# print(results.companyname)

# # for r in results:
#     # print(r.companyname)

# # df = pd.DataFrame([r._asdict() for r in results])
# # df.to_feather('temp/Setting.feather')

# # df = pd.read_feather('temp/Setting.feather')

# # print(df.head())



# from datetime import date
# import calendar

# year = date.today().year
# month = date.today().month
# day = date.today().day
# months = list(calendar.month_name[1:])
# week_number = date(year, month, day).isocalendar()[1]
# week = Week(year, week_number)
# week_plus1 = Week(year, week_number+1)

# print(year)
# print(month)
# print(day)
# print(months)
# print(week_number)


import pandas as pd
import numpy as np

def getWeekNums(year):
    start_date = '1/1/' + year
    end_date = '31/12/' + year
    data = pd.date_range(start = start_date, end = end_date, freq = 'D')

    df = pd.DataFrame(data, columns = ['date'])

    df['w'] = df['date'].dt.strftime('%V').astype(int)
    df['s'] = df['w'].shift(1)
    df['week_num'] = np.where(df['s'].isna(), 1, df['w'] + 1)
    group = df.groupby('week_num').agg({'date': ['min', 'max']}).reset_index()
    group['week'] = 'WK' + group['week_num'].astype(str) + \
                    ' (' + group['date']['min'].dt.strftime('%d/%m') + \
                    ' - ' + group['date']['max'].dt.strftime('%d/%m') + ')'

    return group['week'].to_list()
    # return group

print(getWeekNums('2023'))
