import numpy as np
import pandas as pd

# date = datetime.date(2023, 12, 31)
# week_number_new = date.isocalendar().week
# year = date.isocalendar().year
# print(str(year) + '-' + str(week_number_new))

def getWeekNums(year):
    start_date = '1/1/' + year
    end_date = '12/31/' + year
    data = pd.date_range(start = start_date, end = end_date, freq = 'D')
    data = pd.DataFrame(data, columns = ['date'])
    data['year_calendar'] = data['date'].dt.isocalendar().year
    data['week_calendar'] = data['date'].dt.isocalendar().week

    group = data.groupby(['year_calendar', 'week_calendar']).agg({'date': ['min', 'max']}).reset_index()

    group['week_num'] = np.where(group['week_calendar'][0] == 52,
                                 group['week_calendar'] + 1, group['week_calendar'])
    if group['week_num'][0] == 53: group.at[0, 'week_num'] = 1

    group['week'] = 'WK' + group['week_num'].astype(str) + \
                    ' (' + group['date']['min'].dt.strftime('%d/%m') + \
                    ' - ' + group['date']['max'].dt.strftime('%d/%m') + ')'

    return group.drop(['year_calendar', 'week_calendar'], axis = 1)

df = getWeekNums('2023')
print(df)
# print(df['week_num'][0])
# df.at[0, 'week_num'] = 10
# print(df['week_num'][0])
# print(df.loc(0)[0]['week_num'])


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


# import pandas as pd
# import numpy as np

# from datetime import datetime

# import calendar
# m = calendar.month_name[1:]
# print(m)



