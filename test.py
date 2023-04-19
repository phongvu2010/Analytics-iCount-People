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

from datetime import datetime

import calendar
m = calendar.month_name[1:]
print(m)