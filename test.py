# import database as db
# import pandas as pd

# from database import engine, getSession
# from models import Store, NumCrowd, ErrLog, Status

# results = db.getSession().query(NumCrowd).all() #.all() #.filter(m.Store.id>2)
# print(results.companyname)

# for r in results:
#     print(r.companyname)

# df = pd.DataFrame([r._asdict() for r in results])
# df.to_feather('temp/NumCrowd.feather')

# df = pd.read_feather('temp/NumCrowd.feather')

# print(df.head())



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



# import pandas as pd
# import numpy as np
# import plotly.graph_objects as go

# # data
# np.random.seed(42)
# feature = pd.DataFrame({'ds': pd.date_range('20200101', periods = 100*24, freq = 'H'),
#                         'y': np.random.randint(0, 20, 100*24),
#                         'yhat': np.random.randint(0, 20, 100*24),
#                         'price': np.random.choice([6600, 7000, 5500, 7800], 100*24)})

# # resampling
# y = feature.set_index('ds').resample('D')['y'].sum().to_frame()
# y.reset_index(inplace = True)

# # plotly setup
# fig = go.Figure()
# fig.add_trace(go.Scatter(x = y.index, y = y.y))

# # x-ticks preparations
# x_dates = y.ds
# tickvals = np.arange(0, y.shape[0]).astype(int)#[0::40]
# ticktext = x_dates

# # update tickmarks
# fig.update_xaxes(
#                  # tickangle = 45,
#                  tickmode = 'array',
#                  tickvals = tickvals,
#                  ticktext = [d.strftime('%Y-%m-%d') for d in ticktext])

# fig.show()


