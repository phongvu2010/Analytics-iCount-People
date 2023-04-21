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




from st_pages import Page, Section, show_pages, add_page_title

show_pages(
    [
        Page("test.py", "Home", "🏠"),
        # Can use :<icon-name>: or the actual icon
        # Page("example_app/example_one.py", "Example One", ":books:"),
        # Since this is a Section, all the pages underneath it will be indented
        # The section itself will look like a normal page, but it won't be clickable
        Section(name="Reports", icon=":pig:"),
        # The pages appear in the order you pass them
        Page("pages/1_Daily Statistics.py", "Daily report", "📖")
        # Will use the default icon and name based on the filename if you don't
        # pass them
        # You can also pass in_section=False to a page to make it un-indented
        # Page("example_app/example_five.py", "Example Five", "🧰", in_section=False),
    ]
)

add_page_title()  # Optional method to add title and icon to current page