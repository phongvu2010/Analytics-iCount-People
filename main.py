# streamlit run main.py

import streamlit as st
import calendar
import numpy as np
import pandas as pd
import refesh_data as db
import mockup_page as mp

from datetime import date, datetime

mp.set_pages()
mp.hidden_style()

authen = mp.login()
authen_status = st.session_state['authentication_status']

@st.cache_resource
def getStore():
    return db.dbStore()

@st.cache_resource
def getNumCrowd():
    return db.dbNumCrowd()

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

    return group

def filter_data(data, store = 0, d = None, w = None, m = None, q = None, y = None):
    if (not y) and d:
        # print('Daily')
        data = data[data.recordtime.dt.date == d]
    else:
        data = data[data.recordtime.dt.year == y]
        if w:
            # print('Weekly')
            data = data[data.recordtime.dt.date >= datetime.fromtimestamp(w[0]) & data.recordtime.dt.date <= datetime.fromtimestamp(w[1])]
        elif m:
            # print('Monthly')
            data = data[data.recordtime.dt.strftime('%B') == m]
        elif q:
            # print('Quarter')
            data = data[data.recordtime.dt.to_period('Q').dt.strftime('%q').astype(int) == q + 1]

    if store > 0:
        data = data[data.storeid == store]

    return data

if authen_status:
    mp.sidebar_info(authen)
    db_store = getStore()

    with st.container():
        st.header('statistics report'.title())

        top_menu = st.columns(4)
        with top_menu[0]:
            display = tuple([''] + db_store['name'].to_list())
            options = list(range(len(display)))
            store_selected = st.selectbox('Store:', options, format_func = lambda x: display[x])

        with top_menu[1]:
            option = st.selectbox('By:', ('Daily', 'Weekly', 'Monthly', 'Quarter', 'Yearly'), index = 2)

        with top_menu[2]:
            d, w, m, q, y = None, None, None, None, None
            if option == 'Daily':
                d = st.date_input('Date:', date.today())
            elif option == 'Weekly':
                y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
                with top_menu[3]:
                    weeks = getWeekNums(str(y))
                    display = tuple(weeks['week'])
                    options = list(range(len(display)))
                    w = st.selectbox('Week:', options, format_func = lambda x: display[x])
                    w = (weeks.loc(0)[w][1], weeks.loc(0)[w][2])
            elif option == 'Monthly':
                y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
                with top_menu[3]: m = st.selectbox('Month:', calendar.month_name[1:], index = date.today().month - 1)
            elif option == 'Quarter':
                y = st.selectbox('Year', reversed(range(2018, date.today().year + 1)))
                display = ('Spring', 'Summer', 'Autumn', 'Winter')
                options = list(range(len(display)))
                print(display)
                print(options)
                with top_menu[3]: q = st.selectbox('Quarter:', options, format_func = lambda x: display[x], index = (date.today().month - 1) // 3)
            else:
                y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))

        st.write('The average number is according to every store by every day/week/month/year.')

        data = filter_data(getNumCrowd().copy(), db_store.loc[store_selected - 1, 'tid'], d, w, m, q, y)
        st.write(data)

        # st.write(f'Store : { store_selected } - { type(store_selected) }')        
        # if store_selected > 0:
        #     st.write(db_store.loc[store_selected - 1, 'tid'])

        # st.write(f'Daily : { d } - { type(d) }')
        # st.write(f'Weekly : { w } - { type(w) }')
        # st.write(f'Monthly : { m } - { type(m) }')
        # st.write(f'Quarter : { q } - { type(q) }')
        # st.write(f'Yearly : { y } - { type(y) }')
