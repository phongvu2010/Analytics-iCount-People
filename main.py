# streamlit run main.py

import calendar
import dataset as ds
import mockups
import numpy as np
import pandas as pd
import streamlit as st

from datetime import date

@st.cache_data
def getStore():
    return ds.dbStore()

@st.cache_data
def getNumCrowd():
    return ds.dbNumCrowd()

def getWeekNums(year):
    start_date = '1/1/' + year
    end_date = '12/31/' + year
    data = pd.date_range(start = start_date, end = end_date, freq = 'D')
    data = pd.DataFrame(data, columns = ['date'])
    data['year_calendar'] = data['date'].dt.isocalendar().year
    data['week_calendar'] = data['date'].dt.isocalendar().week

    group = data.groupby(['year_calendar', 'week_calendar']) \
                .agg({'date': ['min', 'max']}).reset_index()

    group['week_num'] = np.where(group['week_calendar'][0] == 52,
                                 group['week_calendar'] + 1, group['week_calendar'])
    if group['week_num'][0] == 53: group.at[0, 'week_num'] = 1

    group['week'] = 'WK' + group['week_num'].astype(str) + \
                    ' (' + group['date']['min'].dt.strftime('%d/%m/%y') + \
                    ' - ' + group['date']['max'].dt.strftime('%d/%m/%y') + ')'
    return group

@st.cache_data
def filter_data(data, store, date = None, year = None, week = None, month = None, quarter = None):
    if date:
        data = data[data.recordtime.dt.date == date]
    else:
        data = data[data.recordtime.dt.year == year]
        if isinstance(week, int):
            data = data[data.recordtime.dt.strftime('%W').astype(int) == week]
        elif month:
            data = data[data.recordtime.dt.strftime('%B') == month]
        elif isinstance(quarter, int):
            data = data[data.recordtime.dt.to_period('Q').dt.strftime('%q').astype(int) == quarter + 1]
    
    if store > 0:
        data = data[data.storeid == store]
    return data.sort_values(by = 'recordtime', ascending = True).reset_index(drop = True)

@st.cache_data
def clean_data(data, option, period = None):
    if not data.empty:
        data.drop(columns = ['position', 'storeid'], axis = 1, inplace = True)

        data['in_num'] = data.in_num.where(data.in_num < 200, data.in_num * 0.01).apply(np.int64)
        data['out_num'] = data.out_num.where(data.out_num < 200, data.out_num * 0.01).apply(np.int64)

        if option == 'Daily':
            freqs = ['5min', '15min', '30min', 'H']
            data = data.resample(freqs[period], on = 'recordtime').sum()
        elif option == 'Weekly':
            data = data.resample('D', on = 'recordtime').sum().reset_index()
            data['day_name'] = data.recordtime.dt.day_name()
            data.set_index(['recordtime', 'day_name'], inplace = True)
        elif option == 'Monthly':
            data = data.resample('D', on = 'recordtime').sum()
        else:
            data = data.resample('M', on = 'recordtime').sum().reset_index()
            data['recordtime'] = data.recordtime.dt.strftime('%Y-%m')
            data.set_index('recordtime', inplace = True)
        return data
    return None

mockups.set_pages()
mockups.style()
mockups.add_logo()

if 'authentication_status' not in  st.session_state: st.session_state.authentication_status = ''

authen = mockups.login()
authen_status = st.session_state['authentication_status']

if authen_status:
    stores = getStore()
    option_selected = None
    date_selected = None
    period_selected = None
    year_selected = None
    week_selected = None
    month_selected = None
    quarter_selected = None

    with st.sidebar:
        st.image('images/logo_vanhanhmall.png', use_column_width = True)

        with st.expander(f'Welcome *{ st.session_state.username }*', expanded = False):
            authen.logout('Logout', 'main')

        display = tuple(['All'] + stores['name'].to_list())
        options = list(range(len(display)))
        store_selected = st.selectbox('Store:', options, format_func = lambda x: display[x])

        option_selected = st.selectbox('By:', ('Daily', 'Weekly', 'Monthly', 'Quarter', 'Yearly'), index = 2)

        if option_selected == 'Daily':
            date_selected = st.date_input('Date:', date.today())
            display = ('By every 5 minutes', 'By every 15 minutes', 'By every 30 minutes', 'By every hour')
            options = list(range(len(display)))
            period_selected = st.radio( 'Period', options, format_func = lambda x: display[x], index = 3)
        else:
            year_selected = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
            if option_selected == 'Weekly':
                weeks = getWeekNums(str(year_selected))
                display = tuple(weeks['week'])
                options = list(range(len(display)))
                week_selected = st.selectbox('Week:', options, format_func = lambda x: display[x])
            elif option_selected == 'Monthly':
                month_selected = st.selectbox('Month:', calendar.month_name[1:], \
                                              index = date.today().month - 1)
            elif option_selected == 'Quarter':
                display = ('Spring', 'Summer', 'Autumn', 'Winter')
                options = list(range(len(display)))
                quarter_selected = st.selectbox('Quarter:', options, \
                                                format_func = lambda x: display[x], \
                                                index = (date.today().month - 1) // 3)

    with st.container():
        st.title('People Counting System')

        with st.expander('**_STATISTICS REPORT_**', expanded = True):
            # st.write(f'Store : { store_selected } - { type(store_selected) }')
            # if store_selected > 0:
            #     st.write(stores.loc[store_selected - 1, 'tid'])

            # st.write(f'Daily : { date_selected } - { type(date_selected) }')
            # st.write(f'Period : { period_selected } - { type(period_selected) }')
            # st.write(f'Weekly : { week_selected } - { type(week_selected) }')
            # st.write(f'Monthly : { month_selected } - { type(month_selected) }')
            # st.write(f'Quarter : { quarter_selected } - { type(quarter_selected) }')
            # st.write(f'Yearly : { year_selected } - { type(year_selected) }')

            num_rowd = getNumCrowd()
            if store_selected > 0:
                data = filter_data(num_rowd.copy(), \
                                   stores.loc[store_selected - 1, 'tid'], date_selected, \
                                   year_selected, week_selected, month_selected, quarter_selected)
            else:
                data = filter_data(num_rowd.copy(), 0, date_selected, year_selected, \
                                   week_selected, month_selected, quarter_selected)
            df = clean_data(data, option_selected, period_selected)

            st.dataframe(df)
