# streamlit run main.py

import calendar
import dataset as db
import mockups
import numpy as np
import pandas as pd
import streamlit as st

from datetime import date, datetime
from plotly import graph_objs as go

def getStore():
    return db.dbStore()

def getNumCrowd(year = None):
    if not year: year = date.today().year
    return db.dbNumCrowd(year)

def getErrLog():
    return db.dbErrLog()

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

def clean_data(data, option, period = None):
    if not data.empty:
        # data.drop(columns = ['position', 'storeid'], axis = 1, inplace = True)
        data.drop(columns = ['position', 'storeid', 'out_num'], axis = 1, inplace = True)

        data['in_num'] = data.in_num.where(data.in_num < 200, data.in_num * 0.001).apply(np.int64)
        # data['out_num'] = data.out_num.where(data.out_num < 200, data.out_num * 0.001).apply(np.int64)

        if option == 'Daily':
            freqs = ['5min', '15min', '30min', 'H']
            data = data.resample(freqs[period], on = 'recordtime').sum().reset_index()
            data['recordtime'] = data.recordtime.dt.strftime('%H:%M')
            data.set_index('recordtime', inplace = True)
        elif option == 'Weekly':
            data = data.resample('D', on = 'recordtime').sum().reset_index()
            data['day_name'] = data.recordtime.dt.day_name()
            data['recordtime'] = data.recordtime.dt.strftime('%d/%m/%Y')
            data.set_index(['recordtime', 'day_name'], inplace = True)
        elif option == 'Monthly':
            data = data.resample('D', on = 'recordtime').sum().reset_index()
            data['recordtime'] = data.recordtime.dt.strftime('%d/%m/%Y')
            data.set_index('recordtime', inplace = True)
        else:
            data = data.resample('M', on = 'recordtime').sum().reset_index()
            data['recordtime'] = data.recordtime.dt.strftime('%m/%Y')
            data.set_index('recordtime', inplace = True)

        data['Percentage'] = (data.in_num / data.in_num.sum()).map('{:.2%}'.format)
        data['Relative Ratio'] = data.in_num.pct_change().map('{:.2%}'.format, na_action = 'ignore')

        data.rename(columns = {'in_num': 'Quantity'}, inplace = True)
        data.index.names = ['Time']

    return data

mockups.set_pages()
mockups.style()
mockups.add_logo()

if 'authentication_status' not in  st.session_state:
    st.session_state.authentication_status = ''

authen = mockups.login()
authen_status = st.session_state['authentication_status']
username = st.session_state['username']

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
            period_selected = st.radio( 'Period', options,
                                       format_func = lambda x: display[x], index = 3)
        else:
            year_selected = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
            if option_selected == 'Weekly':
                weeks = getWeekNums(str(year_selected))
                display = tuple(weeks['week'])
                options = list(range(len(display)))
                week_selected = st.selectbox('Week:', options,
                                             format_func = lambda x: display[x],
                                             index = int(date.today().strftime('%W')))
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

        if username == 'admin':
            with st.expander('**_ERROR LOG_**', expanded = False):
                err_log = getErrLog()
                err_log = err_log.groupby(['storeid', 'ErrorMessage']).max()
                err_log = err_log.drop(columns = ['ID', 'Errorcode', 'DeviceCode'], axis = 1).reset_index()
                err_log = err_log.merge(stores[['tid', 'name']].rename(columns = {'tid': 'storeid'}),
                                        on = 'storeid', how = 'left').set_index('LogTime')
                err_log.drop('storeid', axis = 1, inplace = True)
                err_log.insert(0, 'Name', err_log.pop('name'))
                err_log.sort_index(ascending = False, inplace = True)
                err_log.Name = err_log.Name.replace(r'\s+', ' ', regex = True)
                err_log.ErrorMessage = err_log.ErrorMessage.replace(r'\s+', ' ', regex = True)

                st.dataframe(err_log, use_container_width = True)

        with st.expander('**_STATISTICS REPORT_**', expanded = True):
            num_rowd = getNumCrowd(year_selected)
            if store_selected > 0:
                data = filter_data(num_rowd.copy(), \
                                   stores.loc[store_selected - 1, 'tid'], date_selected, \
                                   year_selected, week_selected, month_selected, quarter_selected)
            else:
                data = filter_data(num_rowd.copy(), 0, date_selected, year_selected, \
                                   week_selected, month_selected, quarter_selected)
            data = clean_data(data, option_selected, period_selected)

            if not data.empty:
                fig = go.Figure()

                if option_selected == 'Weekly':
                    fig.add_trace(go.Bar(x = data.index.levels[0], y = data.Quantity, name = 'Quantity', showlegend = False))
                    # fig.add_trace(go.Bar(x = data.index.levels[0], y = data.out_num, name = 'Out', showlegend = False))

                    fig.update_xaxes(tickmode = 'array',
                                    tickvals = data.index.levels[0],
                                    ticktext = [datetime.strptime(d, '%d/%m/%Y').strftime('%A') for d in data.index.levels[0]])
                else:
                    fig.add_trace(go.Bar(x = data.index, y = data.Quantity, name = 'Quantity', showlegend = False))
                    # fig.add_trace(go.Bar(x = data.index, y = data.out_num, name = 'Out', showlegend = False))

                layout = go.Layout(
                    autosize = True,
                    height = 300,
                    margin = go.layout.Margin(l = 10, r = 10, b = 5, t = 30, pad = 0)
                )
                fig.update_layout(layout)

                st.plotly_chart(fig, use_container_width = True)
                st.dataframe(data, use_container_width = True)
            else:
                st.warning('No data ...')
