import calendar
import numpy as np
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml

from datetime import date
from plotly import graph_objs as go
from yaml.loader import SafeLoader

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

@st.cache_data(ttl = 900, show_spinner = False)
def filter(data, store = 0, date = None, year = None, week = None, month = None, quarter = None):
    if date:
        data = data[data.recordtime.dt.date == date]
    else:
        data = data[data.recordtime.dt.year == year]
        if week:
            data = data[data.recordtime.dt.strftime('%W').astype(int) == week]
        elif month:
            data = data[data.recordtime.dt.strftime('%B') == month]
        elif quarter:
            data = data[data.recordtime.dt.to_period('Q').dt.strftime('%q').astype(int) == quarter + 1]
    if store > 0:
        data = data[data.storeid == store]
    return data.sort_values(by = 'recordtime').reset_index(drop = True)

@st.cache_data(ttl = 900, show_spinner = False)
def clean(data, option, period = None):
    data.drop(['out_num', 'position', 'storeid'], axis = 1, inplace = True)
    data['in_num'] = data.in_num.where(data.in_num < 500, data.in_num * 0.0001).apply(np.int64)

    if option == 'Daily':
        freqs = ['15min', '30min', 'H']
        data = data.resample(freqs[period], on = 'recordtime').sum()
        data.index = data.index.strftime('%H:%M')
    elif option == 'Weekly':
        data = data.resample('D', on = 'recordtime').sum()
        data['Day'] = data.index.day_name()
        data = data[['Day', 'in_num']]
        data.index = data.index.strftime('%d/%m/%Y')
    elif option == 'Monthly':
        data = data.resample('D', on = 'recordtime').sum()
        data.index = data.index.strftime('%d/%m/%Y')
    else:
        data = data.resample('M', on = 'recordtime').sum()
        data.index = data.index.strftime('%m/%Y')

    data['Percentage'] = (data.in_num / data.in_num.sum()).map('{:.2%}'.format)
    data['Relative Ratio'] = data.in_num.pct_change().map('{:.2%}'.format, na_action = 'ignore')

    data.rename(columns = {'in_num': 'Quantity'}, inplace = True)
    data.index.names = ['Time']

    return data

# Basic Page Configuration
# Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(
    page_title = 'People Counting System', page_icon = '📈', layout = 'wide'
)

from database import dbStore, dbNumCrowd, dbErrLog

with open('style.css') as f: st.markdown(f'<style>{ f.read() }</style>', unsafe_allow_html = True)

if 'authentication_status' not in  st.session_state:
    st.session_state.authentication_status = ''

with open('config.yaml') as file:
    config = yaml.load(file, Loader = SafeLoader)
    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )
    authenticator.login('Login', 'main')

    if st.session_state.authentication_status is False:
        st.error('Username/password is incorrect')
    elif st.session_state.authentication_status is None:
        st.warning('Please enter your username and password')

if st.session_state.authentication_status:
    stores = dbStore()
    date_, period_, year_, week_, month_, quarter_ = None, None, None, None, None, None

    with st.sidebar:
        st.image('logo.png', use_column_width = True)

        with st.expander(f'Welcome *{ st.session_state.username.title() }*', expanded = False):
            authenticator.logout('Logout', 'main')

        display = tuple(['All'] + stores['name'].to_list())
        store_ = st.selectbox('Store:', display)

        option_ = st.selectbox('By:', ('Daily', 'Weekly', 'Monthly', 'Quarter', 'Yearly'), index = 2)

        if option_ == 'Daily':
            date_ = st.date_input('Date:', date.today())

            display = ('By every 15 minutes', 'By every 30 minutes', 'By every hour')
            options = list(range(len(display)))
            period_ = st.radio('Period', options, format_func = lambda x: display[x], index = 2)
        else:
            year_ = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))

            if option_ == 'Weekly':
                weeks = getWeekNums(str(year_))
                display = tuple(weeks['week'])
                options = list(range(len(display)))
                week_ = st.selectbox('Week:', options,
                                     format_func = lambda x: display[x],
                                     index = int(date.today().strftime('%W')))
            elif option_ == 'Monthly':
                month_ = st.selectbox('Month:', calendar.month_name[1:], \
                                              index = date.today().month - 1)
            elif option_ == 'Quarter':
                display = ('Spring', 'Summer', 'Autumn', 'Winter')
                options = list(range(len(display)))
                quarter_ = st.selectbox('Quarter:', options, \
                                                format_func = lambda x: display[x], \
                                                index = (date.today().month - 1) // 3)

    with st.container():
        st.title('People Counting System')

        if st.session_state.username == 'admin':
            with st.expander('**_ERROR LOG_**', expanded = False):
                err_log = dbErrLog()
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
            y = year_ if year_ else date_.year
            num_crowd = dbNumCrowd(y)
            if not num_crowd.empty:
                storeid = 0 if store_ == 'All' else stores[stores['name'] == store_]['tid'].iloc[0]

                data = filter(num_crowd, storeid, date_, year_, week_, month_, quarter_)
                data = clean(data, option_, period_)

                fig = go.Figure()
                fig.add_trace(go.Bar(x = data.index, y = data.Quantity, name = 'Quantity', showlegend = False))
                fig.update_layout(
                    go.Layout(autosize = True, height = 300, margin = go.layout.Margin(l = 10, r = 10, b = 5, t = 30, pad = 0))
                )

                st.plotly_chart(fig, use_container_width = True)
                st.dataframe(data, use_container_width = True)
            else: st.warning('No data ...')
