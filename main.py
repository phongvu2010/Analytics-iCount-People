# streamlit run main.py

import calendar
import numpy as np
import pandas as pd
import refesh_data as db
import streamlit as st
import streamlit_authenticator as stauth
import yaml

from datetime import date
from yaml.loader import SafeLoader

# Basic Page Configuration
# Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(
    page_title = 'Main - People Counting System',
    page_icon = '📈',
    layout = 'wide'
)

# hashed_passwords = stauth.Hasher(['admin', 'user']).generate()
# print(hashed_passwords)

if 'authentication_status' in st.session_state:
    st.session_state['authentication_status'] = ''

with open('.streamlit/config.yaml') as file:
    config = yaml.load(file, Loader = SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

name, authentication_status, username = authenticator.login('Login', 'main')
authen_status = st.session_state['authentication_status']

@st.cache_resource
def getSetting():
    return db.dbSetting()

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

    return group['week'].to_list()

if authen_status:
    db_setting = getSetting()
    db_store = getStore()
elif authen_status is False:
    st.error('Username/password is incorrect')

if authen_status:
    with st.sidebar:
        st.header(f'Welcome *{st.session_state["name"]}*')

        st.write(db_setting.companyname)
        st.write(db_setting.companyaddress)
        st.write(db_setting.companytel)

        authenticator.logout('Logout', 'main')

    with st.container():
        st.header('STATISTICS REPORT')

        top_menu = st.columns(4)
        with top_menu[0]:
            l_store = ['All'] + db_store['name'].to_list()
            store_selected = st.selectbox('Store:', l_store)

        with top_menu[1]:
            option = st.selectbox('By:', ('Daily', 'Weekly', 'Monthly', 'Quarter', 'Yearly'), index = 2)

        with top_menu[2]:
            if option == 'Daily':
                d = st.date_input('Date:', date.today())
            elif option == 'Weekly':
                y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
                with top_menu[3]: w = st.selectbox('Week:', getWeekNums(str(y)))
            elif option == 'Monthly':
                y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
                with top_menu[3]: m = st.selectbox('Month:', calendar.month_name[1:], index = date.today().month - 1)
            elif option == 'Quarter':
                y = st.selectbox('Year', reversed(range(2018, date.today().year + 1)))
                with top_menu[3]: q = st.selectbox('Quarter:', (1, 2, 3, 4), index = (date.today().month - 1) // 3)
            else:
                y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))

        

        st.write('The average number is according to every store by every day/week/month/year.')
        # st.write('Your birthday is:', d)
