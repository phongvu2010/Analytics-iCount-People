# streamlit run main.py
import refesh_data as db
import streamlit as st
import streamlit_authenticator as stauth
import yaml

from st_pages import Page, show_pages, add_page_title
from yaml.loader import SafeLoader

# Basic Page Configuration
# Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(
    page_title = 'Main - People Counting System',
    page_icon = '📈',
    layout = 'centered'
)

# hashed_passwords = stauth.Hasher(['admin', 'user']).generate()
# print(hashed_passwords)

# Optional -- adds the title and icon to the current page
add_page_title()

# Specify what pages should be shown in the sidebar, and what their titles and icons
# should be
show_pages(
    [
        Page('main.py', 'Home', '🏠'),
        Page('pages/errLog.py', 'Err Log', ':books:'),
    ]
)

with open('.streamlit/config.yaml') as file:
    config = yaml.load(file, Loader = SafeLoader)

if 'authentication_status' in st.session_state:
    st.session_state['authentication_status'] = ''

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

name, authentication_status, username = authenticator.login('Login', 'main')

authen_status = st.session_state['authentication_status']
if authen_status is False:
    st.error('Username/password is incorrect')
elif authen_status is None:
    st.warning('Please enter your username and password')

@st.cache_resource
def getSetting():
    return db.dbSetting()

if authen_status:
    db_setting = getSetting()

    with st.sidebar:
        st.header(f'Welcome *{st.session_state["name"]}*')
        st.write(db_setting.companyname)
        st.write(db_setting.companyaddress)
        st.write(db_setting.companytel)

        authenticator.logout('Logout', 'main')

    with st.container():
        st.header('Information')
        st.write('The average number is according to every store by every day/week/month/year.')
