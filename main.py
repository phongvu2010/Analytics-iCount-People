# streamlit run main.py

import database as db
import streamlit as st
import streamlit_authenticator as stauth
import yaml

from models import Store, NumCrowd, ErrLog, Status, Setting
from streamlit_option_menu import option_menu
from yaml.loader import SafeLoader

### Basic Page Configuration
# Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(
    page_title = 'Main - People Counting System',
    page_icon = '📈',
    layout = 'centered'
)

# hashed_passwords = stauth.Hasher(['admin', 'user']).generate()
# print(hashed_passwords)

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
if authen_status is False:
    st.error('Username/password is incorrect')
elif authen_status is False:
    st.warning('Please enter your username and password')

@st.cache_resource
def getSetting():
    return db.getSession().query(Setting).first()

if authen_status:
    db_setting = getSetting()

with st.sidebar:
    if authen_status:
        st.header(f'Welcome *{st.session_state["name"]}*')
        st.write(db_setting.companyname)
        st.write(db_setting.companyaddress)
        st.write(db_setting.companytel)
        choose = option_menu(
            "App Gallery", ["About", "Photo Editing", "Project Planning", "Python e-Course", "Contact"],
            icons = ['house', 'camera fill', 'kanban', 'book','person lines fill'],
            menu_icon = "app-indicator", default_index = 0,
            styles = {
                # "container": {"padding": "5!important", "background-color": "#fafafa"},
                # "icon": {"color": "orange", "font-size": "25px"}, 
                # "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                # "nav-link-selected": {"background-color": "#02ab21"},
            }
        )
        # st.success('Select a page above.')
        
        authenticator.logout('Logout', 'main')

with st.container():
    if authen_status:
        st.header('Information')
        st.write('The average number is according to every store by every day/week/month/year.')
