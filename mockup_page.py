# streamlit run login.py

import refesh_data as db
import streamlit as st
import streamlit_authenticator as stauth
import yaml

from yaml.loader import SafeLoader

def set_pages():
    # Basic Page Configuration
    # Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
    st.set_page_config(
        page_title = 'People Counting System',
        page_icon = '📈',
        layout = 'centered',
        # initial_sidebar_state = 'collapsed'
    )

# hashed_passwords = stauth.Hasher(['admin', 'user']).generate()
# print(hashed_passwords)

def login():
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

    if not st.session_state.authentication_status:
        st.error('Username/password is incorrect')
    elif st.session_state.authentication_status is None:
        st.warning('Please enter your username and password')

    return authenticator

def hidden_style():
    st.markdown('''
        <style>
            #MainMenu { visibility: hidden; }
            footer  { visibility: hidden; }
        </style>
    ''', unsafe_allow_html = True)

@st.cache_resource
def getSetting():
    return db.dbSetting()

def sidebar_info(authenticator):
    with st.sidebar:
        st.header(f'Welcome *{ st.session_state.name }*')

        db_setting = getSetting()
        st.write(db_setting.companyname)
        st.write(db_setting.companyaddress)
        st.write(db_setting.companytel)

        authenticator.logout('Logout', 'main')
