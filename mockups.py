import streamlit as st
import streamlit_authenticator as stauth
import yaml

from yaml.loader import SafeLoader

def set_pages(sidebar: str = 'expanded'):   # 'collapsed'
    # Basic Page Configuration
    # Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
    st.set_page_config(
        page_title = 'People Counting System',
        page_icon = '📈',
        layout = 'centered',
        initial_sidebar_state = sidebar
    )

def hidden_style():
    st.markdown('''
        <style>
            #MainMenu { visibility: hidden; }
            footer  { visibility: hidden; }
        </style>
    ''', unsafe_allow_html = True)

def add_logo():
    st.markdown('''
        <style>
            [data-testid="stSidebarNav"] {
                background-image: url(https://static.ybox.vn/2021/5/4/1622090809441-logo%20Vanhanhmall%20-%20tagline-01.png);
                background-repeat: no-repeat;
                margin-top: 25px;
                background-position: center top;;
            }
        </style>
    ''', unsafe_allow_html = True)

def login():
    # hashed_passwords = stauth.Hasher(['admin', 'user']).generate()
    # print(hashed_passwords)

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

    return authenticator
