# streamlit run main.py

import calendar
import streamlit as st
import mockups

from datetime import date

mockups.set_pages()
mockups.hidden_style()
mockups.add_logo()

if 'authentication_status' not in  st.session_state:
    st.session_state.authentication_status = ''

authen = mockups.login()
authen_status = st.session_state['authentication_status']

if authen_status:
    st.header('People Counting System')

    with st.sidebar:
        # st.header(f'Welcome *{ st.session_state.name }*')
        # authen.logout('Logout', 'main')

        option = st.selectbox('By:', ('Daily', 'Weekly', 'Monthly', 'Quarter', 'Yearly'), index = 2)

        if option == 'Daily':
            d = st.date_input('Date:', date.today())
        else:
            y = st.selectbox('Year:', reversed(range(2018, date.today().year + 1)))
            if option == 'Weekly':
                # w = st.selectbox('Week:', options, format_func = lambda x: display[x])
                w = st.selectbox('Week:', (1, 2, 3, 4))
            elif option == 'Monthly':
                m = st.selectbox('Month:', calendar.month_name[1:], index = date.today().month - 1)
            elif option == 'Quarter':
                display = ('Spring', 'Summer', 'Autumn', 'Winter')
                options = list(range(len(display)))
                q = st.selectbox('Quarter:', options, format_func = lambda x: display[x], index = (date.today().month - 1) // 3)
