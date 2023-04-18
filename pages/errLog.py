import database as db
import streamlit as st

# from models import Store, NumCrowd, ErrLog, Status, Setting
### Basic Page Configuration
# Find more emoji here: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(
    page_title = 'ErrLog - People Counting System',
    page_icon = '📈',
    layout = 'centered'
)

# pages/sensitive_page.py
if st.session_state['name'] != 'Admin':
    st.warning("You must log-in to see the content of this sensitive page! Head over to the log-in page.")
    st.stop()  # App won't run anything after this line

# sensitive_stuffs()