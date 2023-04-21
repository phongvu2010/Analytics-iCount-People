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

st.write('The average number is according to every store by every day/week/month/year.')