import database as db
import pandas as pd
import streamlit as st

from models import Store, NumCrowd, ErrLog, Status
from sqlalchemy import extract

# Uses st.cache_data to only rerun when the query changes or after 15 min.
@st.cache_data(ttl = 900)
def dbErrLog():
    results = db.getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/ErrLog.feather')

@st.cache_data(ttl = 900)
def dbNumCrowd(year = None):
    results = db.getSession().query(NumCrowd)
    if year: results = results.filter(extract('year', NumCrowd.recordtime) == year)
    results = results.all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/NumCrowd.feather')

# @st.cache_data(ttl = 900)
# def dbSetting():
#     return db.getSession().query(Setting).first()
#     return pd.read_feather('temp/Setting.feather')

# @st.cache_data(ttl = 900)
# def dbStatus():
#     results = db.getSession().query(Status).all()
#     return pd.DataFrame([r._asdict() for r in results])
#     return pd.read_feather('temp/Status.feather')

@st.cache_data(ttl = 86400)
def dbStore():
    results = db.getSession().query(Store).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/Store.feather')
