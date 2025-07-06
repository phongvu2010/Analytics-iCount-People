import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib import parse

@st.cache_resource
def connectDB():
    db_host = st.secrets['DB_HOST']
    db_port = st.secrets['DB_PORT']
    db_name = st.secrets['DB_NAME']
    db_user = st.secrets['DB_USER']
    db_pass = parse.quote_plus(st.secrets['DB_PASS'])
    db_driver = st.secrets['DB_DRIVER']

    DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

    return create_engine(DATABASE_URL)

# Tạo engine kết nối tới DB
engine = connectDB()

# Tạo một phiên (session) để tương tác với DB
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class cho các ORM models
Base = declarative_base()

# Initialize connection - Dependency để inject session vào mỗi request
@st.cache_resource
def getSession():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()










# import pandas as pd

# from datetime import date
# from models import Store, NumCrowd, ErrLog, Status
# from sqlalchemy import extract

# @st.cache_data(ttl = 86400, show_spinner = False)
# def dbStore():
#     query = getSession().query(Store)

#     return pd.read_sql(sql = query.statement, con = engine)
#     # return pd.DataFrame([r._asdict() for r in results])

# @st.cache_data(ttl = 900, show_spinner = False)
# def dbNumCrowd(year = None):
#     query = getSession().query(NumCrowd)
#     if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

#     return pd.read_sql(sql = query.statement, con = engine)

# @st.cache_data(ttl = 3600, show_spinner = False)
# def dbErrLog():
#     query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

#     return pd.read_sql(sql = query.statement, con = engine)

# @st.cache_data(ttl = 3600, show_spinner = False)
# def dbStatus():
#     query = getSession().query(Status)

#     return pd.read_sql(sql = query.statement, con = engine)
