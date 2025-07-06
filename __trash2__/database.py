import pandas as pd
import streamlit as st

from datetime import date
from models import Store, NumCrowd, ErrLog, Status
from sqlalchemy import create_engine, extract
from sqlalchemy.orm import sessionmaker
from urllib import parse

@st.cache_resource
def connectDB():
    env = st.secrets['development']

    db_host = env['DB_HOST']
    db_port = env['DB_PORT']
    db_name = env['DB_NAME']
    db_user = env['DB_USER']
    db_pass = parse.quote_plus(str(env['DB_PASS']))

    DATABASE_URL = f'mssql+pyodbc://{ db_user }:{ db_pass }@{ db_host }:{ db_port }/{ db_name }?driver=SQL Server'

    return create_engine(DATABASE_URL)

engine = connectDB()
Session = sessionmaker(autocommit = False, autoflush = False, bind = engine)

# Initialize connection.
@st.cache_resource
def getSession():
    session = Session()
    try:
        return session
    finally:
        session.close()

@st.cache_data(ttl = 86400, show_spinner = False)
def dbStore():
    query = getSession().query(Store)

    return pd.read_sql(sql = query.statement, con = engine)
    # return pd.DataFrame([r._asdict() for r in results])

@st.cache_data(ttl = 900, show_spinner = False)
def dbNumCrowd(year = None):
    query = getSession().query(NumCrowd)
    if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

    return pd.read_sql(sql = query.statement, con = engine)

@st.cache_data(ttl = 3600, show_spinner = False)
def dbErrLog():
    query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

    return pd.read_sql(sql = query.statement, con = engine)

@st.cache_data(ttl = 3600, show_spinner = False)
def dbStatus():
    query = getSession().query(Status)

    return pd.read_sql(sql = query.statement, con = engine)
