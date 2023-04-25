# Import needed libraries
import streamlit as st

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib import parse

env = 'development'

db_host = st.secrets[env]['DB_HOST']
db_port = parse.quote_plus(str(st.secrets[env]['DB_PORT']))
db_name = st.secrets[env]['DB_NAME']
db_user = parse.quote_plus(st.secrets[env]['DB_USER'])
db_pass = parse.quote_plus(st.secrets[env]['DB_PASS'])

DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver=SQL Server'

engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(autocommit = False, autoflush = False, bind = engine)

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def getSession():
    session = Session()
    try:
        return session
    finally:
        session.close()
