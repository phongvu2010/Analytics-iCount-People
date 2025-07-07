import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib import parse

@st.cache_resource
def connect_db():
    db_host = st.secrets['DB_HOST']
    db_port = st.secrets['DB_PORT']
    db_name = st.secrets['DB_NAME']
    db_user = st.secrets['DB_USER']
    db_pass = parse.quote_plus(st.secrets['DB_PASS'])
    db_driver = st.secrets['DB_DRIVER']
    DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

    return create_engine(DATABASE_URL)

# Tạo engine kết nối tới DB
engine = connect_db()

# Tạo một phiên (session) để tương tác với DB
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Base class cho các ORM models
# Base = declarative_base()

# Initialize connection - Dependency để inject session vào mỗi request
@st.cache_resource
def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()
        print('\n[INFO] Session đã được đóng.')
