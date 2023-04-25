# Import needed libraries
# import os
# import pyodbc
import streamlit as st

# from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib import parse

env = 'production'

# # Initialize connection.
# # Uses st.cache_resource to only run once.
# @st.cache_resource
# def init_connection():
#     return pyodbc.connect(
#         'DRIVER={SQL Server}' +
#         ';SERVER=' + st.secrets[env]['DB_HOST'] +
#         ';DATABASE=' + st.secrets[env]['DB_NAME'] +
#         ';UID=' + st.secrets[env]['DB_USER'] +
#         ';PWD=' + parse.quote_plus(st.secrets[env]['DB_PASS'])
#     )

# conn = init_connection()

# # Perform query.
# # Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl = 600)
# def run_query(query):
#     with conn.cursor() as cur:
#         cur.execute(query)
#         return cur.fetchall()

# rows = run_query('SELECT * FROM store;')
# # import pandas as pd
# # df = pd.read_sql_query('SELECT * FROM products', conn)


# # Print results.
# for row in rows:
#     # st.write(f"{row[0]} has a :{row[1]}:")
#     print(f'{ row[0] } has a :{ row[2] }:')

# # a = st.secrets['production']['DB_HOST']

# # print(a)

# load_dotenv(override = True)
# db_host = os.environ.get('DB_HOST', 'localhost')
# db_port = parse.quote_plus(str(os.environ.get('DB_PORT', 5432)))
# db_name = os.environ.get('DB_NAME', 'statistic')
# db_user = parse.quote_plus(str(os.environ.get('DB_USER', 'postgres')))
# db_pass = parse.quote_plus(str(os.environ.get('DB_PASS', '0000')))

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
