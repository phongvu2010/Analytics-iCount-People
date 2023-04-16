# Import needed libraries
import pandas as pd
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib import parse

load_dotenv(override = True)
db_host = os.environ.get('DB_HOST', 'localhost')
db_port = parse.quote_plus(str(os.environ.get('DB_PORT', 5432)))
db_name = os.environ.get('DB_NAME', 'statistic')
db_user = parse.quote_plus(str(os.environ.get('DB_USER', 'postgres')))
db_pass = parse.quote_plus(str(os.environ.get('DB_PASS', '0000')))

# DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver=SQL Server'
DATABASE_URL = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(autocommit = False, autoflush = False, bind = engine)

def getSession():
    session = Session()
    try:
        yield session
    finally:
        session.close()

# Read Target data into a dataframe
# def get_data(engine, tblName):
#     target = pd.read_sql('SELECT * FROM "' + tblName + '"', engine)

#     return target

# df = get_data(engine, 'num_crowd')
# print(df.head())
