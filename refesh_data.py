import database as db
import pandas as pd

from sqlalchemy.sql import extract

from models import Store, NumCrowd, ErrLog, Status, Setting

def dbErrLog():
    results = db.getSession().query(ErrLog).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/ErrLog.feather')

def dbNumCrowd():
    results = db.getSession().query(NumCrowd).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/NumCrowd.feather')

def dbNumCrowd(year):
    results = db.getSession().query(NumCrowd).filter(extract('year', NumCrowd.recordtime) == year).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/NumCrowd.feather')

def dbSetting():
    return db.getSession().query(Setting).first()
    # return pd.read_feather('temp/Setting.feather')

def dbStatus():
    results = db.getSession().query(Status).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/Status.feather')

def dbStore():
    results = db.getSession().query(Store).all()
    return pd.DataFrame([r._asdict() for r in results])
    # return pd.read_feather('temp/Store.feather')
