import database as db
# import pandas as pd

from models import Store, NumCrowd, ErrLog, Status, Setting

def dbErrLog():
    return db.getSession().query(ErrLog).all()
    # return pd.read_feather('temp/ErrLog.feather')

def dbNumCrowd():
    return db.getSession().query(NumCrowd).all()
    # return pd.read_feather('temp/NumCrowd.feather')

def dbSetting():
    return db.getSession().query(Setting).first()
    # return pd.read_feather('temp/Setting.feather')

def dbStatus():
    return db.getSession().query(Status).all()
    # return pd.read_feather('temp/Status.feather')

def dbStore():
    return db.getSession().query(Store).all()
    # return pd.read_feather('temp/Store.feather')
