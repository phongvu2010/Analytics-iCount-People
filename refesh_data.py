# import database as db
import pandas as pd

# from models import Store, NumCrowd, ErrLog, Status, Setting

def dbSetting():
    # return db.getSession().query(Setting).first()
    return pd.read_feather('temp/Setting.feather')
