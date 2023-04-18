import database as db
import pandas as pd

from database import engine, getSession
from models import Store, Camera, NumCrowd, ErrLog, Status, Setting, User

# results = db.getSession().query(Setting).all() #.filter(m.Store.id>2)
# df = pd.DataFrame([r._asdict() for r in results])
# df.to_feather('temp/Setting.feather')

df = pd.read_feather('temp/NumCrowd.feather')

print(df.head())
