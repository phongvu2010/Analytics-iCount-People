import database as db
import pandas as pd

from database import engine, getSession
from models import Store, Camera, NumCrowd, ErrLog, Status, Setting#, User

results = db.getSession().query(Setting).first() #.all() #.filter(m.Store.id>2)
print(results.companyname)

# for r in results:
    # print(r.companyname)

# df = pd.DataFrame([r._asdict() for r in results])
# df.to_feather('temp/Setting.feather')

# df = pd.read_feather('temp/Setting.feather')

# print(df.head())
