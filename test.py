import database as db
import models as m
import pandas as pd

from database import engine, getSession
from models import Store, Camera, NumCrowd, ErrLog, Status, Setting, User

results = db.getSession().query(User).all() #.filter(m.Store.id>2)
df = pd.DataFrame([r._asdict() for r in results])
df.to_csv('temp/User.csv')
print(df)
