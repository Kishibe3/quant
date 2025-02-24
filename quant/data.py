import sqlite3
import re
import pandas as pd

def get(id):
    db = sqlite3.connect('data.db')
    cur = db.cursor()
    if re.compile(r'^[\u4e00-\u9fff/]+$').match(id) and cur.execute(f'select name from sqlite_master where type = "table" and name = "{id}"').fetchone():
        return pd.read_sql(f'select * from {id}', db)
    cur.close()
    db.close()

