import sqlite3
import pandas as pd

def get(id):
    db = sqlite3.connect('data.db')
    if id == '股票基本':
        return pd.read_sql('select * from 股票基本', db)
    

