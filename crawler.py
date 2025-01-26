import pandas as pd
import os
from cleasing import cleasing_stock_basic

# 負責下載零散少量的資料
# 若往後有更多零散資料要下載，可整合為單個scrapy spider

def get_stock_basic():
    if not os.path.exists('raw data'):
        os.makedirs('raw data')
    if not os.path.exists('raw data/stock basic.csv'):
        def reg(df):
            return pd.concat([df['有價證券代號及名稱'].str.replace('\u3000', ' ').str.split(' ', expand = True).rename({0: '代號', 1: '名稱'}, axis = 'columns'), df.drop(columns = ['有價證券代號及名稱'])], axis = 'columns').reset_index(drop = True)
        security = pd.read_html('https://isin.twse.com.tw/isin/C_public.jsp?strMode=2', header = 0, encoding = 'cp950')[0].drop(columns = ['國際證券辨識號碼(ISIN Code)', 'CFICode', '備註'])
        stock_tse = reg(security[1 : security[security.iloc[:, 0] == '上市認購(售)權證'].index[0]])
        security = pd.read_html('https://isin.twse.com.tw/isin/C_public.jsp?strMode=4', header = 0, encoding = 'cp950')[0].drop(columns = ['國際證券辨識號碼(ISIN Code)', 'CFICode', '備註'])
        stock_otc = reg(security[security[security.iloc[:, 0] == '股票'].index[0] + 1 : security[security.iloc[:, 0] == '特別股'].index[0]])
        stock = pd.concat([stock_tse, stock_otc]).sort_values(by = '代號').reset_index(drop=True)
        stock.to_csv('raw data/stock basic.csv', index = False)
    cleasing_stock_basic()

get_stock_basic()
