import re
import os
import json
import sqlite3
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

def cleasing_stock_basic():
    if not os.path.exists('raw data/stock basic.csv'):
        return
    
    db = sqlite3.connect('data.db')
    cur = db.cursor()
    cur.execute('create table if not exists 股票基本(代號, 名稱, 上市日, 市場別, 產業別)')
    db.commit()
    pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str}).to_sql('股票基本', db, if_exists = 'replace', index = False)
    db.close()


def alter_table_column(table_name, new_code):  # 如果有新掛牌股票，會為新掛牌股票在資料庫過去日期的資料補上None值
    db = sqlite3.connect('data.db')
    cur = db.cursor()
    old_code = [c[1] for c in cur.execute('pragma table_info(' + table_name + ')').fetchall()]
    cur.execute('create table new_' + table_name + '(時間, "' + '", "'.join(new_code) + '")')
    insec = list(set(old_code) & set(new_code))
    cur.execute('insert into new_' + table_name + '(時間, "' + '", "'.join(insec) + '") select 時間, "' + '", "'.join(insec) + '" from ' + table_name)
    cur.execute('drop table ' + table_name)
    cur.execute('alter table new_' + table_name + ' rename to ' + table_name)
    db.commit()
    db.close()

def cleasing_stock_trading_info():
    if not os.path.exists('raw data/stock trading info/twse') or not os.path.exists('raw data/stock trading info/tpex') or not os.path.exists('raw data/stock basic.csv'):
        return
    
    db = sqlite3.connect('data.db')
    cur = db.cursor()
    stock_code = pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()
    if cur.execute('select name from sqlite_master where type = "table" and name = "開盤價"').fetchone():
        old_code = [c[1] for c in cur.execute('pragma table_info(開盤價)').fetchall()]
        if len(set(stock_code) - set(old_code)) > 0 or len(set(old_code) - set(stock_code)) > 0:  # 有新掛牌股票或下市股票，需要調整資料庫欄位
            alter_table_column('開盤價', stock_code)
            alter_table_column('最高價', stock_code)
            alter_table_column('最低價', stock_code)
            alter_table_column('收盤價', stock_code)
            alter_table_column('成交量', stock_code)
            alter_table_column('成交值', stock_code)
            alter_table_column('成交筆數', stock_code)
    else:
        cur.execute('create table 開盤價(時間, "' + '", "'.join(stock_code) + '")')
        cur.execute('create table 最高價(時間, "' + '", "'.join(stock_code) + '")')
        cur.execute('create table 最低價(時間, "' + '", "'.join(stock_code) + '")')
        cur.execute('create table 收盤價(時間, "' + '", "'.join(stock_code) + '")')
        cur.execute('create table 成交量(時間, "' + '", "'.join(stock_code) + '")')
        cur.execute('create table 成交值(時間, "' + '", "'.join(stock_code) + '")')
        cur.execute('create table 成交筆數(時間, "' + '", "'.join(stock_code) + '")')
        db.commit()

    def read_file(file):
        f1 = open(f'raw data/stock trading info/twse/{file}', 'r')
        f2 = open(f'raw data/stock trading info/tpex/{file}', 'r')
        r = [[file.replace('.json', '').replace(' ', '/'), sk, '--', '--', '--', '--', '0', '0', '0'] for sk in stock_code]
        try:
            dt = {it[0]: it[1:] for it in [r[:1] + r[5:9] + [r[2], r[4], r[3]] for r in json.load(f1)['tables'][8]['data'] if r[0] in stock_code] + \
            [r[:1] + r[4:7] + [r[2]] + r[7:10] for r in json.load(f2)['tables'][0]['data'] if r[0] in stock_code]}
        except Exception as e:
            print(e)
            print(file)
            exit()
        f1.close()
        f2.close()
        for i, sk in enumerate(stock_code):
            it = dt.get(sk)
            if it is not None:
                r[i][2:] = it
        return r
    
    files = sorted(list(set(fn for fn in os.listdir('raw data/stock trading info/twse') if os.path.isfile(f'raw data/stock trading info/twse/{fn}') and os.path.isfile(f'raw data/stock trading info/tpex/{fn}')) - set(it[0].replace('/', ' ') + '.json' for it in cur.execute('select 時間 from 開盤價').fetchall())))
    if len(files) > 0:
        dfs = [pd.DataFrame(sum(map(read_file, files[i : i + 365]), []), columns = ['時間', '代號', '開盤價', '最高價', '最低價', '收盤價', '成交量', '成交值', '成交筆數']) for i in range(0, len(files), 365)]
        for df in dfs:
            cur.executemany('insert into 開盤價 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '開盤價', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            cur.executemany('insert into 最高價 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '最高價', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            cur.executemany('insert into 最低價 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '最低價', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            cur.executemany('insert into 收盤價 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '收盤價', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            cur.executemany('insert into 成交量 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '成交量', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            cur.executemany('insert into 成交值 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '成交值', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            cur.executemany('insert into 成交筆數 values(?' + (', ?' * len(stock_code)) + ')', pd.pivot_table(df, values = '成交筆數', index = '時間', columns = '代號', aggfunc = lambda x: 'NaN' if '-' in x.iloc[0] else float(x.iloc[0].replace(',', ''))).reset_index().replace('NaN', None).values.tolist())
            db.commit()
    db.close()


def cleasing_financial_statements():
    if not os.path.exists('raw data/financial statements') or not os.path.exists('raw data/stock basic.csv'):
        return
    
    def rp(a):
        return a.replace('合計', '').replace('淨額', '').replace('總計', '').replace('總額', '').replace('淨現金流入（流出）', '現金流量').replace('\u3000', '')
    def nb(a):
        return (float(re.sub('[,\\(\\)]', '', a)) if '.' in a else int(re.sub('[,\\(\\)]', '', a))) * (1 - 2 * int('(' in a))
    def pru(its):  # 簡化報表的項目
        sk, rt, tp = [], [], []
        for n, v in its:
            while len(sk) > n.count('\u3000') and rt[sk[-1]][1] != 'NaN':
                if sk[-1] == len(rt) - 2 and rp(rt[sk[-1]][0]) == rp(rt[-1][0]):
                    rt.pop()
                sk.pop()
                tp = []
            if v == '':
                rt += tp + [[n, 'NaN']]
                tp = []
                sk.append(len(rt) - 1)
            elif len(sk) > 0 and rp(n) == rp(rt[sk[-1]][0]):
                rt[sk[-1]][1] = nb(v)
                rt += tp
                tp = [[n, nb(v)]]
            else:
                tp.append([n, nb(v)])
        if len(sk) > 0 and sk[-1] == len(rt) - 2 and rp(rt[sk[-1]][0]) == rp(rt[-1][0]):
            rt.pop()
        if len(sk) == 0:
            rt += tp
        return rt
    # 糾正各報表中對不起來的項目與錯誤字元：
    # 資產負債表
    # 當期所得稅負債 -> 本期所得稅負債   BUG? 以前的資料可能有 當期所得稅負債－非流動 的可能嗎?
    # \u3000預收股款（權益項下）之約當發行股數 -> 預收股款（權益項下）之約當發行股數
    # \u3000母公司暨子公司所持有之母公司庫藏股股數（單位：股） -> 母公司暨子公司所持有之母公司庫藏股股數（單位：股）
    #
    # 損益表
    # 營業毛利（毛損）、營業毛利（毛損）淨額 此2項之間的所有項目都增加\u3000的前綴，成為 營業毛利（毛損） 的子節點
    # 此2項的科目成為其子科目的前綴
    #   淨利（損）歸屬於：
    #   綜合損益總額歸屬於：
    #
    # 現金流量表
    # 營業活動之現金流量－間接法 -> 營業活動之現金流量
    # 不影響現金流量之收益費損項目 -> 收益費損項目
    # 呆帳費用提列（轉列收入）數 -> 預期信用減損損失（利益）數／呆帳費用提列（轉列收入）數
    # 與營業活動相關之資產／負債變動數 -> 與營業活動相關之資產及負債之淨變動
    # 營業活動之淨現金流入（流出） -> \u3000營業活動之淨現金流入（流出）

    # BUG? 2801/2024 3.html的綜合損益表中的許多項目都是NaN 金控可能有不同會計處理方式？
    # BUG? 綜合損益表中的 員工訓練費用 研究發展費用 該擺在 營業費用 下還是 營業費用/其他業務及管理費用 下？
    def org(fn):
        if not os.path.exists(fn):
            return [[], [], []]
        with open(fn, 'r', encoding = 'cp950', errors = 'replace') as f:
            cnt = f.read()
        if '查無資料' in cnt or '檔案不存在' in cnt:
            return [[], [], []]
        cnt = cnt.replace('當期所得稅負債', '本期所得稅負債')\
            .replace('\u3000預收股款（權益項下）之約當發行股數', '預收股款（權益項下）之約當發行股數')\
            .replace('\u3000母公司暨子公司所持有之母公司庫藏股股數（單位：股）', '母公司暨子公司所持有之母公司庫藏股股數（單位：股）')\
            .replace('營業活動之現金流量－間接法', '營業活動之現金流量')\
            .replace('不影響現金流量之收益費損項目', '收益費損項目')\
            .replace('與營業活動相關之資產／負債變動數', '與營業活動相關之資產及負債之淨變動')\
            .replace('營業活動之淨現金流入（流出）', '\u3000營業活動之淨現金流入（流出）')
        cnt = re.sub('(?<!／)呆帳費用提列（轉列收入）數', '預期信用減損損失（利益）數／呆帳費用提列（轉列收入）數', cnt)
        cnt = re.sub(r'\((?=[\u4e00-\u9fff])', '（', cnt)
        cnt = re.sub(r'(?<=[\u4e00-\u9fff])\)', '）', cnt)
        cnt = re.sub(r'(?<=[\u4e00-\u9fff])-(?=[\u4e00-\u9fff])', '－', cnt)

        year = fn.split('/')[-1].split()[0]
        if int(year) >= 2019:
            tbs = BeautifulSoup(cnt, 'html.parser').select('table')[:3]
            for tb in tbs:
                for en in tb.select('.en'):
                    en.decompose()
            lt = [[[td.get_text().replace(' ', '') for td in tr.select('td')[1:3]] for tr in tb.select('tr')[2:]] for tb in tbs]
        else:
            tbs = BeautifulSoup(cnt, 'html.parser').select('table')[1:4]
            for tb in tbs:
                for tbh in tb.select('.tblHead'):
                    tbh.decompose()
            lt = [[[td.get_text()[1:].replace(' ', '') for td in tr.select('td')[:2]] for tr in tb.select('tr')] for tb in tbs]
        s = list(map(lambda x: x.replace('\u3000', ''), list(zip(*lt[0]))[0]))
        if '負債及權益' not in s:
            lt[0] = lt[0][:s.index('負債')] + [['負債及權益', '']] + \
                [['\u3000' + it[0], it[1]] for it in lt[0][s.index('負債'):s.index('權益總額') + 1]] + \
                [['\u3000負債及權益總計', lt[0][s.index('資產總額')][1]]] + lt[0][s.index('權益總額') + 1:]
        s = list(map(lambda x: x.replace('\u3000', ''), list(zip(*lt[1]))[0]))
        if '淨利（損）歸屬於：' in s:
            lt[1] = lt[1][:s.index('淨利（損）歸屬於：')] + \
                [['淨利（損）歸屬於：' + it[0].replace('\u3000', ''), it[1]] for it in lt[1][s.index('淨利（損）歸屬於：') + 1:s.index('綜合損益總額歸屬於：')]] + \
                [['綜合損益總額歸屬於：' + it[0].replace('\u3000', ''), it[1]] for it in lt[1][s.index('綜合損益總額歸屬於：') + 1:s.index('基本每股盈餘')]] + \
                lt[1][s.index('基本每股盈餘'):]
        if '營業毛利（毛損）' in s:  # 1409/2024 3.html沒有此項
            for i in range(s.index('營業毛利（毛損）'), s.index('營業毛利（毛損）淨額') + 1):
                lt[1][i][0] = '\u3000' + lt[1][i][0]
            lt[1] = lt[1][:s.index('營業毛利（毛損）')] + [['營業毛利（毛損）', '']] + lt[1][s.index('營業毛利（毛損）'):]
        rt = list(map(pru, lt))
        for its in rt:
            for it in its:
                if any(c in it[0] for c in ['-', '(', ')', '/']):
                    raise Exception(f'不允許的字元出現：{it[0]} 在 {fn}')
        return rt
    def mrg(its, te = None):  # 整合多個財報成同一棵樹
        if te is None:
            te = {'c': 0}
        sk = []
        for i in range(len(its)):
            while len(sk) > its[i][0].count('\u3000'):
                sk.pop()
            if len(sk) < its[i][0].count('\u3000'):
                sk.append(its[i - 1][0].lstrip('\u3000'))
            p = te
            for k in sk:
                p = p[k]
            n = its[i][0].lstrip('\u3000')
            if n in p:
                p[n]['v'] += ['NaN'] * (te['c'] - len(p[n]['v'])) + [its[i][1]]
            else:
                p[n] = {'v': ['NaN'] * te['c'] + [its[i][1]]}
        te['c'] += 1
        return te

    stock_code = pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()
    db = sqlite3.connect('data.db')
    cur = db.cursor()
    if cur.execute('select name from sqlite_master where type = "table" and name = "資產負債表/資產"').fetchone():
        if cur.execute('select 時間 from "資產負債表/資產"').fetchone():
            sy, ss = map(int, cur.execute('select 時間 from "資產負債表/資產"').fetchall()[-1][0].split('/Q'))
            ss += 1
            if ss == 5:
                sy, ss = sy + 1, 1
        else:
            sy, ss, _ = map(int, '2013/01/01'.split('/'))
            ss = (ss + 2) // 3
        old_code = [c[1] for c in cur.execute('pragma table_info("資產負債表/資產")').fetchall()]
        if len(set(stock_code) - set(old_code)) > 0 or len(set(old_code) - set(stock_code)) > 0:  # 有新掛牌股票或下市股票，需要調整資料庫欄位
            for tb in cur.execute('select name from sqlite_master where type = "table"'):
                alter_table_column(tb[0], stock_code)
    else:
        sy, ss, _ = map(int, '2013/01/01'.split('/'))
        ss = (ss + 2) // 3
    ey, es = datetime.now().year, datetime.today().strftime('%m/%d')
    if es < '03/31':
        ey, es = ey - 1, 3
    elif es < '05/15':
        ey, es = ey - 1, 4
    elif es < '08/14':
        es = 1
    elif es < '11/14':
        es = 2
    else:
        es = 3
    seasons = [f'{y}/Q{s}' for y in range(sy, ey + 1) for s in range([1, ss][y == sy], [5, es + 1][y == ey])]
    for c in stock_code:
        te = [None] * 3
        for y in range(sy, ey + 1):
            for s in range([1, ss][y == sy], [5, es + 1][y == ey]):
                its = org(f'raw data/financial statements/{c}/{y} {s}.html')
                te = [mrg(its[i], te[i]) for i in range(3)]
        for i in range(3):
            sk, p = [[]], None
            while len(sk) > 0:
                ph, p = sk.pop(), te[i]
                for k in ph:
                    p = p[k]
                if 'v' in p:
                    if len(p['v']) < te[i]['c']:
                        p['v'] += ['NaN'] * (te[i]['c'] - len(p['v']))
                    tb = '/'.join([['資產負債表', '綜合損益表', '現金流量表'][i]] + ph)
                    if cur.execute('select name from sqlite_master where type="table" and name="' + tb + '"').fetchone() is None:
                        cur.execute('create table "' + tb + '"(時間, "' + '", "'.join(stock_code) + '")')
                        cur.executemany('insert into "' + tb + '"(時間) values(?)', [[f'{y}/Q{s}'] for y in range(2013, ey + 1) for s in range(1, [5, es + 1][y == ey])])
                    cur.executemany('update "' + tb + '" set "' + c + '" = ? where 時間 = ?', list(zip(p['v'], seasons)))
                for k in p:
                    if k != 'v' and k != 'c':
                        sk.append(ph + [k])
        db.commit()
    db.close()


"""
import sqlite3, os, re, time, pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
def rp(a):
    return a.replace('合計', '').replace('淨額', '').replace('總計', '').replace('總額', '').replace('淨現金流入（流出）', '現金流量').replace('\u3000', '')

def nb(a):
    return (float(re.sub('[,\\(\\)]', '', a)) if '.' in a else int(re.sub('[,\\(\\)]', '', a))) * (1 - 2 * int('(' in a))

def pru(its):  # 簡化報表的項目
    sk, rt, tp = [], [], []
    for n, v in its:
        while len(sk) > n.count('\u3000') and rt[sk[-1]][1] != 'NaN':
            if sk[-1] == len(rt) - 2 and rp(rt[sk[-1]][0]) == rp(rt[-1][0]):
                rt.pop()
            sk.pop()
            tp = []
        if v == '':
            rt += tp + [[n, 'NaN']]
            tp = []
            sk.append(len(rt) - 1)
        elif len(sk) > 0 and rp(n) == rp(rt[sk[-1]][0]):
            rt[sk[-1]][1] = nb(v)
            rt += tp
            tp = [[n, nb(v)]]
        else:
            tp.append([n, nb(v)])
    if len(sk) > 0 and sk[-1] == len(rt) - 2 and rp(rt[sk[-1]][0]) == rp(rt[-1][0]):
        rt.pop()
    if len(sk) == 0:
        rt += tp
    return rt

def org(fn):
    if not os.path.exists(fn):
        return [[], [], []]
    with open(fn, 'r', encoding = 'cp950', errors = 'replace') as f:
        cnt = f.read()
    if '查無資料' in cnt or '檔案不存在' in cnt:
        return [[], [], []]
    cnt = cnt.replace('當期所得稅負債', '本期所得稅負債')\
        .replace('\u3000預收股款（權益項下）之約當發行股數', '預收股款（權益項下）之約當發行股數')\
        .replace('\u3000母公司暨子公司所持有之母公司庫藏股股數（單位：股）', '母公司暨子公司所持有之母公司庫藏股股數（單位：股）')\
        .replace('營業活動之現金流量－間接法', '營業活動之現金流量')\
        .replace('不影響現金流量之收益費損項目', '收益費損項目')\
        .replace('與營業活動相關之資產／負債變動數', '與營業活動相關之資產及負債之淨變動')\
        .replace('營業活動之淨現金流入（流出）', '\u3000營業活動之淨現金流入（流出）')
    cnt = re.sub('(?<!／)呆帳費用提列（轉列收入）數', '預期信用減損損失（利益）數／呆帳費用提列（轉列收入）數', cnt)
    cnt = re.sub(r'\((?=[\u4e00-\u9fff])', '（', cnt)
    cnt = re.sub(r'(?<=[\u4e00-\u9fff])\)', '）', cnt)
    cnt = re.sub(r'(?<=[\u4e00-\u9fff])-(?=[\u4e00-\u9fff])', '－', cnt)
    year = fn.split('/')[-1].split()[0]
    if int(year) >= 2019:
        tbs = BeautifulSoup(cnt, 'html.parser').select('table')[:3]
        for tb in tbs:
            for en in tb.select('.en'):
                en.decompose()
        lt = [[[td.get_text().replace(' ', '') for td in tr.select('td')[1:3]] for tr in tb.select('tr')[2:]] for tb in tbs]
    else:
        tbs = BeautifulSoup(cnt, 'html.parser').select('table')[1:4]
        for tb in tbs:
            for tbh in tb.select('.tblHead'):
                tbh.decompose()
        lt = [[[td.get_text()[1:].replace(' ', '') for td in tr.select('td')[:2]] for tr in tb.select('tr')] for tb in tbs]
    s = list(map(lambda x: x.replace('\u3000', ''), list(zip(*lt[0]))[0]))
    if '負債及權益' not in s:
        lt[0] = lt[0][:s.index('負債')] + [['負債及權益', '']] + \
            [['\u3000' + it[0], it[1]] for it in lt[0][s.index('負債'):s.index('權益總額') + 1]] + \
            [['\u3000負債及權益總計', lt[0][s.index('資產總額')][1]]] + lt[0][s.index('權益總額') + 1:]
    s = list(map(lambda x: x.replace('\u3000', ''), list(zip(*lt[1]))[0]))
    if '淨利（損）歸屬於：' in s:
        lt[1] = lt[1][:s.index('淨利（損）歸屬於：')] + \
            [['淨利（損）歸屬於：' + it[0].replace('\u3000', ''), it[1]] for it in lt[1][s.index('淨利（損）歸屬於：') + 1:s.index('綜合損益總額歸屬於：')]] + \
            [['綜合損益總額歸屬於：' + it[0].replace('\u3000', ''), it[1]] for it in lt[1][s.index('綜合損益總額歸屬於：') + 1:s.index('基本每股盈餘')]] + \
            lt[1][s.index('基本每股盈餘'):]
    if '營業毛利（毛損）' in s:
        for i in range(s.index('營業毛利（毛損）'), s.index('營業毛利（毛損）淨額') + 1):
            lt[1][i][0] = '\u3000' + lt[1][i][0]
        lt[1] = lt[1][:s.index('營業毛利（毛損）')] + [['營業毛利（毛損）', '']] + lt[1][s.index('營業毛利（毛損）'):]
    rt = list(map(pru, lt))
    for its in rt:
        for it in its:
            if any(c in it[0] for c in ['-', '(', ')', '/']):
                raise Exception(f'不允許的字元出現：{it[0]} 在 {fn}')
    return rt

def mrg(its, te = None):  # 整合多個財報成同一棵樹
    if te is None:
        te = {'c': 0}
    sk = []
    for i in range(len(its)):
        while len(sk) > its[i][0].count('\u3000'):
            sk.pop()
        if len(sk) < its[i][0].count('\u3000'):
            sk.append(its[i - 1][0].lstrip('\u3000'))
        p = te
        for k in sk:
            p = p[k]
        n = its[i][0].lstrip('\u3000')
        if n in p:
            p[n]['v'] += ['NaN'] * (te['c'] - len(p[n]['v'])) + [its[i][1]]
        else:
            p[n] = {'v': ['NaN'] * te['c'] + [its[i][1]]}
    te['c'] += 1
    return te

def ptt(te, s = 0):
    for k in te:
        if k == 'v' or k == 'c':
            continue
        print(('  ' * s) + k + ': ' + str(te[k]['v']))
        if len(te[k]) > 1:
            ptt(te[k], s + 1)

sy, ss, _ = map(int, '2013/01/01'.split('/'))
ss = (ss + 2) // 3
ey, es = datetime.now().year, datetime.today().strftime('%m/%d')
if es < '03/31':
    ey, es = ey - 1, 3
elif es < '05/15':
    ey, es = ey - 1, 4
elif es < '08/14':
    es = 1
elif es < '11/14':
    es = 2
else:
    es = 3

db = sqlite3.connect('test.db')
cur = db.cursor()
stock_code = pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()
c = '2330'
te = [None] * 3
for y in range(sy, ey + 1):
    for s in range([1, ss][y == sy], [5, es + 1][y == ey]):
        its = org(f'raw data/financial statements/{c}/{y} {s}.html')
        te = [mrg(its[i], te[i]) for i in range(3)]

seasons = [f'{y}/Q{s}' for y in range(sy, ey + 1) for s in range([1, ss][y == sy], [5, es + 1][y == ey])]
for i in range(3):
    sk, p = [[]], None
    while len(sk) > 0:
        ph, p = sk.pop(), te[i]
        for k in ph:
            p = p[k]
        if 'v' in p:
            if len(p['v']) < te[i]['c']:
                p['v'] += ['NaN'] * (te[i]['c'] - len(p['v']))
            tb = '/'.join([['資產負債表', '綜合損益表', '現金流量表'][i]] + ph)
            if cur.execute('select name from sqlite_master where type="table" and name="' + tb + '"').fetchone() is None:
                cur.execute('create table "' + tb + '"(時間, "' + '", "'.join(stock_code) + '")')
                cur.executemany('insert into "' + tb + '"(時間) values(?)', [[s] for s in seasons])
            cur.executemany('update "' + tb + '" set "' + c + '" = ? where 時間 = ?', list(zip(p['v'], seasons)))
        for k in p:
            if k != 'v' and k != 'c':
                sk.append(ph + [k])
    ptt(te[i])
    print('---')

db.commit()
pd.read_sql('select * from "資產負債表/資產"', db)
db.close()

---

its = [['資產', ''], ['\u3000遞延所得稅資產', '5,393'], ['\u3000其他非流動資產', '2,646'], ['\u3000\u3000預付設備款', '0'], ['\u3000\u3000存出保證金', '2,646'], ['\u3000資產總計', '8,039']]
its = [['應收帳款－關係人淨額', ''], ['\u3000應收帳款－關係人', '2,601'], ['\u3000應收帳款－關係人淨額', '2,601']]
its = [['應收票據淨額', ''], ['\u3000應收票據', '102,773'], ['\u3000備抵呆帳－應收票據', '3,074'], ['\u3000應收票據淨額', '99,699']]
its = [['資產', '8,039'], ['\u3000遞延所得稅資產', '5,393'], ['\u3000其他非流動資產', '2,646']]
its = [['資產', ''], ['\u3000遞延所得稅資產', '5,393'], ['\u3000其他非流動資產', ''], ['\u3000\u3000預付設備款', '0'], ['\u3000\u3000存出保證金', '2,646'], ['\u3000\u3000其他非流動資產總計', '2,646'], ['\u3000資產總計', '8,039']]

def stpt(l, s = 1):
    if not True in map(lambda x: isinstance(x, list), l):
        print(l, end = '')
        return
    print('[', end = '')
    for i, il in enumerate(l):
        stpt(il, s + 1)
        if i != len(l) - 1:
            print(',\n' + (' ' * s), end = '')
    print([']', ']\n'][s == 1], end = '')

def ptt(te, s = 0):
    for k in te:
        if k == 'v' or k == 'c':
            continue
        print(('  ' * s) + k + ': ' + str(te[k]['v']))
        if len(te[k]) > 1:
            ptt(te[k], s + 1)

pd.set_option('display.max_columns', None)

---

def pru(its):
    i = 0
    def sk(f = None):
        nonlocal i
        r, p, n = [], i, None
        c = 0 if f is None else its[f][0].count('\u3000') + 1
        while i < len(its) and (n is None or its[i][0].count('\u3000') >= c):
            if its[i][1] == '':
                r += [[it[0], nb(it[1])] for it in its[p:i]]
                i += 1
                sb, p = sk(i - 1)
                r += sb
            elif f is not None and rp(its[i][0]) == rp(its[f][0]):
                r += [[it[0], nb(it[1])] for it in its[p:i]]
                p = i
                n = nb(its[i][1])
                i += 1
            else:
                i += 1
        if f is None:
            return r + [[it[0], nb(it[1])] for it in its[p:i]]
        if len(r) == 1 and rp(r[0][0]) == rp(its[f][0]):
            r = []
        return [[its[f][0], n]] + r, p + 1
    return sk()

"""