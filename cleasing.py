import re
import os
import json
import sqlite3
import pandas as pd
import warnings
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


def cleasing_stock_trading_info():
    if not os.path.exists('raw data/stock trading info/twse') or not os.path.exists('raw data/stock trading info/tpex') or not os.path.exists('raw data/stock basic.csv'):
        return
    
    db = sqlite3.connect('data.db')
    cur = db.cursor()
    stock_code = pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()
    if cur.execute('select name from sqlite_master where type = "table" and name = "開盤價"').fetchone():
        old_code = [c[1] for c in cur.execute('pragma table_info(開盤價)').fetchall()][1:]
        if len(set(stock_code) - set(old_code)) > 0 or len(set(old_code) - set(stock_code)) > 0:  # 有新掛牌股票或下市股票，需要調整資料庫欄位
            insec = list(set(old_code) & set(stock_code))
            for tb in ['開盤價', '最高價', '最低價', '收盤價', '成交量', '成交值', '成交筆數']:
                cur.execute('create table "new_' + tb + '"(時間, "' + '", "'.join(stock_code) + '")')
                cur.execute('insert into "new_' + tb + '"(時間, "' + '", "'.join(insec) + '") select 時間, "' + '", "'.join(insec) + '" from "' + tb + '"')
                cur.execute('drop table "' + tb + '"')
                cur.execute('alter table "new_' + tb + '" rename to "' + tb + '"')
                db.commit()
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
        dt = {it[0]: it[1:] for it in [r[:1] + r[5:9] + [r[2], r[4], r[3]] for r in json.load(f1)['tables'][8]['data'] if r[0] in stock_code] + \
            [r[:1] + r[4:7] + [r[2]] + r[7:10] for r in json.load(f2)['tables'][0]['data'] if r[0] in stock_code]}
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


def stpt(l, s = 1):
    if True not in map(lambda x: isinstance(x, list), l):
        print(l, end = '')
        return
    print('[', end = '')
    for i, il in enumerate(l):
        stpt(il, s + 1)
        if i != len(l) - 1:
            print(',\n' + (' ' * s), end = '')
    print([']', ']\n'][s == 1], end = '')

def cleasing_financial_statements():
    if not os.path.exists('raw data/financial statements') or not os.path.exists('raw data/stock basic.csv'):
        return
    
    def rp(a):
        return a.replace('合計', '').replace('淨額', '').replace('總計', '').replace('總額', '').replace('淨現金流入（流出）', '現金流量').replace('\u3000', '')
    def nb(a):
        if a == '':
            return 'NaN'
        return (float(re.sub('[,\\(\\)]', '', a)) if '.' in a else int(re.sub('[,\\(\\)]', '', a))) * (1 - 2 * int('(' in a))
    def pru(its):  # 簡化報表的項目
        i = 0
        def sk(f = None):  # 父節點在its中的index
            nonlocal i
            rt = []  # 本層與以下層應該回傳的結果
            n = None  # 父節點的數值
            p = i  # 本層中名稱與父節點相對應的its中的index
            c = 0 if f is None else its[f][0].count('\u3000') + 1  # 本層項目的前綴應有多少個\u3000
            while i < len(its) and its[i][0].count('\u3000') >= c:
                assert its[i][0].count('\u3000') <= i and (i == 0 or its[i][0].count('\u3000') <= its[i - 1][0].count('\u3000') + 1), 'wrong indentation'
                if its[i][0].count('\u3000') > c:
                    rt += [[it[0], nb(it[1])] for it in its[p:i]]
                    sb, sn = sk(i - 1)
                    if sn is not None:  # 指定從下層得知的父節點的數值
                        rt[-1][1] = sn
                    rt += sb
                    p = i
                elif f is not None and rp(its[i][0]) == rp(its[f][0]):
                    rt += [[it[0], nb(it[1])] for it in its[p:i]]
                    p = i
                    n = nb(its[i][1])
                    i += 1
                else:
                    i += 1
            if not (p == i - 1 and f is not None and rp(its[p][0]) == rp(its[f][0])):
                rt += [[it[0], nb(it[1])] for it in its[p:i]]
            if len(rt) == 1 and f is not None and rp(rt[0][0]) == rp(its[f][0]):
                rt = []
            return rt, n
        rt = sk()[0]
        for it in rt:
            if it[1] == 'NaN':
                raise Exception('Empty value')
        return rt
    def ctt(t):  # 簡化財報上的時間範圍
        if '上半年度' in t:
            return [1, 2]
        elif '年度' in t:
            return [1, 4]
        rt = re.search(r'(?<=第)\d(?=季)', t)
        if rt is not None:
            n = int(rt.group())
            return [n, n]
        rt = list(map(lambda x: (int(x) + 2) // 3, re.findall(r'\d+(?=月)', t)))
        if len(rt) == 1:
            rt = rt * 2
        return rt

    # 糾正各報表中對不起來的項目與錯誤字元：
    # 資產負債表
    # 無活絡市場之債券投資－非流動 -> 無活絡市場之債務工具投資－非流動
    # 當期所得稅負債 -> 本期所得稅負債   BUG? 以前的資料可能有 當期所得稅負債－非流動 的可能嗎?
    # \u3000待註銷股本股數 -> 待註銷股本股數
    # \u3000預收股款（權益項下）之約當發行股數 -> 預收股款（權益項下）之約當發行股數
    # \u3000母公司暨子公司所持有之母公司庫藏股股數（單位：股） -> 母公司暨子公司所持有之母公司庫藏股股數（單位：股）
    #
    # 損益表
    # 採用權益法之關聯企業及合資損益之份額 -> 採用權益法認列之關聯企業及合資損益之份額
    # 營業毛利（毛損）、營業毛利（毛損）淨額 此2項之間的所有項目都增加\u3000的前綴，成為 營業毛利（毛損） 的子節點
    # 此2項的科目成為其子科目的前綴
    #   淨利（損）歸屬於：
    #   綜合損益總額歸屬於：
    #
    # 現金流量表
    # 營業活動之淨現金流入（流出） -> 營業活動之淨現金流入（流出）－間接法 or 營業活動之淨現金流入（流出）－直接法
    # 不影響現金流量之收益費損項目 -> 收益費損項目
    # 呆帳費用提列（轉列收入）數 -> 預期信用減損損失（利益）數／呆帳費用提列（轉列收入）數
    # 與營業活動相關之資產／負債變動數 -> 與營業活動相關之資產及負債之淨變動
    # 營業活動之淨現金流入（流出） -> \u3000營業活動之淨現金流入（流出）－間接法 or \u3000營業活動之淨現金流入（流出）－直接法

    # BUG? 2801/2024 3.html的綜合損益表中的許多項目都是NaN 金控可能有不同會計處理方式？
    # BUG? 綜合損益表中的 員工訓練費用 研究發展費用 該擺在 營業費用 下還是 營業費用/其他業務及管理費用 下？
    def org(fn):
        if not os.path.exists(fn):
            return [[], [], []], None
        with open(fn, 'r', encoding = 'cp950', errors = 'replace') as f:
            cnt = f.read()
        if '查無資料' in cnt or '檔案不存在' in cnt:
            return [[], [], []], None
        elif '會計師查核' not in cnt:
            warnings.warn(f'File {fn} is not complete. Please download it again.')
            return [[], [], []], None
        cnt = cnt.replace('無活絡市場之債券投資－非流動', '無活絡市場之債務工具投資－非流動')\
            .replace('當期所得稅負債', '本期所得稅負債')\
            .replace('\u3000待註銷股本股數', '待註銷股本股數')\
            .replace('\u3000預收股款（權益項下）之約當發行股數', '預收股款（權益項下）之約當發行股數')\
            .replace('\u3000母公司暨子公司所持有之母公司庫藏股股數（單位：股）', '母公司暨子公司所持有之母公司庫藏股股數（單位：股）')\
            .replace('採用權益法之關聯企業及合資損益之份額', '採用權益法認列之關聯企業及合資損益之份額')\
            .replace('不影響現金流量之收益費損項目', '收益費損項目')\
            .replace('與營業活動相關之資產／負債變動數', '與營業活動相關之資產及負債之淨變動')
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
            timespan = [ctt(tb.select('tr')[1].select('th')[2].get_text()) for tb in tbs]
            lt = [[[td.get_text().replace(' ', '') for td in tr.select('td')[1:3]] for tr in tb.select('tr')[2:]] for tb in tbs]
        else:
            tbs = BeautifulSoup(cnt, 'html.parser').select('table')[1:4]
            timespan = [ctt(tb.select('tr')[0].select('th')[1].get_text()) for tb in tbs]
            lt = [[[td.get_text()[1:].replace(' ', '') for td in tr.select('td')[:2]] for tr in tb.select('tr') if tr.select('td')[:2] != []] for tb in tbs]
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
        s = list(map(lambda x: x.replace('\u3000', ''), list(zip(*lt[2]))[0]))
        if '營業活動之現金流量－直接法' in s:
            lt[2][s.index('營業活動之淨現金流入（流出）')][0] = '\u3000營業活動之淨現金流入（流出）－直接法'
            lt[2] = lt[2][:s.index('營業活動之現金流量－直接法')] + [['\u3000營業活動之淨現金流入（流出）－間接法', lt[2][s.index('營業活動之淨現金流入（流出）')][1]]] + \
                lt[2][s.index('營業活動之現金流量－直接法'):]
        else:
            lt[2][s.index('營業活動之淨現金流入（流出）')][0] = '\u3000營業活動之淨現金流入（流出）－間接法'
        try:
            rt = list(map(pru, lt))
        except Exception:
            print(fn)
            exit()
        return rt, timespan

    db = sqlite3.connect('data.db')
    cur = db.cursor()
    stock_code = pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()

    # 有新掛牌股票或下市股票時，需要調整資料庫欄位
    if cur.execute('select name from sqlite_master where type = "table" and name = "資產負債表/資產"').fetchone():
        old_code = [c[1] for c in cur.execute('pragma table_info("資產負債表/資產")')][1:]
        if len(set(stock_code) - set(old_code)) > 0 or len(set(old_code) - set(stock_code)) > 0:
            print('altering table column')
            insec = list(set(old_code) & set(stock_code))
            for tb in cur.execute('select name from sqlite_master where type = "table"').fetchall():
                cur.execute('create table "new_' + tb[0] + '"(時間, "' + '", "'.join(stock_code) + '")')
                cur.execute('insert into "new_' + tb[0] + '"(時間, "' + '", "'.join(insec) + '") select 時間, "' + '", "'.join(insec) + '" from "' + tb[0] + '"')
                cur.execute('drop table "' + tb[0] + '"')
                cur.execute('alter table "new_' + tb[0] + '" rename to "' + tb[0] + '"')
                db.commit()
    
    # 結束年份與季節
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
    
    # 清理資料時間會很長，程式執行時可能意外中止，為了能從中斷點繼續執行程式需要知道更早的開始年份與季節(sy, ss)還有中斷的股票代號(skip2code)
    skip2code = ''
    if cur.execute('select name from sqlite_master where type = "table" and name = "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金"').fetchone():
        if cur.execute('select 時間 from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金"').fetchone():  # 資產負債表帳列之現金及約當現金 是財報中的最後一個項目
            # 找尋要重新處理的資料的時間起始點
            ss = ''
            for c in stock_code[::-1]:
                nv = [s[0] for s in cur.execute(f"""
                with recursive cte as (
                    select rowid, 時間, "{c}"
                    from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金"
                    where rowid = (
                        select max(rowid)
                        from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金"
                    ) and "{c}" is null

                    union all

                    select t.rowid, t.時間, t."{c}"
                    from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金" t
                    join cte on t.rowid = cte.rowid - 1
                    where t."{c}" is null
                )
                select 時間 from cte order by rowid asc
                """)]
                if len(nv) == 0:
                    break
                if ss != '' and nv[0] >= ss:  # 沒有比ss更早的缺失值了，無法再讓ss更早了
                    break
                for s in nv:
                    if ss != '' and s >= ss:
                        break
                    with open(f'raw data/financial statements/{c}/{s.replace('/Q', ' ')}.html', 'r', encoding = 'cp950', errors = 'replace') as f:
                        cnt = f.read()
                    if "資產負債表帳列之現金及約當現金" in cnt:  # 實際上財報上有資料但資料庫中卻是null，代表該處確實缺失資料
                        if ss == '' or s < ss:
                            ss = s
                        break
            if ss == '':  # 無缺失資料
                ss = cur.execute('select 時間 from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金" order by rowid desc').fetchone()[0]
                sy, ss = map(int, ss.split('/Q'))
                ss += 1
                if ss == 5:
                    sy += 1
                    ss = 1
            else:
                sy, ss = map(int, ss.split('/Q'))
            
            if not cur.execute('select 時間 from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金" order by rowid desc').fetchone()[0] < f'{ey}/Q{es}':  # 資料庫中的最新時間等同當季
                # 找尋要跳到的股票代號
                for c in stock_code:
                    if skip2code != '':
                        break
                    nv = [s[0] for s in cur.execute(f"""
                    with recursive cte as (
                        select rowid, 時間, "{c}"
                        from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金"
                        where rowid = (
                            select max(rowid)
                            from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金"
                        ) and "{c}" is null
                        
                        union all
                        
                        select t.rowid, t.時間, t."{c}"
                        from "現金流量表/期末現金及約當現金餘額/資產負債表帳列之現金及約當現金" t
                        join cte on t.rowid = cte.rowid - 1
                        where t."{c}" is null
                    )
                    select 時間 from cte order by rowid desc
                    """)]
                    if len(nv) == 0:
                        continue
                    for s in nv:
                        if s < f'{sy}/Q{ss}':  # 更早之前的資料都是完整的，不用找了
                            break
                        with open(f'raw data/financial statements/{c}/{s.replace('/Q', ' ')}.html', 'r', encoding = 'cp950', errors = 'replace') as f:
                            cnt = f.read()
                        if "資產負債表帳列之現金及約當現金" in cnt:  # 實際上財報上有資料但資料庫中卻是null，代表該處確實缺失資料
                            skip2code = c
                            break
        else:
            sy, ss = map(int, '2013/Q1'.split('/Q'))
    else:
        sy, ss = map(int, '2013/Q1'.split('/Q'))

    # 處理資料的原則：
    # 表格的數字都是代表單一季的資料。當沒有財報時，表格的該季資料就是None。如果無法取得單季資料但有多季加總的資料，會盡量減少資料涵蓋的季節數並將資料與時間範圍儲存為json字串到表格中
    # json字串的格式： [數字, '開始季~結束季']
    print('inserting data into database')
    for c in stock_code:
        if skip2code != '' and c < skip2code:
            continue
        for y in range(sy, ey + 1):
            dts, tsns = [], []
            for s in range([1, ss][y == sy], [5, es + 1][y == ey]):
                its, tsn = org(f'raw data/financial statements/{c}/{y} {s}.html')
                sk, dt = [], {}
                if tsn is not None:
                    for i in range(3):
                        sols = [([], tsn[i][:])]
                        for ts in range(tsn[i][0] - 1, tsn[i][1] - 1):
                            if tsns[ts] is None:
                                continue
                            for sol in sols:
                                tas, rem = sol
                                if rem[0] == tsns[ts][i][0]:
                                    ntas = tas[:]
                                    ntas.append(ts)
                                    sols.append((ntas, [tsns[ts][i][1] + 1, rem[1]]))
                        sols = sols[-1]  # 該減去哪些季財報的資料，盡量讓涵蓋本季的資料範圍縮小到單季

                        for j in range(len(its[i])):
                            while len(sk) > its[i][j][0].count('\u3000'):
                                sk.pop()
                            if len(sk) < its[i][j][0].count('\u3000'):
                                sk.append(its[i][j - 1][0].lstrip('\u3000'))
                            ky = '/'.join([['資產負債表', '綜合損益表', '現金流量表'][i]] + sk + [its[i][j][0].lstrip('\u3000')])
                            dt[ky] = its[i][j][1]
                            n = its[i][j][1]
                            for sbit in sols[0]:
                                if ky in dts[sbit]:  # 如果前幾季的財報資料沒有該項目，就以0為計算，也就是直接跳過不減
                                    n -= dts[sbit][ky]
                            if sols[1][0] != sols[1][1]:
                                n = json.dumps([n, '~'.join(map(str, sols[1]))])  # 無法縮小到單季的範圍，需要特別標記涵蓋的時間範圍

                            if cur.execute('select name from sqlite_master where type = "table" and name = "' + ky + '"').fetchone() is None:
                                cur.execute('create table "' + ky + '"(時間, "' + '", "'.join(stock_code) + '")')
                                cur.executemany('insert into "' + ky + '"(時間) values(?)', [[f'{ty}/Q{ts}'] for ty in range(2013, ey + 1) for ts in range(1, [5, es + 1][ty == ey])])
                            else:
                                tsy, tss = map(int, cur.execute('select 時間 from "' + ky + '" order by rowid desc').fetchone()[0].split('/Q'))
                                if f'{tsy}/Q{tss}' < f'{ey}/Q{es}':
                                    tss += 1
                                    if tss == 5:
                                        tsy += 1
                                        tss = 1
                                    cur.executemany('insert into "' + ky + '"(時間) values(?)', [[f'{ty}/Q{ts}'] for ty in range(tsy, ey + 1) for ts in range([1, tss][ty == tsy], [5, es + 1][ty == ey])])
                            cur.execute('update "' + ky + '" set "' + c + '" = ? where 時間 = ?', [n, f'{y}/Q{s}'])
                dts.append(dt)
                tsns.append(tsn)
        db.commit()
        print(f'{c} done')
    db.close()


"""
TODO financial statement的bug可能有很多，盡量加一些自動判斷的條件與assertion！

# PROBLEM 目前的資料抓取範圍可能有選擇性偏誤，導致實際上策略會選到將來會下市的股票但我回測資料中卻沒這些股票，高估策略的績效
# -> 哪裡能找出所有過去的股票代號？
# REPORT finlab資料有誤：1341缺少了2018/Q3的資料導致現金流量表數字錯誤，1613的2016/Q3資料誤植到2017/Q3(資產負債表帳列之現金及約當現金)

---

import os, pandas as pd
from datetime import datetime
stock_code = pd.read_csv('raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()
for sk in stock_code:
    for y in range(sy, ey + 1):
        for s in range([1, ss][y == sy], [5, es + 1][y == ey]):
            if not os.path.exists(f'raw data/financial statements/{sk}/{y} {s}.html'):
                continue
            with open(f'raw data/financial statements/{sk}/{y} {s}.html', 'r', encoding = 'cp950', errors = 'replace') as f:
                cnt = f.read()
            if not ('會計師查核' in cnt or '查無資料' in cnt or '檔案不存在' in cnt):
                print(f'raw data/financial statements/{sk}/{y} {s}.html')
4554/2016 2
4804/2014 4 ~ 2018 4
6693/2018 2

---

td = [[['營業收入', ''], ['\u3000銷貨收入淨額', ''], ['\u3000\u3000銷貨收入', '2,969,625'], ['\u3000\u3000\u3000銷貨收入', '2,969,625'], ['\u3000\u3000銷貨收入淨額', '2,969,625'], ['\u3000營業收入合計', '2,969,625']],
 [['資產', ''], ['\u3000遞延所得稅資產', '5,393'], ['\u3000其他非流動資產', '2,646'], ['\u3000\u3000預付設備款', '0'], ['\u3000\u3000存出保證金', '2,646'], ['\u3000資產總計', '8,039']],
 [['應收帳款－關係人淨額', ''], ['\u3000應收帳款－關係人', '2,601'], ['\u3000應收帳款－關係人淨額', '2,601']],
 [['應收票據淨額', ''], ['\u3000應收票據', '102,773'], ['\u3000備抵呆帳－應收票據', '3,074'], ['\u3000應收票據淨額', '99,699']],
 [['資產', '8,039'], ['\u3000遞延所得稅資產', '5,393'], ['\u3000其他非流動資產', '2,646']],
 [['資產', ''], ['\u3000遞延所得稅資產', '5,393'], ['\u3000其他非流動資產', ''], ['\u3000\u3000預付設備款', '0'], ['\u3000\u3000存出保證金', '2,646'], ['\u3000\u3000其他非流動資產總計', '2,646'], ['\u3000資產總計', '8,039'], ['籌資活動之淨現金流入（流出）', '-195,459'], ['期末現金及約當現金餘額', '-324,594']],
 [['營業收入', ''], ['\u3000營業收入', ''], ['\u3000\u3000營業收入淨額', '10'], ['\u3000營業收入合計', '10']],
 [['營業收入', ''], ['\u3000銷貨收入淨額', ''], ['\u3000\u3000應收票據', '102'], ['\u3000\u3000銷貨收入淨額', '102'], ['\u3000營業收入合計', '102']]]
for its in td:
    stpt(pru(its))
    print('---')

---

pd.set_option('display.max_columns', None)

"""