import scrapy
from scrapy.downloadermiddlewares.retry import get_retry_request
import time
import os
import json
from datetime import datetime, timedelta
import pandas as pd

# 爬取上市上櫃公司三大報表並儲存網頁
class FinstatSpider(scrapy.Spider):
    name = 'financialstatements'
    ignore_overrun_until = 0

    def __init__(self):
        if not os.path.exists('../raw data/stock basic.csv'):
            raise FileNotFoundError('raw data/stock basic.csv 不存在，請執行crawler.py以生成此檔案')
        
        self.stock = pd.read_csv('../raw data/stock basic.csv', dtype = {'代號': str})['代號'].to_list()

        earliest_date = '2013/01/01'
        self.sy, self.ss, _ = map(int, earliest_date.split('/'))
        self.ss = (self.ss + 2) // 3
        self.ey, self.es = datetime.now().year, datetime.today().strftime('%m/%d')
        if self.es < '03/31':
            self.ey, self.es = self.ey - 1, 3
        elif self.es < '05/15':
            self.ey, self.es = self.ey - 1, 4
        elif self.es < '08/14':
            self.es = 1
        elif self.es < '11/14':
            self.es = 2
        else:
            self.es = 3
    
    def start_requests(self):
        for sk in self.stock:
            if not os.path.exists(f'../raw data/financial statements/{sk}'):
                os.makedirs(f'../raw data/financial statements/{sk}')
            yield scrapy.Request(url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={sk}&SYEAR={self.ey}&SSEASON={self.es}&REPORT_ID=C', callback = self.check_id, meta = {'stock': sk, 'year': self.ey, 'season': self.es, 'id': 'C'})
    
    # check REPORT_ID should be C or A
    def check_id(self, response):
        if b'Overrun' in response.body or b'SECURITY' in response.body:
            url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={response.meta['stock']}&SYEAR={self.ey}&SSEASON={self.es}&REPORT_ID=C'
            if time.time() > self.ignore_overrun_until:
                self.log(f'Pause 20s. for {'overrun' if b'Overrun' in response.body else 'security'}. {url}')
                time.sleep(20)
                self.ignore_overrun_until = time.time() + 10
            yield get_retry_request(response.request, spider = self, reason = 'Overrun' if b'Overrun' in response.body else 'Security')
        else:
            sk = response.meta['stock']
            if bytes('查無資料', 'cp950') in response.body or bytes('檔案不存在', 'cp950') in response.body:
                for y in range(self.sy, self.ey + 1):
                    for s in range([1, self.ss][y == self.sy], [5, self.es + 1][y == self.ey]):
                        if not os.path.exists(f'../raw data/financial statements/{sk}/{y} {s}.html'):
                            yield scrapy.Request(url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={sk}&SYEAR={y}&SSEASON={s}&REPORT_ID=A', callback = self.retry_overrun, meta = {'stock': sk, 'year': y, 'season': s, 'id': 'A'})
            else:
                if not os.path.exists(f'../raw data/financial statements/{sk}/{self.ey} {self.es}.html'):
                    self.save(response.body, sk, self.ey, self.es)
                for y in range(self.sy, self.ey + 1):
                    for s in range([1, self.ss][y == self.sy], [5, self.es][y == self.ey]):
                        if not os.path.exists(f'../raw data/financial statements/{sk}/{y} {s}.html'):
                            yield scrapy.Request(url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={sk}&SYEAR={y}&SSEASON={s}&REPORT_ID=C', callback = self.retry_overrun, meta = {'stock': sk, 'year': y, 'season': s, 'id': 'C'})
    
    # retry overrun cases
    def retry_overrun(self, response):
        sk, y, s, id = response.meta['stock'], response.meta['year'], response.meta['season'], response.meta['id']
        if b'Overrun' in response.body or b'SECURITY' in response.body:
            url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={sk}&SYEAR={y}&SSEASON={s}&REPORT_ID={id}'
            if time.time() > self.ignore_overrun_until:
                self.log(f'Pause 20s. for {'overrun' if b'Overrun' in response.body else 'security'}. {url}')
                time.sleep(20)
                self.ignore_overrun_until = time.time() + 10
            yield get_retry_request(response.request, spider = self, reason = 'Overrun' if b'Overrun' in response.body else 'Security')
        else:
            self.save(response.body, sk, y, s)
    
    def save(self, body, sk, y, s):
        self.log(f'save raw data/financial statements/{sk}/{y} {s}.html')
        with open(f'../raw data/financial statements/{sk}/{y} {s}.html', 'wb') as f:
            f.write(body)


# 爬取上市上櫃每日交易資訊並儲存
class TradingInfoSpider(scrapy.Spider):
    name = 'stocktradinginfo'

    def start_requests(self):
        if not os.path.exists('../raw data/stock trading info/twse'):
            os.makedirs('../raw data/stock trading info/twse')
        if not os.path.exists('../raw data/stock trading info/tpex'):
            os.makedirs('../raw data/stock trading info/tpex')
        
        earliest_date = '2013/01/01'
        date = datetime(*map(int, earliest_date.split('/')))
        delta = timedelta(days = 1)
        
        while date <= datetime.now():
            if date.weekday() < 5:
                if not os.path.exists(f'../raw data/stock trading info/twse/{date.strftime('%Y %m %d')}.json'):
                    yield scrapy.Request(url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALLBUT0999&date=' + date.strftime('%Y%m%d'), callback = self.save_twse, meta = {'date': date})
                if not os.path.exists(f'../raw data/stock trading info/tpex/{date.strftime('%Y %m %d')}.json'):
                    yield scrapy.Request(url = 'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&se=EW&d=' + str(int(date.strftime('%Y/%m/%d').split('/')[0]) - 1911) + '/' + date.strftime('%m/%d'), callback = self.save_tpex, meta = {'date': date})
            date += delta
    
    def save_twse(self, response):
        resp = json.loads(response.text)
        if resp['stat'] != 'OK':
            return
        
        date = response.meta['date']
        self.log(f'save raw data/stock trading info/twse/{date.strftime('%Y %m %d')}.json')
        with open(f'../raw data/stock trading info/twse/{date.strftime('%Y %m %d')}.json', 'wb') as f:
            f.write(response.body)

    def save_tpex(self, response):
        resp = json.loads(response.text)
        if resp['tables'][0]['totalCount'] == 0:
            return

        date = response.meta['date'].strftime('%Y %m %d')
        self.log(f'save raw data/stock trading info/tpex/{date}.json')
        with open(f'../raw data/stock trading info/tpex/{date}.json', 'wb') as f:
            f.write(response.body)

