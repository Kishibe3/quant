scrapy startproject crawler
mv crawler.py crawler/crawler/spiders/scrapy_crawler.py
mv crawler.py crawler/crawler/settings.py
python crawler.py
cd crawler
scrapy crawl financialstatements
scrapy crawl stocktradinginfo
