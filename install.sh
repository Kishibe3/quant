scrapy startproject crawler
mv scrapy_crawler.py crawler/crawler/spiders/scrapy_crawler.py
mv settings.py crawler/crawler/settings.py
python crawler.py
cd crawler
scrapy crawl stocktradinginfo
scrapy crawl financialstatements
