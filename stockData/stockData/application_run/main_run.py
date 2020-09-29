import stockData.stockData.spiders.scraper as sc
import threading
import time

if __name__ == '__main__':
    spider = threading.Thread(target=sc.startSpider)
    spider.start()

    print("sabir")