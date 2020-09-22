import scrapy
from scrapy.crawler import CrawlerProcess
from string import ascii_uppercase
from datetime import datetime
import os
import pandas as pd
from multiprocessing import Process
import time


class ScrapeMarket(scrapy.Spider):
    name = "quotes"
    marketData = {}
    martketDict = {}
    done = False
    dirname = os.path.dirname(__file__)
    symbolFile = os.path.join(dirname, 'DataOut\mrktSymbols\mrkt_symbols.csv')
    symbolAvFile = os.path.join(dirname, 'DataOut\mrktSymbols\mrkt_avb_symbols.csv')
    dataFile = os.path.join(dirname, 'DataOut\weeklyData\mrkt_data.csv')

    def getTime(self):
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        return current_time
    def getMin(self):
        now = datetime.now()
        current_time = now.strftime("%M")
        return current_time
    def getDate(self):
        return datetime.now().strftime("%A")

    def start_requests(self):
        urls = []
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        current_day = now.strftime("%d")
        weekno = datetime.today().weekday()
        print("Current Time =", current_time)
        self.martketDict = pd.read_csv(self.symbolFile, header=None, index_col=0, squeeze=True).to_dict()
        mrktSymbols = list(self.martketDict.keys())

        # for symbol in mrktSymbols:
        #         urls.append('https://finance.yahoo.com/quote/' + str(symbol))
        # for url in urls:
        #         yield scrapy.Request(url=url, callback=self.parseMarket)


        if current_day == "21" and current_time == "18:07":
            urls = []
            # for c in ascii_uppercase:
            #     urls.append('http://eoddata.com/stocklist/TSX/' + c + '.htm')
            # for url in urls:
            #     yield scrapy.Request(url=url, callback=self.parseSymbol)
            for symbol in mrktSymbols:
                urls.append('https://finance.yahoo.com/quote/' + str(symbol))
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parseAvailableSym)


        # while True:
        #     if current_time == "9:30" and weekno<5:


    def parseSymbol(self, response):
        marketSymbol = response.css("tr.ro A::text").getall()
        marketNameResponse = response.css("tr.ro td::text").getall()
        marketName = []
        for data in range(len(marketNameResponse)):
            if data % 8 == 0:
                marketName.append(marketNameResponse[data])
        for name in range(len(marketName)):
            self.marketData[marketSymbol[name]] = marketName[name]
        marketSymbol = response.css("tr.re A::text").getall()
        marketNameResponse = response.css("tr.re td::text").getall()
        marketName = []
        for data in range(len(marketNameResponse)):
            if data % 8 == 0:
                marketName.append(marketNameResponse[data])
        for name in range(len(marketName)):
            self.marketData[marketSymbol[name]] = marketName[name]
        with open(self.symbolFile, 'w') as f:
            for key in self.marketData.keys():
                f.write("%s,%s\n" % (key, self.marketData[key]))

    def parseMarket(self, response):
        marketValue = response.xpath('//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]//text()').extract()
        if len(marketValue) == 1 and float(marketValue[0]) > 0:
            url = response.url.split("/")
            symbol = url[len(url) -1]
            name = self.martketDict[symbol]
            with open(self.dataFile, 'a') as f:
                f.write("{0},{1},{2},{3},{4}\n" .format(str(name),str(symbol),str(marketValue[0]), str(self.getDate()), str(self.getTime())))
    def parseAvailableSym(self, response):
        marketValue = response.xpath('//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]//text()').extract()
        if len(marketValue) == 1:
            url = response.url.split("/")
            symbol = url[len(url) -1]
            name = self.martketDict[symbol]
            with open(self.symbolAvFile, 'a') as f:
                f.write("%s,%s\n" % (symbol, name))



process = CrawlerProcess(settings={
    "FEEDS": {
        "items.json": {"format": "json"},
    },
})


def getTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    return current_time


def getMin():
    now = datetime.now()
    current_time = now.strftime("%M")
    return current_time


def getDate():
    return datetime.now().strftime("%A")

def execute_crawling():
    process.crawl(ScrapeMarket)
    process.start()

def getRequiredMarketData():
    dirname = os.path.dirname(__file__)
    symbolAvFile = os.path.join(dirname, 'DataOut\mrktSymbols\mrkt_avb_symbols.csv')
    dataFile = os.path.join(dirname, 'DataOut\weeklyData\mrkt_data.csv')
    df = pd.read_csv(dataFile)
    priceData = df['price'] < 20.0
    df = df[priceData]
    symList = list(df['symbol'])
    symDf = pd.read_csv(symbolAvFile)
    availSym = symDf.symbol.isin(symList)
    symDf = symDf[availSym]
    os.remove(symbolAvFile)
    os.remove(dataFile)
    symDf.to_csv(symbolAvFile)
    df.to_csv(dataFile)


if __name__ == '__main__':
    getRequiredMarketData()
    while True:
        if int(getMin()) % 1 == 0:
            p = Process(target=execute_crawling)
            p.start()
            p.join()
            break
            time.sleep(120.0)
