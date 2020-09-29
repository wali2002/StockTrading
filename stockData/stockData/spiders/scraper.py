import scrapy
from scrapy.crawler import CrawlerProcess
from string import ascii_uppercase
from datetime import datetime
import os
import pandas as pd
from multiprocessing import Process
import time,sys
import stockData.stockData.common.date_util as dt
from scrapy.utils.log import configure_logging
import stockData.stockData.processdata.processTrades as pt

dirname = os.path.dirname(__file__)
symbolAvFile = os.path.join(dirname, 'DataOut\mrktSymbols\mrkt_avb_symbols.csv')
dataFile = os.path.join(dirname, 'DataOut\weeklyData\mrkt_data.csv')
symbolFile = os.path.join(dirname, 'DataOut\mrktSymbols\mrkt_symbols.csv')


class ScrapeMarket(scrapy.Spider):
    name = "quotes"
    marketData = {'name':'symbol'}
    martketDict = {}

    def start_requests(self):
        urls = []
        now = datetime.now()
        current_time = dt.getTime()
        current_day = now.strftime("%d")
        print("Current Time =", current_time)
        try:
            self.martketDict = pd.read_csv(symbolFile, header=None, index_col=0, squeeze=True).to_dict()
            self.martketDictAv = pd.read_csv(symbolAvFile, header=None, index_col=0, squeeze=True).to_dict()

        except:
            print('market error')
        mrktSymbols = list(self.martketDict.keys())
        mrktSymbolsAv = list(self.martketDictAv.keys())
        time = current_day == "21" and current_time == "18:07"
        if self.run == "market":
            for symbol in mrktSymbolsAv:
                urls.append('https://finance.yahoo.com/quote/' + str(symbol))
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parseMarket)
        elif self.run == "symbol" or time:
            urls = []
            for c in ascii_uppercase:
                urls.append('http://eoddata.com/stocklist/TSX/' + c + '.htm')
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parseSymbol)
        elif self.run == 'real-symbol' or time:
            for symbol in mrktSymbols:
                urls.append('https://finance.yahoo.com/quote/' + str(symbol))
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parseAvailableSym)


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
        with open(symbolFile, 'w') as f:
            for key in self.marketData.keys():
                f.write("%s,%s\n" % (key, self.marketData[key]))

    def parseMarket(self, response):
        if response.status != 200:
            pass
        if response.status == 200:
            marketValue = response.xpath('//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]//text()').extract()
            if len(marketValue) == 1 and float(marketValue[0]) > 0:
                url = response.url.split("/")
                symbol = url[len(url) -1]
                name = self.martketDict[symbol]
                tradeProces = getTrade(symbol,name,marketValue[0])
                tradeProces.addTrade()
                with open(dataFile, 'a') as f:
                    f.write("{0},{1},{2},{3},{4},{5},{6}\n" .format(str(name),str(symbol),str(marketValue[0]), str(dt.getDay()), str(dt.getTime()), str(dt.getDate()),str(dt.getMinAndHour())))
    def parseAvailableSym(self, response):
        marketValue = response.xpath('//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]//text()').extract()
        if len(marketValue) == 1:
            url = response.url.split("/")
            symbol = url[len(url) -1]
            name = self.martketDict[symbol]
            with open(symbolAvFile, 'a') as f:
                f.write("%s,%s\n" % (symbol, name))


def addHeadersToFile():
    name = "name"
    symbol = 'symbol'
    with open(symbolAvFile, 'a') as f:
        if os.stat(symbolAvFile).st_size == 0:
            f.write("%s,%s\n" % (symbol, name))
    with open(dataFile, 'a') as f:
        if os.stat(dataFile).st_size == 0:
            f.write("name,symbol,price,day,time,date,minute\n")
    with open(symbolFile, 'a') as f:
        if os.stat(symbolFile).st_size == 0:
            f.write("name,symbol\n")

process = CrawlerProcess(settings={
    "FEEDS": {
        "items.json": {"format": "json"},
    },
    'LOG_ENABLED': False,
})


def execute_crawling():
    process.crawl(ScrapeMarket, run='market')
    process.start()

def round_down(num, divisor=3):
    return num - (num%divisor)

def getTrade(symbol, name, marketValue):
    pt.ProcessTrades(symbol, name, marketValue, str(dt.getDate()), str(dt.getMinAndHour()))

def getRequiredMarketData():
    df = pd.read_csv(dataFile)
    if len(df) > 1:
        priceData = df['price'] < 15.0
        df = df[priceData]
        dffil = df['time'].isin(["09:30","09:31","09:32",'09:33','09:34'])
        firstTrade = df[dffil]
        avg = df.groupby(['symbol'])['price'].mean().reset_index()
        symbolsToFilter = []
        for row in avg.iterrows():
            for firstRow in firstTrade.iterrows():
                if row[1]['symbol'] == firstRow[1]['symbol'] and row[1]['price'] == firstRow[1]['price']:
                    symbolsToFilter.append(row[1]['symbol'])
        dfFilter = df['symbol'].isin(symbolsToFilter)
        df = df[~dfFilter]
        symList = list(df['symbol'])
        symDf = pd.read_csv(symbolAvFile)
        availSym = symDf.symbol.isin(symList)
        symDf = symDf[availSym]
        df["minute"] = df["minute"].apply(lambda x: round_down(x))
        os.remove(symbolAvFile)
        os.remove(dataFile)
        symDf.to_csv(symbolAvFile,index=False)
        df.to_csv(dataFile,index=False)


def startSpider():
    configure_logging(install_root_handler=True)
    addHeadersToFile()
    run = True
    print("Trade Started")
    while True:
        if dt.getTime() == '12:40' or run:
            startMin = int(dt.getMin())
            print("Start: " + str(dt.getFullTime()))
            p = Process(target=execute_crawling)
            p.start()
            p.join()
            endMin = int(dt.getMin())
            print("End: " + str(dt.getFullTime()))
            sleeptTime = 180 - (endMin - startMin)
            if sleeptTime > 0:
                time.sleep(sleeptTime)
            run = True
            getRequiredMarketData()
        if int(dt.getMinAndHour()) > 1630:
            getRequiredMarketData()
            run = False
    print("Trade Ended at " + getTime())
    trade = pt.ProcessTrades()
    trade.clearTrades()

