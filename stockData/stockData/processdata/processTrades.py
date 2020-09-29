import pandas as pd
import stockData.stockData.analyze.analyze_data as ad
import stockData.stockData.common.date_util as dt
from stockData.stockData.analyze.analyze_data import yesterdayTrade


class Trades:

    def __init__(self, symbol, name, price, date, time):
        self.symbol = symbol
        self.name = name
        self.price = price
        self.date = date
        self.time = time


class ProcessTrades(Trades):
    tradeList = []

    def getSpecificTrade(self, symbol, time):
        specificTrade = list(filter(lambda trade: trade.symbol == symbol and trade.time == time, self.tradeList))
        return specificTrade[0]

    def addTrade(self):
        self.tradeList.append(Trades(self.symbol, self.name, self.value, self.date, self.time))

    def clearTrades(self):
        self.tradeList.clear()

    def averageTradeEval(self, symbol, time,match):
        trade = self.getSpecificTrade(symbol, time)
        dfMonthly = ad.findMonthlyAverage()
        dfWeekly = ad.findWeeklyAverage()
        dfDayName = ad.findSameDayNameAverage()

        monthlyAvgPrice = self.getAvgPrice(trade, dfMonthly)
        weeklyAvgPrice = self.getAvgPrice(trade, dfWeekly)
        dayNameAvgPrice = self.getAvgPrice(trade, dfDayName)
        monthPecentage = self.getPercentage(trade.price, monthlyAvgPrice)
        weekPecentage = self.getPercentage(trade.price, weeklyAvgPrice)
        dayPecentage = self.getPercentage(trade.price, dayNameAvgPrice)

        if dayPecentage < 3:
            match['dayname'].append(True)
        elif weekPecentage < 3:
            match['weekly'].append(True)
        elif monthPecentage < 3:
            match['monthly'].append(True)

        return match

    def previousDayEval(self, symbol, time,match):
        trade = self.getSpecificTrade(symbol, time)
        yesterTrade = ad.yesterdayTrade()
        yesterTradeSymbol = yesterTrade['symbol'] == symbol
        yesterTradeTime = yesterTrade['time'] == time
        yesterTrade = yesterTrade[yesterTradeSymbol]
        yesterTrade = yesterTrade[yesterTradeTime]

        previousDayPrice = yesterTrade.iloc[0]['price']
        previousDayPercent = self.getPercentage(trade.price,previousDayPrice)
        if previousDayPercent < 3:
          match['previousDay'].append(True)
            
    def evaluateTrade(self, symbol, time):
        match = {"dayName":[],'weekly':[], 'monthly': [], 'previousDay': []}


    def getAvgPrice(self, trade, dfAvg):
        filterMonthly = dfAvg['price'] == trade.price
        return dfAvg[filterMonthly].iloc[0]['price']

    def getPercentage(self,price, avgPrice):
        isGreaterThanOne = (price - avgPrice) > 1
        if isGreaterThanOne:
            return 100 - (avgPrice/price * 100)
        else:
            return 100 (price/avgPrice * 100)





