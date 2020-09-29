import pandas as pd
import stockData.stockData.common.date_util as dt
import os,sys,pathlib
import datetime
from pandas_cache import pd_cache, timeit

root = str(pathlib.Path(os.path.dirname(sys.modules['__main__'].__file__)).parent)
dataFile = os.path.join(root, 'spiders\DataOut\weeklyData\mrkt_data.csv')

def round_down(num, divisor=3):
    return num - (num%divisor)

@timeit
@pd_cache
def findAverage(numberOfDays : int):
    dataPd = pd.read_csv(dataFile)
    startDate= dt.getDateInt(dt.getDateTimetoStr(dt.getMinusDays(dt.getToday(),numberOfDays)))
    endDate = dt.getDateInt(dt.getDateTimetoStr(dt.getToday()))
    weekStartPdFilter = startDate <= dataPd["date"]
    weekEndPdFilter = dataPd["date"] <= endDate
    dataPd = dataPd[weekEndPdFilter]
    dataPd = dataPd[weekStartPdFilter]
    dataPd = dataPd.groupby(['minute', 'symbol'])['price'].mean().reset_index()
    return dataPd


@timeit
@pd_cache
def findSameDayNameAverage():
    dataPd = pd.read_csv(dataFile)
    dataPd = dataPd.groupby(['minute', 'symbol', 'day'])['price'].mean().reset_index()
    return dataPd


@timeit
@pd_cache
def yesterdayTrade():
    dataPd = pd.read_csv(dataFile)
    previousDate = dt.getDateInt(dt.getDateTimetoStr(dt.getYesterdayDate(dt.getToday())))
    previousDayFilter = dataPd['date'] == previousDate
    return dataPd[previousDayFilter]

def findWeeklyAverage():
    return findAverage(7)
def findMonthlyAverage():
    return findAverage(30)


