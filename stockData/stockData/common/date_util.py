from datetime import datetime,timedelta

def getTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    return current_time

def getFullTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time

def getSec():
    now = datetime.now()
    current_time = now.strftime("%M")
    current_time_sec = now.strftime("%S")
    return (int(current_time) * 60) + int(current_time_sec)

def getDayName(date):
    return date.strftime("%A")

def getTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    return current_time
def getMin():
    now = datetime.now()
    current_time = now.strftime("%M")
    return current_time
def getMinAndHour():
    now = datetime.now()
    current_time = now.strftime("%H%M")
    return current_time

def getDay():
    return datetime.now().strftime("%A")

def getDate():
    return datetime.now().strftime("%Y%m%d")

def getToday():
    return datetime.now()

def getDateInt(date : str):
    return int(date)

def getStrToDateTime(date : str):
    return datetime.strptime(date, "%Y%m%d")

def getDateTimetoStr(date):
    return datetime.strftime(date, "%Y%m%d")

def getStartOfWeek(date):
    return date - timedelta(7)

def getMinusDays(date,daysToMinus):
    return date - timedelta(daysToMinus)

def getEndOfWeek(date):
    return date

def getYesterdayDate(date):
    yesterday = date - timedelta(1)
    if getDayName(yesterday) == "Saturday":
        return yesterday - timedelta(1)
    elif getDayName(yesterday) == "Sunday":
        return yesterday - timedelta(2)


