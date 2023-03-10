import schedule
import datetime
import time
import requests
from loguru import logger
import redis
from redis.commands.json.path import Path
from dotenv import load_dotenv
import os
import mysql.connector
import pandas as pd
import functools
import pytz
from apscheduler import schedulers
import requests

def get_yf_realtime_data():
    target_dic={"tw_index":"^TWII",
    "0050":"0050.TW",
    "0051":"0051.TW",
    "Dow Jones Industrial Average":"^DJI",
    "S&P 500":"^GSPC",
    "NASDAQ Composite":"^IXIC",
    "BTC/USDT":"BTC-USD",
    "ETH/USDT":"ETH-USD",
    "BNB/USDT":"BNB-USD"}

    realtime_dic={}
    try:
        for key,value in target_dic.items():
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{value}?region=US&lang=en-US&includePrePost=false&interval=2m&useYfid=true&range=1d&corsDomain=finance.yahoo.com&.tsrc=finance"
            payload={}
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64"}
            response = requests.request("GET", url, headers=headers, data=payload)
            if (response.json()["chart"]["result"]):
                sparkline=response.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"][-1::-4]
                data={
                    "symbol":response.json()["chart"]["result"][0]["meta"]["symbol"],
                    "last_time":response.json()["chart"]["result"][0]["meta"]["regularMarketTime"],
                    "timezone":response.json()["chart"]["result"][0]["meta"]["timezone"],
                    "latest_price":response.json()["chart"]["result"][0]["meta"]["regularMarketPrice"],
                    "previouse_close_price":response.json()["chart"]["result"][0]["meta"]["chartPreviousClose"],
                    "dataGranularity":response.json()["chart"]["result"][0]["meta"]["dataGranularity"],
                    "range":response.json()["chart"]["result"][0]["meta"]["range"],
                    "timestamp":response.json()["chart"]["result"][0]["timestamp"],
                    "price_high":response.json()["chart"]["result"][0]["indicators"]["quote"][0]["high"],
                    "price_close":sparkline,
                    "price_low":response.json()["chart"]["result"][0]["indicators"]["quote"][0]["low"],
                    "price_open":response.json()["chart"]["result"][0]["indicators"]["quote"][0]["open"],
                    "price_volume":response.json()["chart"]["result"][0]["indicators"]["quote"][0]["volume"],}
            else:
                data={
                    "code":response.json()["chart"]["error"]["code"],
                    "description":response.json()["chart"]["error"]["description"]
                }
            realtime_dic[key]=data
        load_dotenv()
        client = redis.Redis(host=os.getenv('Redis_host'), port=os.getenv('Redis_port'),password=os.getenv('Redis_password'))
        client.json().set('realtime:dashboard', Path.root_path(), realtime_dic)

        UTC_timezone = pytz.timezone("UTC") 
        current_time = datetime.datetime.now(UTC_timezone)
        logger.log("YH", f"UTC Time now is {current_time}")
        logger.log("YH", f"set realtime data to {os.getenv('Redis_host')}")
    except Exception as e:
        logger.error(f"{e}")

def get_tw_all_realtime_market():
    load_dotenv()
    token=os.getenv('FinMindTolen')
    url = "https://api.finmindtrade.com/api/v4/taiwan_stock_tick_snapshot"
    parameter = {
        "token": token, # ???????????????????????????
    }
    try:
        resp = requests.get(url, params=parameter)
        data = resp.json()
        if(data['data']):
            client = redis.Redis(host=os.getenv('Redis_host'), port=os.getenv('Redis_port'),password=os.getenv('Redis_password'))
            client.json().set('realtime:TW_realtime', Path.root_path(), data["data"])
            UTC_timezone = pytz.timezone("UTC") 
            current_time = datetime.datetime.now(UTC_timezone)
            logger.log("TW_realtime", f"UTC Time now is {current_time}")
            logger.log("TW_realtime", f"set realtime data to {os.getenv('Redis_host')}")
    except Exception as e:
        logger.error(f"{e}")
def get_us_all_daily_market():
    date=(datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    load_dotenv()
    token=os.getenv('FinMindTolen')
    url = 'https://api.finmindtrade.com/api/v4/data'
    parameter = {
    "dataset": "USStockPrice",
    "start_date": date,
    "token": token,
    }
    try:
        resp = requests.get(url, params=parameter)
        data = resp.json()
        if(len(data['data'])>1):
            client = redis.Redis(host=os.getenv('Redis_host'), port=os.getenv('Redis_port'),password=os.getenv('Redis_password'))
            client.json().set('realtime:US_daily', Path.root_path(),data['data'])
            UTC_timezone = pytz.timezone("UTC") 
            current_time = datetime.datetime.now(UTC_timezone)
            logger.log("US_daily", f"UTC Time now is {current_time}")
            logger.log("US_daily", f"set realtime data to {os.getenv('Redis_host')}")
    except Exception as e:
        logger.error(f"{e}")
def get_crypto_all_realtime_market():
    load_dotenv()
    coin_list=[]
    crypto_symbol=[]
    try:
        for i in range (1,8):
            url=f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page={i}&price_change_percentage=1h%2C24h%2C7d"
            data=requests.get(url)
            coin_list=coin_list+data.json()
        
        for coin in coin_list:
            crypto_symbol.append({"symbol":coin["id"],"name":coin["name"]})

        client = redis.Redis(host=os.getenv('Redis_host'), port=os.getenv('Redis_port'),password=os.getenv('Redis_password'))
        client.json().set('realtime:crypto', Path.root_path(), {"data":coin_list})
        client = redis.Redis(host=os.getenv('Redis_host'), port=os.getenv('Redis_port'),password=os.getenv('Redis_password'))
        client.json().set('realtime:crypto_symbol', Path.root_path(), {"symbol":crypto_symbol})

        res = requests.get('https://api.coingecko.com/api/v3/search/trending')
        top_7=res.json()
        client.json().set('realtime:crypto_top7', Path.root_path(), top_7)
        UTC_timezone = pytz.timezone("UTC") 
        current_time = datetime.datetime.now(UTC_timezone)
        logger.log("Crypto_realtime", f"UTC Time now is {current_time}")
        logger.log("Crypto_realtime", f"set realtime data to {os.getenv('Redis_host')}")
    except Exception as e:
        logger.error(f"{e}")
def getAndInsert_Symbol_daily(region):
    load_dotenv()
    db_config = {
        'host' : os.getenv('sqlHost'),
        'user' : os.getenv('sqlUser'),
        'password' : os.getenv('sqlPassword'),
        'database' : os.getenv('sqlDatabase'),
        'port' : os.getenv('sqlPort')
    }
    cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "rds",pool_size=10, **db_config)
    connect_objt=cnxpool.get_connection()
    cursor = connect_objt.cursor(buffered=True)

    if(region=="TW"):
        SymbolTable="TwSymbols"
        StockTable="TwStockTable"
        Dataset="TaiwanStockPrice"
    elif(region=="US"):
        SymbolTable="UsSymbols"
        StockTable="UsStockTable"
        Dataset="USStockPrice"

    url = "https://api.finmindtrade.com/api/v4/data"
    

    sql=f"select distinct symbol from {SymbolTable};"
    cursor.execute(sql)
    symbols=cursor.fetchall()
    for symbol in symbols:
        symbol=symbol[0]
        sql=f"select id from {SymbolTable} where symbol = %s;"
        val=(symbol,)
        cursor.execute(sql,val)
        symbolId=cursor.fetchone()[0]

        sql=f"select date from {StockTable} where symbol = %s ORDER BY Date DESC;"
        val=(symbolId,)
        cursor.execute(sql,val)
        last_date=cursor.fetchone()
        if(last_date):
            parameter = {
            "dataset": Dataset,
            "data_id": symbol,
            "start_date": last_date[0]+ datetime.timedelta(1),
            "end_date": datetime.datetime.now().date(),
            "token": os.getenv('FinMindTolen'), # ???????????????????????????
            }

            for index in range(1,6,1):
                try:
                    resp = requests.get(url, params=parameter)
                    data = resp.json()
                    data = pd.DataFrame(data["data"])
                    if data.empty:
                        logger.log(region,f"{symbol} is latest")
                    else :
                        logger.log(region,f"updating {symbol}")
                        for i in range (0,data.shape[0]):
                            sql=f"INSERT INTO {StockTable}(symbol,date,open,high,low,close,volume) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                            if(region=="TW"):
                                val=(symbolId,data.iloc[i]["date"],data.iloc[i]["open"].item(),data.iloc[i]["max"].item(),data.iloc[i]["min"].item(),data.iloc[i]["close"].item(),data.iloc[i]["Trading_Volume"].item())
                            elif(region=="US"):
                                val=(symbolId,data.iloc[i]["date"],data.iloc[i]["Open"].item(),data.iloc[i]["High"].item(),data.iloc[i]["Low"].item(),data.iloc[i]["Close"].item(),data.iloc[i]["Volume"].item())
                            cursor.execute(sql,val)
                            connect_objt.commit()
                        logger.log(region,f"{symbol} update finish")
                    time.sleep(1)
                    break
                except Exception as e:
                    logger.error(f"{symbol} error {index} time,{e}")
                    time.sleep(30)
    cursor.close()
    connect_objt.close()

def check_answer():
    load_dotenv()
    db_config = {
        'host' : os.getenv('sqlHost'),
        'user' : os.getenv('sqlUser'),
        'password' : os.getenv('sqlPassword'),
        'database' : os.getenv('sqlDatabase'),
        'port' : os.getenv('sqlPort')
    }
    cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "rds",pool_size=10, **db_config)
    connect_objt=cnxpool.get_connection()
    cursor = connect_objt.cursor(buffered=True)
    date=datetime.datetime.today().strftime("%Y-%m-%d")
    sql="select id,market,symbol,date,price,direct,createrId from GameTable where date < %s AND isFinish = 0"
    val=(date,)
    cursor.execute(sql,val)
    games=cursor.fetchall()
    for game in games:
        if(game[1]=="us_stock"):
            symbol=game[2]
            sql="select UsStockTable.date,UsStockTable.close,UsSymbols.symbol,UsSymbols.companyName from UsStockTable INNER JOIN UsSymbols ON UsStockTable.symbol = UsSymbols.id where UsSymbols.symbol = %s and UsStockTable.date = %s"
            val=(symbol,game[3])
            cursor.execute(sql,val)
            record=cursor.fetchone()
            if(record):
                if(game[5]=="up"):
                    if(game[4]<record[1]):
                        isReach=True
                    if(game[4]>record[1]):
                        isReach=False
                elif(game[5]=="down"):
                    if(game[4]>record[1]):
                        isReach=True
                    if(game[4]<record[1]):
                        isReach=False
                sql="UPDATE GameTable SET isReach = %s,isFinish = %s WHERE id = %s;"
                val=(isReach,True,game[0])
                cursor.execute(sql,val)
                connect_objt.commit()
                if(isReach):
                    sql="UPDATE UserTable SET win = win +1 WHERE id = %s;"
                    val=(game[6],)
                else:
                    sql="UPDATE UserTable SET lose = lose +1 WHERE id = %s;"
                    val=(game[6],)
                cursor.execute(sql,val)
                connect_objt.commit()

        elif(game[1]=="tw_stock"):
            symbol=game[2]
            sql="select TwStockTable.date,TwStockTable.close,TwSymbols.symbol,TwSymbols.companyName from TwStockTable INNER JOIN TwSymbols ON TwStockTable.symbol = TwSymbols.id where TwSymbols.symbol = %s and TwStockTable.date = %s"
            val=(symbol,game[3])
            cursor.execute(sql,val)
            record=cursor.fetchone()
            if(record):
                if(game[5]=="up"):
                    if(game[4]<record[1]):
                        isReach=True
                    if(game[4]>record[1]):
                        isReach=False
                elif(game[5]=="down"):
                    if(game[4]>record[1]):
                        isReach=True
                    if(game[4]<record[1]):
                        isReach=False
                sql="UPDATE GameTable SET isReach = %s,isFinish = %s WHERE id = %s;"
                val=(isReach,True,game[0])
                cursor.execute(sql,val)
                connect_objt.commit()
                if(isReach):
                    sql="UPDATE UserTable SET win = win +1 WHERE id = %s;"
                    val=(game[6],)
                else:
                    sql="UPDATE UserTable SET lose = lose +1 WHERE id = %s;"
                    val=(game[6],)
                cursor.execute(sql,val)
                connect_objt.commit()
        else:
            gamedate=f"{game[3].day}-{game[3].month}-{game[3].year}"
            data = requests.get(f'https://api.coingecko.com/api/v3/coins/{game[2]}/history?date={gamedate}')
            if(data.json()["market_data"]["current_price"]["usd"]):
                price=data.json()["market_data"]["current_price"]["usd"]
                if(game[5]=="up"):
                    if(game[4]<price):
                        isReach=True
                    if(game[4]>price):
                        isReach=False
                elif(game[5]=="down"):
                    if(game[4]>price):
                        isReach=True
                    if(game[4]<price):
                        isReach=False
                sql="UPDATE GameTable SET isReach = %s,isFinish = %s WHERE id = %s;"
                val=(isReach,True,game[0])
                cursor.execute(sql,val)
                connect_objt.commit()
                if(isReach):
                    sql="UPDATE UserTable SET win = win +1 WHERE id = %s;"
                    val=(game[6],)
                else:
                    sql="UPDATE UserTable SET lose = lose +1 WHERE id = %s;"
                    val=(game[6],)
                cursor.execute(sql,val)
                connect_objt.commit()
    cursor.close()
    connect_objt.close()

if __name__ == '__main__':
    new_level = logger.level("YH", no=38, color="<white>", icon="    ")
    new_level_TW = logger.level("TW", no=40, color="<green>", icon="****")
    new_level2_TW_realtime = logger.level("TW_realtime", no=40, color="<green>", icon="****")
    new_level3_US = logger.level("US", no=42, color="<red>", icon="****")
    new_level4_US_daily = logger.level("US_daily", no=42, color="<red>", icon="****")
    new_level5_crypto_realtime = logger.level("Crypto_realtime", no=42, color="<blue>", icon="****")
    logger.add("./logger/{time:YYYY-MM-DD-HH-mm!UTC}.log",format="{time:YYYY-MM-DD at HH:mm:ss}|{level.icon} {level} {level.icon}|  {message}",colorize = True, rotation="1 days")
    schedule.every(60).seconds.do(get_yf_realtime_data)
    schedule.every(60).seconds.do(get_crypto_all_realtime_market)
    schedule.every(15).seconds.do(get_tw_all_realtime_market)
    schedule.every().day.at("09:00").do(get_us_all_daily_market) 
    ## Tokyo time because EC2 in Tokyo
    schedule.every().day.at("19:00").do(getAndInsert_Symbol_daily,region="US") 
    schedule.every().day.at("16:00").do(getAndInsert_Symbol_daily,region="TW")
    schedule.every().day.at("23:00").do(check_answer)
    # schedule.every().second.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
    # get_yf_realtime_data()