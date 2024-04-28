import datetime as dt
from datetime import datetime
import yfinance as yf
import pandas as pd
import numpy as np


nifty_file=pd.read_csv('Tredcode_Stocks.csv')
nifty_file['Yahoo_Symbol']='Hello World'
nifty_file.Yahoo_Symbol= nifty_file.Symbol + '.NS'
nifty_file.to_csv('Nifty_yahoo_ticker.csv')
nifty_tickers=pd.read_csv('Nifty_yahoo_ticker.csv')
stocks=nifty_tickers['Yahoo_Symbol'].tolist()
#start = dt.date.today()-dt.timedelta(1)
s = datetime.strftime(dt.date.today(),"%Y-%m-%d") + " 09:15:00"
start=datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
end = dt.datetime.today()
cl_price = pd.DataFrame() 
ohlcv_data = {}  
ohlcv_data_day = {}  




for i in range(0,len(stocks)):
    ticker=stocks[i]
    #if(i==0 or i==1):
        #ticker=ticker.replace(".NS","")
    ohlcv_data[ticker] = yf.download(ticker,start,end,interval="1m")
    ohlcv_data_day[ticker] = yf.download(ticker,start,end)

new_df=pd.DataFrame()
Company={}
Low_Range={}
High_Range={}
Company["ticker"]=[]
Low_Range["ticker"]=[]
High_Range["ticker"]=[]

for i in range(0,len(stocks)):
    ticker=stocks[i]
    l=len(ohlcv_data[ticker])
    #if(i==0 or i==1):
        #ticker=ticker.replace(".NS","")
    
    
    c= ohlcv_data[ticker]["Close"][l-1]
    o= ohlcv_data[ticker]["Open"][l-1]
    
    if(ohlcv_data[ticker]["Close"][0] < ohlcv_data[ticker]["Open"][0] and c > o):
        Company["ticker"].append(ticker)
        Low_Range["ticker"].append(ohlcv_data_day[ticker]["Low"][0])
        High_Range["ticker"].append(ohlcv_data_day[ticker]["High"][0])
        
    elif(ohlcv_data[ticker]["Close"][0] > ohlcv_data[ticker]["Open"][0] and c < o):
        Company["ticker"].append(ticker)
        Low_Range["ticker"].append(ohlcv_data_day[ticker]["Low"][0])
        High_Range["ticker"].append(ohlcv_data_day[ticker]["High"][0])
    
        
new_df["Company"]=np.array(Company["ticker"])
new_df["Low_Range"]=np.array(Low_Range["ticker"]) 
new_df["High_Range"]=np.array(High_Range["ticker"])

scrips=pd.read_csv("Opening_Range.csv",usecols= ['Company','Low_Range','High_Range'])
master_df = pd.concat([scrips,new_df],ignore_index=True)
master_df.drop_duplicates(subset=["Company"],keep='first',inplace=True)

master_df.to_csv("Opening_Range.csv",index=False)






