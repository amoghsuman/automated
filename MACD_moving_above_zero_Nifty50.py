import datetime as dt
import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials


nifty_file=pd.read_csv('Nifty50list.csv')
nifty_file['Yahoo_Symbol']='Hello World'
nifty_file.Yahoo_Symbol= nifty_file.Symbol + '.NS'
nifty_file.to_csv('Nifty_yahoo_ticker.csv')
nifty_tickers=pd.read_csv('Nifty_yahoo_ticker.csv')
stocks=nifty_tickers['Yahoo_Symbol'].tolist()
start = "2021-06-01"
end = dt.datetime.today()
cl_price = pd.DataFrame() 
ohlcv_data = {} 





for i in range(0,50):
    ticker=stocks[i]
    ohlcv_data[ticker] = yf.download(ticker,start,end,interval="1h")

def MACD(DF,a,b,c):
    """function to calculate MACD
       typical values a = 12; b =26, c =9"""
    df = DF.copy()   
    df["MA_Fast"]=df["Adj Close"].ewm(span=a,min_periods=a).mean()       
    df["MA_Slow"]=df["Adj Close"].ewm(span=b,min_periods=b).mean()    
    df["MACD"]=round(df["MA_Fast"]-df["MA_Slow"],2)     
    df["Signal"]=round(df["MACD"].ewm(span=c,min_periods=c).mean(),2)     
    df.dropna(inplace=True)   
    return df

new_df=pd.DataFrame()
Company={}
Open={}
Close={}
Adj_Close={}
High={}
Low={}
MACD_Level={}
MACD_Signal={}
Company["ticker"]=[]
Open["ticker"]=[]
Close["ticker"]=[]
Adj_Close["ticker"]=[]
High["ticker"]=[]
Low["ticker"]=[]
MACD_Signal["ticker"]=[]
MACD_Level["ticker"]=[]
for i in range(0,50):
    ticker=stocks[i]
    df = MACD(ohlcv_data[ticker], 12, 26, 9) 
    df.sort_values(by=['Datetime'], inplace=True, ascending=False)       
    if(len(df)>1 and df["MACD"][1]<0 and df["MACD"][0]>=0):
        a_row=pd.Series([ticker,ohlcv_data[ticker]["Open"][0],ohlcv_data[ticker]["Close"][0],ohlcv_data[ticker]["Adj Close"][0],ohlcv_data[ticker]["High"][0],ohlcv_data[ticker]["Low"][0],df["MACD"][0],df["Signal"][0]])
        Open["ticker"].append(round(ohlcv_data[ticker]["Open"][0],2))
        Company["ticker"].append(ticker)
        Close["ticker"].append(round(ohlcv_data[ticker]["Close"][0],2))
        Adj_Close["ticker"].append(round(ohlcv_data[ticker]["Adj Close"][0],2))
        High["ticker"].append(round(ohlcv_data[ticker]["High"][0],2))
        Low["ticker"].append(round(ohlcv_data[ticker]["Low"][0],2))
        MACD_Signal["ticker"].append(round(df["Signal"][0],2))
        MACD_Level["ticker"].append(round((df["MACD"][0]),2))
        
new_df["Company"]=np.array(Company["ticker"])
new_df["Open"]=np.array(Open["ticker"])
new_df["Close"]=np.array(Close["ticker"])
new_df["Adj Close"]=np.array(Adj_Close["ticker"])
new_df["High"]=np.array(High["ticker"])
new_df["Low"]=np.array(Low["ticker"])
new_df["MACD Level"]=np.array(MACD_Level["ticker"])
new_df["MACD Signal"]=np.array(MACD_Signal["ticker"])


filename=dt.datetime.today() 

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)

sh=client.create(filename)
sh.share('insidersbakchod@gmail.com', perm_type='user', role='owner')
new_df.to_csv(filename+'.csv',index=False)

with open(filename+'.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(sh.id, data=content)



