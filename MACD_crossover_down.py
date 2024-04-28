import datetime as dt
import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials


nifty_file=pd.read_csv('Stocks with F&O.csv')
nifty_file['Yahoo_Symbol']='Hello World'
nifty_file.Yahoo_Symbol= nifty_file.Symbol + '.NS'
nifty_file.to_csv('Nifty_yahoo_ticker.csv')
nifty_tickers=pd.read_csv('Nifty_yahoo_ticker.csv')
stocks=nifty_tickers['Yahoo_Symbol'].tolist()
start = dt.date.today()-dt.timedelta(15)
end = dt.datetime.today()
cl_price = pd.DataFrame() 
ohlcv_data = {}  





for i in range(0,158):
    ticker=stocks[i]
    if(i==0 or i==1):
        ticker=ticker.replace(".NS","")
    ohlcv_data[ticker] = yf.download(ticker,start,end,interval="30m")

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
for i in range(0,158):
    ticker=stocks[i]
    if(i==0 or i==1):
        ticker=ticker.replace(".NS","")
    df = MACD(ohlcv_data[ticker], 12, 26, 9) 
    #df.sort_values(by=['Date'], inplace=True, ascending=False)       
    if(len(df)>1 and (df["MACD"][len(df)-2] > df["Signal"][len(df)-2]) and (df["MACD"][len(df)-1] < df["Signal"][len(df)-1])):
        sym=ticker.replace(".NS","")
        a_row=pd.Series([sym,ohlcv_data[ticker]["Open"][len(df)-1],ohlcv_data[ticker]["Close"][len(df)-1],ohlcv_data[ticker]["Adj Close"][len(df)-1],ohlcv_data[ticker]["High"][len(df)-1],ohlcv_data[ticker]["Low"][len(df)-1],df["MACD"][len(df)-1],df["Signal"][len(df)-1]])
        Open["ticker"].append(round(ohlcv_data[ticker]["Open"][len(df)-1],2))
        Company["ticker"].append(sym)
        Close["ticker"].append(round(ohlcv_data[ticker]["Close"][len(df)-1],2))
        Adj_Close["ticker"].append(round(ohlcv_data[ticker]["Adj Close"][len(df)-1],2))
        High["ticker"].append(round(ohlcv_data[ticker]["High"][len(df)-1],2))
        Low["ticker"].append(round(ohlcv_data[ticker]["Low"][len(df)-1],2))
        MACD_Signal["ticker"].append(round(df["Signal"][len(df)-1],2))
        MACD_Level["ticker"].append(round((df["MACD"][len(df)-1]),2))
        
new_df["Company"]=np.array(Company["ticker"])
new_df["Open"]=np.array(Open["ticker"])
new_df["Close"]=np.array(Close["ticker"])
new_df["Adj Close"]=np.array(Adj_Close["ticker"])
new_df["High"]=np.array(High["ticker"])
new_df["Low"]=np.array(Low["ticker"])
new_df["MACD Level"]=np.array(MACD_Level["ticker"])
new_df["MACD Signal"]=np.array(MACD_Signal["ticker"])



filename= "MACD > 0" 

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)

sh=client.open(filename)
sh.add_worksheet(title="G<R", rows="3000", cols="20")
wks = sh.get_worksheet(3)
wks.update([new_df.columns.values.tolist()] + new_df.values.tolist())



