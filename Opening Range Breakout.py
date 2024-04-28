import datetime as dt
from datetime import datetime
import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials


nifty_file=pd.read_csv('Opening_Range.csv')
stocks=nifty_file['Company'].tolist()

if(datetime.datetime.today().weekday()==0):
    s = datetime.strftime(dt.date.today()-dt.timedelta(3),"%Y-%m-%d") + " 14:30:00"
else:
    s = datetime.strftime(dt.date.today()-dt.timedelta(1),"%Y-%m-%d") + " 14:30:00"

start=datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
end = dt.datetime.today()
cl_price = pd.DataFrame() 
ohlcv_data = {}    




for i in range(0,len(stocks)):
    ticker=stocks[i]
    #if(i==0 or i==1):
        #ticker=ticker.replace(".NS","")
    ohlcv_data[ticker] = yf.download(ticker,start,end,interval="1m")

new_df=pd.DataFrame()
Company={}
Signal={}
Company["ticker"]=[]
Signal["ticker"]=[]

for i in range(0,len(stocks)):
    ticker=stocks[i]
    l=len(ohlcv_data[ticker])
    #if(i==0 or i==1):
        #ticker=ticker.replace(".NS","")
    
    
    c= ohlcv_data[ticker]["Close"][l-1]
    v= ohlcv_data[ticker]["Volume"][l-1]
    
    ser= ohlcv_data[ticker]['Volume'].rolling(window=50).mean()
    l2= len(ser)
    
    if(c > nifty_file['High_Range'][i] and c > ohlcv_data[ticker]["Close"][50] and v > 2.5*ser[l2-1]):
        Company["ticker"].append(ticker)
        Signal["ticker"].append("Breakout")
        
    elif(c < nifty_file['Low_Range'][i] and c < ohlcv_data[ticker]["Close"][50]):
        Company["ticker"].append(ticker)
        Signal["ticker"].append("Breakdown")
    
        
new_df["Company"]=np.array(Company["ticker"])
new_df["Signal"]=np.array(Signal["ticker"])

scrips=pd.read_csv("Opening_Range_Breakout.csv",usecols= ['Company','Signal'])
master_df = pd.concat([scrips,new_df],ignore_index=True)
master_df.drop_duplicates(subset=["Company"],keep='first',inplace=True)

master_df.to_csv("Opening_Range_Breakout.csv",index=False)


filename= "Opening_Range_Breakout" 

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('csvtogs.json', scope)
client = gspread.authorize(credentials)

sh=client.open(filename)
wks = sh.get_worksheet(0)
wks.update([master_df.columns.values.tolist()] + master_df.values.tolist())



