import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials



kijun_lookback  = 26
tenkan_lookback =  9
chikou_lookback = 26
senkou_span_projection = 26
senkou_span_b_lookback = 52
new_df=pd.DataFrame(columns=['Symbol','Kijun-sen','Senkou-span A','Senkou-span B'])
Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
for i in range(0,501):
    symbol=Nifty500list['Symbol'][i]+'.NS'
    Data=yf.download(symbol, start=dt.datetime.today()-dt.timedelta(100), end=dt.datetime.today())
    Data['Kijun-sen']=np.nan
    Data['Tenkan-sen']=np.nan
    Data['Senkou-span A']=np.nan
    Data['Senkou-span B']=np.nan
    
    for i in range(len(Data)):
        try:
            Data['Kijun-sen'][i] = max(Data['High'][i - kijun_lookback:i + 1]) + min(Data['Low'][i - kijun_lookback:i + 1])
    
        except ValueError:
            pass
        
    Data['Kijun-sen'][:] = Data['Kijun-sen'][:] / 2
    Data['Kijun-sen'] = Data['Kijun-sen'].apply(lambda x: round(x, 2))
    
    for i in range(len(Data)):
        try:
            Data['Tenkan-sen'][i] = max(Data['High'][i - tenkan_lookback:i + 1]) + min(Data['Low'][i - tenkan_lookback:i + 1])
    
        except ValueError:
            pass
        
    Data['Tenkan-sen'][:] = Data['Tenkan-sen'][:] / 2
    Data['Tenkan-sen'] = Data['Tenkan-sen'].apply(lambda x: round(x, 2))
    
    senkou_span_a = (Data['Kijun-sen'][:] + Data['Tenkan-sen'][:]) / 2
    Data['Senkou-span A']=senkou_span_a
    Data['Senkou-span A'] = Data['Senkou-span A'].apply(lambda x: round(x, 2))
    #senkou_span_a = np.reshape(senkou_span_a, (-1, 1))
    
    for i in range(senkou_span_b_lookback,len(Data)):
        try:
            Data['Senkou-span B'][i] = max(Data['High'][i - senkou_span_b_lookback:i + 1]) + min(Data["Low"][i - senkou_span_b_lookback:i + 1])
    
        except ValueError:
            pass
    
    Data['Senkou-span B'][:] = Data['Senkou-span B'][:] / 2 
    Data['Senkou-span B'] = Data['Senkou-span B'].apply(lambda x: round(x, 2))
    #senkou_span_b = Data['Senkou-span B'][:]
    #senkou_span_b = np.reshape(senkou_span_b, (-1, 1))
    
    Data.sort_values(by=['Date'], inplace=True, ascending=False)
    
    if(Data['Adj Close'][0]>0.5*Data['Kijun-sen'][0] and Data['Adj Close'][0]<1.5*Data['Kijun-sen'][0] and Data['Senkou-span A'][0]>Data['Senkou-span B'][0] and Data['Adj Close'][0]>Data['Senkou-span A'][0] and Data['Adj Close'][0]>Data['Senkou-span B'][0]):
        df2 = {'Symbol': symbol, 'Kijun-sen':Data['Kijun-sen'][0] , 'Senkou-span A': Data['Senkou-span A'][0],'Senkou-span B': Data['Senkou-span B'][0]}
        new_df = new_df.append(df2, ignore_index = True)
        
filename=dt.datetime.today().strftime("%d %b %Y")   
        
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)
sh=client.open(filename)
sh.add_worksheet(title="Ichimoku", rows="3000", cols="20")
wks = sh.get_worksheet(4)
wks.update([new_df.columns.values.tolist()] + new_df.values.tolist())
    



