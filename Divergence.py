import numpy as np
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def divergence(symbol):
    data={}
    data[symbol]=yf.download(symbol, start="2020-08-17", end=dt.datetime.today())
    
    def rsi(df, n):
        "function to calculate RSI"
        delta = df["Adj Close"].diff().dropna()
        u = delta * 0
        d = u.copy()
        u[delta > 0] = delta[delta > 0]
        d[delta < 0] = -delta[delta < 0]
        u[u.index[n-1]] = np.mean( u[:n]) 
        u = u.drop(u.index[:(n-1)])
        d[d.index[n-1]] = np.mean( d[:n]) 
        d = d.drop(d.index[:(n-1)])
        rs = u.ewm(com=n,min_periods=n).mean()/d.ewm(com=n,min_periods=n).mean()
        return 100 - 100 / (1+rs) 
    
    Data=pd.DataFrame(data[symbol])
    df=Data.copy()
    df.drop(['Volume'],axis=1, inplace=True)
    Data.drop(['Close','Volume'],axis=1, inplace=True)
    RSI=rsi(Data,14)
    RSI=pd.DataFrame(RSI)
    Data = pd.concat([Data,RSI], ignore_index=True, axis = 1)
    Data=Data.to_numpy()
    
    bullish=np.zeros(len(Data),dtype=int)
    bearish=np.zeros(len(Data),dtype=int)
    lower_barrier=35
    upper_barrier=65
    width=50
    # Bullish Divergence
    for i in range(len(Data)):
       try:
           if Data[i, 4] < lower_barrier:
               
               for a in range(i + 1, i + width):
                   if Data[a, 4] > lower_barrier:
                       
                        for r in range(a + 1, a + width):
                           if Data[r, 4] < lower_barrier and \
                            Data[r, 4] > Data[i, 4] and Data[r, 3] < 1.03*Data[i, 3]:                                                        
                                
                                for s in range(r + 1, r + width): 
                                    if Data[s, 4] > lower_barrier:
                                        bullish[s]=1
                                        break
                                    else:
                                        continue
                           else:
                                continue
                        else:
                            continue
                   else:
                        continue
                    
       except IndexError:
            pass
    
     # Bearish Divergence
    for i in range(len(Data)):
        try:
            if Data[i, 4] > upper_barrier:
                
                for a in range(i + 1, i + width): 
                    if Data[a, 4] < upper_barrier:
                        for r in range(a + 1, a + width):
                            if Data[r, 4] > upper_barrier and \
                            Data[r, 4] < Data[i, 4] and Data[r, 3] > Data[i,3]:                                      
                                for s in range(r + 1, r + width):
                                    if Data[s, 4] < upper_barrier:
                                        bearish[s]=1
                                        break
                                    else:
                                        continue
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
        except IndexError:
            pass
    
    
    df['Bullish Divergence']=bullish
    df['Bearish Divergence']=bearish
    df['Open'] = df['Open'].apply(lambda x: round(x, 2))
    df['Low'] = df['Low'].apply(lambda x: round(x, 2))
    df['High'] = df['High'].apply(lambda x: round(x, 2))
    df['Close'] = df['Close'].apply(lambda x: round(x, 2))
    df['Adj Close'] = df['Adj Close'].apply(lambda x: round(x, 2))
    df.sort_values(by=['Date'], inplace=True, ascending=False)
    symbol=symbol.replace(".NS","")
    if(df['Bullish Divergence'][0]==1 or df['Bearish Divergence'][0]==1):
        data={'Symbol':[symbol],'Open':[df['Open'][0]], 'Low': [df['Low'][0]], 'High': [df['High'][0]], 'Close': [df['Close'][0]], 'Adj Close': [df['Adj Close'][0]],'Bullish Divergence':[df['Bullish Divergence'][0]] , 'Bearish Divergence':[df['Bearish Divergence'][0]]}
        df2=pd.DataFrame(data)
        return df2


Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
final_df=pd.DataFrame()
for i in range(0,412):
    sym=Nifty500list['Symbol'][i]+'.NS'
    df2=pd.DataFrame(divergence(sym))
    final_df=pd.concat([final_df, df2], ignore_index = True)

for i in range(413,501):
    sym=Nifty500list['Symbol'][i]+'.NS'
    df2=pd.DataFrame(divergence(sym))
    final_df=pd.concat([final_df, df2], ignore_index = True)
    
filename=dt.datetime.today().strftime("%d %b %Y")
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)
sh=client.open(filename)
sh.add_worksheet(title="Divergence", rows="3000", cols="20")
wks = sh.get_worksheet(1)
wks.update([final_df.columns.values.tolist()] + final_df.values.tolist())
#final_df.to_csv('Master.csv',index=False)