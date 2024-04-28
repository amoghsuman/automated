
import statsmodels.api as sm
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials

Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
new_df=pd.DataFrame()

def slope(ser,n):
    "function to calculate the slope of regression line for n consecutive points on a plot"
    ser = (ser - ser.min())/(ser.max() - ser.min())
    x = np.array(range(len(ser)))
    x = (x - x.min())/(x.max() - x.min())
    slopes = [i*0 for i in range(n-1)]
    for i in range(n,len(ser)+1):
        y_scaled = ser[i-n:i]
        x_scaled = x[:n]
        x_scaled = sm.add_constant(x_scaled)
        model = sm.OLS(y_scaled,x_scaled)
        results = model.fit()
        slopes.append(results.params[-1])
    slope_angle = (np.rad2deg(np.arctan(np.array(slopes))))
    return np.array(slope_angle)

def fn():
    symbol=Nifty500list['Symbol'][i]+'.NS'
    cl_price = pd.DataFrame()
    ohlcv_data = {} 
    cl_price_dct={}
    vol_dct={}
    
    ohlcv_data[symbol] =yf.download(symbol, start=dt.datetime.today()-dt.timedelta(90), end=dt.datetime.today())
    
    df=ohlcv_data[symbol]   
    df=df[~df.index.duplicated()]  
    cl_price_dct[symbol]=df["Adj Close"] 
    cl_price=pd.DataFrame(cl_price_dct)
    vol_dct[symbol]=df["Volume"]
    vol=pd.DataFrame(vol_dct)
    vol.fillna(method='bfill',axis=0,inplace=True)
    cl_price.fillna(method='bfill',axis=0,inplace=True) 
    daily_return = cl_price.pct_change() 
    
    exponential_moving_avg2=cl_price.ewm(span=50,min_periods=50).mean()
    
    
    final_df=pd.DataFrame()
    final_df=ohlcv_data[symbol]
    final_df['50D EMA']=exponential_moving_avg2[symbol]
    final_df["50D EMA Slope"] = slope(exponential_moving_avg2[symbol],5)
    final_df['% change']=daily_return[symbol]*100
    final_df['50D Avg Vol']=vol[symbol].rolling(window=50).mean()
    final_df['Vol Ratio']=vol[symbol]/final_df['50D Avg Vol']
    final_df.sort_values(by=['Date'], inplace=True, ascending=False)
    
    
    final_df['Open'] = final_df['Open'].apply(lambda x: round(x, 2))
    final_df['Low'] = final_df['Low'].apply(lambda x: round(x, 2))
    final_df['High'] = final_df['High'].apply(lambda x: round(x, 2))
    final_df['Close'] = final_df['Close'].apply(lambda x: round(x, 2))
    final_df['Volume'] = final_df['Volume'].apply(lambda x: round(x, 2))
    final_df['50D EMA'] = final_df['50D EMA'].apply(lambda x: round(x, 2))
    final_df['50D EMA Slope'] = final_df['50D EMA Slope'].apply(lambda x: round(x, 2))
    final_df['Adj Close'] = final_df['Adj Close'].apply(lambda x: round(x, 2))
    final_df['% change'] = final_df['% change'].apply(lambda x: round(x, 2))
    final_df['50D Avg Vol'] = final_df['50D Avg Vol'].apply(lambda x: round(x, 2))
    final_df['Vol Ratio'] = final_df['Vol Ratio'].apply(lambda x: round(x, 2))
    if(len(final_df)>1 and final_df['50D EMA Slope'][0]>0 and (1.1*final_df['50D EMA'][0]>final_df['Adj Close'][0]) and (0.9*final_df['50D EMA'][0]<final_df['Adj Close'][0])):
        data = {'Symbol': [symbol],'50D EMA':[final_df['50D EMA'][0]],'50D EMA Slope':[final_df['50D EMA Slope'][0]] ,'% Change':[final_df['% change'][0]] , 'Vol Ratio': [final_df['Vol Ratio'][0]]}
        df2=pd.DataFrame(data)
        return df2
    
for i in range(0,238):
    df2=fn()
    new_df=pd.concat([new_df, df2], ignore_index = True)

for i in range(239,412):
    df2=fn()
    new_df=pd.concat([new_df, df2], ignore_index = True)

    
for i in range(413,501):
    df2=fn()
    new_df=pd.concat([new_df, df2], ignore_index = True)

    
    
    
filename=dt.datetime.today().strftime("%d %b %Y")    

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)
sh=client.open(filename)
sh.add_worksheet(title="Price within 10% of 10 WMA", rows="3000", cols="20")
wks = sh.get_worksheet(5)
wks.update([new_df.columns.values.tolist()] + new_df.values.tolist())



   



