
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
import numpy as np
import statsmodels.api as sm
from oauth2client.service_account import ServiceAccountCredentials


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


def updateSheet(symbol,ind):
    cl_price = pd.DataFrame() 
    ohlcv_data = {} 
    cl_price_dct={}
    vol_dct={}
    #df_index=pd.DataFrame()
    
    ohlcv_data[symbol] =yf.download(symbol, start="2018-09-17", end=dt.date.today(),interval="1wk")
    #df_index=yf.download("^NSEI", start="2007-09-17", end=dt.date.today()-dt.timedelta(4),interval="1wk")
    
    df=ohlcv_data[symbol]   
    df=df[~df.index.duplicated()]   
    cl_price_dct[symbol]=df["Adj Close"] 
    cl_price=pd.DataFrame(cl_price_dct)
    vol_dct[symbol]=df["Volume"]
    vol=pd.DataFrame(vol_dct)
    vol.fillna(method='bfill',axis=0,inplace=True)
    cl_price.fillna(method='bfill',axis=0,inplace=True)
    daily_return = cl_price.pct_change() 
    
    exponential_moving_avg1=cl_price.ewm(span=10,min_periods=10).mean() 
    exponential_moving_avg2=cl_price.ewm(span=20,min_periods=20).mean()
    exponential_moving_avg3=cl_price.ewm(span=30,min_periods=30).mean()
    #exponential_moving_avg4=cl_price.ewm(span=200,min_periods=200).mean()
    
    
    final_df=pd.DataFrame()
    final_df=ohlcv_data[symbol]
    final_df['30W EMA']=exponential_moving_avg3[symbol]
    final_df['10W EMA']=exponential_moving_avg1[symbol]
    final_df['20W EMA']=exponential_moving_avg2[symbol]
    final_df['10W EMA Slope']=slope(final_df["10W EMA"],5)
    final_df['20W EMA Slope']=slope(final_df["20W EMA"],5)
    final_df['30W EMA Slope']=slope(final_df["30W EMA"],5)
    final_df.sort_values(by=['Date'], inplace=True, ascending=False)
    mx=max(final_df['Adj Close'][0],final_df['Adj Close'][1],final_df['Adj Close'][2])
    mn=min(final_df['Adj Close'][0],final_df['Adj Close'][1],final_df['Adj Close'][2])
    ratio=mx/mn
    
            
    ten_W={}
    ten_W[symbol] =[""]
    twenty_W={}
    twenty_W[symbol] =[""]
    thirty_W={}
    thirty_W[symbol] =[""]
    ten_W_slope={}
    ten_W_slope[symbol] =[""]
    twenty_W_slope={}
    twenty_W_slope[symbol] =[""]
    thirty_W_slope={}
    thirty_W_slope[symbol] =[""]
    
    
    if (final_df['10W EMA'][0]-0.05*final_df['10W EMA'][0]<final_df['Adj Close'][0] and final_df['Adj Close'][0]<final_df['10W EMA'][0]+0.05*final_df['10W EMA'][0]):
        ten_W[symbol].append("Yes")
    else:
        ten_W[symbol].append("No") 
            
    if (final_df['20W EMA'][0]-0.05*final_df['20W EMA'][0]<final_df['Adj Close'][0] and final_df['Adj Close'][0]<final_df['20W EMA'][0]+0.05*final_df['20W EMA'][0]):
        twenty_W[symbol].append("Yes")
    else:
        twenty_W[symbol].append("No") 
            
    if (final_df['30W EMA'][0]-0.05*final_df['30W EMA'][0]<final_df['Adj Close'][0] and final_df['Adj Close'][0]<final_df['30W EMA'][0]+0.05*final_df['30W EMA'][0]):
        thirty_W[symbol].append("Yes")
    else:
        thirty_W[symbol].append("No") 
        
    if (final_df['10W EMA Slope'][0]>0):
        ten_W_slope[symbol].append("Yes")
    else:
        ten_W_slope[symbol].append("No")
        
    if (final_df['20W EMA Slope'][0]>0):
        twenty_W_slope[symbol].append("Yes")
    else:
        twenty_W_slope[symbol].append("No")
        
    if (final_df['30W EMA Slope'][0]>0):
        thirty_W_slope[symbol].append("Yes")
    else:
        thirty_W_slope[symbol].append("No")
        
    if (ratio>0.98 and ratio<1.02 and final_df['30W EMA Slope'][0]>0):
        tightClose="Yes"
    else:
        tightClose="No"
        
    
    
   
    final_df['Adj Close'] = final_df['Adj Close'].apply(lambda x: round(x, 2))
    final_df['10W EMA'] = final_df['10W EMA'].apply(lambda x: round(x, 2))
    final_df['30W EMA'] = final_df['30W EMA'].apply(lambda x: round(x, 2))
    final_df['20W EMA'] = final_df['20W EMA'].apply(lambda x: round(x, 2))
    
    
    #new_symbol=symbol+'.csv'
    #final_df=pd.read_csv(new_symbol)
    sym=symbol.replace(".NS","")
    data={'Symbol':[sym],'Industry':[ind],'Adj Close':[final_df['Adj Close'][0]],'10W +-5%':[ten_W[symbol][1]],'20W +-5%':[twenty_W[symbol][1]],'30W +-5%':[thirty_W[symbol][1]],'10W Slope +ve':[ten_W_slope[symbol][1]],'20W Slope +ve':[twenty_W_slope[symbol][1]],'30W Slope +ve':[thirty_W_slope[symbol][1]],'TightClose':[tightClose]}
    df=pd.DataFrame.from_dict(data)
    return df
    
    #final_df['200D EMA'] = final_df['200D EMA'].apply(lambda x: round(x, 2))
    #new_symbol=symbol.replace(".NS","")
    #symb=new_symbol+'.csv'
    #final_df.to_csv(symb,date_format='%d %b %Y')
    
    
    



Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol','Industry'])
master_df=pd.DataFrame(columns=['Symbol','Industry','Adj Close','10W +-5%','20W +-5%','30W +-5%','10W Slope +ve','20W Slope +ve','30W Slope +ve','TightClose'])
for i in range(0,501):
    sym=Nifty500list['Symbol'][i]+'.NS'
    ind=Nifty500list['Industry'][i]
    row_df=updateSheet(sym,ind)
    master_df = pd.concat([master_df,row_df],ignore_index=True)
    
master_df.fillna("NaN", inplace = True)
filename="Weekly Mastersheet"
    
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
"https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    
credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)

    
sh=client.open(filename)
sh.add_worksheet(title="Magic Line", rows="3000", cols="20")
wks = sh.get_worksheet(1)
wks.update([master_df.columns.values.tolist()] + master_df.values.tolist())
    



