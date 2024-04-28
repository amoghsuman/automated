
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials


def MACD(DF,a,b,c):
    """function to calculate MACD
       typical values a = 12; b =26, c =9"""
    df = DF.copy()  
    df["MA_Fast"]=df["Adj Close"].ewm(span=a,min_periods=a).mean()       
    df["MA_Slow"]=df["Adj Close"].ewm(span=b,min_periods=b).mean()   
    df["MACD"]=round(df["MA_Fast"]-df["MA_Slow"],2)       
    df["Signal"]=round(df["MACD"].ewm(span=c,min_periods=c).mean(),2)     
    return df


def updateSheet(symbol):
    cl_price = pd.DataFrame() 
    ohlcv_data = {} 
    cl_price_dct={}
    vol_dct={}
    #df_index=pd.DataFrame()
    
    ohlcv_data[symbol] =yf.download(symbol, start="2007-09-17", end=dt.date.today(),interval="1wk")
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
    exponential_moving_avg2=cl_price.ewm(span=30,min_periods=30).mean()
    exponential_moving_avg3=cl_price.ewm(span=5,min_periods=5).mean()
    #exponential_moving_avg4=cl_price.ewm(span=200,min_periods=200).mean()
    
    
    final_df=pd.DataFrame()
    final_df=ohlcv_data[symbol]
    final_df['5W EMA']=exponential_moving_avg3[symbol]
    final_df['10W EMA']=exponential_moving_avg1[symbol]
    final_df['30W EMA']=exponential_moving_avg2[symbol]
    #final_df['200D EMA']=exponential_moving_avg4[symbol]
    final_df['% change']=daily_return[symbol]*100
    final_df['50D Avg Vol']=vol[symbol].rolling(window=10).mean()
    final_df['Vol Ratio']=vol[symbol]/final_df['50D Avg Vol']

    
    
    down_vol={}
    MACD_greater_than_zero={}
    down_vol[symbol]=[0]
    MACD_greater_than_zero[symbol]=[""]
    macd_df = MACD(ohlcv_data[symbol], 12, 26, 9)
    
    for i in range(1,len(final_df)):
        if(final_df['% change'][i]<0):
            down_vol[symbol].append(final_df['Volume'][i])
        else:
            down_vol[symbol].append(0)
        
       
        if(macd_df["MACD"][i]>0):
            MACD_greater_than_zero[symbol].append("Yes")
        elif(macd_df["MACD"][i]<=0):
            MACD_greater_than_zero[symbol].append("No")
        else:
            MACD_greater_than_zero[symbol].append("")
            
    final_df['Down Vol'] = np.array(down_vol[symbol])
    prev_10D_maxdownvol=final_df['Down Vol'].rolling(window=4).max().shift(1).fillna(0)
    final_df['Prev 10D Max DownVol']=prev_10D_maxdownvol
            
    pocket_pivot={}
    pocket_pivot[symbol] =[""]
    
    for i in range(1,len(final_df)):
        if (final_df['% change'][i]>=0) and (final_df['Volume'][i]>final_df['Prev 10D Max DownVol'][i]) and \
            ((final_df['10W EMA'][i]-0.05*final_df['10W EMA'][i]<final_df['Adj Close'][i] and final_df['Adj Close'][i]<final_df['10W EMA'][i]+0.05*final_df['10W EMA'][i]) or \
                (final_df['5W EMA'][i]-0.05*final_df['5W EMA'][i]<final_df['Adj Close'][i] and final_df['Adj Close'][i]<final_df['5W EMA'][i]+0.05*final_df['5W EMA'][i])):
                    pocket_pivot[symbol].append("Yes")
        else:
            pocket_pivot[symbol].append("No") 
           
    
    final_df['Pocket Pivot'] = np.array(pocket_pivot[symbol])
    final_df['MACD > 0']=np.array(MACD_greater_than_zero[symbol])
    final_df.sort_values(by=['Date'], inplace=True, ascending=False)
    
   
    final_df['Open'] = final_df['Open'].apply(lambda x: round(x, 2))
    final_df['Low'] = final_df['Low'].apply(lambda x: round(x, 2))
    final_df['High'] = final_df['High'].apply(lambda x: round(x, 2))
    final_df['Close'] = final_df['Close'].apply(lambda x: round(x, 2))
    final_df['Volume'] = final_df['Volume'].apply(lambda x: round(x, 2))
    final_df['Adj Close'] = final_df['Adj Close'].apply(lambda x: round(x, 2))
    final_df['10W EMA'] = final_df['10W EMA'].apply(lambda x: round(x, 2))
    final_df['30W EMA'] = final_df['30W EMA'].apply(lambda x: round(x, 2))
    final_df['5W EMA'] = final_df['5W EMA'].apply(lambda x: round(x, 2))
    #final_df['200D EMA'] = final_df['200D EMA'].apply(lambda x: round(x, 2))
    final_df['% change'] = final_df['% change'].apply(lambda x: round(x, 2))
    final_df['50D Avg Vol'] = final_df['50D Avg Vol'].apply(lambda x: round(x, 2))
    final_df['Vol Ratio'] = final_df['Vol Ratio'].apply(lambda x: round(x, 2))
    new_symbol=symbol.replace(".NS","")
    symb=new_symbol+'.csv'
    final_df.to_csv(symb,date_format='%d %b %Y')
    
    #filename=dt.datetime.today().strftime("%d %b %Y")
    
    #scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             #"https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    
    #credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    #client = gspread.authorize(credentials)

    
    #sh=client.open(filename)
    #sh.add_worksheet(title="Weekly", rows="3000", cols="20")
    #wks = sh.get_worksheet(1)
    #wks.update([final_df.columns.values.tolist()] + final_df.values.tolist())
    
    
    


Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
for i in range(0,501):
    sym=Nifty500list['Symbol'][i]+'.NS'
    updateSheet(sym)



