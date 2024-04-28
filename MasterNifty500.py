
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
    df_index=pd.DataFrame()
    
    ohlcv_data[symbol] =yf.download(symbol, start="2007-09-17", end=dt.datetime.today())
    df_index=yf.download("^NSEI", start="2007-09-17", end=dt.datetime.today())
    
    df=ohlcv_data[symbol]   
    df=df[~df.index.duplicated()]   
    cl_price_dct[symbol]=df["Adj Close"] 
    cl_price=pd.DataFrame(cl_price_dct)
    vol_dct[symbol]=df["Volume"]
    vol=pd.DataFrame(vol_dct)
    vol.fillna(method='bfill',axis=0,inplace=True)
    cl_price.fillna(method='bfill',axis=0,inplace=True)
    daily_return = cl_price.pct_change() 
    
    exponential_moving_avg1=cl_price.ewm(span=21,min_periods=21).mean() 
    exponential_moving_avg2=cl_price.ewm(span=50,min_periods=50).mean()
    exponential_moving_avg3=cl_price.ewm(span=10,min_periods=10).mean()
    exponential_moving_avg4=cl_price.ewm(span=200,min_periods=200).mean()
    
    
    final_df=pd.DataFrame()
    final_df=ohlcv_data[symbol]
    final_df['10D EMA']=exponential_moving_avg3[symbol]
    final_df['21D EMA']=exponential_moving_avg1[symbol]
    final_df['50D EMA']=exponential_moving_avg2[symbol]
    final_df['200D EMA']=exponential_moving_avg4[symbol]
    shifted_6mon=cl_price[symbol].shift(125)
    shifted_1yr=cl_price[symbol].shift(250)
    final_df['% 6 mon Change']=((cl_price[symbol]-shifted_6mon)/shifted_6mon)*100
    final_df['% 1 yr Change']=((cl_price[symbol]-shifted_1yr)/shifted_1yr)*100
    shifted_6mon_idx=df_index['Adj Close'].shift(125)
    shifted_1yr_idx=df_index['Adj Close'].shift(250)
    final_df['% 6 mon Change Nifty']=((df_index['Adj Close']-shifted_6mon_idx)/shifted_6mon_idx)*100
    final_df['% 1 yr Change Nifty']=((df_index['Adj Close']-shifted_1yr_idx)/shifted_1yr_idx)*100
    final_df['% change']=daily_return[symbol]*100
    final_df['RS(1yr)']=((1+final_df['% 1 yr Change']/100)/(1+final_df['% 1 yr Change Nifty']/100)-1)*100
    final_df['% 50D MA']=((cl_price[symbol]-cl_price[symbol].rolling(window=50).mean())/cl_price[symbol].rolling(window=50).mean())*100
    final_df['50D Avg Vol']=vol[symbol].rolling(window=50).mean()
    final_df['Vol Ratio']=vol[symbol]/final_df['50D Avg Vol']
    final_df['High - Low']=final_df['High']-final_df['Low']
    final_df['40D ATR']=final_df['High - Low'].rolling(window=40).mean()
    shifted_1dayClose=final_df['Adj Close'].shift(1)
    final_df['GapUp Close']=final_df['Open']-shifted_1dayClose
    shifted_1dayHigh=final_df['High'].shift(1)
    final_df['GapUp High']=final_df['Open']-shifted_1dayHigh
    
    
    down_vol={}
    buyable_GapUpClose={}
    buyable_GapUpHigh={}
    MACD_greater_than_zero={}
    down_vol[symbol]=[0]
    buyable_GapUpClose[symbol]=[""]
    buyable_GapUpHigh[symbol]=[""]
    MACD_greater_than_zero[symbol]=[""]
    macd_df = MACD(ohlcv_data[symbol], 12, 26, 9)
    
    for i in range(1,len(final_df)):
        if(final_df['% change'][i]<0):
            down_vol[symbol].append(final_df['Volume'][i])
        else:
            down_vol[symbol].append(0)
        
        if(final_df['Vol Ratio'][i]>=1.5 and final_df['GapUp Close'][i]>0 and final_df['GapUp Close'][i]>=0.75*final_df['40D ATR'][i] and final_df['% change'][i]>0):
            buyable_GapUpClose[symbol].append("Yes")
        else:
            buyable_GapUpClose[symbol].append("No")
            
        if(final_df['Vol Ratio'][i]>=1.5 and final_df['GapUp High'][i]>0 and final_df['GapUp High'][i]>=0.75*final_df['40D ATR'][i] and final_df['% change'][i]>0):
            buyable_GapUpHigh[symbol].append("Yes")
        else:
            buyable_GapUpHigh[symbol].append("No")
        if(macd_df["MACD"][i]>0):
            MACD_greater_than_zero[symbol].append("Yes")
        elif(macd_df["MACD"][i]<=0):
            MACD_greater_than_zero[symbol].append("No")
        else:
            MACD_greater_than_zero[symbol].append("")
            
    final_df['Down Vol'] = np.array(down_vol[symbol])
    prev_10D_maxdownvol=final_df['Down Vol'].rolling(window=10).max().shift(1).fillna(0)
    final_df['Prev 10D Max DownVol']=prev_10D_maxdownvol
            
    pocket_pivot={}
    pocket_pivot[symbol] =[""]
    
    for i in range(1,len(final_df)):
        if (final_df['% change'][i]>=0) and (final_df['Volume'][i]>final_df['Prev 10D Max DownVol'][i]) and \
            ((final_df['21D EMA'][i]-0.05*final_df['21D EMA'][i]<final_df['Adj Close'][i] and final_df['Adj Close'][i]<final_df['21D EMA'][i]+0.05*final_df['21D EMA'][i]) or \
                (final_df['50D EMA'][i]-0.05*final_df['50D EMA'][i]<final_df['Adj Close'][i] and final_df['Adj Close'][i]<final_df['50D EMA'][i]+0.05*final_df['50D EMA'][i]) or \
                    (final_df['10D EMA'][i]-0.05*final_df['10D EMA'][i]<final_df['Adj Close'][i] and final_df['Adj Close'][i]<final_df['10D EMA'][i]+0.05*final_df['10D EMA'][i])):
                            pocket_pivot[symbol].append("Yes")
        else:
            pocket_pivot[symbol].append("No") 
           
    
    final_df['Pocket Pivot'] = np.array(pocket_pivot[symbol])
    final_df['Buyable GapUpClose'] = np.array(buyable_GapUpClose[symbol])
    final_df['Buyable GapUpHigh'] = np.array(buyable_GapUpHigh[symbol])
    final_df['MACD > 0']=np.array(MACD_greater_than_zero[symbol])
    final_df.sort_values(by=['Date'], inplace=True, ascending=False)
    
   
    final_df['Open'] = final_df['Open'].apply(lambda x: round(x, 2))
    final_df['Low'] = final_df['Low'].apply(lambda x: round(x, 2))
    final_df['High'] = final_df['High'].apply(lambda x: round(x, 2))
    final_df['Close'] = final_df['Close'].apply(lambda x: round(x, 2))
    final_df['Volume'] = final_df['Volume'].apply(lambda x: round(x, 2))
    final_df['Adj Close'] = final_df['Adj Close'].apply(lambda x: round(x, 2))
    final_df['21D EMA'] = final_df['21D EMA'].apply(lambda x: round(x, 2))
    final_df['50D EMA'] = final_df['50D EMA'].apply(lambda x: round(x, 2))
    final_df['10D EMA'] = final_df['10D EMA'].apply(lambda x: round(x, 2))
    final_df['200D EMA'] = final_df['200D EMA'].apply(lambda x: round(x, 2))
    final_df['High - Low'] = final_df['High - Low'].apply(lambda x: round(x, 2))
    final_df['GapUp Close'] = final_df['GapUp Close'].apply(lambda x: round(x, 2))
    final_df['GapUp High'] = final_df['GapUp High'].apply(lambda x: round(x, 2))
    final_df['40D ATR'] = final_df['40D ATR'].apply(lambda x: round(x, 2))
    final_df['% 6 mon Change'] = final_df['% 6 mon Change'].apply(lambda x: round(x, 2))
    final_df['% 1 yr Change'] = final_df['% 1 yr Change'].apply(lambda x: round(x, 2))
    final_df['% 6 mon Change Nifty'] = final_df['% 6 mon Change Nifty'].apply(lambda x: round(x, 2))
    final_df['% 1 yr Change Nifty'] = final_df['% 1 yr Change Nifty'].apply(lambda x: round(x, 2))
    final_df['RS(1yr)'] = final_df['RS(1yr)'].apply(lambda x: round(x, 2))
    final_df['% change'] = final_df['% change'].apply(lambda x: round(x, 2))
    final_df['% 50D MA'] = final_df['% 50D MA'].apply(lambda x: round(x, 2))
    final_df['50D Avg Vol'] = final_df['50D Avg Vol'].apply(lambda x: round(x, 2))
    final_df['Vol Ratio'] = final_df['Vol Ratio'].apply(lambda x: round(x, 2))
    new_symbol=symbol.replace(".NS","")
    symb=new_symbol+'.csv'
    final_df.to_csv(symb,date_format='%d %b %Y')
    
    
    new_symbol=symbol.replace(".NS","")
    sym=new_symbol+".csv"
    final_df.to_csv(sym,date_format='%d %b %Y')
    
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open(new_symbol)
    
    with open(new_symbol+".csv", 'r') as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)
    
    
    


Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
for i in range(0,501):
    sym=Nifty500list['Symbol'][i]+'.NS'
    updateSheet(sym)



