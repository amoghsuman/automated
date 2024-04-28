
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials



Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
new_df=pd.DataFrame(columns=['Symbol','Adj Close','% Change','RS(1yr)','% 50D MA'])
for i in range(0,501):
    symbol=Nifty500list['Symbol'][i]+'.NS'
    cl_price = pd.DataFrame()
    ohlcv_data = {} 
    cl_price_dct={}
    vol_dct={}
    df2=pd.DataFrame()
    df_index=pd.DataFrame()
    
    df_index=yf.download("^NSEI", start="2020-01-01", end=dt.datetime.today())
    ohlcv_data[symbol] =yf.download(symbol, start="2020-01-01", end=dt.datetime.today())
    
    df=ohlcv_data[symbol]   
    df=df[~df.index.duplicated()]  
    cl_price_dct[symbol]=df["Adj Close"] 
    cl_price=pd.DataFrame(cl_price_dct)
    vol_dct[symbol]=df["Volume"]
    vol=pd.DataFrame(vol_dct)
    vol.fillna(method='bfill',axis=0,inplace=True)
    cl_price.fillna(method='bfill',axis=0,inplace=True) 
    daily_return = cl_price.pct_change() 
    
    
    
    final_df=pd.DataFrame()
    final_df=ohlcv_data[symbol]
    shifted_1yr=cl_price[symbol].shift(250)
    shifted_1yr_idx=df_index['Adj Close'].shift(250)
    final_df['% 1 yr Change Nifty']=((df_index['Adj Close']-shifted_1yr_idx)/shifted_1yr_idx)*100
    final_df['% 1 yr Change']=((cl_price[symbol]-shifted_1yr)/shifted_1yr)*100
    final_df['% change']=daily_return[symbol]*100
    final_df['RS(1yr)']=((1+final_df['% 1 yr Change']/100)/(1+final_df['% 1 yr Change Nifty']/100)-1)*100
    final_df['% 50D MA']=((cl_price[symbol]-cl_price[symbol].rolling(window=50).mean())/cl_price[symbol].rolling(window=50).mean())*100
    final_df.sort_values(by=['Date'], inplace=True, ascending=False)
    
    
    final_df['Adj Close'] = final_df['Adj Close'].apply(lambda x: round(x, 2))
    final_df['% change'] = final_df['% change'].apply(lambda x: round(x, 2))
    final_df['RS(1yr)'] = final_df['RS(1yr)'].apply(lambda x: round(x, 2))
    final_df['% 50D MA'] = final_df['% 50D MA'].apply(lambda x: round(x, 2))
    if(len(final_df)>1 and ((final_df['RS(1yr)'][1]<0 or final_df['% 50D MA'][1]<0) and (final_df['RS(1yr)'][0]>0 and final_df['% 50D MA'][0]>0))):
        df2 = {'Symbol': symbol,'Adj Close':final_df['Adj Close'][0] ,'% Change':final_df['% change'][0] , 'RS(1yr)': final_df['RS(1yr)'][0], '% 50D MA':final_df['% 50D MA'][0] }
        new_df = new_df.append(df2, ignore_index = True)
    
filename=dt.datetime.today().strftime("%d %b %Y")    

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)
sh=client.open(filename)
sh.add_worksheet(title="RS(1yr) > 0 &&  %50D MA > 0 ", rows="3000", cols="20")
wks = sh.get_worksheet(5)
wks.update([new_df.columns.values.tolist()] + new_df.values.tolist())



   



