
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials



Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
new_df=pd.DataFrame(columns=['Symbol','% Change','Vol Ratio'])
for i in range(0,501):
    symbol=Nifty500list['Symbol'][i]+'.NS'
    cl_price = pd.DataFrame()
    ohlcv_data = {} 
    cl_price_dct={}
    vol_dct={}
    df2=pd.DataFrame()
    
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
    
    
    
    final_df=pd.DataFrame()
    final_df=ohlcv_data[symbol]
    final_df['% change']=daily_return[symbol]*100
    final_df['50D Avg Vol']=vol[symbol].rolling(window=50).mean()
    final_df['Vol Ratio']=vol[symbol]/final_df['50D Avg Vol']
    final_df.sort_values(by=['Date'], inplace=True, ascending=False)
    
    
    final_df['Open'] = final_df['Open'].apply(lambda x: round(x, 2))
    final_df['Low'] = final_df['Low'].apply(lambda x: round(x, 2))
    final_df['High'] = final_df['High'].apply(lambda x: round(x, 2))
    final_df['Close'] = final_df['Close'].apply(lambda x: round(x, 2))
    final_df['Volume'] = final_df['Volume'].apply(lambda x: round(x, 2))
    final_df['Adj Close'] = final_df['Adj Close'].apply(lambda x: round(x, 2))
    final_df['% change'] = final_df['% change'].apply(lambda x: round(x, 2))
    final_df['50D Avg Vol'] = final_df['50D Avg Vol'].apply(lambda x: round(x, 2))
    final_df['Vol Ratio'] = final_df['Vol Ratio'].apply(lambda x: round(x, 2))
    if(len(final_df)>1 and final_df['Vol Ratio'][0]>1.5 and final_df['% change'][0]>0):
        df2 = {'Symbol': symbol, '% Change':final_df['% change'][0] , 'Vol Ratio': final_df['Vol Ratio'][0]}
        new_df = new_df.append(df2, ignore_index = True)
    
filename=dt.datetime.today().strftime("%d %b %Y")    

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)
sh=client.open(filename)
sh.add_worksheet(title="VolRatio>1.5 & %Change>0", rows="3000", cols="20")
wks = sh.get_worksheet(2)
wks.update([new_df.columns.values.tolist()] + new_df.values.tolist())



   



