

import numpy as np
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials


cl_price_Nifty = pd.DataFrame() 

df_index=pd.DataFrame()
pct_change_Nifty=pd.DataFrame()
final_df=pd.DataFrame()
df=pd.DataFrame()


df_index=yf.download("^NSEI", start="2010-09-17", end=dt.datetime.today())


pct_change_Nifty = df_index['Adj Close'].pct_change()


df_index['% change Nifty']=pct_change_Nifty




final_df['% change Nifty']=df_index['Adj Close'].pct_change()
final_df['Open Nifty'] = df_index['Open']
final_df['Open Nifty'] = final_df['Open Nifty'].apply(lambda x: round(x, 4))
final_df['Close Nifty']=df_index['Close']
final_df['Close Nifty'] = final_df['Close Nifty'].apply(lambda x: round(x, 4))
shifted_close=final_df['Close Nifty'].shift(1)
final_df['% Gapup']=(final_df['Open Nifty']-shifted_close)/shifted_close
final_df['Date']=final_df.index
final_df.set_index('Open Nifty', inplace=True)
final_df['Day'] = final_df['Date'].dt.day_name()


final_df['% Gapup'] = final_df['% Gapup'].apply(lambda x: round(x, 4))*100
final_df['% change Nifty'] = final_df['% change Nifty'].apply(lambda x: round(x, 4))*100

final_df.sort_values(by=['Date'], inplace=True, ascending=False)


final_df.to_csv('Nifty on Thursday.csv',date_format='%d %b %Y')






