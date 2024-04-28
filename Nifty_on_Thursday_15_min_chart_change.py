# -*- coding: utf-8 -*-


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


df_index=yf.download("^NSEI", start="2021-03-03", end="2021-03-18",interval='15m')


pct_change_Nifty = df_index['Adj Close'].pct_change()


df_index['% change Nifty']=pct_change_Nifty



final_df['Open Nifty'] = df_index['Open']
final_df['Open Nifty'] = final_df['Open Nifty'].apply(lambda x: round(x, 4))
final_df['Close Nifty']=df_index['Close']
final_df['Close Nifty'] = final_df['Close Nifty'].apply(lambda x: round(x, 4))

final_df['Date']=final_df.index
final_df.set_index('Open Nifty', inplace=True)
final_df['Day'] = final_df['Date'].dt.day_name()
final_df['date']=final_df['Date'].dt.date




filtered_df = final_df.loc[final_df['Date'].dt.weekday == 3]
shifted=filtered_df['Close Nifty'].shift(17)
filtered_df['% Chg 11am to 3:30pm']=(filtered_df['Close Nifty']-shifted)/shifted
filtered_df['% Chg 11am to 3:30pm'] = filtered_df['% Chg 11am to 3:30pm'].apply(lambda x: round(x, 4))*100
shifted2=filtered_df['Close Nifty'].shift(11)
filtered_df['% Chg 12:30pm to 3:30pm']=(filtered_df['Close Nifty']-shifted2)/shifted2
filtered_df['% Chg 12:30pm to 3:30pm'] = filtered_df['% Chg 12:30pm to 3:30pm'].apply(lambda x: round(x, 4))*100
shifted3=filtered_df['Close Nifty'].shift(7)
filtered_df['% Chg 1:30pm to 3:30pm']=(filtered_df['Close Nifty']-shifted3)/shifted3
filtered_df['% Chg 1:30pm to 3:30pm'] = filtered_df['% Chg 1:30pm to 3:30pm'].apply(lambda x: round(x, 4))*100
shifted4=filtered_df['Close Nifty'].shift(3)
filtered_df['% Chg 2:30pm to 3:30pm']=(filtered_df['Close Nifty']-shifted4)/shifted4
filtered_df['% Chg 2:30pm to 3:30pm'] = filtered_df['% Chg 2:30pm to 3:30pm'].apply(lambda x: round(x, 4))*100

filtered_df.drop_duplicates(subset ="date",
                     keep = 'last', inplace = True)
filtered_df.sort_values(by=['Date'], inplace=True, ascending=False)
filtered_df.set_index('date', inplace=True)
filtered_df.drop(['Date','Close Nifty','Day'], axis = 1,inplace=True)
filtered_df.to_csv('Nifty on Thursday 3.csv',date_format='%d %b %Y')





