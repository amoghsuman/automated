
import pandas as pd
import gspread
import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials

def updateSheet(symbol,ind,master_df):
    new_symbol=symbol+'.csv'
    final_df=pd.read_csv(new_symbol)
    if(final_df['Pocket Pivot'][0]=='Yes'):
        data={'Symbol':[symbol],'Industry':[ind],'Adj Close':[final_df['Adj Close'][0]],'5W EMA':[final_df['5W EMA'][0]],'10W EMA':[final_df['10W EMA'][0]],'30W EMA':[final_df['30W EMA'][0]],'% Change':final_df['% change'][0],'Vol Ratio':final_df['Vol Ratio'][0],'Pocket Pivot':[final_df['Pocket Pivot'][0]],'MACD > 0':[final_df['MACD > 0'][0]]}
        df=pd.DataFrame.from_dict(data)
        return df
    
Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol','Industry'])
master_df=pd.DataFrame()

for i in range(0,501):
    sym=Nifty500list['Symbol'][i]
    ind=Nifty500list['Industry'][i]
    row_df=updateSheet(sym,ind,master_df)
    master_df = pd.concat([master_df,row_df],ignore_index=True)

filename="Weekly Mastersheet"
master_df.to_csv(filename+'.csv',index=False)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)

sh=client.open(filename)
#sh.share('insidersbakchod@gmail.com', perm_type='user', role='owner')
#sh.share('amoghsuman@gmail.com', perm_type='user', role='reader')
#sh.share('mahalasandeep1187.sm@gmail.com', perm_type='user', role='reader')
#sh.share('muditg2020@email.iimcal.ac.in', perm_type='user', role='reader')

with open(filename+'.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(sh.id, data=content)
    




