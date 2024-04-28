
import pandas as pd
import gspread
import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials

def updateSheet(symbol,master_df):
    new_symbol=symbol+'.csv'
    final_df=pd.read_csv(new_symbol)
    if(final_df['Pocket Pivot'][0]=='Yes'):
        data={'Symbol':[symbol],'Adj Close':[final_df['Adj Close'][0]],'% Change':final_df['% change'][0],'%50D MA':final_df['% 50D MA'][0],'Vol Ratio':final_df['Vol Ratio'][0],'RS(1yr)':[final_df['RS(1yr)'][0]],'Pocket Pivot':[final_df['Pocket Pivot'][0]],'Buyable GapUpClose':[final_df['Buyable GapUpClose'][0]],'Buyable GapUpHigh':[final_df['Buyable GapUpHigh'][0]]}
        df=pd.DataFrame.from_dict(data)
        return df
    elif (final_df['Buyable GapUpClose'][0]=='Yes'):
        data={'Symbol':[symbol],'Adj Close':[final_df['Adj Close'][0]],'% Change':final_df['% change'][0],'%50D MA':final_df['% 50D MA'][0],'Vol Ratio':final_df['Vol Ratio'][0],'RS(1yr)':[final_df['RS(1yr)'][0]],'Pocket Pivot':[final_df['Pocket Pivot'][0]],'Buyable GapUpClose':[final_df['Buyable GapUpClose'][0]],'Buyable GapUpHigh':[final_df['Buyable GapUpHigh'][0]]}
        df=pd.DataFrame.from_dict(data)
        return df
    elif (final_df['Buyable GapUpHigh'][0]=='Yes'):
        data={'Symbol':[symbol],'Adj Close':[final_df['Adj Close'][0]],'% Change':final_df['% change'][0],'%50D MA':final_df['% 50D MA'][0],'Vol Ratio':final_df['Vol Ratio'][0],'RS(1yr)':[final_df['RS(1yr)'][0]],'Pocket Pivot':[final_df['Pocket Pivot'][0]],'Buyable GapUpClose':[final_df['Buyable GapUpClose'][0]],'Buyable GapUpHigh':[final_df['Buyable GapUpHigh'][0]]}
        df=pd.DataFrame.from_dict(data)
        return df
    
Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol'])
master_df=pd.DataFrame()

for i in range(0,501):
    sym=Nifty500list['Symbol'][i]
    row_df=updateSheet(sym,master_df)
    master_df = pd.concat([master_df,row_df],ignore_index=True)

filename=dt.datetime.today().strftime("%d %b %Y")
master_df.to_csv(filename+'.csv',index=False)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)

sh=client.create(filename)
sh.share('insidersbakchod@gmail.com', perm_type='user', role='owner')
sh.share('amoghsuman@gmail.com', perm_type='user', role='reader')
sh.share('mahalasandeep1187.sm@gmail.com', perm_type='user', role='reader')
sh.share('muditg2020@email.iimcal.ac.in', perm_type='user', role='reader')

with open(filename+'.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(sh.id, data=content)
    




