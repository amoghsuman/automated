import pandas as pd
import gspread
import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials


def updateSheet(symbol,ind):
    new_symbol=symbol+'.csv'
    final_df=pd.read_csv(new_symbol)
    if(len(final_df)>1 and ((final_df['RS(1yr)'][1]<0 or final_df['% 50D MA'][1]<0) and (final_df['RS(1yr)'][0]>0 and final_df['% 50D MA'][0]>0))):
        df2 = {'Symbol': [symbol],'Industry':[ind],'Adj Close':[final_df['Adj Close'][0]] ,'% Change':[final_df['% change'][0]] , 'RS(1yr)': [final_df['RS(1yr)'][0]], '% 50D MA':[final_df['% 50D MA'][0]] }
        data=pd.DataFrame.from_dict(df2)
        return data
    
Nifty500list=pd.read_csv('Nifty500list.csv',usecols= ['Symbol','Industry'])
master_df=pd.DataFrame(columns=['Symbol','Industry','Adj Close','% Change','RS(1yr)','% 50D MA'])

for i in range(0,501):
    sym=Nifty500list['Symbol'][i]
    ind=Nifty500list['Industry'][i]
    row_df=updateSheet(sym,ind)
    master_df = pd.concat([master_df,row_df],ignore_index=True)

filename=dt.datetime.today().strftime("%d %b %Y")

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(credentials)
sh=client.open(filename)
sh.add_worksheet(title="RS(1yr) > 0 &&  %50DMA > 0 ", rows="3000", cols="20")
wks = sh.get_worksheet(2)
wks.update([master_df.columns.values.tolist()] + master_df.values.tolist())
