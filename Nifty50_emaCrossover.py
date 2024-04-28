import pandas as pd
import os
from datetime import datetime
from kiteconnect import KiteConnect
import logging
import datetime as dt
import numpy as np
import statsmodels.api as sm
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import random

cwd = os.getcwd()

#generate trading session
access_path = os.path.join(cwd,'access_token.txt')
key_path = os.path.join(cwd,'api_key.txt')
access_token = open(access_path,'r').read()
key_secret = open(key_path,'r').read().split()
kite = KiteConnect(api_key=key_secret[0])
kite.set_access_token(access_token)


#get dump of all NSE instruments
instrument_dump = kite.instruments()
instrument_df = pd.DataFrame(instrument_dump)

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

def MACD(DF,a,b,c):
    """function to calculate MACD
       typical values a = 12; b =26, c =9"""
    df = DF.copy()   
    df["MA_Fast"]=df["close"].ewm(span=a,min_periods=a).mean()       
    df["MA_Slow"]=df["close"].ewm(span=b,min_periods=b).mean()    
    df["MACD"]=round(df["MA_Fast"]-df["MA_Slow"],2)     
    df["Signal"]=round(df["MACD"].ewm(span=c,min_periods=c).mean(),2)     
    df.dropna(inplace=True)   
    return df

def fetchOHLC(ticker,interval,duration):
    """extracts historical data and outputs in the form of dataframe"""
    instrument = instrumentLookup(instrument_df,ticker)
    if instrument == -1:
        return -1
    data = pd.DataFrame(kite.historical_data(instrument,dt.date.today()-dt.timedelta(duration), dt.date.today(),interval))
    data.set_index("date",inplace=True)
    return data


def instrumentLookup(instrument_df,symbol):
    """Looks up instrument token for a given script from instrument dump"""
    try:
        return instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]
    except:
        return -1


def quote(ticker):
    instrument = instrumentLookup(instrument_df,ticker)
    data = pd.DataFrame(kite.quote(instrument))
    return data

# get ohlcv data for any ticker by start date and end date
def fetchOHLCExtended(ticker,inception_date, interval):
    """extracts historical data and outputs in the form of dataframe
       inception date string format - dd-mm-yyyy"""
    instrument = instrumentLookup(instrument_df,ticker)
    from_date = dt.datetime.strptime(inception_date, '%d-%m-%Y')
    to_date = dt.date.today()
    data = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    while True:
        if from_date.date() >= (dt.date.today() - dt.timedelta(100)):
            data = data.append(pd.DataFrame(kite.historical_data(instrument,from_date,dt.date.today(),interval)),ignore_index=True)
            break
        else:
            to_date = from_date + dt.timedelta(100)
            data = data.append(pd.DataFrame(kite.historical_data(instrument,from_date,to_date,interval)),ignore_index=True)
            from_date = to_date
    data.set_index("date",inplace=True)
    return data


data = fetchOHLCExtended("NIFTY 50","01-01-2018","day")
data.dropna(how='all',inplace=True)
data['%Change']=round(((data['close']-data['close'].shift(1))/data['close'].shift(1))*100,4)
Amount_infused={}
ETFs_bought={}
NAV={}
highestTillNow = {}
Commulative_Amount_Infused={}
Current_Investment_Value={}
PNL={}
histo={}
Amount_infused['Sensex'] = [0]
ETFs_bought['Sensex'] = [0]
Commulative_Amount_Infused['Sensex'] = [0]
Current_Investment_Value['Sensex'] = [0]
NAV['Sensex'] = [0]
highestTillNow['Sensex'] = [0]
PNL['Sensex'] = [0]
histo['Sensex'] = [0]
buy = 0
shares_bought = 0
amount_to_invest = 0
amount_infused_tillnow = 0
shares_bought_till_now = 0
flag = 0
pnl=0
buy_point = 0
current_nav = 0
shares_issued = 1000
for i in range(1,len(data)):
    if(i<90):
        Amount_infused['Sensex'].append(0)
        Commulative_Amount_Infused['Sensex'].append(amount_infused_tillnow)
        ETFs_bought['Sensex'].append(0)
        PNL['Sensex'].append(pnl)
        histo['Sensex'].append(0)
        #Current_Investment_Value['Sensex'].append(data['close'][i]*shares_bought_till_now)
        #NAV['Sensex'].append(current_nav)
        continue
    
    df = data[:i]
    new_df = df[-90:]
    
    if(len(df)<1):
        continue
    
    ema_10 = new_df['close'].ewm(span=10,min_periods=10).mean()
    ema_50 = new_df['close'].ewm(span=50,min_periods=50).mean()
    ema_slope = slope(ema_50, 5)

    if  ema_10[len(ema_10)-2]< ema_50[len(ema_50)-2] and ema_10[len(ema_10)-1]> ema_50[len(ema_50)-1] and flag==0:
        flag = 1
        amount_to_invest=1000000
        shares_bought=int(amount_to_invest/data['close'][i])
        if(shares_bought>0):
            #shares_bought_till_now+=shares_bought
            #amount_infused_tillnow+=amount_to_invest
            #if(current_nav!=0):
                #shares_issued += int(amount_to_invest/current_nav)
            #current_nav = round((data['close'][i]*shares_bought_till_now)/shares_issued, 4)
            Amount_infused['Sensex'].append(amount_to_invest)
            #Commulative_Amount_Infused['Sensex'].append(amount_infused_tillnow)
            ETFs_bought['Sensex'].append(shares_bought)
            PNL['Sensex'].append(pnl)
            #Current_Investment_Value['Sensex'].append(data['close'][i]*shares_bought_till_now)
            #NAV['Sensex'].append(current_nav)
        else:
            Amount_infused['Sensex'].append(0)
            #Commulative_Amount_Infused['Sensex'].append(amount_infused_tillnow)
            ETFs_bought['Sensex'].append(0)
            PNL['Sensex'].append(pnl)
            #Current_Investment_Value['Sensex'].append(data['close'][i]*shares_bought_till_now)
            #NAV['Sensex'].append(current_nav)
    elif ema_slope[len(ema_slope)-1]<0 and flag==1:
        flag=0
        pnl+=(data['close'][i]*shares_bought - amount_to_invest)
        Amount_infused['Sensex'].append(0)
        #Commulative_Amount_Infused['Sensex'].append(amount_infused_tillnow)
        ETFs_bought['Sensex'].append(0-shares_bought)
        PNL['Sensex'].append(pnl)
        #Current_Investment_Value['Sensex'].append(data['close'][i]*shares_bought_till_now)
        #NAV['Sensex'].append(current_nav)
        
    else:
        Amount_infused['Sensex'].append(0)
        #Commulative_Amount_Infused['Sensex'].append(amount_infused_tillnow)
        ETFs_bought['Sensex'].append(0)
        PNL['Sensex'].append(pnl)

data['Amount_infused']=np.array(Amount_infused['Sensex'])
data['ETFs_bought']=np.array(ETFs_bought['Sensex'])
#data['Commulative_Amount_Infused']=np.array(Commulative_Amount_Infused['Sensex'])
data['PNL'] = np.array(PNL['Sensex'])
#data['Current_Investment_Value']=np.array(Current_Investment_Value['Sensex'])
data.sort_index(inplace=True,ascending=False)
data.to_csv('Nifty50_EMA_Crossover.csv')

        


