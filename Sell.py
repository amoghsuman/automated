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

instrument_dump = kite.instruments("NFO")
instrument_df = pd.DataFrame(instrument_dump)



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


def instrumentLookup(instrument_df,symbol):
    """Looks up instrument token for a given script from instrument dump"""
    try:
        return instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]
    except:
        return -1





bought_csv = pd.read_csv('Bought.csv')
sold=pd.DataFrame()
sell={}
buy_price={}
sell_price={}
sell['ticker']=[]
buy_price['ticker']=[]
sell_price['ticker']=[]
for i in range(0,len(bought_csv)):
    sym = bought_csv['Symbol'][i]
    ohlc=fetchOHLC(sym,"30minute",12)
    if type(ohlc)==int or len(ohlc)<1:
        continue
    df = MACD(ohlc, 12, 26, 9) 
    if(len(df)<1):
        continue
    macd_slope = slope(df["MACD"], 5)
    if macd_slope[len(macd_slope)-1]<0 :
        sell['ticker'].append(sym)
        buy_price['ticker'].append(bought_csv['BuyPrice'][i])
        sell_price['ticker'].append(ohlc['close'][len(ohlc)-1])
        
sold['Symbol'] = np.array(sell['ticker'])
sold["BuyPrice"] = np.array(buy_price['ticker'])
sold['SellPrice'] = np.array(sell_price['ticker'])
sold.to_csv('Sold.csv')