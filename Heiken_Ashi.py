# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 17:44:12 2022

@author: HP
"""
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
instrument_dump = kite.instruments("NFO")
instrument_df = pd.DataFrame(instrument_dump)
ltp = kite.ltp("NSE:NIFTY BANK")
fut_df = instrument_df[instrument_df["segment"]=="NFO-OPT"]
fut_df = fut_df[fut_df["name"]=="BANKNIFTY"]
fut_df = fut_df[fut_df["strike"]>ltp["NSE:NIFTY BANK"]["last_price"] -1500]
fut_df = fut_df[fut_df["strike"]<ltp["NSE:NIFTY BANK"]["last_price"] + 1500]
fut_df.sort_values(by=['expiry'], inplace=True, ascending=True)
nearest_expiry = fut_df["expiry"].unique()[1]
fut_df = fut_df[fut_df["expiry"] == nearest_expiry]
fut_df.reset_index(drop=True, inplace=True)
fut_df.sort_values(by=['strike'], inplace=True, ascending=True)


def doji(ohlc_df):    
    """returns dataframe with doji candle column"""
    df = ohlc_df.copy()
    avg_candle_size = abs(df["close"] - df["open"]).median()
    df["doji"] = abs(df["close"] - df["open"]) <=  (0.05 * avg_candle_size)
    return df

def maru_bozu(ohlc_df):    
    """returns dataframe with maru bozu candle column"""
    df = ohlc_df.copy()
    l=len(ohlc_df)
    avg_candle_size = abs(df["close"] - df["open"]).median()
    df["h-c"] = df["high"][l-1]-df["close"][l-1]
    df["l-o"] = df["low"][l-1]-df["open"][l-1]
    df["h-o"] = df["high"][l-1]-df["open"][l-1]
    df["l-c"] = df["low"][l-1]-df["close"][l-1]
    isDeciciveCandle = (df["close"][l-1] - df["open"][l-1] > 2*avg_candle_size) and (df["open"][l-1] - df["low"][l-1] < 0.005*avg_candle_size) 
    return isDeciciveCandle


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


def quote(ticker):
    instrument = instrumentLookup(instrument_df,ticker)
    data = pd.DataFrame(kite.quote(instrument))
    return data

new_df=pd.DataFrame()
Company={}
OI={}
put_call_ratio={}
macd = {}
deciciveCandle={}
MACD_slope = {}
closeAboveEmas={}
signal={}
bought={}
buy_point={}
OI["ticker"]=[]
deciciveCandle["ticker"]=[]
Company["ticker"]=[]
put_call_ratio["ticker"]=[]
macd["ticker"]=[]
MACD_slope["ticker"]=[]
closeAboveEmas["ticker"]=[]
signal["ticker"] = []
bought['ticker']=[]
buy_point['ticker']=[]
isMACDgrtThanZero = False;
isMACDSlopeGrtThanZero = False;
isDecicive = False;
heikenAshiClosingAboveEMA = False;

bought_csv = pd.read_csv('Bought.csv')

for i in range(0,len(fut_df)):
    isMACDgrtThanZero = False;
    isMACDSlopeGrtThanZero = False;
    isDecicive = False;
    heikenAshiClosingAboveEMA = False;
    sym=fut_df['tradingsymbol'][i]
    data = quote(sym)
    time.sleep(random.random())
    ohlc=fetchOHLC(sym,"30minute",12)
    if type(ohlc)==int or len(ohlc)<1:
        continue
    df = MACD(ohlc, 12, 26, 9) 
    if(len(df)<1):
        continue
    macd_slope = slope(df["MACD"], 5)
    Company["ticker"].append(sym[-7:])
    OI["ticker"].append(data.values[10])
    if(df["MACD"][len(df)-1]>0):
        macd["ticker"].append("Yes")
        isMACDgrtThanZero = True
    else:
        macd["ticker"].append("No")
    
    if(macd_slope[len(macd_slope)-1]>0):
        MACD_slope["ticker"].append("Yes")
        isMACDSlopeGrtThanZero = True;
    else:
        MACD_slope["ticker"].append("No")
        
    exponential_moving_avg_10_period = ohlc['close'].ewm(span=10,min_periods=10).mean()
    exponential_moving_avg_30_period = ohlc['close'].ewm(span=30,min_periods=30).mean()
    length=len(exponential_moving_avg_10_period)
        
    rel_df = ohlc
    #assigning existing columns to new variable HAdf
    HAdf = rel_df[['open', 'high', 'low', 'close']]

    HAdf['close'] = round(((rel_df['open'] + rel_df['high'] + rel_df['low'] + rel_df['close'])/4),2)
    #round function to limit results to 2 decimal places

    for i in range(len(rel_df)):
        if i == 0:
            HAdf.iat[0,0] = round(((rel_df['open'].iloc[0] + rel_df['close'].iloc[0])/2),2)
        else:
            HAdf.iat[i,0] = round(((HAdf.iat[i-1,0] + HAdf.iat[i-1,3])/2),2)

    HAdf['high'] = HAdf.loc[:,['open', 'close']].join(rel_df['high']).max(axis=1)
    HAdf['low'] = HAdf.loc[:,['open', 'close']].join(rel_df['low']).min(axis=1)
    if(maru_bozu(HAdf)):
        deciciveCandle["ticker"].append("Yes")
        isDecicive = True
    else:
        deciciveCandle["ticker"].append("No")
    
    if(HAdf['close'][len(HAdf)-1]>exponential_moving_avg_10_period[length-1] and HAdf['close'][len(HAdf)-1]>exponential_moving_avg_30_period[length-1]):
        closeAboveEmas["ticker"].append("Yes")
        heikenAshiClosingAboveEMA = True
    else:
        closeAboveEmas["ticker"].append("No")
    if(isMACDSlopeGrtThanZero and isDecicive and heikenAshiClosingAboveEMA):
        signal["ticker"].append("BUY")
        bought['ticker'].append(sym)
        buy_point['ticker'].append(ohlc['close'][len(ohlc)-1])
    else:
        signal["ticker"].append("")
        
if(len(bought_csv)==0):
    buy=pd.DataFrame()
    buy['Symbol'] = np.array(bought['ticker']) 
    buy['BuyPrice'] = np.array(buy_point['ticker'])
    buy.to_csv('Bought.csv')
        
    
new_df["Symbol"] = np.array(Company["ticker"])
new_df["OI"] = np.array(OI["ticker"])
new_df["MACD > 0"] = np.array(macd["ticker"])
new_df["MACD_slope > 0"] = np.array(MACD_slope["ticker"])
new_df["Is Decicive Candle"] = np.array(deciciveCandle["ticker"])
new_df["Heiken Ashi Closing Above EMAs"] = np.array(closeAboveEmas["ticker"])
new_df["Signal"] = np.array(signal["ticker"])
new_df.sort_values(by=['Symbol'], inplace=True, ascending=False)
new_df.reset_index(drop=True, inplace=True)

j=0
while j<(len(new_df)-1):
    if(new_df['Symbol'][j][:-2] == new_df['Symbol'][j+1][:-2]):
        put_call = new_df["OI"][j]/new_df["OI"][j+1]
        put_call_ratio["ticker"].append(round(put_call,4))
        put_call_ratio["ticker"].append(round(put_call,4))
        j+=2
    elif j == len(new_df)-2:
        put_call_ratio["ticker"].append("")
        put_call_ratio["ticker"].append("")
        j+=2
        
if j == len(new_df)-1:
    put_call_ratio["ticker"].append("")
    
new_df["PUT_CALL Ratio"] = np.array(put_call_ratio["ticker"])
new_df['Call/Put'] = new_df['Symbol'].astype(str).apply(lambda x: x[-2:])
new_df['Strike'] = new_df['Symbol'].astype(str).apply(lambda x: x[:-2])
new_df['Strike'] = pd.to_numeric(new_df['Strike'])
df_ce = new_df[new_df["Call/Put"] == "CE"]
df_ce = df_ce[df_ce["Strike"]>ltp["NSE:NIFTY BANK"]["last_price"] -500]
df_ce = df_ce[df_ce["Strike"]<ltp["NSE:NIFTY BANK"]["last_price"] +2000]
df_pe = new_df[new_df["Call/Put"] == "PE"]
df_pe = df_pe[df_pe["Strike"]<ltp["NSE:NIFTY BANK"]["last_price"] +500]
df_pe = df_pe[df_pe["Strike"]>ltp["NSE:NIFTY BANK"]["last_price"] -2000]
new_df = pd.concat([df_ce, df_pe], axis=0)
new_df.sort_values(by=['OI'], inplace=True, ascending=False)
new_df.drop('Call/Put', axis=1, inplace=True)
new_df.drop('Strike', axis=1, inplace=True)
new_df.to_csv("BankNifty_Buy.csv", index=False)

    
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('csvtogs.json', scope)
client = gspread.authorize(credentials)

spreadsheet = client.open('BankNifty_Buy')

with open('BankNifty_Buy.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(spreadsheet.id, data=content)





