# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 17:44:12 2022

@author: HP
"""
import pandas as pd
import os
from datetime import datetime
from kiteconnect import KiteConnect
import datetime as dt
import numpy as np
import statsmodels.api as sm


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
ltp = kite.ltp("NSE:NIFTY BANK")
fut_df = instrument_df[instrument_df["segment"]=="NFO-FUT"]





def placeMarketOrder(symbol,buy_sell,quantity):    
    # Place an intraday market order on NSE
    if buy_sell == "buy":
        t_type=kite.TRANSACTION_TYPE_BUY
    elif buy_sell == "sell":
        t_type=kite.TRANSACTION_TYPE_SELL
    kite.place_order(tradingsymbol=symbol,
                    exchange='NFO',
                    transaction_type=t_type,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_CNC,
                    variety=kite.VARIETY_REGULAR)

def maru_bozu(ohlc_df):    
    """returns dataframe with maru bozu candle column"""
    df = ohlc_df.copy()
    l=len(ohlc_df)
    avg_candle_size = abs(df["close"] - df["open"]).median()
    df["h-c"] = df["high"][l-1]-df["close"][l-1]
    df["l-o"] = df["low"][l-1]-df["open"][l-1]
    df["h-o"] = df["high"][l-1]-df["open"][l-1]
    df["l-c"] = df["low"][l-1]-df["close"][l-1]
    isDeciciveCandle = (df["close"][l-1] - df["open"][l-1] > 1.5*avg_candle_size) and (df["close"][l-1] - df["open"][l-1] < 3*avg_candle_size) and (df["open"][l-1] - df["low"][l-1] < 0.005*avg_candle_size) 
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

ohlc = fetchOHLC("NIFTY 50","60minute",30)
new_df=pd.DataFrame()


macd = {}
deciciveCandle={}
MACD_slope = {}
closeAboveEmas={}
closeBelowEmas={}
signal={}
bought={}
PNL={}
date={}
deciciveCandle["ticker"]=[]
macd["ticker"]=[]
MACD_slope["ticker"]=[]
closeAboveEmas["ticker"]=[]
closeBelowEmas['ticker']=[]
signal["ticker"] = []
bought['ticker']=[]
PNL['ticker']=[]
date['ticker']=[]
buy_point =0
pnl=0
flag=0
isMACDgrtThanZero = False;
isMACDSlopeGrtThanZero = False;
isDecicive = False;
heikenAshiClosingAboveEMA = False;
heikenAshiClosingBelowEMA = False;
exp_mov_30SlopePositive = False;



    

df = MACD(ohlc, 12, 26, 9) 
df['date'] = df.index

macd_slope = slope(df["MACD"], 5)
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
    

    
rel_df = ohlc
#assigning existing columns to new variable HAdf
HAdf = rel_df[['open', 'high', 'low', 'close']]

HAdf['close'] = round(((rel_df['open'] + rel_df['high'] + rel_df['low'] + rel_df['close'])/4),2)
#round function to limit results to 2 decimal places
exponential_moving_avg_10_period = HAdf['close'].ewm(span=10,min_periods=10).mean()
exponential_moving_avg_30_period = HAdf['close'].ewm(span=30,min_periods=30).mean()
exp_30Slope = slope(exponential_moving_avg_30_period, 5);
length=len(exponential_moving_avg_10_period)

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

if(HAdf['close'][len(HAdf)-1]>exponential_moving_avg_10_period[length-1] and HAdf['close'][len(HAdf)-1]>exponential_moving_avg_30_period[length-1] and exp_30Slope[len(exp_30Slope)-1]>0):
    closeAboveEmas["ticker"].append("Yes")
    heikenAshiClosingAboveEMA = True
else:
    closeAboveEmas["ticker"].append("No")
    
""" if(HAdf['close'][len(HAdf)-1]<exponential_moving_avg_10_period[length-1] and exp_30Slope[len(exp_30Slope)-1]<0):
    closeBelowEmas["ticker"].append("Yes")
    heikenAshiClosingBelowEMA = True
else:
    closeBelowEmas["ticker"].append("No")    """
    
    
# Fetch position details
positions_df = kite.positions()

positions = pd.DataFrame.from_dict(positions_df['day'])

# Fetch holding details
holdings = kite.holdings()


if(isMACDSlopeGrtThanZero and isDecicive and  heikenAshiClosingAboveEMA and HAdf['close'][len(HAdf)-1] > HAdf['open'][len(HAdf)-1]):
    for i in range(0,len(positions)):
        if(positions['tradingsymbol'][i] == "NIFTY22AUGFUT"):
            flag=1
            break;
    for i in range(0,len(holdings)):
        if(holdings[i]['tradingsymbol'] == "NIFTY22AUGFUT"):
            flag=1
            break;
    if(flag==0):
        placeMarketOrder("NIFTY22AUGFUT","buy",50)
    
    
#elif(flag==1 and df['date'][len(df)-1].strftime("%H:%M:%S").find("15:15:00")!=-1):
    #flag=0
    #pnl=((df['close'][len(df)-1] - buy_point)/buy_point)*100
    #signal["ticker"].append("Sell")
    #PNL['ticker'].append(pnl)
    
    
elif(((HAdf['close'][len(HAdf)-1]<exponential_moving_avg_10_period[length-1]) or macd_slope[len(macd_slope)-1]<0)):
    for i in range(0,len(positions)):
        if(positions['tradingsymbol'][i] == "NIFTY22AUGFUT"):
            flag=1
            break;
    for i in range(0,len(holdings)):
        if(holdings[i]['tradingsymbol'] == "NIFTY22AUGFUT"):
            flag=1
            break;
    if(flag==1):
        placeMarketOrder("NIFTY22AUGFUT","sell",50)  
                
    
    
        

    

    

        


        






