import datetime as dt
import pandas as pd
from kiteconnect import KiteConnect
import os
import numpy as np


cwd = os.chdir("C:/Users/HP/Desktop/Zerodha_API_Testing")

#generate trading session
access_token = open("access_token.txt",'r').read()
key_secret = open("api_key.txt",'r').read().split()
kite = KiteConnect(api_key=key_secret[0])
kite.set_access_token(access_token)


#get dump of all NSE instruments
instrument_dump = kite.instruments("NSE")
instrument_df = pd.DataFrame(instrument_dump)
instrument_df.to_csv("NSE_Instruments_31122019.csv",index=False)


def instrumentLookup(instrument_df,symbol):
    """Looks up instrument token for a given script from instrument dump"""
    try:
        return instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]
    except:
        return -1


def fetchOHLC(ticker,interval,duration):
    """extracts historical data and outputs in the form of dataframe"""
    instrument = instrumentLookup(instrument_df,ticker)
    if instrument == -1:
        return -1
    data = pd.DataFrame(kite.historical_data(instrument,dt.date.today()-dt.timedelta(duration), dt.date.today(),interval))
    data.set_index("date",inplace=True)
    return data

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

def placeMarketOrder(symbol,buy_sell,quantity):    
    # Place an intraday market order on NSE
    if buy_sell == "buy":
        t_type=kite.TRANSACTION_TYPE_BUY
    elif buy_sell == "sell":
        t_type=kite.TRANSACTION_TYPE_SELL
    kite.place_order(tradingsymbol=symbol,
                    exchange=kite.EXCHANGE_NSE,
                    transaction_type=t_type,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_MIS,
                    variety=kite.VARIETY_REGULAR)

Nifty500list=pd.read_csv('NiftyNext1000.csv',usecols= ['Symbol'])
new_df=pd.DataFrame()
Company={}
Company["ticker"]=[]

for i in range(0,251):
    sym=Nifty500list['Symbol'][i]
    ohlc=fetchOHLC(sym,"60minute",15)
    if  type(ohlc) == int:
        continue
    
    df = MACD(ohlc, 12, 26, 9) 
    l=len(df)
    ltp = kite.ltp("NSE:"+sym)
    holdings = kite.holdings()
    
    if(len(df)>1 and df["MACD"][l-2]<0 and df["MACD"][l-1]>=0 and ltp["NSE:"+sym]["last_price"]<2000):
        #placeMarketOrder(sym, "buy", 1)
        Company["ticker"].append(sym)
        
    #for j in range (0,len(holdings)):
        #if(holdings[j]["tradingsymbol"]==sym and len(df)>1 and (df["MACD"][len(df)-2] > df["Signal"][len(df)-2]) and (df["MACD"][len(df)-1] < df["Signal"][len(df)-1])):
            #placeMarketOrder(sym, "sell", holdings[j]["quantity"])
            
            
new_df["Company"]=np.array(Company["ticker"])






    

        
    

        
    
    
    
    
        




