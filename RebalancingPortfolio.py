from kiteconnect import KiteConnect
import os
import pandas as pd
import gspread
from kiteconnect import KiteConnect
from oauth2client.service_account import ServiceAccountCredentials



cwd = os.chdir("C:/Users/HP/Desktop/Zerodha_API_Testing")

#generate trading session
access_token = open("access_token.txt",'r').read()
key_secret = open("api_key.txt",'r').read().split()
kite = KiteConnect(api_key=key_secret[0])
kite.set_access_token(access_token)


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

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('csvtogs.json', scope)
client = gspread.authorize(credentials)

spreadsheet = client.open('RebalancingPortfolio')
sheet = spreadsheet.worksheet('Sheet1')
dataframe = pd.DataFrame(sheet.get_all_records())

initialAmount = 30000

for i in range(0, len(dataframe)):
    sym = dataframe['Symbol'][i]
    ltp = kite.ltp("NSE:"+sym)
    sharesToBuy = int((initialAmount * dataframe['Weightage'][i])/ltp)
    placeMarketOrder(sym, "buy", sharesToBuy)
