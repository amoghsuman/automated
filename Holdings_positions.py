# -*- coding: utf-8 -*-
"""
Zerodha Kite Connect Intro

@author: Mayank Rasu (http://rasuquant.com/wp/)
"""
from kiteconnect import KiteConnect
import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


cwd = os.chdir("C:/Users/HP/Desktop/Zerodha_API_Testing")

#generate trading session
access_token = open("access_token.txt",'r').read()
key_secret = open("api_key.txt",'r').read().split()
kite = KiteConnect(api_key=key_secret[0])
kite.set_access_token(access_token)


# Fetch quote details
quote = kite.quote("NSE:INFY")

# Fetch last trading price of an instrument
ltp = kite.ltp("NSE:INFY")

# Fetch order details
orders = kite.orders()

# Fetch position details
positions = kite.positions()

# Fetch holding details
holdings = kite.holdings()
 
positions_df = pd.DataFrame.from_dict(positions)
positions_df.to_csv("Positions.csv", index=False)
holdings_df = pd.DataFrame(holdings)
holdings_df.to_csv("Holdings.csv", index=False)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('csvtogs.json', scope)
client = gspread.authorize(credentials)

spreadsheet = client.open('Holdings')

with open('Holdings.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(spreadsheet.id, data=content)

sh=client.open("Holdings")
sh.add_worksheet(title="Positions", rows="3000", cols="20")
wks = sh.get_worksheet(1)
wks.update([positions_df.columns.values.tolist()] + positions_df.values.tolist())
