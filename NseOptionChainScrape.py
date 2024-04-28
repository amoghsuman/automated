import requests
import json
import pandas as pd
import numpy as np

new_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'

headers = {'User-Agent': '96.0.4664.110'}
page = requests.get(new_url,headers=headers)
dajs = json.loads(page.text)

expiry_dt = '18-Aug-2022'
ce_values = [data['CE'] for data in dajs['records']['data'] if "CE" in data and data['expiryDate'] == expiry_dt]
pe_values = [data['PE'] for data in dajs['records']['data'] if "PE" in data and data['expiryDate'] == expiry_dt]

ce_dt = pd.DataFrame(ce_values).sort_values(['strikePrice'])
pe_dt = pd.DataFrame(pe_values).sort_values(['strikePrice'])

ce_dt = ce_dt[['strikePrice', 'openInterest', 'changeinOpenInterest', 'totalTradedVolume', 'lastPrice']]
pe_dt = pe_dt[['strikePrice', 'openInterest', 'changeinOpenInterest', 'totalTradedVolume', 'lastPrice']]
PCR = pe_dt['openInterest']/ce_dt['openInterest']
ce_dt['PCR'] = np.array(PCR)
pe_dt['PCR'] = np.array(PCR)
maxPCR = 0 
putToWrite = ''
callToWrite = ''
for i in range(25,len(ce_dt)):
    if(ce_dt['PCR'][i] != 'inf' and ce_dt['PCR'][i] != '' and ce_dt['changeinOpenInterest'][i] < 0):
        if(maxPCR < ce_dt['PCR'][i]):
            maxPCR = ce_dt['PCR'][i]
            if(ce_dt['changeinOpenInterest'][i-1] < 0 and ce_dt['changeinOpenInterest'][i-2] < 0 and ce_dt['changeinOpenInterest'][i-3] < 0):
                putToWrite = ce_dt['strikePrice'][i]
            
            
minPCR = 10000
for i in range(25,len(pe_dt)):
    if(pe_dt['PCR'][i] != 'inf' and pe_dt['PCR'][i] != '' and pe_dt['changeinOpenInterest'][i] < 0):
        if(minPCR > pe_dt['PCR'][i]):
            minPCR = pe_dt['PCR'][i]
            if(pe_dt['changeinOpenInterest'][i-1] < 0 and pe_dt['changeinOpenInterest'][i-2] < 0 and pe_dt['changeinOpenInterest'][i-3] < 0):
                callToWrite = pe_dt['strikePrice'][i]
        
df = pd.DataFrame()
rec = []
rec.append("BankNifty PE : " + str(putToWrite))
rec.append("BankNifty CE : " + str(callToWrite))
            
new_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'

headers = {'User-Agent': '96.0.4664.110'}
page = requests.get(new_url,headers=headers)
dajs = json.loads(page.text)

expiry_dt = '18-Aug-2022'
ce_values = [data['CE'] for data in dajs['records']['data'] if "CE" in data and data['expiryDate'] == expiry_dt]
pe_values = [data['PE'] for data in dajs['records']['data'] if "PE" in data and data['expiryDate'] == expiry_dt]

ce_dt = pd.DataFrame(ce_values).sort_values(['strikePrice'])
pe_dt = pd.DataFrame(pe_values).sort_values(['strikePrice'])

ce_dt = ce_dt[['strikePrice', 'openInterest', 'changeinOpenInterest', 'totalTradedVolume', 'lastPrice']]
pe_dt = pe_dt[['strikePrice', 'openInterest', 'changeinOpenInterest', 'totalTradedVolume', 'lastPrice']]
PCR = pe_dt['openInterest']/ce_dt['openInterest']
ce_dt['PCR'] = np.array(PCR)
pe_dt['PCR'] = np.array(PCR)
maxPCR = 0 
putToWrite = ''
callToWrite = ''
for i in range(25,len(ce_dt)):
    if(ce_dt['PCR'][i] != 'inf' and ce_dt['PCR'][i] != '' and ce_dt['changeinOpenInterest'][i] < 0):
        if(maxPCR < ce_dt['PCR'][i]):
            maxPCR = ce_dt['PCR'][i]
            if(ce_dt['changeinOpenInterest'][i-1] < 0 and ce_dt['changeinOpenInterest'][i-2] < 0 and ce_dt['changeinOpenInterest'][i-3] < 0):
                putToWrite = ce_dt['strikePrice'][i]
            
            
minPCR = 10000
for i in range(25,len(pe_dt)):
    if(pe_dt['PCR'][i] != 'inf' and pe_dt['PCR'][i] != '' and pe_dt['changeinOpenInterest'][i] < 0):
        if(minPCR > pe_dt['PCR'][i]):
            minPCR = pe_dt['PCR'][i]
            if(pe_dt['changeinOpenInterest'][i-1] < 0 and pe_dt['changeinOpenInterest'][i-2] < 0 and pe_dt['changeinOpenInterest'][i-3] < 0):
                callToWrite = pe_dt['strikePrice'][i]
        
        
rec.append("Nifty PE : " + str(putToWrite))
rec.append("Nifty CE : " + str(callToWrite))
df["Rec"] = np.array(rec)
df.to_csv("Recommendation.csv", index = False)
ce_dt.to_csv("Call.csv", index = False)
pe_dt.to_csv("Put.csv", index = False)


