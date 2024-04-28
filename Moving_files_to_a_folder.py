# -*- coding: utf-8 -*-
import pygsheets
import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

#authorize the pygsheets
gc = pygsheets.authorize()

filename=dt.datetime.today().strftime("%d %b %Y")
#open the spreadsheet
sh = gc.open(filename)

# get the worksheet and its id    
name=sh.id

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) 

drive = build('drive', 'v3', credentials=creds)
folderId = 'pmjU9qBZU8C7xTtP9lBKcHi_6L50V'
drive.files().update(fileId=name, addParents=folderId, removeParents='root').execute()
