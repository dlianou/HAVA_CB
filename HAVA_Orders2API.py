# Import libraries
import requests
import json
import pandas as pd
import numpy as np
import math
from pandas.io.json import json_normalize
#import requests_cache
#requests_cache.install_cache()
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
import pymysql
import time
import sys
from IPython.core.display import clear_output
from datetime import date, timedelta, datetime
import configparser
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import googleapiclient.discovery
import os
import pygsheets

# Read credentials from config.ini file
config_obj = configparser.ConfigParser()
config_obj.read("C:\\Users\\dlian\\PyCharmProjects\\HAVA\\configfile.ini")
dbparam = config_obj["mysql"]
APIcred = config_obj["ClickBankAPI"]

# Make json printouts prettier
def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

# Fiddle with CB API before writing fxn
api_url = 'https://api.clickbank.com/rest/1.3/orders2/list'
headers = {'Accept': 'application/json',
           'Authorization':'DEV-1GJR9CHL870QS9809IGKVINKQMQAQ521:API-IQSHPRF242L5L7JU0JSJ1RPKJ248AB0I'}


header = {
        'Accept': 'application/json',
        'Authorization': APIcred["password"],
        'page': '1'
         }

payload = {
    'startDate': '2022-03-01',
    'endDate': '2022-03-31',
    #'vendor': 'metabofix',
    'type': 'RFND'
}
#response = requests.get(api_url,headers=headers, params=payload)


def clickbank_api(header, payload):
    # define headers and url
    #url = 'https://api.clickbank.com/rest/1.3/orders2/list'
    url = 'https://api.clickbank.com/rest/1.3/tickets/list'

    response = requests.get(url, headers=header, params=payload)
    return response

r=clickbank_api(header, payload)
r.json()['orderData']

r.status_code


## Test 5

responses = []
page = 1

for page in range(1,9999):

 try:
    header = {
        'Accept': 'application/json',
        'Authorization': APIcred["password"],
        #'Authorization':'DEV-Q1EH0S89RNJSDHQ9VVOS4M0JFOGQVF96:API-VPADTGRF4HI63IBM79ATHOA9I4G98C0H',
        'page': str(page)
    }

    payload = {
        'startDate': '2022-05-01',
        'endDate': '2022-06-19',
        #'vendor': 'metabofix',
        'type' : 'RFND'
        #'type': 'CGBK'
    }

    # print some output so we can see the status
    #print("Requesting page{}/{}".format(page, total_pages))
    # clear the output to make things neater
    #clear_output(wait=True)

    # make the API call
    response = clickbank_api(header, payload)
    #responses = response.json()['orderData']

    # if response.status_code == 206:
    #     responses.append(response.json()['orderData'])
    #     #page = +1
    #     break

    responses.append(response.json()['orderData'])
    #responses = response.json()['orderData']

    page = +1
 except:
     break
# Convert to data frame
df = [pd.DataFrame(responses[r]) for r in range(0, int(len(responses)))]

# Then use pandas.concat() fxn to turn the list into a single dataframe
test_trx = pd.concat(df)

## Manipulate the Data
# Create a working copy
tmp_df = test_trx.copy()

# check data types
tmp_df.dtypes
tmp_df.info()

# Convert object to Series
tmp_df['transactionTime'] = pd.Series(tmp_df['transactionTime'])
tmp_df['transactionTime'] = pd.to_datetime(tmp_df['transactionTime'], utc=True)
#tmp_df['transactionTime'] = tmp_df['transactionTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
tmp_df['transactionTime'] = tmp_df['transactionTime'].dt.strftime('%Y-%m-%d')
#tmp_df['transactionTime'].dt.date
tmp_df['totalOrderAmount'] =pd.to_numeric(tmp_df['totalOrderAmount'])

# Keep the necessary columns and forget the rest
tmp_df = tmp_df.loc[:, ['transactionTime','vendor','receipt','transactionType','totalOrderAmount']]

## Send dataframe to google sheet

#Authenticating To A Google Cloud Project With A .JSON Service Account Key
with open('ClickBank_HAVA/hava_key.json') as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(info)

# Authenticating With Google Sheets With Pyghseets
client = pygsheets.authorize(service_account_file='ClickBank_HAVA/hava_key.json')

# Connect To a Specific Google sheet
#spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1eILnMX9zfwxmJ538mYaiM1s6J_aE1ADbRMyBxPuQc_w/edit#gid=0'
#sheet_data = client.sheet.get('1eILnMX9zfwxmJ538mYaiM1s6J_aE1ADbRMyBxPuQc_w')
sheet = client.open_by_key('1eILnMX9zfwxmJ538mYaiM1s6J_aE1ADbRMyBxPuQc_w')

# Select A Specific Google Worksheet
#wks = sheet.worksheet_by_title('Sheet1')
wks = sheet.worksheet_by_title('goldvida_refunds')
print(wks)

# Upload Data To A Google Sheet From A Pandas DataFrame
wks.set_dataframe(tmp_df, start=(1,1), extend=True)

# Append data to existing gsheet
#wks.append_table(values=values, start='A1', end=None, dimension='ROWS', overwrite=Fal
values = ['transactionTime', 'vendor','receipt','transactionType','totalOrderAmount']
wks.append_table(values=tmp_df.values.tolist())

# Alternative method to send df to gsheets

# from google.oauth2 import service_account
# import googleapiclient.discovery
#
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# SERVICE_ACCOUNT_FILE = 'hava_key.json'
#
# credentials = service_account.Credentials.from_service_account_file(
#         SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# service = build('sheets', 'v4', http=creds.authorize(Http()))
#
# ###
#
# gc = gspread.service_account(filename='C:\\Users\\dlian\\PycharmProjects\\HAVA\\hava_key.json')
#
# sh = gc.open("HAVA_TEST")
# worksheet = sh.sheet1
# worksheet.update([tmp_df.columns.values.tolist()] + tmp_df.values.tolist())


