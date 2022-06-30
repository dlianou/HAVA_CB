# Import libraries
import requests
import json
import pandas as pd
import numpy as np
import math
from pandas.io.json import json_normalize
import requests_cache
requests_cache.install_cache()
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
import pymysql
import time
import sys
from IPython.core.display import clear_output
from datetime import date, timedelta, datetime
import configparser

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
api_url = 'https://api.clickbank.com/rest/1.3/tickets/list'
headers = {'Accept': 'application/json',
           'Authorization':'DEV-1GJR9CHL870QS9809IGKVINKQMQAQ521:API-IQSHPRF242L5L7JU0JSJ1RPKJ248AB0I'}

def headers_api(page):
    headers = {'Accept': 'application/json',
           'Authorization':'DEV-1GJR9CHL870QS9809IGKVINKQMQAQ521:API-IQSHPRF242L5L7JU0JSJ1RPKJ248AB0I',
           'page':'page'}
    return headers

headers = {
        'Accept': 'application/json',
        'Authorization': APIcred["password"],
        'page': '1'
         }

payload = {
    'createDateFrom':'2022-01-01',
    'createDateTo':'2022-01-05',
}
response = requests.get(api_url,headers=headers, params=payload)


def clickbank_api(headers, payload):
    # define headers and url
    url = 'https://api.clickbank.com/rest/1.3/tickets/list'

    response = requests.get(url, headers=headers, params=payload)
    return response

r=clickbank_api(headers, payload)

r.status_code

## All together

# Initial Counter
responses = []
page=1
total_pages=50
while page <= total_pages:
    headers = {
        'Accept': 'application/json',
        'Authorization': APIcred["password"],
        'page': str(page)
    }

    payload = {
        'createDateFrom': '2022-01-01',
        'createDateTo': '2022-01-05',
    }

    # print some output so we can see the status
    print("Requesting page{}/{}".format(page, total_pages))
    # clear the output to make things neater
    clear_output(wait=True)

    # make the API call
    response = clickbank_api(headers, payload)

    # if we get an error, print the response and stop the loop
    if response.status_code != 200:
        print(response.text)
        break

    # Append the results(responses) to a list
    responses.append(response.json()['ticketData'])

    # increment the page number
    page += 1

# Convert to data frame
df = [pd.DataFrame(responses[r]) for r in range(0, int(len(responses)))]

# Then use pandas.concat() fxn to turn the list into a single dataframe
test_trx = pd.concat(df)
test_trx.sort_values(by=['ticketid'], inplace=True)

print(test_trx.shape)

# Find duplicates
#test_trx.loc[test_trx['ticketid']=='21367072']

# remove duplicates
new_df = test_trx.drop_duplicates(subset = ['ticketid'])
print(new_df.shape)

tmp_df = test_trx['ticketid']
tmp_df_unique = tmp_df.drop_duplicates()
print(tmp_df_unique.shape)


## Test 1

# Initial Counter
responses = []
page=1
total_pages=10
while page <= total_pages:
    headers = {
        'Accept': 'application/json',
        'Authorization': APIcred["password"],
        'page': str(page)
    }

    payload = {
        'createDateFrom': '2022-01-01',
        'createDateTo': '2022-01-04',
    }

    # print some output so we can see the status
    print("Requesting page{}/{}".format(page, total_pages))
    # clear the output to make things neater
    clear_output(wait=True)

    # make the API call
    response = clickbank_api(headers, payload)

    # if we get an error, print the response and stop the loop
    # if response.status_code == 206:
    #     page += 1
    #     # Append the results(responses) to a list
    #     responses.append(response.json()['ticketData'])
    #     print(response.text)
    # elif response.status_code == 200:
    #     responses.append(response.json()['ticketData'])

    total_results = len(response.json()['ticketData'])
    total_pages = int(round(total_results/100,1))

    responses.append(response.json()['ticketData'])


    page += 1

    # increment the page number

# Convert to data frame
df = [pd.DataFrame(responses[r]) for r in range(0, int(len(responses)))]

# Then use pandas.concat() fxn to turn the list into a single dataframe
test_trx = pd.concat(df)
test_trx.sort_values(by=['ticketid'], inplace=True)

print(test_trx.shape)