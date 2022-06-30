# Import libraries
import requests
import json
import pandas as pd
import configparser
from google.oauth2 import service_account
from datetime import date, timedelta, datetime
import pygsheets
# import requests_cache
# requests_cache.install_cache()
import time

# Read credentials from config.ini file
config_obj = configparser.ConfigParser()
config_obj.read("C:\\Users\\dlian\\PycharmProjects\\HAVA\\ClickBank_HAVA\\configfile.ini")
dbparam = config_obj["mysql"]
APIcred = config_obj["ClickBankAPI"]


# Make json printouts prettier
def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


# Define Funciton to call ClickBank API
def clickbank_api(header, payload):
    # define headers and url
    url = 'https://api.clickbank.com/rest/1.3/orders2/list'
    # url = 'https://api.clickbank.com/rest/1.3/tickets/list'

    response = requests.get(url, headers=header, params=payload)
    return response

# We want to run a cron once a week that fetches chargebacks from the last 7 days


def get_gv_chbks():
    ## Test 5

    responses = []
    page = 1

    Start_Date = date.today() - timedelta(days=9)
    Start_Date = Start_Date.strftime("%Y-%m-%d")
    End_Date = date.today() - timedelta(days=3)
    End_Date = End_Date.strftime("%Y-%m-%d")

    for page in range(1, 9999):
        try:
            header = {
                'Accept': 'application/json',
                'Authorization': APIcred["password"],
                # 'Authorization':'DEV-Q1EH0S89RNJSDHQ9VVOS4M0JFOGQVF96:API-VPADTGRF4HI63IBM79ATHOA9I4G98C0H',
                'page': str(page)
            }

            payload = {
                'startDate': Start_Date,
                'endDate': End_Date,
                 # 'vendor': 'metabofix',
                #'type': 'RFND'
                'type': 'CGBK'
            }

            # make the API call
            response = clickbank_api(header, payload)

                # if we get an error, print the response and stop the loop
                # if response.status_code != 200 or response.status_code != 206:
                #     break
                #     print(response.status_code)

            responses.append(response.json()['orderData'])

            # if it's not a cached result, sleep
            # if not getattr(response, 'from_cache', False):
            #     time.sleep(0.25)

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
    # Convert to datetime to manipulate date
    # tmp_df['transactionTime'] = pd.to_datetime(tmp_df['transactionTime'])
    tmp_df['transactionTime'] = pd.to_datetime(tmp_df['transactionTime'], utc=True)
    # subtract 7 hours to go back to original
    tmp_df['transactionTime'] = tmp_df['transactionTime'] - timedelta(hours=7)
    # Only keep date portion of date
    tmp_df['transactionTime'] = tmp_df['transactionTime'].dt.date
    tmp_df['totalOrderAmount'] = pd.to_numeric(tmp_df['totalOrderAmount'])

    # Keep the necessary columns and forget the rest

    tmp_df = tmp_df.loc[:, ['transactionTime', 'vendor', 'receipt', 'transactionType', 'totalOrderAmount']]
    gs_tmp_df = tmp_df.copy()

    ## Send dataframe to google sheet

    # Authenticating To A Google Cloud Project With A .JSON Service Account Key
    with open('C:\\Users\\dlian\\PycharmProjects\HAVA\\ClickBank_HAVA\\hava_key.json') as source:
        info = json.load(source)
    credentials = service_account.Credentials.from_service_account_info(info)

    # Authenticating With Google Sheets With Pyghseets
    client = pygsheets.authorize(
        service_account_file='C:\\Users\\dlian\\PycharmProjects\\HAVA\\ClickBank_HAVA\\hava_key.json')

    # Connect To a Specific Google sheet
    # spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1eILnMX9zfwxmJ538mYaiM1s6J_aE1ADbRMyBxPuQc_w/edit#gid=0'
    # sheet_data = client.sheet.get('1eILnMX9zfwxmJ538mYaiM1s6J_aE1ADbRMyBxPuQc_w')
    sheet = client.open_by_key('1eILnMX9zfwxmJ538mYaiM1s6J_aE1ADbRMyBxPuQc_w')

    # Select A Specific Google Worksheet
    # wks = sheet.worksheet_by_title('Sheet1')
    wks = sheet.worksheet_by_title('goldvida_chargebacks')
    #print(wks)

    # Upload Data To A Google Sheet From A Pandas DataFrame
    #wks.set_dataframe(gs_tmp_df, start=(1, 1), extend=True)


    # Append data to existing gsheet if sheet is not empty
    #  Convert datetime objects in a dataframe to string before appending to google sheets.
    #  Since we use json for uploading via service account and json doesn’t have built-in type date/time values and it will cause an error.
    gs_tmp_df['transactionTime'] = pd.to_datetime(gs_tmp_df['transactionTime'])
    # convert to string so append method can read it
    gs_tmp_df['transactionTime'] = gs_tmp_df['transactionTime'].astype(str)
    # Append data to gsheet
    wks.append_table(values=gs_tmp_df.values.tolist())




if __name__ == '__main__':
    get_gv_chbks()


