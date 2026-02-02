## install
#pip install python-dotenv
#pip install psycopg2-binary
#pip install clickhouse-connect
#pip install --upgrade certifi
#pip install --upgrade requests certifi urllib3 ## for post data in hmis production certificate issue

import urllib3 ## for disable warning of Certificate
urllib3.disable_warnings() ## for disable warning of Certificate

import ssl
#import requests

from concurrent.futures import ThreadPoolExecutor
import requests
import certifi  ## for post data in hmis production certificate issue
import json
from datetime import datetime,date
import nepali_datetime
# main.py
from dotenv import load_dotenv
import os

load_dotenv()

from utils import (
    configure_logging,
    log_info,log_error,get_aggregated_data_values,
    push_dataValueSet_in_dhis2,
    get_bs_month_start_end, get_between_dates_iso, sendEmail
   
)

#print("OpenSSL version:", ssl.OPENSSL_VERSION)
#print("Certifi CA bundle:", requests.certs.where())

DHIS2_GET_API_URL = os.getenv("DHIS2_GET_API_URL")
DHIS2_GET_USER = os.getenv("DHIS2_GET_USER")
DHIS2_GET_PASSWORD = os.getenv("DHIS2_GET_PASSWORD")

DHIS2_POST_API_URL = os.getenv("DHIS2_POST_API_URL")
DHIS2_POST_USER = os.getenv("DHIS2_POST_USER")
DHIS2_POST_PASSWORD = os.getenv("DHIS2_POST_PASSWORD")

DENGUE_AGGREGATE_DE = os.getenv("DENGUE_AGGREGATE_DE")
CLIMET_DENGUE_AGGREGATE_DE = os.getenv("CLIMET_DENGUE_AGGREGATE_DE")
DEFAULT_CATEGORY_OPTION_COMBO = os.getenv("DEFAULT_CATEGORY_OPTION_COMBO")
ORG_UNIT_LEVEL = os.getenv("ORG_UNIT_LEVEL")

aggregated_data_value_url = f"{DHIS2_GET_API_URL}analytics.json"

dataValueSet_endPoint = f"{DHIS2_POST_API_URL}dataValueSets" 

#print( f" DHIS2_GET_USER. { DHIS2_GET_USER }, DHIS2_GET_PASSWORD  { DHIS2_GET_PASSWORD} " )

#DHIS2_AUTH_POST = ("hispdev", "Devhisp@1")
#session_post = requests.Session()
#session_post.auth = DHIS2_AUTH_POST

# Create a session object for persistent connection
#session_get = requests.Session()
#session_get.auth = DHIS2_AUTH_GET

raw_auth = os.getenv("DHIS2_AUTH")

if raw_auth is None:
    raise ValueError("DHIS2_AUTH is missing in .env")

if ":" not in raw_auth:
    raise ValueError("DHIS2_AUTH must be in user:password format")

user, pwd = raw_auth.split(":", 1)
#session_get.auth = (user, pwd)
'''
session_get = requests.Session()
session_get.auth = (DHIS2_GET_USER, DHIS2_GET_PASSWORD)

session_post = requests.Session()
session_post.auth = (DHIS2_POST_USER, DHIS2_POST_PASSWORD)
'''

#session_get.verify = False


def main_with_logger():

    configure_logging()

    current_time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print( f"pushing to Climent Aggregated Data Value process start . { current_time_start }" )
    log_info(f"pushing to Climent Aggregated Data Value process start  . { current_time_start }")

    session_get = requests.Session()
    session_get.auth = (DHIS2_GET_USER, DHIS2_GET_PASSWORD)

    session_post = requests.Session()
    session_post.auth = (DHIS2_POST_USER, DHIS2_POST_PASSWORD)

    #session = requests.Session()
    #session_post.verify = certifi.where()

    ## current nepali date/month/period
    # Get the current Nepali date and time
    current_nepali_datetime = nepali_datetime.datetime.now()
    # Extract the month number
    nepali_current_month_number = current_nepali_datetime.month

    # Extract the Nepali Year
    nepali_current_year = current_nepali_datetime.year

    # Get the month name (optional, if you need the name instead of the number)
    nepali_current_month_name = current_nepali_datetime.strftime("%B")

    #print(certifi.where()) 

    print(f"Current Nepali Year: {nepali_current_year}")
    print(f"Current Nepali month number: {nepali_current_month_number}")
    print(f"Current Nepali month name: {nepali_current_month_name}")

    # Example: current nelai month to month startdate and enddate
    start, end = get_bs_month_start_end(nepali_current_year, nepali_current_month_number)
    #start, end = get_bs_month_start_end(2082, 1)

    print("Start Current BS:", start)
    print("End Current BS:", end)
    print("Start Current AD:", start.to_datetime_date())
    print("End Current AD:", end.to_datetime_date())

    current_nepali_monthly_period = start.strftime("%Y-%m-%d").split("-")[0] + "" + start.strftime("%Y-%m-%d").split("-")[1]
    
    print(f"current_nepali_monthly_period {current_nepali_monthly_period}")
    log_info(f"current_nepali_monthly_period {current_nepali_monthly_period}")

    current_iso_monthly_period = start.to_datetime_date().strftime("%Y-%m-%d").split("-")[0] + "" + start.to_datetime_date().strftime("%Y-%m-%d").split("-")[1]
    
    print(f"current_iso_monthly_period {current_iso_monthly_period}")
    log_info(f"current_iso_monthly_period {current_iso_monthly_period}")

    # Convert date objects to string
    #Previous month calculation (IMPORTANT PART)
    
    if nepali_current_month_number == 1:
        prev_nepali_year = nepali_current_year - 1
        prev_nepali_month_number = 12
    else:
        prev_nepali_year = nepali_current_year
        prev_nepali_month_number = nepali_current_month_number - 1

    # Create a date in previous month to extract name
    #Previous Nepali month name
    prev_nepali_date = nepali_datetime.date(
        prev_nepali_year,
        prev_nepali_month_number,
        1
    )

    prev_nepali_month_name = prev_nepali_date.strftime("%B")

    print(f"Previous Nepali Year: {prev_nepali_year}")
    print(f"Previous Nepali month number: {prev_nepali_month_number}")
    print(f"Previous Nepali month name: {prev_nepali_month_name}")

    #Previous Nepali month name
    #Previous month start & end date (BS + AD)
    previous_start, previous_end = get_bs_month_start_end(
        prev_nepali_year,
        prev_nepali_month_number
    )

    print("Previous Month Start BS:", previous_start)
    print("Previous Month End BS:", previous_end)
    print("Previous Month Start AD:", previous_start.to_datetime_date())
    print("Previous Month End AD:", previous_end.to_datetime_date())


    '''
    previousIsoDatePeriods = get_between_dates_iso(previous_start.to_datetime_date(), previous_end.to_datetime_date())
    print(" previous dates:" ,previousIsoDatePeriods)
    print(f"previousIsoDatePeriods {len(previousIsoDatePeriods)}")
    log_info(f"previousIsoDatePeriods {len(previousIsoDatePeriods)}")

    '''

    previous_nepali_monthly_period = previous_start.strftime("%Y-%m-%d").split("-")[0] + "" + previous_start.strftime("%Y-%m-%d").split("-")[1]
    print(f"previous_nepali_monthly_period {previous_nepali_monthly_period}")
    log_info(f"previous_nepali_monthly_period {previous_nepali_monthly_period}")

    previous_iso_monthly_period = previous_start.to_datetime_date().strftime("%Y-%m-%d").split("-")[0] + "" + previous_start.to_datetime_date().strftime("%Y-%m-%d").split("-")[1]
    print(f"previous_iso_monthly_period {previous_iso_monthly_period}")
    log_info(f"previous_iso_monthly_period {previous_iso_monthly_period}")

    # get all dates between startdate,enddate
    #dates = get_between_dates("2023-01-28", "2023-02-03")

    #isoDatePeriods = get_between_dates_iso(start.to_datetime_date(), end.to_datetime_date())
    #print(f"isoDatePeriods {len(isoDatePeriods)}")
    #log_info(f"isoDatePeriods {len(isoDatePeriods)}")

    aggregated_data_values = get_aggregated_data_values( aggregated_data_value_url, session_get, previous_nepali_monthly_period, DENGUE_AGGREGATE_DE, ORG_UNIT_LEVEL )
    print(f"aggregated_data_values  DataValueSize {len(aggregated_data_values) }")
    log_info(f"aggregated_data_values DataValueSize {len(aggregated_data_values) } ")

    tempDataValues = list()
    dataValueSet_payload = {}
    if aggregated_data_values:
        for agg_dataValue in aggregated_data_values:
            dataValue = {
                "dataElement": CLIMET_DENGUE_AGGREGATE_DE,
                "categoryOptionCombo": DEFAULT_CATEGORY_OPTION_COMBO,
                "value": int(float(agg_dataValue[3])),
                "period": previous_iso_monthly_period,
                "orgUnit": agg_dataValue[1]
            }
            tempDataValues.append(dataValue)

        dataValueSet_payload = {
            "dataValues":tempDataValues
        }
        #print( f"dataValueSet_payload . { dataValueSet_payload }" )
        push_dataValueSet_in_dhis2( dataValueSet_endPoint, session_post, dataValueSet_payload )
    
if __name__ == "__main__":

    #main()
    main_with_logger()
    current_time_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print( f"pushing to Climent Aggregated Data Value process finished . { current_time_end }" )
    log_info(f"pushing to Climent Aggregated Data Value process finished . { current_time_end }")

    try:
        #sendEmail()
        print( f"pushing to Climent Aggregated Data Value process finished . { current_time_end }" )
    except Exception as e:
        log_error(f"Email failed: {e}")


    #sendEmail()
    #print(f"total_patient_count. {total_patient_count}, null_patient_id_count. {null_patient_id_count}, event_push_count {event_push_count}")
    #log_info(f"total_patient_count. {total_patient_count}, null_patient_id_count. {null_patient_id_count}, event_push_count {event_push_count}")
    