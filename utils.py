# utils.py

import requests
import logging

import certifi  ## for post data in hmis production certificate issue


import json
import smtplib
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
from urllib.parse import quote

## for nepali date
import nepali_datetime
from datetime import datetime, timedelta, date

#from datetime import timedelta

from dotenv import load_dotenv
import os
import glob
load_dotenv()

FROM_EMAIL_ADDR = os.getenv("FROM_EMAIL_ADDR")
FROM_EMAIL_PASSWORD = os.getenv("FROM_EMAIL_PASSWORD")

from constants import LOG_FILE
#from app import QueueLogHandler

DHIS2_API_URL = os.getenv("DHIS2_API_URL")


# ADD THIS PART (UI streaming) for print in HTML Page in response
#Add a global log queue
import queue
log_queue = queue.Queue()
#Add a Queue logging handler
#import logging

'''
class QueueLogHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))
'''

import logging
import queue

log_queue = queue.Queue()

class QueueHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))


def configure_logging():

    #Optional (Advanced, but useful)
    '''
    import sys
    sys.stdout.write = lambda msg: logging.info(msg)
    logging.info(f"[job:{job_id}] step 1")
    '''

    LOG_DIR = "logs"
    #os.makedirs(LOG_DIR, exist_ok=True)

    os.makedirs(LOG_DIR, exist_ok=True)
    assert LOG_DIR != "/" and LOG_DIR != "" #### Never delete outside log folder.

    # Create unique log filename
    #log_filename = f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_filename = LOG_FILE
    #log_filename = f"{LOG_FILE}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_path = os.path.join(LOG_DIR, log_filename)

    #logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    '''
    logging.basicConfig(filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            QueueLogHandler()   # 👈 THIS is the key
        ]
    )
    '''
    # ✅ ADD THIS (UI streaming)
    '''
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not any(isinstance(h, QueueLogHandler) for h in root_logger.handlers):
        queue_handler = QueueLogHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        queue_handler.setFormatter(formatter)
        root_logger.addHandler(queue_handler)
    '''

def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

#################################
## for CLIMET DATA EXCHANGE ######


def get_aggregated_data_values( aggregated_data_value_url, session_get, previous_nepali_monthly_period, aggregated_de, org_unit_level ):
    
    #https://hmis.gov.np/hmis/api/analytics.json?dimension=dx:XBZPrqWn6OG&dimension=pe:208208&dimension=ou:LEVEL-4&outputIdScheme=UID
    
    analytics_data_value_url = (
        f"{aggregated_data_value_url}"
        f"?dimension=ou:LEVEL-{org_unit_level}"
        f"&dimension=dx:{aggregated_de}"
        f"&dimension=pe:{previous_nepali_monthly_period}"
        f"&outputIdScheme=UID"
    )
    
    #print(f"analytics_data_value_url: {analytics_data_value_url}")

    response_agg_datavalues = session_get.get( analytics_data_value_url, verify=False )
    if response_agg_datavalues.status_code == 200:
        response_pi_data = response_agg_datavalues.json()
        pi_dataValues = response_pi_data.get('rows', [])
        return pi_dataValues 
    else:
        return []


def push_dataValueSet_in_dhis2( dataValueSet_endPoint, session_post, dataValueSet_payload ):
    #print(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")
    #logging.info(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")

    #session_post = requests.Session()
    #session_post.verify = certifi.where()
    #verify=False,
    response = session_post.post(
        dataValueSet_endPoint,
        data=json.dumps(dataValueSet_payload),
        headers={"Content-Type": "application/json"}
    )
    conflictsDetails = ""
    if response.status_code == 200:
        #print(f"DataValue created successfully.  Row No : {row_no} . orgUnit : {orgUnit} . response . {response.status_code}")
        #print(f"DataValue created successfully.  Row No : {row_no} . orgUnit : {orgUnit} . response . {response.json()}")

        #print(f" DataValue created successfully : response . {response.json()} : response . {response.status_code}")

        #conflictsDetails   = response.json().get("response", {}).get("conflicts")
        description   = response.json().get("response", {}).get("description")
        impCount = response.json().get("response", {}).get("importCount").get("imported")
        updateCount = response.json().get("response", {}).get("importCount").get("updated")
        ignoreCount = response.json().get("response", {}).get("importCount").get("ignored")

        #conflictsDetails   = response.json().get("conflicts",[])
        #description   = response.json().get("description", {})
        #print(f"DataValue created successfully description : {description}")
        #impCount = response.json().get("importCount", {}).get("imported")
        #updateCount = response.json().get("importCount", {}).get("updated")
        #ignoreCount = response.json().get("importCount", {}).get("ignored")

        print(f"DataValue created successfully. importCount : {impCount}. updateCount : {updateCount}. ignoreCount : {ignoreCount}. description : {description}")
        logging.info(f"DataValue created successfully. importCount : {impCount}. updateCount : {updateCount}. ignoreCount: {ignoreCount}. description : {description}")
        #logging.info(f"conflictsDetails : {conflictsDetails}")
        #print(f"conflictsDetails : {conflictsDetails}")
        #logging.info(f"DataValue created successfully : {response.text}")
        #print(f"DataValue created successfully : {response.text}")
    else:
        #print(f"Failed to create dataValueSet. Error: {response.text}")
        conflictsDetails   = response.json().get("response", {}).get("conflicts")
        description   = response.json().get("response", {}).get("description")
        impCount = response.json().get("response", {}).get("importCount").get("imported")
        updateCount = response.json().get("response", {}).get("importCount").get("updated")
        ignoreCount = response.json().get("response", {}).get("importCount").get("ignored")
        
        print(f"DataValue created successfully. impCount : {impCount}. updateCount : {updateCount}. ignoreCount : {ignoreCount}. description : {description}")
        logging.info(f"DataValue created successfully. impCount : {impCount}. updateCount : {updateCount}. ignoreCount: {ignoreCount}. description : {description}")
        
        print(f"Failed to create dataValueSet. conflictsDetails: {conflictsDetails}")
        logging.info(f"conflictsDetails : {conflictsDetails}")
        logging.error(f"Failed to dataValueSet events . conflictsDetails : {conflictsDetails} . error details: {response.json()} .Error: {response.text}")


def get_bs_month_start_end(bs_year, bs_month):
    # Start of Nepali month
    start_date = nepali_datetime.date(bs_year, bs_month, 1)

    # Start of next month
    if bs_month == 12:
        next_month = nepali_datetime.date(bs_year + 1, 1, 1)
    else:
        next_month = nepali_datetime.date(bs_year, bs_month + 1, 1)

    # End of month
    end_date = next_month - timedelta(days=1)

    return start_date, end_date

def get_between_dates_iso(start_date, end_date):
    """
    start_date, end_date format: YYYY-MM-DD
    returns list of dates in YYYYMMDD format
    """

    # Convert date objects to string
    if isinstance(start_date, date):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, date):
        end_date = end_date.strftime("%Y-%m-%d")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    arr = []
    current = start

    while current <= end:
        arr.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    print(f"iso_periods_date size {len(arr)}")
    logging.info(f"iso_periods_date size {len(arr)}")
    date_iso_periods_list = ";".join(arr)

    return date_iso_periods_list


#######################################

def sendEmail():
    # creates SMTP session
    #s = smtplib.SMTP('smtp.gmail.com', 587)
    # start TLS for security
    #s.starttls()
    # Authentication
    #s.login("ipamis@hispindia.org", "IPAMIS@12345")
    # message to be sent
    
    # message to be sent
    #message = "Message_you_need_to_send"

    # sending the mail
    #s.sendmail("ipamis@hispindia.org", "mithilesh.thakur@hispindia.org",message)
    #print(f"Email send to mithilesh.thakur@hispindia.org")
    # terminating the session
    #s.quit()
    
    #fromaddr = "dss.nipi@hispindia.org"
    fromaddr = FROM_EMAIL_ADDR
    # list of email_id to send the mail
    #li = ["mithilesh.thakur@hispindia.org", "saurabh.leekha@hispindia.org","dpatankar@nipi-cure.org","mohinder.singh@hispindia.org"]
    #li = ["mithilesh.thakur@hispindia.org","sumit.tripathi@hispindia.org","RKonda@fhi360.org"]
    li = ["mithilesh.thakur@hispindia.org"]

    for toaddr in li:

        #toaddr = "mithilesh.thakur@hispindia.org"
        
        # instance of MIMEMultipart 
        msg = MIMEMultipart() 
        
        # storing the senders email address   
        msg['From'] = fromaddr 
        
        # storing the receivers email address  
        msg['To'] = toaddr 
        
        # storing the subject  
        msg['Subject'] = "Climet data push from nepalhmis to climent instance log file"
        
        # string to store the body of the mail 
        #body = "Python Script test of the Mail"

        today_date = datetime.now().strftime("%Y-%m-%d")
        #updated_odk_api_url = f"{ODK_API_URL}?$filter=__system/submissionDate ge {today_date}"
        updated_odk_api_url = f"{today_date}"

        body = f"Climet data push from nepalhmis to climent instance log file"
        
        # attach the body with the msg instance 
        msg.attach(MIMEText(body, 'plain')) 
        
        
        # open the file to be sent  

        LOG_DIR = "logs"
        PATTERN = "*_dataValueSet_post.log"

        # Find latest matching log file
        log_files = glob.glob(os.path.join(LOG_DIR, PATTERN))
        if not log_files:
            raise FileNotFoundError("No log files found")

        latest_log = max(log_files, key=os.path.getmtime)

        filename = LOG_FILE
        #attachment = open(filename, "rb") 
        attachment = open(latest_log, "rb") 
        
        # instance of MIMEBase and named as p 
        p = MIMEBase('application', 'octet-stream') 
        
        # To change the payload into encoded form 
        p.set_payload((attachment).read()) 
        
        # encode into base64 
        encoders.encode_base64(p) 
        
        p.add_header('Content-Disposition', "attachment; filename= %s" % filename) 
        
        # attach the instance 'p' to instance 'msg' 
        msg.attach(p) 
        try:
            # creates SMTP session 
            s = smtplib.SMTP('smtp.gmail.com', 587) 
            
            # start TLS for security 
            s.starttls() 
            
            # Authentication 
            #s.login(fromaddr, "NIPIODKHispIndia@123")
            #s.login(fromaddr, "dztnzuvhbxlauwxy") ## set app password App Name Mail as on 22/12/2025
            s.login(fromaddr, FROM_EMAIL_PASSWORD)
            

            # Converts the Multipart msg into a string 
            text = msg.as_string() 
            
            # sending the mail 
            s.sendmail(fromaddr, toaddr, text) 
            print(f"mail send to: {toaddr}")
            log_info(f"mail send to: {toaddr}")
            # terminating the session 
            s.quit()
        except Exception as exception:
            print("Error: %s!\n\n" % exception)
