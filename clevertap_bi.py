#First lets start by downloading important python packages-

import time
import pandas as pd
import requests
import json
from string import Template
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#Your credentials,and event name here, refer to linked document to understand where to find these- https://developer.clevertap.com/docs/api-quickstart-guide

clientid = " " #your client id here
pwd = ' ' #your password here
ACCOUNT_NAME = '' #your account name here
EVENT_NAME = "mention_your_event_name_here" #your event_name here
REGION = 'eu'
FROM = '20220101' #desired start date
TO = '20220831'   #desired end date

#No action needed
folder_name = ACCOUNT_NAME + "_" + EVENT_NAME
print("Folder name:", folder_name)
print("-----")
if not os.path.exists(folder_name):
    os.makedirs(folder_name)


#No action needed

headers = {'X-CleverTap-Account-Id': clientid,
           'X-CleverTap-Passcode': pwd}


#No action needed

def write_csv(base_folder, df):
    # print(df.shape)
    path = base_folder + "/event_data_" + FROM + "_" + TO + ".csv"
    with open(path, 'a') as f:
        df.to_csv(f, header=f.tell() == 0, index=False)


#Mention the event name under event_name and event properties in the format whose example is given below, you can find event properties associated with a desired event under #settings and schema-

def process_results(r, event_name):
    if event_name == 'mention_your_event_name_here': #your event_name here 
        try:
            f_dict = {}
            event_props = r['event_props'] #mention the event properties you want by substituting them with dummy properties commented below
#           f_dict['account_id'] = event_props.get('Account ID', 0)
#           f_dict['account_name'] = event_props.get('Account Name', 0)
#           f_dict['account_type'] = event_props.get('Account Type',0)
#           f_dict['user_name'] = event_props.get('User Name',0)
            f_dict['event_ts'] = r['ts'] 
            return f_dict
        except:
            return False

            #No action needed
def get_cursor_results(url1, cursor, headers, event_name):
    TOTAL_COUNT = 0
    while(True):
        url2 = url1 + "&cursor=" + cursor
        r2 = requests.get(url2, headers=headers, verify=False)

        res_dict = json.loads(r2.content)
        if('records' in res_dict):

            processed_records = []
            for i in res_dict['records']:
                processed_record_current = process_results(i, event_name)
                if(processed_record_current):
                    processed_records.append(processed_record_current)

            TOTAL_COUNT += len(processed_records)
            print(TOTAL_COUNT, "events processed")
            dftemp = pd.DataFrame(processed_records)
            write_csv(folder_name, dftemp)

        if('next_cursor' in res_dict):
            cursor = res_dict['next_cursor']

        elif(res_dict['status'] == 'fail' and res_dict['code'] == 2):
            print('fail, retrying after sleeping for 2 seconds')
            time.sleep(2)

        else:
            print("Processing over")
            print(TOTAL_COUNT, "total events processed")
            return TOTAL_COUNT

def get_result_event_query(q, url1, headers, region, event_name):

    r = requests.post(url1, headers=headers, data=q, verify=False)
    if r.ok:
        cursor = json.loads(r.content)['cursor']
        TOTAL_EVENTS = get_cursor_results(url1, cursor, headers, event_name)
        return TOTAL_EVENTS
    else:
        print("Error in R1")
        return r.status_code

def make_query_event(EVENT_NAME, FROM, TO, ADVANCE={}):
    st = '''{
    "event_name":"$EVENT_NAME","from":$FROM,"to":$TO
    }'''
    s = Template(st).substitute(EVENT_NAME=EVENT_NAME, FROM=FROM, TO=TO, ADVANCE=ADVANCE)
    return json.dumps(json.loads(s))


url1 = f'https://api.clevertap.com/1/events.json?batch_size=5000'


# ADVANCE = '''{
#         "did_all": [
#             {
#                 "event_name": "Item Purchased",
#                 "from": $FROM,
#                 "to": $TO,
#                 "event_properties": [
#                     {
#                         "name": "Product Name",
#                         "operator": "contains",
#                         "value": "Klotthe Multicolor"
#                     }
#                 ]
#             }
#         ]
#     }'''

#ADVANCE = json.dumps(json.loads(Template(ADVANCE).substitute(FROM=FROM, TO=TO)))

q = make_query_event(EVENT_NAME, FROM, TO)
print("Query:", json.dumps(json.loads(q), indent=4))
print('------')

result_searched = get_result_event_query(q, url1, headers, REGION, EVENT_NAME)
print("------")
print("Output written to", folder_name)

