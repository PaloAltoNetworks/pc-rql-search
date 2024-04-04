from operator import mod
import lib
import json
import csv
import re
import datetime
from dateutil import tz
from datetime import datetime as dt

import logging
logging.basicConfig()
py_logger = logging.getLogger("pcpi")
py_logger.setLevel(10)

from pcpi import saas_session_manager

def run_rql_hs():
    #Read Config
    config = lib.ConfigHelper()
    session_manager = saas_session_manager.SaaSSessionManager('Tenant', config.pc_user, config.pc_pass, "https://" + config.pc_api_base)
    session = session_manager.create_cspm_session()
    rql = config.pc_rql

    payload = {
        "query": rql,
        "limit": 1,
        "timeRange": {
            "relativeTimeType": "BACKWARD",
            "type": "relative",
            "value": {
                "amount": 24,
                "unit": "hour"
            }
        },
        "withResourceJson": True,
        "heuristicSearch": True
    }
    csv_headers = ["Resource Name", "Service", "Account", "Region Name", "Last Modified", "Deleted" ]
    dy_headers_all = []


    #Get headers for CSV and verify RQL
    res = session.request('POST', '/search/config', payload)

    if res.status_code in session.success_status:
        res_data = res.json()
        if 'dynamicColumns' in res.json()['data']: 
            for col in res_data['data']['dynamicColumns']:
                if not col in csv_headers:
                    csv_headers.append(col)
                    dy_headers_all.append(col)
    else:
        py_logger.error('Config Search Failed')
        print('Steps to troubleshoot:')
        print('1) Verify Credentials are Valid')
        print('2) Verify RQL is valid using the Prisma Cloud UI')

    #Dump headers to file
    filename = config.pc_file_name
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        writer.writerow(csv_headers)


    #Define CSV Output Function
    def dump_to_csv(res_details, res_data, counter, total_rows):
        import csv
        # time_offset = 28800
        dy_headers = dy_headers_all
        filename = config.pc_file_name
        with open(filename, "a", newline='', encoding='utf-8') as f:
            writer = object
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, quotechar = "'")
            for res in res_data['items']:
                new_data = []
                #2023-01-30T19:36:07.921Z
                # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%dT%H:%M:%S'), str(res['deleted']).lower()]
                
                # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], res['insertTs'], str(res['deleted']).lower()]

                name = res['name']
                service = res['service']
                accountName = res['accountName']
                regionName = res['regionName']
                time_stamp = str(datetime.datetime.fromtimestamp(res['insertTs']/ 1000.0, tz=datetime.timezone.utc))[:-9].replace(' ', 'T')+'Z'
                # time_stamp = datetime.datetime.fromtimestamp(res['insertTs']/1000.0).isoformat()[:-3]+'Z'
                deleted = str(res['deleted']).lower()
                csv_data = [f'\"{name}\"', f'\"{service}\"', f'\"{accountName}\"', f'\"{regionName}\"', f'\"{time_stamp}\"', deleted]

                if 'dynamicData' in res:
                    headers_order = []
                    headers_order = dy_headers
                    for header in headers_order:
                        found = False

                        for ele in res['dynamicData']:
                            if header == ele:
                                blob = res['dynamicData'][ele]

                                if type(blob) == dict:
                                    new_data.append(blob)
                                
                                else:
                                    blob = str(blob).lower()
                                    new_data.append(f'{blob}')


                                found = True
                        if found == False:
                            new_data.append('None')

                    csv_data.extend(new_data)
                
                writer.writerows([csv_data])

    #Run HS RQL
    total_rows = session.config_search_request_function(payload, dump_to_csv)
    #Done
    print(f'Got {total_rows} rows.')


def main():
    # RQL_Async = RQLAsync()
    # RQL_Async.run()
    run_rql_hs()

if __name__ == "__main__":
    main()
