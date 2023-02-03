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
    session_manager = saas_session_manager.SaaSSessionManager('Tenant', config.pc_user, config.pc_pass, "https://" + config.pc_api_base, True, py_logger)
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

    session.headers.update({"Accept": "text/csv"})
    res = session.request('POST', '/search/config', payload)
    session.headers.pop('Accept')

    tz_list = res.text.split('\n')[2].split(',"')[1].split('",')[0].split(' ')[4:] #getting locale info from CSV

    add_column_order = res.text.split('\n')[1].split(',')[7:]
    add_column_headers = res.text.split('\n')[1].split(',')
    print(add_column_order)

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
        if config.utc:
            writer.writerow(csv_headers)
        else:
            writer.writerow(add_column_headers)

    #Define CSV Output Function
    def dump_to_csv(res_details, res_data, counter, total_rows):
        import csv
        time_offset = 28800
        dy_headers = dy_headers_all
        filename = config.pc_file_name
        with open(filename, "a", newline='', encoding='utf-8') as f:
            writer = object
            if config.utc:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, quotechar = "'")
            else:
                writer = csv.writer(f)
                
            for res in res_data['items']:
                new_data = []
                #2023-01-30T19:36:07.921Z
                # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%dT%H:%M:%S'), str(res['deleted']).lower()]
                
                # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], res['insertTs'], str(res['deleted']).lower()]
                if config.utc ==  True:
                    #8 hours behind the CSV from the UI for some reason. Add 8 hours (time_offset value is 8 hours in seconds)
                    name = res['name']
                    service = res['service']
                    accountName = res['accountName']
                    regionName = res['regionName']
                    time_stamp = datetime.datetime.fromtimestamp(res['insertTs']/1000 + time_offset).isoformat()[:-3]+'Z'
                    deleted = str(res['deleted']).lower()
                    csv_data = [f'\"{name}\"', f'\"{service}\"', f'\"{accountName}\"', f'\"{regionName}\"', f'\"{time_stamp}\"', deleted]
                else:
                    #convert
                    tz_code = tz_list[0]
                    tz_locale = tz_list[1]
                    to_zone = tz.gettz(tz_code)
                    utc = datetime.datetime.fromtimestamp(res['insertTs']/1000)
                    timestamp_with_zone = utc.astimezone(to_zone)
                    locale_ts = timestamp_with_zone.strftime("%b %d, %Y %I.%M") + ' ' + tz_code + ' ' + tz_locale

                    csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], f'{locale_ts}', str(res['deleted']).lower()]


                if 'dynamicData' in res:
                    headers_order = []
                    if config.utc:
                        headers_order = dy_headers
                    else:
                        headers_order = add_column_order

                    for header in headers_order:
                        found = False

                        for ele in res['dynamicData']:
                            if header == ele:
                                blob = res['dynamicData'][ele]
                                if config.utc:
                                    new_data.append(f'\"{blob}\"')
                                else:
                                    new_data.append(f'{blob}')
                                found = True
                        if found == False:
                            new_data.append('None')
                        

                    csv_data.extend(new_data)
                else:
                    if config.utc ==  True:
                        #8 hours behind the CSV from the UI for some reason. Add 8 hours (time_offset value is 8 hours in seconds)
                        name = res['name']
                        service = res['service']
                        accountName = res['accountName']
                        regionName = res['regionName']
                        time_stamp = datetime.datetime.fromtimestamp(res['insertTs']/1000 + time_offset).isoformat()[:-3]+'Z'
                        deleted = str(res['deleted']).lower()
                        csv_data = [f'\"{name}\"', f'\"{service}\"', f'\"{accountName}\"', f'\"{regionName}\"', f'\"{time_stamp}\"', deleted]
                    else:
                        #convert
                        tz_code = tz_list[0]
                        tz_locale = tz_list[1]
                        to_zone = tz.gettz(tz_code)
                        utc = datetime.datetime.fromtimestamp(res['insertTs']/1000)
                        timestamp_with_zone = utc.astimezone(to_zone)
                        locale_ts = timestamp_with_zone.strftime("%b %d, %Y %I.%M") + ' ' + tz_code + ' ' + tz_locale

                        csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], f'{locale_ts}', str(res['deleted']).lower()]
                
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
