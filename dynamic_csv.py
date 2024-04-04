from operator import mod
import lib
import json
import csv
import re
import datetime
from dateutil import tz
from datetime import datetime as dt


from pcpi import saas_session_manager

def perform_rql_search():
    #Read Config
    config = lib.ConfigHelper()
    session_manager = saas_session_manager.SaaSSessionManager('Tenant', config.pc_user, config.pc_pass, "https://" + config.pc_api_base)
    session = session_manager.create_cspm_session()
    rql = config.pc_rql

    payload = {
        "query": rql,
        "limit": 2000,
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
    
    items = []

    csv_headers = ["Resource Name", "Service", "Account", "Region Name", "Last Modified", "Deleted" ]
    dynamic_headers = []

    counter = 0
    while True:
        api_url = ''
        if counter == 0:
            api_url = '/search/config'
        else:
            api_url = '/search/config/page'

        #call page endpoint
        # res = s.post(api_url, headers=headers, json=payload)
        res = session.request('POST', api_url, json=payload)

        #update res_data with the paginated response
        if counter == 0:
            res_data = res.json()['data']
        else:
            res_data = res.json()

        for item in res_data.get('items',[]):
            items.append(item)

        if res.status_code in session.success_status:

            if 'dynamicColumns' in res_data: 
                for col in res_data['dynamicColumns']:
                    if not col in csv_headers:
                        csv_headers.append(col)
                        dynamic_headers.append(col)

        #Exit case
        if 'nextPageToken' not in res_data:
            break

        #update payload
        payload.update({'pageToken': res_data.get('nextPageToken')})

        counter += 1

    return (items, csv_headers, dynamic_headers)

# def run_rql_hs():
#     #Read Config
#     config = lib.ConfigHelper()
#     session_manager = saas_session_manager.SaaSSessionManager('Tenant', config.pc_user, config.pc_pass, "https://" + config.pc_api_base, True, py_logger)
#     session = session_manager.create_cspm_session()
#     rql = config.pc_rql

#     payload = {
#         "query": rql,
#         "limit": 1,
#         "timeRange": {
#             "relativeTimeType": "BACKWARD",
#             "type": "relative",
#             "value": {
#                 "amount": 24,
#                 "unit": "hour"
#             }
#         },
#         "withResourceJson": True,
#         "heuristicSearch": True
#     }
#     csv_headers = ["Resource Name", "Service", "Account", "Region Name", "Last Modified", "Deleted" ]
#     dy_headers_all = []


#     #Get headers for CSV and verify RQL
#     res = session.request('POST', '/search/config', payload)

#     if res.status_code in session.success_status:
#         res_data = res.json()
#         if 'dynamicColumns' in res.json()['data']: 
#             for col in res_data['data']['dynamicColumns']:
#                 if not col in csv_headers:
#                     csv_headers.append(col)
#                     dy_headers_all.append(col)
#     else:
#         py_logger.error('Config Search Failed')
#         print('Steps to troubleshoot:')
#         print('1) Verify Credentials are Valid')
#         print('2) Verify RQL is valid using the Prisma Cloud UI')

#     #Dump headers to file
#     filename = config.pc_file_name
#     with open(filename, "w", newline='', encoding='utf-8') as f:
#         writer = csv.writer(f)

#         writer.writerow(csv_headers)


#Define CSV Output Function
def dump_to_csv(items, csv_headers, dynamic_headers):
    import csv
    # time_offset = 28800

    config = lib.ConfigHelper()
    filename = config.pc_file_name

    with open(filename, "a", newline='', encoding='utf-8') as f:
        writer = object
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, quotechar = "'")

        writer.writerow(csv_headers)

        for item in items:
            new_data = []
            #2023-01-30T19:36:07.921Z
            # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%dT%H:%M:%S'), str(res['deleted']).lower()]
            
            # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], res['insertTs'], str(res['deleted']).lower()]

            name = item['name']
            service = item['service']
            accountName = item['accountName']
            regionName = item['regionName']
            time_stamp = str(datetime.datetime.fromtimestamp(item['insertTs']/ 1000.0, tz=datetime.timezone.utc))[:-9].replace(' ', 'T')+'Z'
            # time_stamp = datetime.datetime.fromtimestamp(res['insertTs']/1000.0).isoformat()[:-3]+'Z'
            deleted = str(item['deleted']).lower()
            csv_data = [f'\"{name}\"', f'\"{service}\"', f'\"{accountName}\"', f'\"{regionName}\"', f'\"{time_stamp}\"', deleted]

            if 'dynamicData' in item:
                headers_order = []
                headers_order = dynamic_headers
                for header in headers_order:
                    found = False

                    for ele in item['dynamicData']:
                        if header == ele:
                            blob = item['dynamicData'][ele]
                            if type(blob) == bool:
                                blob = str(blob).lower()
                                new_data.append(f'{blob}')
                            else:
                                new_data.append(f'\"{blob}\"')
                            # else:
                            #     new_data.append(f'{blob}')
                            found = True
                    if found == False:
                        new_data.append('None')

                csv_data.extend(new_data)
            
            writer.writerows([csv_data])

def main():
    # RQL_Async = RQLAsync()
    # RQL_Async.run()
    items, csv_headers, dynamic_headers = perform_rql_search()
    dump_to_csv(items, csv_headers, dynamic_headers)

if __name__ == "__main__":
    main()
