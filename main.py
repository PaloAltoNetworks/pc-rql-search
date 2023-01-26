from operator import mod
import lib
import json
import csv
import re
import datetime

import logging
logging.basicConfig()
py_logger = logging.getLogger("pcpi")
py_logger.setLevel(10)

from pcpi import saas_session_manager


# class RQLAsync():
#     def __init__(self):
#         self.config = lib.ConfigHelper()
#         self.csv_writer = lib.CsvWriter()
#         self.pc_sess = lib.PCSession(self.config.pc_user, self.config.pc_pass, self.config.pc_cust,
#                                      self.config.pc_api_base)
#         self.csvheader = ["Resource Name", "Service", "Account", "Region Name", "Last Modified", "Deleted" ]

#     def get_pcs_accounts(self,cloud_type):
#         #pull in cloud accounts but only the relavant ones for the specified RQL
#         self.url = "https://" + self.config.pc_api_base + "/cloud/name?onlyActive=true&cloudType=" + cloud_type
#         self.pc_sess.authenticate_client()
#         response = self.pc_sess.client.get(self.url)
#         pcs_accounts_json = response.json()

#         return pcs_accounts_json

#     def insert_cloud_acct(self,src_str, insert_str, pos):
#         #this inserts the cloud.account line into RQL 
#         return f'{src_str[:pos]}{insert_str}{src_str[pos:]}'

#     def rql_search(self):
#         results = []
#         body_params= {}

#         #determine which cloud type the query is set to use
#         match = re.search('\w*\s*=\s*\'(\w+)', self.config.pc_rql)
#         if match:
#             query_cloud_type = match.group(1)
#             #calls the get accounts and uses the regex to only pull relavant cloud accts
#             pcs_accounts = self.get_pcs_accounts(query_cloud_type)
#         else:
#             print("RQL doesn't appear to be formatted correctly, please double check and try again.\n")
        
#         print_count = 0
#         for acct in pcs_accounts:
#             if acct['cloudType'] == query_cloud_type:
#                 pos = self.config.pc_rql.find(' api.name')
#                 #needed to use .replace with literal to backspace single quotes in account names
#                 mod_rql = self.insert_cloud_acct(self.config.pc_rql,' cloud.account IN ( ' + '\'' + acct['name'].replace(r"'",r"\'") + '\' )' + ' AND',pos)

#                 if print_count % 10 == 0 or pcs_accounts.index(acct) == len(pcs_accounts) -1:
#                     print("Running RQL", pcs_accounts.index(acct) + 1, "of", len(pcs_accounts))
#                 print_count += 1

#                 self.url = "https://" + self.config.pc_api_base + "/search/config"
#                 self.pc_sess.authenticate_client()
#                 payload = json.dumps(
#                     {"withResourceJson":"false",
#                     "limit": 100,
#                     "timeRange": {
#                         "type":"relative",
#                         "value": {
#                             "unit": "hour",
#                             "amount": 24
#                         },
#                         "relativeTimeType": "BACKWARD"
#                     },
#                     "query": mod_rql
#                 })

#                 response = self.pc_sess.client.post(self.url,payload)
#                 if response == '400':
#                     print('Something appears to be wrong with RQL query')
#                 else:
#                     print(response.status_code)
#                     resp_json = response.json()
#                     if 'items' in resp_json['data']:
#                         results.extend(resp_json['data']['items'])
#                     #add dynamic columns to csvheader
#                     if 'dynamicColumns' in resp_json['data']: 
#                         for col in resp_json['data']['dynamicColumns']:
#                             if not col in self.csvheader:
#                                 self.csvheader.append(col)
#                     if 'nextPageToken' in resp_json['data']:
#                         body_params['pageToken'] = resp_json['data']['nextPageToken']
#                         body_params['withResourceJson'] = False
#                         body_params['limit'] = 100
#                     while 'pageToken' in body_params:
#                         self.url = "https://" + self.config.pc_api_base + "/search/config/page"
#                         self.pc_sess.authenticate_client()
#                         paged_response = self.pc_sess.client.post(self.url,json=body_params)
#                         paged_resp_json = paged_response.json()
#                         if 'items' in paged_resp_json:
#                             results.extend(paged_resp_json['items'])
#                         if 'nextPageToken' in paged_resp_json:
#                             body_params['pageToken'] = paged_resp_json['nextPageToken']
#                         else:
#                             body_params.pop('pageToken', None)


#         self.csv_writer.write([self.csvheader])
#         count = 0
#         for res in results:
#             #if res['name'] == "Nexus-repo":
#                 count += 1
#                 newdata = []
#                 csvdata = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%d %H:%M:%S'), res['deleted']]
#                 if 'dynamicData' in res:
#                     for ele in res['dynamicData']:
#                         newdata.append(res['dynamicData'][ele])

#                     csvdata.extend(newdata)
#                 else:
#                     csvdata = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%d %H:%M:%S'), res['deleted']]
#                 self.csv_writer.append([csvdata])

#         print("Number of resources processed: {}".format(count))

#     def run(self):
#         self.rql_search()


def run_rql_hs():
    #Read Config
    config = lib.ConfigHelper()
    session_manager = saas_session_manager.SaaSSessionManager('Tenant', config.pc_user, config.pc_pass, "https://" + config.pc_api_base, True, py_logger)
    session = session_manager.create_cspm_session()
    rql = config.pc_rql

    payload = {
        "query": rql,
        "limit": 200,
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
        dy_headers = dy_headers_all
        filename = config.pc_file_name
        with open(filename, "a", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for res in res_data['items']:
                new_data = []
                csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%d %H:%M:%S'), res['deleted']]
                if 'dynamicData' in res:
                    for header in dy_headers:
                        found = False
                        for ele in res['dynamicData']:
                            if header == ele:
                                new_data.append(res['dynamicData'][ele])
                                found = True
                        if found == False:
                            new_data.append('None')
                        

                    csv_data.extend(new_data)
                else:
                    csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%d %H:%M:%S'), res['deleted']]
                
                writer.writerows([csv_data])

    #Run HS RQL
    session.config_search_request_function(payload, dump_to_csv)
    #Done


def main():
    # RQL_Async = RQLAsync()
    # RQL_Async.run()
    run_rql_hs()

if __name__ == "__main__":
    main()
