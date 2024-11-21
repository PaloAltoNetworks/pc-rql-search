import lib
import csv
import datetime
from datetime import datetime as dt
import csv

import logging
logging.basicConfig()
py_logger = logging.getLogger("pcpi")
py_logger.setLevel(10)

from pcpi import saas_session_manager

#Define CSV Output Function
def dump_to_csv(writer, res_data, dynamic_headers):
    for item in res_data['items']:
        dynamic_data_list = []

        #["Asset Name", "Service", "Account", "Region Name", "Last Modified", "Deleted"]
        name = item['name'] #Asset Name
        service = item['service'] #Service Name
        accountName = item['accountName'] #Account Name
        regionName = item['regionName'] #Region Name
        time_stamp = str(datetime.datetime.fromtimestamp(item['insertTs']/ 1000.0, tz=datetime.timezone.utc))[:-9].replace(' ', 'T')+'Z' # Last Modified
        deleted = str(item['deleted']).lower() #Deleted
        
        csv_data_list = [name, service, accountName, regionName, time_stamp, deleted]

        if 'dynamicData' in item:
            for header in dynamic_headers:
                found = False
                for ele in item['dynamicData']:
                    if header == ele:
                        blob = item['dynamicData'][ele]

                        if type(blob) == dict:
                            dynamic_data_list.append(blob)
                        
                        else:
                            blob = str(blob).lower()
                            dynamic_data_list.append(blob)

                        found = True
                if found == False:
                    dynamic_data_list.append('None')

            csv_data_list.extend(dynamic_data_list)
        
        writer.writerows([csv_data_list])

def paginate_and_write_to_csv(session, payload, csv_headers, dynamic_headers, config):
    limit = 2000
    total_rows = 0
    counter = 0

    # Force best practices with HS
    payload.update({"heuristicSearch": True, "limit": limit, "withResourceJson": True})

    with open(config.pc_file_name, 'w') as file_pointer:

        writer = csv.writer(file_pointer, quoting=csv.QUOTE_ALL, doublequote=True, quotechar="\"")

        # Write headers:
        writer.writerow(csv_headers + dynamic_headers)

        while True:
            # Make API call
            endpoint = '/search/api/v2/config' if counter == 0 else '/search/config/page'
            res = session.request('POST', endpoint, json=payload)
            
            # Validate API call
            if res.status_code not in session.success_status:
                session.logger.error(f"API call failed with status code: {res.status_code}")
                break

            # Extract data from API call
            res_data = res.json()

            # Update total rows
            total_rows += res_data.get('totalRows', 0)

            # Call the provided function
            dump_to_csv(writer, res_data, dynamic_headers)

            session.logger.info(f"Processing page {counter}, got {res_data.get('totalRows', 0)} items, total items: {total_rows}")

            # Check if there's a next page
            if 'nextPageToken' not in res_data:
                break

            counter += 1
            # Update payload for the next page
            payload.update({'pageToken': res_data['nextPageToken']})
            

        session.logger.info(f"Completed processing. Total rows: {total_rows}")
        return total_rows

def get_dynamic_columns(session, rql):
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
        "withResourceJson": False,
        "heuristicSearch": False
    }

    #Get headers for CSV and verify RQL
    res = session.request('POST', '/search/api/v2/config', payload)

    dynamic_headers = res.json().get('dynamicColumns',[])

    return dynamic_headers

def main():
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
        "withResourceJson": False,
        "heuristicSearch": False
    }
    csv_headers = ["Asset Name", "Service", "Account", "Region Name", "Last Modified", "Deleted"]

    dynamic_headers = get_dynamic_columns(session, rql)

    paginate_and_write_to_csv(session, payload, csv_headers, dynamic_headers, config)
    

if __name__ == "__main__":
    main()
