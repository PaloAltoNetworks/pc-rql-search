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


def run_config_network_rql():
    #Read Config
    config = lib.ConfigHelper()
    session_manager = saas_session_manager.SaaSSessionManager('Tenant', config.pc_user, config.pc_pass, "https://" + config.pc_api_base)
    session = session_manager.create_cna_session()
    rql = config.pc_rql

    payload = {
        # "query": "config from network where source.resource.type = 'Instance' and source.cloud.type = 'AWS' and dest.network = '0.0.0.0/32'",
        "query": rql,
        "timeRange":
        {
            "type":"to_now",
            "value":"epoch"
        },
        'baseUrl': session.api_url,
        "namespace": session.prisma_id
    }

    res = session.request('POST', '/cnssearches', json=payload)

    return res.json()

#Define CSV Output Function
def dump_to_csv(network_res):
    config = lib.ConfigHelper()

    filename = config.pc_file_name

    headers = ["Source Instance ID","Source Account","Source VPC","Source Name","Destination Network","Policy Action"]

    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = object
        writer = csv.writer(f)

        writer.writerow(headers)
        for resource_id in network_res['data']['nodes'].keys():
            new_data = []
            #2023-01-30T19:36:07.921Z
            # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], datetime.datetime.fromtimestamp(res['insertTs']/1000.).strftime('%Y-%m-%dT%H:%M:%S'), str(res['deleted']).lower()]
            
            # csv_data = [res['name'], res['service'], res['accountName'], res['regionName'], res['insertTs'], str(res['deleted']).lower()]
            source_instance_id = resource_id
            source_name = network_res['data']['nodes'][resource_id]['nodeData']['name']
            source_account = network_res['data']['nodes'][resource_id]['nodeData']['accountId']
            source_vpc = network_res['data']['nodes'][resource_id]['nodeData'].get('VPCID', 'NONE')
            destination_network_ips = network_res['data']['sourceDestinationMap'].get(resource_id, {})
            for key in destination_network_ips.keys():
                destination_network = key
                policy_action = destination_network_ips[key]['verdict']

                csv_data = [source_instance_id, source_account, source_vpc, source_name, destination_network, policy_action]
            
                writer.writerow(csv_data)

if __name__ == '__main__':
    data = run_config_network_rql()
    dump_to_csv(data)