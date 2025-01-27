import os
import yaml


class ConfigHelper(object):
    def __init__(self):
        config = self.read_yml('configs')
        self.pc_user = config["prisma_cloud"]["username"]
        self.pc_pass = config["prisma_cloud"]["password"]
        self.pc_cust = config["prisma_cloud"]["customer_name"]
        self.pc_api_base = config["prisma_cloud"]["api_base"]
        self.pc_file_name = config["prisma_cloud"]["filename"]
        self.pc_rql = config["prisma_cloud"]["rql"]
        try:
            self.utc = config["prisma_cloud"]["utc"]
        except:
            self.utc = True
        try:
            self.transform_dict = config['prisma_cloud']['transform_dict']
        except:
            self.transform_dict = False


    @classmethod
    def read_yml(self, f):
        yml_path = os.path.join(os.path.dirname(__file__), "../config/%s.yml" % f)
        with open(yml_path,'r') as stream:
            return yaml.safe_load(stream)
