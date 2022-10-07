from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import time
import json

class PCSession(object):
    # Max number of retries for any reason
    max_retries = 5
    # Always retry on these statuses, within the requests session.
    # We retry for auth failure (401) within the SDK code. See try_wrapper().
    retry_statuses = [429, 500, 502, 503, 504]



    def __init__(self,userid,userpass,customer_name,api_base):
        self.api_id = userid
        self.api_pass = userpass
        self.cust_name = customer_name
        self.api_base_url = api_base
        self.auth_token = None
        self.build_client()
        return None

    def build_client(self):
        """Build client object for class instantiation."""
        self.client = requests.Session()
        self.retries = Retry(total=self.max_retries,
                             status_forcelist=self.retry_statuses,
                             backoff_factor=1)
        self.redlock_http_adapter = HTTPAdapter(pool_connections=1,
                                                pool_maxsize=10,
                                                max_retries=self.retries)
        self.session_mount = "https://"
        self.client.mount(self.session_mount, self.redlock_http_adapter)
        return None

    def get_auth_token(self, endpoint, body):

        token = None
        resp = self.client.post(endpoint, json=body)
        if resp.status_code == 200:
            auth_resp_json = resp.json()
            token = auth_resp_json["token"]
        if resp.status_code == 401:
            token = "BAD"
        return token

    def authenticate_client(self):

        success = False
        prefix = "https://"  + self.api_base_url
        endpoint = prefix + "/login"
        body = {"username":self.api_id,"password":self.api_pass,"customerName":self.cust_name}
        max_tries = 5
        for _ in range(max_tries):
            token = self.get_auth_token(endpoint, body)
            if token == "BAD":
                print("Invalid credentials - can not obtain session token.")
            if token is not None:
                self.auth_token = token
                success = True
                break
            else:
                time.sleep(1)
        self.client.headers.update(self.build_header())
        return success

    def build_header(self):
        """This constructs the auth header, required for all API interaction.
        """
        header = {"x-redlock-auth": self.auth_token,
                  "Content-Type": "application/json"}
        return header

    def interact(self, verb, endpoint, params=None, reqbody=None):
        # Build the complete URL
        url = "%s%s" % (self.build_endpoint_prefix(), endpoint)
        # Set up for try/retry
        success = False
        # If we've not authenticated the session, we do it now
        if self.auth_token is None:
            self.authenticate_client()
        success, response, exception = self.try_wrapper(verb, url, params,
                                                        reqbody)
        if success:
            return response
        raise exception

    def try_wrapper(self, verb, url, params, reqbody):
        verb_mapping = {'get': self.client.get,
                        'post': self.client.post,
                        'put': self.client.put,
                        'delete': self.client.delete}
        # Raise ValueError if invalid verb is used
        if verb not in verb_mapping:
            raise ValueError("Invalid HTTP verb for API: %s" % verb)
        if self.auth_token is None:
            self.authenticate_client()
        success, response, exception = self.get_response(verb_mapping[verb],
                                                         verb, url, params,
                                                         reqbody)
        if response.status_code == 401:  # Try to reauth once.
            self.authenticate_client()
            success, response, exception = self.get_response(verb_mapping[verb],  # NOQA
                                                             verb, url, params,
                                                             reqbody)
        return success, response, exception

    def get_response(self, client_method, verb, url, params, reqbody):
        if verb in ['get', 'delete']:
            response = client_method(url, params=params)
        else:
            response = client_method(url, data=json.dumps(reqbody))
        success, exception = utility.parse_status(url, response.status_code,
                                                  response.text)
        return success, response, exception     
