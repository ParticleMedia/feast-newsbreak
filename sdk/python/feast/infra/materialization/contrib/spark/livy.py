import requests
import json
from pydantic import StrictStr

class LivyOperator():

    url: StrictStr

    livy_conf: dict

    def __init__(
        self,
        url,
        conf,
    ):
        super().__init__()
        self.url = url
        self.conf = conf

    def submit(self, args):

        params = {"args": args}
        params.update(self.conf)

        payload = json.dumps(params)
        headers = {
            'Content-Type': 'application/json'
        }

        print(payload)

        response = requests.request("POST", self.url, headers=headers, data=payload)

        print(response.text)

        if response.status_code < 200 or response.status_code >= 300:
            return None

        return response.json()['id']


    def state_query(self, livy_id):
        response = requests.request("GET", f"{self.url}/{livy_id}/state")

        if response.status_code < 200 or response.status_code >= 300:
            return "error http code"

        return response.json()['state']