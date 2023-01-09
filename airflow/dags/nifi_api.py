import requests
import json
from time import time


def pause(secs):
    init_time = time()
    while time() < init_time + secs:
        pass

def get_token(url_nifi_api: str, access_payload: dict):

    header = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
    }
    response = requests.post(
        url_nifi_api + "access/token", headers=header, data=access_payload, verify=False
    )
    return response.content.decode("ascii")

def get_processor(url_nifi_api: str, processor_id: str, token=None):
    """
    Gets and returns a single processor.
    Makes use of the REST API `/processors/{processor_id}`.
    :param url_nifi_api: String
    :param processor_id: String
    :param token: JWT access token
    :returns: JSON object processor
    """

    # Authorization header
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }

    # GET processor and parse to JSON
    response = requests.get(url_nifi_api + f"processors/{processor_id}", headers=header, verify=False)
    return json.loads(response.content)

def update_processor_status(processor_id: str, new_state: str, token, url_nifi_api):
    """Starts or stops a processor by retrieving the processor to get
    the current revision and finally putting a JSON with the desired
    state towards the API.
    :param processor_id: Id of the processor to receive the new state.
    :param new_state: String representing the new state, acceptable
                        values are: STOPPED or RUNNING.
    :param token: a JWT access token for NiFi.
    :param url_nifi_api: URL to the NiFi API
    :return: None
    """

    # Retrieve processor from `/processors/{processor_id}`
    processor = get_processor(url_nifi_api, processor_id, token)

    # Create a JSON with the new state and the processor's revision
    put_dict = {
        "revision": processor["revision"],
        "state": new_state,
        "disconnectedNodeAcknowledged": True,
    }

    # Dump JSON and POST processor
    payload = json.dumps(put_dict).encode("utf8")

    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }

    response = requests.put(
        url_nifi_api + f"processors/{processor_id}/run-status",
        headers=header,
        data=payload,
        verify=False
    )
    #urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return response

def get_processor_state(url_nifi_api: str, processor_id: str, token=None):
    """
    Gets and returns a single processor state.
    Makes use of the REST API 'processors/{processor_id}/state'.
    :param url_nifi_api: String
    :param processor_id: String
    :param token: JWT access token
    :returns: JSON object processor's state
    """

    # Authorization header
    if token is None:
        header = {"Content-Type": "application/json"}
    else:
        header = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(token),
        }

    # GET processor and parse to JSON
    response = requests.get(
        url_nifi_api + f"processors/{processor_id}/state", headers=header
    )
    return json.loads(response.content)

def parse_state(json_obj, state_key: str):
    
    states = json_obj["componentState"]["localState"]["state"]
    for state in states:
        if state["key"] == state_key:
            value = state["value"]
            return value
    raise ValueError(f"Could not find {state_key} ")



def collectFile1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "3bfcc224-0185-1000-41b9-52f6de1c127d" 
    access_payload = {"username" : "gildas","password" : "***"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
    pause(70)
    response = update_processor_status(processor_id,"STOPPED",token,url_nifi_api) 

def collectFile2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "3caf2f1c-0185-1000-d6a1-172c6002053d"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
    pause(70)
    response = update_processor_status(processor_id,"STOPPED",token,url_nifi_api) 

def updateAttribute1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "3fa17b01-0185-1000-e9f9-ed41fd55947b"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    get_processor(url_nifi_api, processor_id, token)
    update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def updateAttribute2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "418398a3-0185-1000-9820-441fe5ac4453"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    get_processor(url_nifi_api, processor_id, token)
    update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def putFile1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "3c20085a-0185-1000-e44a-81beaec69ec7"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    get_processor(url_nifi_api, processor_id, token)
    update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
    pause(80)

def putFile2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "41850f56-0185-1000-08da-a9a14950dd31"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    get_processor(url_nifi_api, processor_id, token)
    update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
    pause(80)

def publishFile1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "510e644f-0185-1000-3a16-c5bf7f101708"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    get_processor(url_nifi_api, processor_id, token)
    update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def publishFile2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "632c0bad-0185-1000-b0a9-a3156f05ee35"
    access_payload = {"username" : "gildas","password" : "KsadligVIC07@"}

    token = get_token(url_nifi_api, access_payload)
    get_processor(url_nifi_api, processor_id, token)
    update_processor_status(processor_id,"RUNNING",token,url_nifi_api)


