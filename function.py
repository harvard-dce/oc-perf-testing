import re
import json
import gzip
import base64
from os import getenv
from metrics import CWPublisher

OC_ADMIN_IP = getenv('OC_ADMIN_IP')
OC_API_USER = getenv('OC_API_USER')
OC_API_PASS = getenv('OC_API_PASS')
OC_CLUSTER = getenv('OC_CLUSTER')
COMPLETED_WORKFLOW_STATES = ['FAILED','SUCCEEDED']

wf_regex = re.compile(r'Workflow (\d+) state set to (\w+)')
op_regex = re.compile(r'Operation ([a-z\-]+) took (\d+)')


def handler(event, ctx):
    print("processing subscribed events")

    # log event data comes gzipped & base64 decoded
    raw_data = event['awslogs']['data']
    compressed_payload = base64.b64decode(raw_data)
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_data = json.loads(uncompressed_payload)

    publisher = CWPublisher(OC_CLUSTER, OC_ADMIN_IP, OC_API_USER, OC_API_PASS)

    for event in log_data['logEvents']:

        event_msg = event['message']
        event_ts = event['timestamp']

        wf_match = wf_regex.search(event_msg)
        op_match = op_regex.search(event_msg)

        if wf_match:
            wf_id, wf_state = wf_match.groups()
            print("Workflow {} is in state {}".format(wf_id, wf_state))

            if wf_state not in COMPLETED_WORKFLOW_STATES:
                continue
            print("Publishing metrics for completed workflow {}".format(wf_id))
            publisher.publish_wf_duration_metric(wf_id)
        elif op_match:
            op_type, duration_ms = op_match.groups()
            publisher.publish_op_duration_metric(op_type, int(duration_ms))
        else:
            continue



