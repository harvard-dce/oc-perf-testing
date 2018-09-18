import re
import json
import gzip
import base64
from os import getenv
from .metrics import publish_workflow_metrics

OC_ADMIN_IP = getenv('OC_ADMIN_IP')
OC_CLUSTER = getenv('OC_CLUSTER')
COMPLETED_WORKFLOW_STATES = ['FAILED','SUCCEEDED']

msg_regex = re.compile("Workflow (\d+) state set to (\w+)")

def handler(event, ctx):
    print("processing subscribed events")

    # log event data comes gzipped & base64 decoded
    raw_data = event['awslogs']['data']
    compressed_payload = base64.b64decode(raw_data)
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_data = json.loads(uncompressed_payload)

    print(json.dumps(log_data, indent=2))

    for event in log_data['logEvents']:

        event_msg = event['message']
        event_ts = event['timestamp']

        m = msg_regex.search(event_msg)
        if m is None:
            continue

        wf_id, wf_state = m.groups()
        print("Workflow {} is in state {}".format(wf_id, wf_state))

        if wf_state not in COMPLETED_WORKFLOW_STATES:
            continue

        print("Publish metrics for completed workflow {}".format(wf_id))
        publish_workflow_metrics(wf_id, OC_ADMIN_IP)

