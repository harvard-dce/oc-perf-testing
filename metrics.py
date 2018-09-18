import arrow
import boto3
import pyhorn

METRIC_NAMESPACE = 'OCWorkflows'

def publish_workflow_metrics(wf_id, oc_cluster, oc_base_url, api_user, api_pass):
    oc = pyhorn.MHClient(oc_base_url, api_user, api_pass)
    cw = boto3.client('cloudwatch')
    wf = oc.workflow(wf_id)
    ops = wf.operations

    wf_completed = arrow.get(ops[-1].completed / 1000)

    cw.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                'MetricName': 'workflow_duration',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': oc_cluster
                    },
                    {
                        'Name': 'WorkflowTitle',
                        'Value': wf.title
                    }
                ],
                'Timestamp': wf_completed.datetime,
                'Value': wf.duration,
                'Unit': 'Milliseconds'
            }
        ]
    )
