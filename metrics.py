import arrow
import boto3
from pyhorn import MHClient


class CWPublisher:

    metric_namespace = 'OCWorkflows'

    def __init__(self, oc_cluster, oc_admin_ip, api_user, api_pass, timeout=30):
        self.oc_cluster = oc_cluster
        self.cw = boto3.client('cloudwatch')
        self.oc = MHClient('http://' + oc_admin_ip, api_user, api_pass, timeout=timeout)

    def publish_wf_duration_metric(self, wf):
        completed_ops = [x for x in wf.operations if hasattr(x, 'completed')]

        wf_completed = arrow.get(completed_ops[-1].completed / 1000)

        self.cw.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': 'workflow_duration',
                    'Dimensions': [
                        {
                            'Name': 'Cluster',
                            'Value': self.oc_cluster
                        },
                        {
                            'Name': 'WorkflowTitle',
                            'Value': wf.title
                        }
                    ],
                    'Timestamp': arrow.utcnow().datetime,
                    'Value': wf.duration(),
                    'Unit': 'Milliseconds'
                }
            ]
        )

    def publish_op_duration_metric(self, op_type, duration):

        metric_name = op_type + '-duration'

        self.cw.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name': 'Cluster',
                            'Value': self.oc_cluster
                        }
                    ],
                    'Timestamp': arrow.utcnow().datetime,
                    'Value': duration,
                    'Unit': 'Milliseconds'
                }
            ]
        )


    def fetch_workflow(self, wf_id):
        return self.oc.workflow(wf_id)


    def fetch_workflows(self, start_date, end_date, count=None):

        if count is None:
            count = 100000

        start_date = arrow.get(start_date)
        if end_date is None:
            end_date = start_date.replace(days=+1)
        else:
            end_date = arrow.get(end_date)

        params = {
            'count': count,
            'fromdate': start_date.format('YYYY-MM-DDTHH:mm:ss') + 'Z',
            'todate': end_date.format('YYYY-MM-DDTHH:mm:ss') + 'Z'
        }

        return self.oc.workflows(**params)
