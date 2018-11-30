import arrow
from pyhorn import MHClient


class WorkflowMetrics:

    def __init__(self, admin_ip, engage_ip, api_user, api_pass, timeout=30):
        self.oc_admin = MHClient('http://' + admin_ip, api_user, api_pass, timeout=timeout)
        self.oc_engage = MHClient('http://' + engage_ip, api_user, api_pass, timeout=timeout)

    def summary(self, days_ago=7):

        end_date = arrow.utcnow()
        start_date = end_date.replace(days=-days_ago)

        params = {
            'count': 999999,
            'fromdate': start_date.format('YYYY-MM-DDTHH:mm:ss') + 'Z',
            'todate': end_date.format('YYYY-MM-DDTHH:mm:ss') + 'Z'
        }

        workflows = self.oc_admin.workflows(**params)
        for wf in workflows:
            continue
        return

    def is_1x(self):
        info = self.oc_admin.me()
        return "user" not in info

    def get_workflow_video_length(self, wf):
        if self.is_1x():
            return self._get_workflow_video_length_1x(wf)
        else:
            mpid = wf.mediapackage.id
