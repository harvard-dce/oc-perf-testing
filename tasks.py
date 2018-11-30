import sys
import csv
import json
import socket
from invoke import task, Collection
from invoke.exceptions import Exit
from os import getenv as env
from os.path import join, dirname
from dotenv import load_dotenv
from metrics import WorkflowMetrics
from fabric import Connection
from iperf3 import TestResult

load_dotenv(join(dirname(__file__), '.env'))

AWS_PROFILE = env('AWS_PROFILE')
AWS_DEFAULT_REGION = env('AWS_DEFAULT_REGION', 'us-east-1')

if AWS_PROFILE is not None:
    import boto3
    boto3.setup_default_session(profile_name=AWS_PROFILE)


def getenv(var, required=True):
    val = env(var)
    if required and val is None:
        raise Exit("{} not defined".format(var))
    return val


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""


@task
def profile_check(ctx):
    if AWS_PROFILE is None:
        print("You do not have 'AWS_PROFILE' set in your environment. "
              "This task will run using your default AWS account/credentials. "
              )
        ok = input('Is this what you want? [y/N] ').lower().strip().startswith('y')
        if not ok:
            raise Exit("Aborting")


@task(pre=[profile_check])
def fio(ctx, runtime=30, data_size=10, app_name="opencast"):

    c = Connection(get_instance_ip(ctx, 'admin1'))
    fieldnames = ['path', 'rw', 'runtime', 'data_size', 'type', 'size', 'KB/s', 'iops', 'clat_usec_mean']
    writer = csv.DictWriter(sys.stdout, fieldnames)
    writer.writeheader()

    cmd_template = (
        "fio --runtime={} --time_based --numjobs=8 --name randrw --direct 1 "
        "--ioengine libaio --bs 16k --rwmixread 70 --size {}G --group_reporting "
        "--rw randrw --filename {} --output-format=json"
    )

    paths = [x.format(app_name) for x in ['/var/{}-workspace', '/var/{}']]
    for path in paths:
        filename = path + "/fio.tmp"

        cmd = "df -hT | awk '{ if ($7 == \"" + path + "\") print $2\" \"$3 }'"
        fstype, size = c.sudo(cmd, hide=True, pty=True).stdout.strip().split()

        cmd = cmd_template.format(runtime, data_size, filename)
        res = c.sudo(cmd, hide=True, pty=True)
        data = json.loads(res.stdout)['jobs'][0]

        for rw in ['read', 'write']:
            writer.writerow({
                'path': path,
                'rw': rw,
                'runtime': runtime,
                'data_size': str(data_size) + "G",
                'type': fstype,
                'size': size,
                'KB/s': data[rw]['bw'],
                'iops': data[rw]['iops'],
                'clat_usec_mean': data[rw]['clat']['mean']
            })


@task(pre=[profile_check])
def iperf3(ctx, server="admin1", client="workers1", parallel=1):

    server_ip = get_instance_ip(ctx, server)
    client_ip = get_instance_ip(ctx, client, private=True)

    fieldnames = ['server',
                  'client',
                  'server_driver',
                  'server_type',
                  'client_driver',
                  'client_type',
                  'parallel',
                  'Mbps',
                  'server_cpu',
                  'client_cpu'
                  ]
    writer = csv.DictWriter(sys.stdout, fieldnames)
    writer.writeheader()

    server_pid = None

    try:
        # get connection to server host
        server_c = Connection(server_ip, connect_timeout=5)

        # get driver/version info from server
        server_driver = server_c.run("ethtool -i eth0 | grep '^driver:' | awk '{ print $2 }'", hide=True).stdout.strip()
        server_version = server_c.run("ethtool -i eth0 | grep '^version:' | awk '{ print $2 }'", hide=True).stdout.strip()
        server_type = server_c.run("ec2metadata --instance-type", hide=True).stdout.strip()

        # 2. run iperf3 in daemon mode & save pid
        server_c.run("iperf3 -s -D", hide=True, pty=True)
        server_pid = server_c.run("pidof -s iperf3", hide=True).stdout.strip()

        client_c = Connection(client_ip, gateway=server_c, connect_timeout=5)

        # get driver/version info from client
        client_driver = client_c.run("ethtool -i eth0 | grep '^driver:' | awk '{ print $2 }'", hide=True).stdout.strip()
        client_version = client_c.run("ethtool -i eth0 | grep '^version:' | awk '{ print $2 }'", hide=True).stdout.strip()
        client_type = client_c.run("ec2metadata --instance-type", hide=True).stdout.strip()

        cmd = "iperf3 -J -c {}".format(server_ip)

        if parallel > 1:
            cmd += " -P {}".format(int(parallel))

        try:
            result = client_c.run(cmd, pty=True, hide=True, warn=True).stdout
            tr = TestResult(result)
            writer.writerow({
                'server': server,
                'client': client,
                'server_driver': server_driver + "/" + server_version,
                'server_type': server_type,
                'client_driver': client_driver + "/" + client_version,
                'client_type': client_type,
                'parallel': parallel,
                'Mbps': tr.sent_Mbps,
                'server_cpu': tr.remote_cpu_total,
                'client_cpu': tr.local_cpu_total
            })

        except socket.timeout:
            print("Connection timed out after 5s to {}".format(conn.host))
        except socket.gaierror as e:
            print(str(e))

    finally:
        if server_pid is not None:
            print("Stopping iperf3 server")
            server_c.run("kill {}".format(server_pid))


@task
def workflows(ctx, days_ago=7):
    oc_api_user = getenv('OC_API_USER')
    oc_api_pass = getenv('OC_API_PASS')
    oc_admin = get_instance_ip(ctx, 'admin1')
    oc_engage = get_instance_ip(ctx, 'engage1')
    wf_metrics = WorkflowMetrics(oc_admin, oc_engage, oc_api_user, oc_api_pass)
    wf_metrics.summary(days_ago)
    return

#=============================================================================#


def get_instance_ip(ctx, hostname, private=False):

    cmd = ("aws {} opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].StackId\" "
           "--output text").format(profile_arg(), getenv('OC_CLUSTER'))

    opsworks_stack_id = ctx.run(cmd, hide=True).stdout.strip()

    if not private:
        cmd = ("aws {} opsworks describe-instances --stack-id {} "
               "--query \"Instances[?Hostname=='{}'].[ElasticIp,Status]\" "
               "--output text").format(profile_arg(), opsworks_stack_id, hostname)

        ip, status = ctx.run(cmd, hide=True).stdout.strip().split()
    else:
        cmd = ("aws {} opsworks describe-instances --stack-id {} "
               "--query \"Instances[?Hostname=='{}'].[Ec2InstanceId,Status]\" "
               "--output text").format(profile_arg(), opsworks_stack_id, hostname)

        instance_id, status = ctx.run(cmd, hide=True).stdout.strip().split()

        cmd = ("aws {} ec2 describe-instances --instance-ids {} "
               "--query \"Reservations[].Instances[].PrivateIpAddress\" "
               "--output text").format(profile_arg(), instance_id)

        ip = ctx.run(cmd, hide=True).stdout.strip()

    if status != "online":
        print('\033[31m' + 'WARNING: {} instance is not online'.format(hostname))
        print('\033[30m')

    return ip


@task
def locust(ctx):
    pass

ns = Collection()

perf_ns = Collection('perf')
perf_ns.add_task(fio)
perf_ns.add_task(iperf3)
perf_ns.add_task(workflows)
perf_ns.add_task(locust)
ns.add_collection(perf_ns)


