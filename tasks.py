import sys
import csv
import json
import shutil
import socket
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, getenv as env
from os.path import join, dirname, exists
from dotenv import load_dotenv
from metrics import CWPublisher
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


def stack_tags():
    tags = "Key=cfn-stack,Value={}".format(getenv('STACK_NAME'))
    extra_tags = getenv("STACK_TAGS")
    if extra_tags is not None:
        tags += " " + extra_tags
    return "--tags {}".format(tags)


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
def create(ctx):
    vpc_subnet_id, vpc_sg_id = vpc_components(ctx)
    oc_admin_ip = get_instance_ip(ctx, 'admin1')
    params = {
        'VpcSubnetId': vpc_subnet_id,
        'VpcSecurityGroupId': vpc_sg_id,
        'CloudwatchLogGroup': "{}_opencast".format(getenv('OC_CLUSTER')),
        'CodeBucket': getenv('CODE_BUCKET'),
        'OCAdminIp': oc_admin_ip,
        'OCCluster': getenv('OC_CLUSTER'),

    }
    __create_or_update(ctx, "create", params)
    __wait_for(ctx, "create")


#@task(pre=[profile_check])
#def update(ctx, stack_name):
#    __create_or_update(ctx, "update")
#    __wait_for(ctx, "update", stack_name)


@task(pre=[profile_check])
def update_lambda(ctx):
    __package(ctx)
    function_name = getenv('STACK_NAME') + "-function"
    s3_key = "oc-workflow-metrics/" + getenv('STACK_NAME') + "/function.zip"
    cmd = ("aws {} lambda update-function-code "
            "--function-name {} --s3-bucket {} --s3-key {}") \
            .format(profile_arg(), function_name, getenv('CODE_BUCKET'), s3_key)
    ctx.run(cmd)

@task(pre=[profile_check])
def delete(ctx):
    cmd = "aws {} cloudformation delete-stack --stack-name {}"\
          .format(profile_arg(), getenv('STACK_NAME'))
    ctx.run(cmd)
    __wait_for(ctx, "delete")


@task(pre=[profile_check])
def harvest(ctx, start_date, end_date=None, count=None):
    oc_admin_ip = get_instance_ip(ctx, 'admin1')
    publisher = CWPublisher(
        getenv('OC_CLUSTER'),
        oc_admin_ip,
        getenv('OC_API_USER'),
        getenv('OC_API_PASS')
    )
    for wf in publisher.fetch_workflows(start_date, end_date, count):
        publisher.publish_workflow_metrics(wf)


@task
def fio(ctx, runtime=30, data_size=10):

    c = Connection(get_instance_ip(ctx, 'admin1'))
    fieldnames = ['path', 'rw', 'runtime', 'data_size', 'type', 'size', 'KB/s', 'iops', 'clat_usec_mean']
    writer = csv.DictWriter(sys.stdout, fieldnames)
    writer.writeheader()

    cmd_template = (
        "fio --runtime={} --time_based --numjobs=8 --name randrw --direct 1 "
        "--ioengine libaio --bs 16k --rwmixread 70 --size {}G --group_reporting "
        "--rw randrw --filename {} --output-format=json"
    )

    for path in ['/var/opencast-workspace', '/var/opencast']:
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


@task
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

#=============================================================================#


def stack_exists(ctx):
    cmd = "aws {} cloudformation describe-stacks --stack-name {}"\
          .format(profile_arg(), getenv('STACK_NAME'))
    res = ctx.run(cmd, hide=True, warn=True, echo=False)
    return res.exited == 0


def vpc_components(ctx):

    cmd = ("aws opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].VpcId\" "
           "--output text").format(getenv('OC_CLUSTER'))
    vpc_id = ctx.run(cmd, hide=True).stdout.strip()

    if vpc_id == "":
        raise Exit("Can't find a cluster named '{}'".format(getenv('OC_CLUSTER')))

    cmd = ("aws {} ec2 describe-subnets --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=PrivateSubnet*' " 
           "--query 'Subnets[0].SubnetId' --output text") \
        .format(profile_arg(), vpc_id)

    subnet_id = ctx.run(cmd, hide=True).stdout.strip()

    cmd = ("aws {} ec2 describe-security-groups --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=OpsworksLayerSecurityGroupCommon' "
           "--query 'SecurityGroups[0].GroupId' --output text") \
        .format(profile_arg(), vpc_id)

    sg_id = ctx.run(cmd, hide=True).stdout.strip()

    return subnet_id, sg_id


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


def cfn_cmd_params(params):
    return " ".join(
        "ParameterKey={},ParameterValue={}".format(k, v)
        for k, v in params.items()
    )


def __create_or_update(ctx, op, params):

    if op == "create" and stack_exists(ctx):
        raise Exit("Stack {} already exists!".format(getenv('STACK_NAME')))

    cmd_params = cfn_cmd_params(params)

    __package(ctx)

    cmd = ("aws {} cloudformation {}-stack "
           "--capabilities CAPABILITY_NAMED_IAM "
           "--stack-name {} "
           "--template-body file://template.yml "
           "--parameters {}"
        ).format(
            profile_arg(),
            op,
            getenv('STACK_NAME'),
            cmd_params
        )
    ctx.run(cmd)


def __package(ctx):

    req_file = join(dirname(__file__), 'function-requirements.txt')
    zip_path = join(dirname(__file__), 'dist/function.zip')
    build_path = join(dirname(__file__), 'dist')
    module_path = join(dirname(__file__), 'function.py')
    module_dist_path = join(build_path, 'function.py')

    if exists(build_path):
        shutil.rmtree(build_path)

    if exists(req_file):
        ctx.run("pip install --no-cache-dir -U -r {} -t {}".format(req_file, build_path))
    else:
        ctx.run("mkdir {}".format(build_path))

    try:
        print("symlinking {} to {}".format(module_path, module_dist_path))
        symlink(module_path, module_dist_path)
    except FileExistsError:
        pass

    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path))

    ctx.run("aws {} s3 cp {} s3://{}/oc-workflow-metrics/{}/function.zip" \
        .format(
            profile_arg(),
            zip_path,
            getenv('CODE_BUCKET'),
            getenv('STACK_NAME')
        )
    )


def __wait_for(ctx, op):
    wait_cmd = ("aws {} cloudformation wait stack-{}-complete "
                "--stack-name {}").format(profile_arg(), op, getenv('STACK_NAME'))
    print("Waiting for stack {} to complete...".format(op))
    ctx.run(wait_cmd)
    print("Done")


ns = Collection()

cfn_ns = Collection('cfn')
cfn_ns.add_task(create)
cfn_ns.add_task(delete)
cfn_ns.add_task(update_lambda)
#ns.add_task(update)
ns.add_collection(cfn_ns)

pub_ns = Collection('pub')
pub_ns.add_task(harvest)
ns.add_collection(pub_ns)

perf_ns = Collection('perf')
perf_ns.add_task(fio)
perf_ns.add_task(iperf3)
ns.add_collection(perf_ns)


