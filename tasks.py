import sys
import csv
import json
import socket
from io import StringIO
from os import getenv as env
from invoke.exceptions import Exit
from contexttimer import Timer
from invoke import task, Collection
from os.path import join, dirname
from dotenv import load_dotenv
from fabric import Connection
from iperf3 import TestResult
from concurrent.futures import ThreadPoolExecutor, as_completed

from util import *

load_dotenv(join(dirname(__file__), '.env'))


@task
def profile_check(ctx):
    profile = env('AWS_PROFILE')
    if profile is None:
        print("You do not have '$AWS_PROFILE' set in your environment. "
              "This task will run using your default AWS account/credentials. "
              )
        ok = input('Is this what you want? [y/N] ').lower().strip().startswith('y')
        if not ok:
            raise Exit("Aborting")


@task(
    help={
        'runtime': 'How long (in seconds) to run the io test. Default is 30.',
        'data-size': 'Total size in (in GB) of io to transfer. Default is 10.'
    },
    pre=[profile_check]
)
def fio(ctx, runtime=30, data_size=10):
    """
     Perform io benchmarking using fio
    """
    app_name = get_app_shortname(ctx)
    admin_ip, _ = get_instance_ip_public_dns(ctx, 'admin1')
    c = Connection(admin_ip)
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


@task(
    help={
        'server': 'Shortname of the node that will act as the iperf3 server. Default is "admin1".',
        'client': 'Shortname of the node that will act as the iperf3 client. Default is "workers1".',
        'parallel': 'Number of parallel client streams to run. Default is 1.'
    },
    pre=[profile_check]
)
def iperf3(ctx, server="admin1", client="workers1", parallel=1):
    """
    Perform network benchmarking between nodes in the cluster using iperf3
    """
    server_ip, _ = get_instance_ip_public_dns(ctx, server)
    client_ip, _ = get_instance_ip_public_dns(ctx, client, private=True)

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
            print("Connection timed out after 5s to {}".format(client_c.host))
        except socket.gaierror as e:
            print(str(e))

    finally:
        if server_pid is not None:
            print("Stopping iperf3 server")
            server_c.run("kill {}".format(server_pid))


@task(
    help={
        'start': 'Date string representing the start of the time period to query.',
        'end': 'Date string representing the end of the time period to query.',
        'days-ago': 'Set the query time range to this many days ago from now. Default is 7'
    },
    pre=[profile_check]
)
def operations(ctx, start=None, end=None, days_ago=7):
    """
    Extract Opencast workflow operation performance data
    """
    admin_ip, _ = get_instance_ip_public_dns(ctx, 'admin1')
    c = Connection(admin_ip)

    db_name = is_1x_stack(ctx) and "matterhorn" or "opencast"
    jobs_table = db_name == "matterhorn" and "mh_job" or "oc_job"
    query = get_operations_query(start, end, days_ago, jobs_table)

    cmd = "sudo -H mysql -B -e '{}' {}".format(query, db_name)
    res = c.run(cmd, hide=True, pty=True)

    df = pd.read_csv(StringIO(res.stdout), sep="\t")

    # make these date objects in case we want to do timeseries stuff
    date_cols = [x for x in df.columns if x.startswith('date_')]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    # anything without a payload we can't get a duration, so not useful
    df = df.loc[df.payload.str.startswith('<?xml', na=False)]

    # get duration from payload and calc perf_ratio
    df['duration'] = df.apply(extract_duration_from_payload, axis=1)
    df['perf_ratio'] = round(df['run_time'] / df['duration'], 3)

    grouped = df.groupby('operation')
    summary = grouped['run_time', 'queue_time', 'perf_ratio'].agg(['count', 'mean']).round(3)
    print(summary.to_csv())


@task
def locust(ctx):
    """
    Not implemented
    """
    pass


@task(
    help={
        'headless': 'Run the chrome in "headless" mode. Default true.'
    },
    pre=[profile_check]
)
def series(ctx, headless=True):
    """
    Creates a canned test series with title "Opencast Performance Testing"
    """
    _, public_dns = get_instance_ip_public_dns(ctx, 'admin1')
    browser_class = is_1x_stack(ctx) and MHBrowser or OCBrowser
    browser = browser_class(public_dns, headless)
    browser.add_series()
    print("done.")


@task(
    help={
        'video': 'Path to the video to upload. Repeat to upload multiple.',
        'series': 'Series title. This has to match what\'s in the upload form select option.',
        'headless': 'Run the chrome in "headless" mode. Default true.',
        'max-concurrent': 'How many concurrent uploads to allow if uploading multiple. Default is 4'
    },
    pre=[profile_check],
    iterable=['video']
)
def events(ctx, video, series="Opencast Performance Testing", headless=True, max_concurrent=4):
    """
    Upload recording(s) using Selenium
    """
    _, public_dns = get_instance_ip_public_dns(ctx, 'admin1')
    browser_class = is_1x_stack(ctx) and MHBrowser or OCBrowser

    def do_upload(vid, series):
        with Timer() as t:
            try:
                print("uploading {} to {}".format(vid, series))
                browser = browser_class(public_dns, headless)
                browser.upload_video(vid, series)
            finally:
                if browser:
                    browser.browser.quit()
        return t.elapsed

    pool = ThreadPoolExecutor(max_workers=max_concurrent)
    future_to_vid = {
        pool.submit(do_upload, vid, series): vid for vid in video
    }

    for future in as_completed(future_to_vid):
        vid = future_to_vid[future]
        try:
            elapsed_seconds = future.result()
        except Exception as e:
            print('%r generated an exception: %s' % (vid, e))
        else:
            print('%r finished uploading in %d seconds' % (vid, elapsed_seconds))


ns = Collection()
perf_ns = Collection('perf')
perf_ns.add_task(fio)
perf_ns.add_task(iperf3)
perf_ns.add_task(operations)
perf_ns.add_task(locust)
ns.add_collection(perf_ns)
create_ns = Collection('create')
create_ns.add_task(events)
create_ns.add_task(series)
ns.add_collection(create_ns)
