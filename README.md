# oc-perf-testing

This project provides a set of tools for performance testing of Opencast Opsworks
clusters. It should be compatible with both 1x and 5x+ opencast variants. The goal
is to provide a means for generating performance data useful for comparing 
different cluster configurations, e.g. instance types, storage, db and job load
choices.

### Setup

Python 3 is required.

The automated selenium tasks for uploading recordings relies on `chromedriver`, so
you'll need both the Google Chrome browser and the `chromedriver` binary. You can most
likely install it via your system's package manager, e.g. `brew`, `apt-get`, etc.,
or [download directly](https://sites.google.com/a/chromium.org/chromedriver/) and
put it on your path somewhere.

1. To bootstrap the python package dependencies you will first need to have `virtualenv` 
   and `pip-tools` installed: `pip install virtualenv pip-tools`
1. Create and activate a python virtualenv in your preferred way.
1. Run `pip-sync` to install the dependencies from the `requirements.txt` file.
1. Run `invoke -l` to see the list of available tasks and ensure deps are installed correctly.

### Configuration

The tasks get their configuration information from a `.env` file in the root project
directory. Copy the included `example.env` to `.env` and update appropriately.

* `AWS_PROFILE` - necessary if you have multiple accounts set up in your `~/.aws/credentials`
* `OC_CLUSTER` - the name of the Opsworks stack you're working with. Both 1x and 5x+ clusters
  are supported and the tasks have built-in logic for figuring out which is being used.
* `OC_ADMIN_USER` - Opencast admin login user (used by Selenium)
* `OC_ADMIN_PASS` - Opencast admin login password (used by Selenium)

### Tasks

Commands are broken up into two namespaces: `perf` and `create`. The tasks in
the `perf` space collect performance data from the cluster and output csv. The `create`
tasks execute Selenium processes for performing recording uploads.

Tasks are executed via the `invoke` command, e.g. `invoke perf.fio`. The usage info
for each task can be viewed with the `-h` flag, e.g. `invoke -h perf.fio`.

A quick summary of each command:

* `create.events` - Upload recording(s) using Selenium
* `create.series` - Create a simple test series using Selenium
* `perf.fio` - Performs io benchmarking
* `perf.iperf3`- Performs network benchmarking between nodes in the cluster
* `perf.locust` - Web load testing (not yet implemented)
* `perf.operations` - Extract Opencast workflow operation performance data

