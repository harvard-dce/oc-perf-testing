import re
import arrow
import pandas as pd
from time import sleep
from os import getenv as env
from os.path import basename, abspath
from invoke.exceptions import Exit
from lxml.etree import fromstring
from splinter import Browser
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def getenv(var, required=True):
    val = env(var)
    if required and val is None:
        raise Exit("{} not defined".format(var))
    return val


def profile_arg():
    profile = env('AWS_PROFILE')
    if profile is not None:
        return "--profile {}".format(profile)
    return ""


def extract_duration_from_payload(row):
    try:
        payload = row['payload']
        # some payloads have two (?!?) xml docs separated by '###';
        # duration should be the same, so just take the first one
        for doc in payload.split('###'):
            root = fromstring(doc.encode('utf-8'))
            duration = root.find('{*}duration')
            if hasattr(duration, 'text'):
                return int(duration.text)
        return pd.np.NaN
    except Exception as e:
        print("Error for row {}: {}".format(row, e))
        print(row['payload'])


def get_operations_query(start, end, days_ago, jobs_table):

    start = start and arrow.get(start) or arrow.utcnow().replace(days=-days_ago)
    end = end and arrow.get(end) or arrow.utcnow()

    query = """
        SELECT
            id, status, payload, run_time, queue_time,
            operation, date_created, date_started, date_completed
        FROM {}
        WHERE
            date_started BETWEEN "{}" AND "{}"
            AND operation NOT LIKE "START_%"
        """ \
        .format(jobs_table, start, end)

    return re.compile(r'\s+').sub(' ', query.strip())


def get_instance_ip_public_dns(ctx, hostname, private=False):

    opsworks_stack_id = get_stack_id(ctx)

    if not private:
        cmd = ("aws {} opsworks describe-instances --stack-id {} "
               "--query \"Instances[?Hostname=='{}'].[ElasticIp,Status,PublicDns]\" "
               "--output text").format(profile_arg(), opsworks_stack_id, hostname)

        ip, status, host = ctx.run(cmd, hide=True).stdout.strip().split()
    else:
        cmd = ("aws {} opsworks describe-instances --stack-id {} "
               "--query \"Instances[?Hostname=='{}'].[Ec2InstanceId,Status]\" "
               "--output text").format(profile_arg(), opsworks_stack_id, hostname)

        instance_id, status = ctx.run(cmd, hide=True).stdout.strip().split()

        cmd = ("aws {} ec2 describe-instances --instance-ids {} "
               "--query \"Reservations[].Instances[].PrivateIpAddress\" "
               "--output text").format(profile_arg(), instance_id)

        ip = ctx.run(cmd, hide=True).stdout.strip()
        host = None

    if status != "online":
        print('\033[31m' + 'WARNING: {} instance is not online'.format(hostname))
        print('\033[30m')

    return ip, host


def get_stack_id(ctx):

    cmd = ("aws {} opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].StackId\" "
           "--output text").format(profile_arg(), getenv('OC_CLUSTER'))

    return ctx.run(cmd, hide=True).stdout.strip()


def get_app_shortname(ctx, stack_id=None):

    if stack_id is None:
        stack_id = get_stack_id(ctx)

    cmd = ("aws {} opsworks describe-apps "
           "--stack-id {} --query \"Apps[0].Shortname\" "
           "--output text").format(profile_arg(), stack_id)

    return ctx.run(cmd, hide=True).stdout.strip()


def is_1x_stack(ctx):

    return get_app_shortname(ctx) == "matterhorn"


class OCBrowser:

    login_css = '.submit'

    def __init__(self, host, headless=True):
        self.browser = Browser('chrome', headless=headless)
        self.host = host

        # log in
        self.browser.visit('http://' + host)
        self.browser.fill('j_username', getenv('OC_ADMIN_USER'))
        self.browser.fill('j_password', getenv('OC_ADMIN_PASS'))
        self.browser.find_by_css(self.login_css).click()

    def upload_video(self, video, series):
        self.browser.find_by_text('Add event').click()
        sleep(1)

        # == wizard screen #1 ==#
        self.browser.find_by_text('No option').first.click()
        self.browser.find_by_css('a.chosen-single').click()
        self.browser.find_by_css('.chosen-search-input').first.fill(series)
        self.browser.find_by_css('li.active-result').click()
        editable = self.browser.find_by_css('td.editable')
        # type num is at idx 1
        editable[1].find_by_tag('div').click()
        editable[1].find_by_tag('div').find_by_tag('input').fill('L01')
        # title is idx 2
        title = 'oc-perf-testing upload {}'.format(basename(video))
        editable[2].find_by_tag('div').click()
        editable[2].find_by_tag('div').find_by_tag('input').fill(title)
        # click off last input to activate next button
        editable[3].find_by_tag('div').click()
        self.browser.find_by_css('.submit').click()
        sleep(1)

        # == wizard screen #2 ==#
        self.browser.find_by_css('input#track_multi').fill(abspath(video))
        self.browser.find_by_css('.submit').click()
        sleep(1)

        # == wizard screen #3 ==#
        self.browser.find_by_css('.submit').click()
        sleep(1)

        # == wizard screen #4 ==#
        workflow_selector_div = self.browser.find_by_xpath('//div[header="Select workflow"]').first
        workflow_selector_div.find_by_tag('span').click()
        workflow_selector_div.find_by_css('li.active-result[data-option-array-index="2"]').click()
        self.browser.find_by_id('publishLive').click()
        self.browser.find_by_css('.submit').click()
        sleep(1)

        # == wizard screen #5 ==#
        self.browser.find_by_css('.submit').click()
        sleep(1)

        # == wizard screen #6 ==#
        self.browser.find_by_css('.submit').click()
        sleep(1)

        upload_notify = self.browser.find_by_css('div[data-message="NOTIFICATIONS.EVENTS_UPLOAD_STARTED"]')
        WebDriverWait(self.browser.driver, timeout=3600, poll_frequency=10).until(
            EC.staleness_of(upload_notify._element)
        )

    def add_series(self):
        series_doc_url = 'http://' + self.host + '/docs.html?path=/series'
        self.browser.visit(series_doc_url)
        self.browser.execute_script("$('div.hidden_form').show()")
        form = self.browser.find_by_css('form[action="/series/"]')
        form.find_by_name('series').fill(CATALOG_XML.strip())
        form.find_by_name('acl').fill(ACL_XML.strip())
        form._element.submit()


class MHBrowser(OCBrowser):

    login_css = 'input[name="submit"]'

    def upload_video(self, video, series):
        # go to the upload page
        self.browser.find_by_id('adminlink').click()
        self.browser.find_by_id('uploadButton').click()

        # set the series
        self.browser.find_by_css('input#dceTermFilter')._element.clear()
        self.browser.find_by_css('input#seriesSelect').fill(series)
        self.browser.find_by_css('ul.ui-autocomplete a').first.click()

        # fill in other req metadata
        title = 'oc-perf-testing upload {}'.format(basename(video))
        self.browser.find_by_css('input#title').fill(title)
        self.browser.find_by_css('input#type').fill('L01')
        self.browser.find_by_css('input#publisher').fill('foo@example.edu')

        # set the upload file
        iframe = self.browser.find_by_css('iframe.uploadForm-container').first
        self.browser.driver.switch_to.frame(iframe._element)
        self.browser.find_by_css('input#file').fill(abspath(video))
        self.browser.driver.switch_to.default_content()

        # set the workflow options
        select = Select(self.browser.find_by_id('workflowSelector')._element)
        select.select_by_value('DCE-auto-publish')
        sleep(1)
        self.browser.find_by_css('input#epiphanUpload').check()

        # upload and wait
        self.browser.find_by_css('button#submitButton').click()

        upload_progress = self.browser.find_by_css('div#progressStage')
        WebDriverWait(self.browser.driver, timeout=3600, poll_frequency=10).until(
            EC.invisibility_of_element(upload_progress._element)
        )



CATALOG_XML = '''
<?xml version="1.0" encoding="UTF-8"?>
<dublincore xmlns="http://www.opencastproject.org/xsd/1.0/dublincore/"
        xmlns:dcterms="http://purl.org/dc/terms/"
        xmlns:oc="http://www.opencastproject.org/matterhorn/">
    <dcterms:creator>Harvard Extension School</dcterms:creator>
    <dcterms:contributor>Henry H. Leitner</dcterms:contributor>
    <dcterms:description>http://extension.harvard.edu</dcterms:description>
    <dcterms:subject>OC-PERF-TESTING E-19997</dcterms:subject>
    <dcterms:identifier>20190119997</dcterms:identifier>
    <dcterms:language>eng</dcterms:language>
    <dcterms:publisher>Harvard University, DCE</dcterms:publisher>
    <oc:annotation>true</oc:annotation>
    <dcterms:title>Opencast Performance Testing</dcterms:title>
</dublincore>
'''

ACL_XML = '''
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<acl xmlns="http://org.opencastproject.security">
    <ace>
        <role>ROLE_ADMIN</role>
        <action>read</action>
        <allow>true</allow>
    </ace>
    <ace>
        <role>ROLE_ADMIN</role>
        <action>write</action>
        <allow>true</allow>
    </ace>
    <ace>
        <role>ROLE_ADMIN</role>
        <action>delete</action>
        <allow>true</allow>
    </ace>
    <ace>
        <role>ROLE_ADMIN</role>
        <action>analyze</action>
        <allow>true</allow>
    </ace>
    <ace>
        <role>ROLE_ANONYMOUS</role>
        <action>read</action>
        <allow>true</allow>
    </ace>
</acl>
        '''
