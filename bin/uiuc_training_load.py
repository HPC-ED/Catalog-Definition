#!/usr/bin/env python3

import argparse
from collections import Counter
from datetime import datetime, timezone
import globus_sdk
import http.client as httplib
import json
import logging
import logging.handlers
import os
from pid import PidFile
import pwd
import random
import re
import sys
import shutil
import signal
import ssl
from time import sleep
import traceback
from urllib.parse import urlparse

import pdb

# Used during initialization before loggin is enabled
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class Router():
    def __init__(self):
        parser = argparse.ArgumentParser(epilog='File SRC|DEST syntax: file:<file path and name')
        parser.add_argument('daemonaction', nargs='?', choices=('start', 'stop', 'restart'), \
                            help='{start, stop, restart} daemon')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {file, http[s]} (default=file)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {file, analyze, or index} (default=index)')
        parser.add_argument('--daemon', action='store_true', \
                            help='Daemonize execution')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./uiuc_training_load.conf', \
                            help='Configuration file default=./uiuc_training_load.conf')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        # Trace for debugging as early as possible
        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        self.config_file = os.path.abspath(self.args.config)
        try:
            with open(self.config_file, 'r') as file:
                conf=file.read()
        except IOError as e:
            eprint('Error "{}" reading config={}'.format(e, self.config_file))
            sys.exit(1)
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            eprint('Error "{}" parsing config={}'.format(e, self.config_file))
            sys.exit(1)

        if self.config.get('PID_FILE'):
            self.pidfile_path =  self.config['PID_FILE']
        else:
            name = os.path.basename(__file__).replace('.py', '')
            self.pidfile_path = '/var/run/{}/{}.pid'.format(name, name)

    def Setup(self):
        # Initialize log level from arguments, or config file, or default to WARNING
        loglevel_str = (self.args.log or self.config.get('LOG_LEVEL', 'WARNING')).upper()
        loglevel_num = getattr(logging, loglevel_str, None)
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(loglevel_num)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', \
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], \
            when='W6', backupCount=999, utc=True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Initialize stdout, stderr
        if self.args.daemon and 'LOG_FILE' in self.config:
            self.stdout_path = self.config['LOG_FILE'].replace('.log', '.daemon.log')
            self.stderr_path = self.stdout_path
            self.SaveDaemonStdOut(self.stdout_path)
            sys.stdout = open(self.stdout_path, 'wt+')
            sys.stderr = open(self.stderr_path, 'wt+')

        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)

        self.logger.info('Starting program=%s pid=%s, uid=%s(%s)' % \
                     (os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        self.src = {}
        self.dest = {}
        for var in ['uri', 'scheme', 'path', 'display']: # Where <full> contains <type>:<obj>
            self.src[var] = None
            self.dest[var] = None
        self.peak_sleep = 10 * 60        # 10 minutes in seconds during peak business hours
        self.off_sleep = 60 * 60         # 60 minutes in seconds during off hours
        self.max_stale = 24 * 60 * 60    # 24 hours in seconds force refresh
        # These attributes have their own database column
        # Some fields exist in both parent and sub-resources, while others only in one
        # Those in one will be left empty in the other, or inherit from the parent
        self.have_column = ['resource_id', 'info_resourceid',
                            'resource_descriptive_name', 'resource_description',
                            'project_affiliation', 'provider_level',
                            'resource_status', 'current_statuses', 'updated_at']
        default_file = 'file:./uiuc_training.json'

        # Verify arguments and parse compound arguments
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            self.args.src = default_file
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['scheme'], self.src['path']) = (self.args.src[0:idx], self.args.src[idx+1:])
        else:
            (self.src['scheme'], self.src['path']) = (self.args.src, None)
        if self.src['scheme'] not in ['file', 'http', 'https']:
            self.logger.error('Source not {file, http, https}')
            sys.exit(1)
        if self.src['scheme'] in ['http', 'https']:
            if self.src['path'][0:2] != '//':
                self.logger.error('Source URL not followed by "//"')
                sys.exit(1)
            self.src['path'] = self.src['path'][2:]
        self.src['uri'] = self.args.src
        self.src['display'] = self.args.src

        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            self.args.dest = 'index'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['scheme'], self.dest['path']) = (self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['scheme'] = self.args.dest
        if self.dest['scheme'] not in ['file', 'analyze', 'index']:
            self.logger.error('Destination not {file, analyze, index}')
            sys.exit(1)
        self.dest['uri'] = self.args.dest
        if self.dest['scheme'] == 'index':
            self.dest['display'] = '{}@uuid={}'.format(self.dest['scheme'], self.config.get('INDEX'))
        else:
            self.dest['display'] = self.args.dest

        if self.src['scheme'] in ['file'] and self.dest['scheme'] in ['file']:
            self.logger.error('Source and Destination can not both be a {file}')
            sys.exit(1)

        # The affiliations we are processing
#        self.AFFILIATIONS = set(self.config.get('AFFILIATIONS', ['ACCESS', 'XSEDE']))
        
        if self.args.daemonaction == 'start':
            if self.src['scheme'] not in ['http', 'https'] or self.dest['scheme'] not in ['warehouse']:
                self.logger.error('Can only daemonize when source=[http|https] and destination=warehouse')
                sys.exit(1)

        self.logger.info('Source: {}'.format(self.src['display']))
        self.logger.info('Destination: {}'.format(self.dest['display']))
        self.logger.info('Config: {}' .format(self.config_file))
        self.logger.info('Log Level: {}({})'.format(loglevel_str, loglevel_num))
#        self.logger.info('Affiliations: ' + ', '.join(self.AFFILIATIONS))

    def SaveDaemonStdOut(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            file = open(path, 'r')
            lines = file.read()
            file.close()
            if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                newpath = '{}.{}'.format(path, ts)
                self.logger.debug('Saving previous daemon stdout to {}'.format(newpath))
                shutil.copy(path, newpath)
        except Exception as e:
            self.logger.error('Exception in SaveDaemonStdOut({})'.format(path))
        return

    def exit_signal(self, signum, frame):
        self.logger.critical('Caught signal={}({}), exiting with rc={}'.format(signum, signal.Signals(signum).name, signum))
        sys.exit(signum)

    def exit(self, rc):
        if rc:
            self.logger.error('Exiting with rc={}'.format(rc))
        sys.exit(rc)

    def Retrieve_Source(self, url):
        urlp = urlparse(url)
        if not urlp.scheme or not urlp.netloc or not urlp.path:
            self.logger.error('Source URL is not valid: {}'.format(url))
            sys.exit(1)
        if urlp.scheme not in ['http', 'https']:
            self.logger.error('Source URL scheme is not valid: {}'.format(url))
            sys.exit(1)
        if ':' in urlp.netloc:
            (host, port) = urlp.netloc.split(':')
        else:
            (host, port) = (urlp.netloc, '')
        if not port:
            port = '80' if urlp.scheme == 'http' else '443'     # Default is HTTPS/443
        
        headers = {'Content-type': 'application/json',
                    'XA-CLIENT': affiliation,
                    'XA-KEY-FORMAT': 'underscore'}
#        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
#   2022-10-21 JP - figure out later the appropriate level of ssl verification
        ctx = ssl.create_default_context()
        conn = httplib.HTTPSConnection(host=host, port=port, context=ctx)
        conn.request('GET', urlp.path, None , headers)
        self.logger.debug('HTTP GET  {}'.format(url))
        response = conn.getresponse()
        data = response.read()
        self.logger.debug('HTTP RESP {} {} (returned {}/bytes)'.format(response.status, response.reason, len(data)))
        try:
            data_json = json.loads(data)
        except ValueError as e:
            self.logger.error('Response not in expected JSON format ({})'.format(e))
            return(None)
            
        self.logger.debug('Retrieved and parsed {}/bytes from URL'.format(len(results)))
        return(data_json)

    def Analyze_Info(self, data_json):
        return

    def Write_Cache(self, file, data_json):
        data = json.dumps(data_json)
        with open(file, 'w') as my_file:
            my_file.write(data)
            my_file.close()
        self.logger.info('Serialized and wrote {} bytes to file={}'.format(len(data), file))
        return(len(data))

    def Read_Cache(self, file):
        with open(file, 'r') as my_file:
            data = my_file.read()
            my_file.close()
        try:
            data_json = json.loads(data)
            self.logger.info('Read and parsed {}/bytes of json from file={}'.format(len(data), file))
            return(data_json)
        except ValueError as e:
            self.logger.error('Error "{}" parsing file={}'.format(e, file))
            sys.exit(1)

    def Warehouse_Info(self, data_json):
        self.cur = {}   # Existing items
        self.new = {}   # New items
        
        confidential_client = globus_sdk.ConfidentialAppAuthClient(
            client_id=self.config['GLOBUS_CLIENT_ID'], client_secret=self.config['GLOBUS_CLIENT_SECRET']
        )

        scopes = 'urn:globus:auth:scope:search.api.globus.org:ingest urn:globus:auth:scope:search.api.globus.org:search'
        cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, scopes)

        self.client = globus_sdk.SearchClient(authorizer=cc_authorizer, app_name='uiuc_training_load')
        
        query_string = {
            'q': 'user query',
            'filters': [ ],
            'facets': [ ],
            'sort': [ ]
        }
        result = self.client.search(self.config['INDEX'], query_string)
        
#        for item in CiderInfrastructure.objects.all():
#            self.cur[item.cider_resource_id] = item
#        self.logger.debug('Retrieved from database {}/items'.format(len(self.cur)))

        PROVIDER = 'urn:ogf.org:glue2:access-ci.org:resource:cider:infrastructure.organizations:897'
        TYPES = ['activity plan', 'assessment', 'assessment item', 'educator curriculum guide', 'lesson plan', 'physical learning resource', 'recorded lesson', 'supporting document', 'textbook', 'unit plan']
        TYPES_LEN = len(TYPES)
        OUTCOMES = ['Basic understanding', 'Proficient', 'Deep knowledge']
        OUTCOMES_LEN = len(OUTCOMES)
        TARGET = ['Researchers', 'Research groups', 'Research communities', 'Research projects', 'Research networks', 'Research managers', 'Research organizations', 'Students', 'Innovators', 'Providers', 'Funders', 'Research Infrastructure Managers', 'Resource Managers', 'Publishers', 'Other']
        TARGET_LEN = len(TARGET)
        EXPERTISE = ['Beginner', 'Intermediate', 'Advanced', 'All']
        EXPERTISE_LEN = len(EXPERTISE)
        DURATION = [30, 60, 90, 120, 240, 360, 480]
        DURATION_LEN = len(DURATION)
        for p_res in data_json['results']:  # Iterating over parent resources
            try:
                if p_res['EntityJSON']['import_source'] != 'Lynda.com':
                    continue
            except:
                continue
            EJ = p_res['EntityJSON']
            entry = {
                'subject': p_res['ID'],
                'visible_to': ['public'],
#                'entry_id': 'std',
                'content': {
                    'Title': EJ['resource_name'],
                    'Abstract': EJ['resource_description'],
                    'Version_Date': p_res['CreationTime'],
                    'Authors': [],
                    'Language': 'en',
                    'Keywords': ['linda'],
                    'URL': EJ['resource_website'],
                    'Resource_URL_Type': 'URL',
                    'License': EJ['data_license'],
                    'Cost': EJ['cost_description'],
                    'Target_Group': [TARGET[random.randint(0,TARGET_LEN-1)]],
                    'Learning_Resource_Type': TYPES[random.randint(0,TYPES_LEN-1)],
                    'Learning_Outcome': [OUTCOMES[random.randint(0,OUTCOMES_LEN-1)]],
                    'Expertise_Level': [EXPERTISE[random.randint(0,EXPERTISE_LEN-1)]],
                    'Rating': random.randint(0, 50) / 10,
                    'Provider_ID': PROVIDER,
                    'Start_Datetime': datetime.now().isoformat(),
                    'Duration': DURATION[random.randint(0,DURATION_LEN-1)]
                },
            }
            self.Warehouse_Entry(entry, batch=1000)
            
#            if p_res['ID'] not in self.cur:
#                self.client.create_entry(
#                    self.config['INDEX'],
#                    entry
#                    )
#            else:
#                self.client.update_entry(
#                    self.config['INDEX'],
#                    entry
#                    )
            self.new[p_res['ID']] = True

#        for id in self.cur:
#            if id not in self.new:
#                try:
#                    CiderInfrastructure.objects.filter(cider_resource_id=id).delete()
#                    self.stats['Delete'] += 1
#                    self.logger.info('Deleted ID={}'.format(id))
#                except (DataError, IntegrityError) as e:
#                    self.logger.error('{} deleting ID={}: {}'.format(type(e).__name__, id, e.message))
        self.Warehouse_Entry(None, batch=0)
        return(True, '')
 
 # Call with batch=1 to update single entry
 # Otherwise ingest by batch, use batch=0 for final flush
    def Warehouse_Entry(self, entry, batch=100, flush=False):
        if entry and batch == 1:
            try:
                self.client.update_entry(self.config['INDEX'], entry)
                self.STATS.update({'Update'})
            except globus_sdk.GlobusAPIError as e:
                self.logger.error(f'Globus API error: code={e.code}, message={e.message}')
                if e.errors:
                    sub = ';'.join([f'code={sub.code}, message={sub.message}' for sub in e.errors])
                    self.logger.error(f'Globus API sub-errors: {sub}')
                raise e
            return

        if not hasattr(self, 'entry_batch'):            # Define buffer if needed
            self.entry_batch = []
        if entry:                                       # Add entry to buffer
            self.entry_batch.append(entry)
        if len(self.entry_batch) < 1:                   # Nothing in buffer
            return
        if len(self.entry_batch) < batch:               # Buffer entry
            return
        # Buffer is full or flush when batch=0
        ingest_data = {
            'ingest_type': 'GMetaList',
            'ingest_data': {
                'gmeta': self.entry_batch
            }
        }
        try:
            self.client.ingest(self.config['INDEX'], ingest_data)
        except globus_sdk.GlobusAPIError as e:
            self.logger.error(f'Globus API error: code={e.code}, message={e.message}')
            if e.errors:
                sub = ';'.join([f'code={sub.code}, message={sub.message}' for sub in e.errors])
                self.logger.error(f'Globus API sub-errors: {sub}')
            raise e
        self.STATS.update({'Update': len(self.entry_batch)})
        self.logger.debug(f'Updated {len(self.entry_batch)} items')
        self.entry_batch = []
        return
        
    def smart_sleep(self, last_run):
        # This functions sleeps, performs refresh checks, and returns when it's time to refresh
        while True:
            if 12 <= datetime.now(timezone.utc).hour <= 24: # Between 6 AM and 6 PM Central (~12 to 24 UTC)
                current_sleep = self.peak_sleep
            else:
                current_sleep = self.off_sleep
            self.logger.debug('sleep({})'.format(current_sleep))
            sleep(current_sleep)

            # Force a refresh every 12 hours at Noon and Midnight UTC
            now_utc = datetime.now(timezone.utc)
            if ( (now_utc.hour < 12 and last_run.hour > 12) or \
                (now_utc.hour > 12 and last_run.hour < 12) ):
                self.logger.info('REFRESH TRIGGER: Every 12 hours')
                return

            # Force a refresh every max_stale seconds
            since_last_run = now_utc - last_run
            if since_last_run.seconds > self.max_stale:
                self.logger.info('REFRESH TRIGGER: Stale {}/seconds above thresdhold of {}/seconds'.format(since_last_run.seconds, self.max_stale) )
                return

            # If recent database update
            if 'CIDER_LAST_URL' in self.config and self.config['CIDER_LAST_URL']:
                ts_json = self.Retrieve_Affiliation_Infrastructure(self.config['CIDER_LAST_URL'])
            try:
                last_db_update = parse_datetime(ts_json['last_update_time'])
                self.logger.info('Last DB update at {} with last refresh at {}'.format(last_db_update, last_run))
                if last_db_update > last_run:
                    self.logger.info('REFRESH TRIGGER: DB update since last run')
                    return
            except Exception as e:
                self.logger.error('{} parsing last_update_time={}: {}'.format(type(e).__name__, ts_json['last_update_time'], e.message))
                last_db_update = None

    def Run(self):
        while True:
            self.start = datetime.now(timezone.utc)
            self.STATS = Counter()
            
            if self.src['scheme'] == 'file':
                RAW = self.Read_Cache(self.src['path'])
            else:
                RAW = self.Retrieve_Source(self.src['uri'])

            if RAW:
                if self.dest['scheme'] == 'file':
                    bytes = self.Write_Cache(self.dest['path'], RAW)
                elif self.dest['scheme'] == 'analyze':
                    self.Analyze_Info(RAW)
                elif self.dest['scheme'] == 'index':
                    (rc, process_message) = self.Warehouse_Info(RAW)
                
                self.end = datetime.now(timezone.utc)
                summary_msg = 'Processed in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.STATS['Update'], self.STATS['Delete'], self.STATS['Skip'])
                self.logger.info(summary_msg)
            if not self.args.daemonaction:
                break
            self.smart_sleep(self.start)

########## CUSTOMIZATIONS END ##########

if __name__ == '__main__':
    router = Router()
    with PidFile(router.pidfile_path):
        try:
            router.Setup()
            rc = router.Run()
        except Exception as e:
            msg = '{} Exception: {}'.format(type(e).__name__, e)
            router.logger.error(msg)
            traceback.print_exc(file=sys.stdout)
            rc = 1
    router.exit(rc)
