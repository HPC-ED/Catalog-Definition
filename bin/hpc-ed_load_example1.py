#!/usr/bin/env python3
##############################################################################
### HPC-ED Training Metadata Publishing Example 1
### Author(s): JP Navarro
### The input source (-s) can be a file or a URL (see an example under data/)
### The config file (-c) configures this program (see an example under conf/)
###
### Modify the CUSTOMIZE HERE section to modify this program to work with your
###    own custom input data format
##############################################################################
import argparse
from collections import Counter
from datetime import datetime, timezone
import globus_sdk
import http.client as httplib
import json
import logging
import logging.handlers
import os
import pdb
import pwd
import sys
import signal
import ssl
import traceback
from urllib.parse import urlparse

def eprint(*args, **kwargs):           # Used before logging is enabled
    print(*args, file=sys.stderr, **kwargs)

class Publisher():
    def __init__(self):
        self.my_name = 'hpc-ed_load_example1'
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--source', action='store', dest='source', default=f'file:data/{self.my_name}.json', \
                            help=f'SOURCE can be file:<file> or an http[s] URL (default=file:data/{self.my_name}.json)')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default=f'conf/{self.my_name}.conf', \
                            help=f'Configuration file default=conf/{self.my_name}.conf')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        if self.args.pdb:              # Trace for debugging as early as possible
            pdb.set_trace()

        # Load configuration
        self.config_file = os.path.abspath(self.args.config)
        try:
            with open(self.config_file, 'r') as file:
                conf=file.read()
        except IOError as e:
            eprint(f'Error "{e}" reading config={self.config_file}')
            sys.exit(1)
        # Parse configuration json
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            eprint(f'Error "{e}" parsing config={self.config_file}')
            sys.exit(1)

    def Setup(self):
        self.logger = logging.getLogger('DaemonLog')
        # Initialize log level from arguments, or config file, or default to WARNING
        loglevel_str = (self.args.log or self.config.get('LOG_LEVEL', 'WARNING')).upper()
        loglevel_num = getattr(logging, loglevel_str, None)
        self.logger.setLevel(loglevel_num)
        logfile = self.config.get('LOG_FILE', f'{self.my_name}.log')
        self.handler = logging.handlers.TimedRotatingFileHandler(logfile, when='W6', backupCount=999, utc=True)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)

        self.logger.info(f'Starting program={os.path.basename(__file__)} pid={os.getpid()}, uid={os.geteuid()}({pwd.getpwuid(os.geteuid()).pw_name})')

        for item in ['GLOBUS_CLIENT_ID', 'GLOBUS_CLIENT_SECRET', 'SCOPES', 'PROVIDER_ID', 'INDEX_ID']:
            if item not in self.config:
                self.logger.error(f'Missing required config parameter: {item}')
                sys.exit(1)
        self.INDEX_ID = self.config['INDEX_ID']
        self.PROVIDER_ID = self.config['PROVIDER_ID']

        self.psource = urlparse(self.args.source)              # URL parsed source: file: or http/https:
        if self.psource.scheme not in ['file', 'http', 'https'] or not self.psource.path:
            self.logger.error(f'Source URL is not valid: {self.args.source}')
            sys.exit(1)

        self.logger.info(f'Source: {self.psource.geturl()}')
        self.logger.info(f'Config: {self.config_file}')
        self.logger.info(f'Log Level: {loglevel_str}({loglevel_num})')

    def exit_signal(self, signum, frame):
        self.logger.critical(f'Caught signal={signum}({signal.Signals(signum).name}), exiting with rc={signum}')
        sys.exit(signum)

    def exit(self, rc):
        if rc:
            self.logger.error(f'Exiting with rc={rc}')
        sys.exit(rc)

    def Retrieve_URL(self, urlp):
        if not urlp.netloc:
            self.logger.error(f'Source URL is not a network location: {urlp.getutl()}')
            sys.exit(1)
        if ':' in urlp.netloc:
            (host, port) = urlp.netloc.split(':')
        else:
            (host, port) = (urlp.netloc, '')
        if not port:
            port = '80' if urlp.scheme == 'http' else '443'     # Default is HTTPS/443
        
        headers = {'Content-type': 'application/json'}
#        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx = ssl.create_default_context()
        conn = httplib.HTTPSConnection(host=host, port=port, context=ctx)
        conn.request('GET', urlp.path, None , headers)
        self.logger.debug(f'HTTP GET  {urlp.path}')
        response = conn.getresponse()
        data = response.read()
        self.logger.debug(f'HTTP RESP {response.status} {response.reason} (returned {len(data)}/bytes)')
        try:
            data_json = json.loads(data)
        except ValueError as e:
            self.logger.error(f'Response not in expected JSON format ({e})')
            return(None)
            
        self.logger.debug(f'Retrieved and parsed {len(results)}/bytes from URL')
        return(data_json)

    def Retrieve_File(self, urlp):
        with open(urlp.path, 'r') as my_file:
            data = my_file.read()
            my_file.close()
        try:
            data_json = json.loads(data)
            self.logger.info(f'Read and parsed {len(data)}/bytes of json from file={urlp.path}')
            return(data_json)
        except ValueError as e:
            self.logger.error(f'Error "{e}" parsing file={file}')
            sys.exit(1)

    def Publish_Data(self, data_json):
        if not isinstance(data_json, list):
            self.logger.error('Input JSON data is not a list')
            sys.exit(1)
        confidential_client = globus_sdk.ConfidentialAppAuthClient( client_id=self.config['GLOBUS_CLIENT_ID'], client_secret=self.config['GLOBUS_CLIENT_SECRET'] )
        cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, self.config['SCOPES'])
        self.SEARCHCLIENT = globus_sdk.SearchClient(authorizer=cc_authorizer, app_name=self.my_name)

        # Retrieve all current subjects for the current provider provider
        self.to_delete = {} # Subjects that weren't refreshed and need to be deleted
        query_string = f'Provider_ID:"{self.PROVIDER_ID}"'      # Quotes forces exact match
        offset = 0
        limit = 1000
        while True:
            try:
                query_results = self.SEARCHCLIENT.search(self.INDEX_ID, query_string, offset=offset, limit=limit)
            except globus_sdk.GlobusAPIError as e:
                self.logger.error(f'Globus API error: code={e.code}, message={e.message}')
                self.logger.error(f'Error text: {e.text}')
                raise e
            for item in query_results.data['gmeta']:
                self.to_delete[item['subject']] = None
            if not query_results.data['has_next_page']:
                break
            offset += limit
                
        self.logger.debug(f'Retrieved for provider {len(self.to_delete)}/items')

        for input in data_json:
            output = {}
#####
# CUSTOMIZE HERE
#####
# HPC-ED REQUIRED fields
#    If your input has different fields, change the right side to grab the correct values
#####
            output['Title'] = input.get('Title')
            output['URL'] = input.get('URL')
            output['Resource_URL_Type'] = input.get('Resource_URL_Type')
            output['Cost'] = input.get('Cost', 'no')
            output['Language'] = input.get('Language', 'en')
            output['Provider_ID'] = self.PROVIDER_ID

#####
# HPC-ED OPTIONAL fields
#    If your input has any of these fields, uncomment and change the right side to grab the correct value
#####
#            output['Abstract'] = input.get('Abstract')
#            output['Version_Date'] = input.get('Version_Date', datetime.now(timezone.utc))
#            output['Authors'] = input.get('Authors')
#            output['Keywords'] = input.get('Keywords')
#            output['License'] = input.get('License')
#            output['Learning_Resource_Type'] = input.get('Learning_Resource_Type')
#            output['Learning_Outcome'] = input.get('Learning_Outcome')
#            output['Target_Group'] = input.get('Target_Group')
#            output['Expertise_Level'] = input.get('Expertise_Level')
#            output['Rating'] = input.get('Rating)
#            output['Start_Datetime'] = input.get('Start_Datetime')
#            output['Duration'] = input.get('Duration')

#####
# Globus REQUIRED fields,
#    Only customize if your local unique id isn't in LOCAL_ID
#####
            GLOBUS_SUBJECT = self.PROVIDER_ID + ':' + input.get('LOCAL_ID')
            GLOBUS_VISIBLE_TO = ['public']
            GLOBUS_ID = None

            entry = {
                'subject': GLOBUS_SUBJECT,
                'visible_to': GLOBUS_VISIBLE_TO,
                'id': GLOBUS_ID,                       # Ignore for now
                'content': output,
            }
            self.Buffer_Entry(entry, batch=1000)
            self.to_delete.pop(GLOBUS_SUBJECT, None)

        self.Buffer_Entry(None, batch=0)

        for subject in self.to_delete:
            try:
                self.SEARCHCLIENT.delete_subject(self.INDEX_ID, subject)
                self.STATS.update({'Delete'})
                self.logger.info(f'Deleted Subject={subject}')
            except globus_sdk.GlobusAPIError as e:
                self.logger.error(f'Globus API error: code={e.code}, message={e.message}')
                self.logger.error(f'Error text: {e.text}')
                raise e
        return(True, '')
 

 # With batch=1 will ingest one entry using GMetaEntry
 # With batch>1 will buffer in batches, with a final batch=0 to flush the buffer
    def Buffer_Entry(self, entry, batch=100):
        if entry and batch == 1:
            self.ingest(entry)

        if not hasattr(self, 'entry_buffer'):            # Initialize buffer if needed
            self.entry_buffer = []
        if entry:                                        # Add entry to buffer
            self.entry_buffer.append(entry)
        if len(self.entry_buffer) < 1:                   # Nothing in buffer
            return
        if len(self.entry_buffer) < batch:               # Buffer isn't full
            return
        # Buffer is full or batch=0
        self.ingest(self.entry_buffer)
        self.entry_buffer = []
        return

    def ingest(self, items):
        if isinstance(items, list):                     # Multiple items
            ingest_data = {
                'ingest_type': 'GMetaList',
                'ingest_data': {
                    'gmeta': items
                }
            }
            count = len(items)
        elif isinstance(items, dict):                   # One item only
            ingest_data = {
                'ingest_type': 'GMetaEntry',
                'ingest_data': items
            }
            count = 1
        else:
            self.logger.error(f'Entries not a list or dictionary, quitting')
            sys.exit()
        try:
            self.SEARCHCLIENT.ingest(self.INDEX_ID, ingest_data)
            self.STATS.update({'Update': count})
        except globus_sdk.GlobusAPIError as e:
            self.logger.error(f'Globus API error: code={e.code}, message={e.message}')
            self.logger.error(f'Error text: {e.text}')
            raise e
        if count > 1:
            self.logger.debug(f'Update batch with {count}/items')
        return

    def Run(self):
        self.start = datetime.now(timezone.utc)
        self.STATS = Counter()
        
        if self.psource.scheme == 'file':
            RAW = self.Retrieve_File(self.psource)
        else:
            RAW = self.Retrieve_URL(self.psource)

        if RAW:
            (rc, process_message) = self.Publish_Data(RAW)
            self.end = datetime.now(timezone.utc)
            summary_msg = f'Processed in {(self.end - self.start).total_seconds():.3f}/seconds: {self.STATS["Update"]}/updates, {self.STATS["Delete"]}/deletes, {self.STATS["Skip"]}/skipped'
            self.logger.info(summary_msg)

########## CUSTOMIZATIONS END ##########

if __name__ == '__main__':
    this_publisher = Publisher()
    try:
        this_publisher.Setup()
        rc = this_publisher.Run()
    except Exception as e:
        this_publisher.logger.error(f'{type(e).__name__} Exception: {e}')
        traceback.print_exc(file=sys.stdout)
        rc = 1
    this_publisher.exit(rc)
