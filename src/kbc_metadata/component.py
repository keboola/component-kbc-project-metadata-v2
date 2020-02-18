import logging
import sys
from kbc.env_handler import KBCEnvHandler
from kbc_metadata.client import MetadataClient
from kbc_metadata.result import MetadataWriter

APP_VERSION = '0.0.1'
TOKEN_SUFFIX = '_Telemetry_token'

KEY_TOKENS = 'tokens'
KEY_MASTERTOKEN = 'master_token'
KEY_DATASETS = 'datasets'
KEY_INCREMENTAL = 'incremental_load'

MANDATORY_PARAMS = [[KEY_TOKENS, KEY_MASTERTOKEN], KEY_DATASETS]

SUPPORTED_DATASETS = ['get_all_configurations', 'get_tokens', 'get_orchestrations',
                      'get_waiting_jobs']

KEY_GET_ALL_CONFIGURATIONS = 'get_all_configurations'
KEY_GET_TOKENS = 'get_tokens'
KEY_GET_ORCHESTRATIONS = 'get_orchestrations'
KEY_GET_WAITING_JOBS = 'get_waiting_jobs'


class ComponentWriters:
    pass


class MetadataComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='DEBUG')
        self.validate_config(MANDATORY_PARAMS)
        logging.info(f"Running component version {APP_VERSION}...")

        self.paramTokens = self.cfg_params[KEY_TOKENS]
        self.paramMasterToken = self.cfg_params[KEY_MASTERTOKEN]
        self.paramDatasets = self.cfg_params[KEY_DATASETS]
        self.paramIncremental = self.cfg_params.get(KEY_INCREMENTAL, True)

        self.client = MetadataClient()
        self.paramClient = self.determineToken()
        self.createWriters()

    def determineToken(self):

        if len(self.paramMasterToken) != 0:

            if len(self.paramMasterToken) > 1:
                logging.warning("More than 1 master token specified. Only first will be used.")

            _mastToken = self.paramMasterToken[0]

            if ('region' not in _mastToken or 'org_id' not in _mastToken or '#token' not in _mastToken):
                logging.exception("Missing mandatory fields from master token specification.")
                sys.exit(1)

            else:
                return 'master'

        elif len(self.paramTokens) != 0:

            for token in self.paramTokens:

                if 'region' not in token or '#key' not in token:
                    logging.exception("Missing mandatory fields for storage token specification.")
                    sys.exit(1)

                else:
                    return 'storage'

        else:
            logging.exception("Neither master, nor storage token specified.")
            sys.exit(1)

    def createWriters(self):

        self.writer = ComponentWriters

        if self.paramDatasets.get(KEY_GET_ORCHESTRATIONS) is True:
            self.writer.orchestrations = MetadataWriter(self.tables_out_path, 'orchestrations', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_WAITING_JOBS) is True:
            self.writer.waiting_jobs = MetadataWriter(self.tables_out_path, 'waiting-jobs', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TOKENS) is True:
            self.writer.tokens = MetadataWriter(self.tables_out_path, 'tokens', self.paramIncremental)

    def getDataForProject(self, prjId, prjToken, prjRegion):

        self.client.initStorageAndSyrup(prjRegion, prjToken, prjId)

        if self.paramDatasets.get(KEY_GET_ORCHESTRATIONS) is True:
            orch = self.client.syrup.getOrchestrations()
            self.writer.orchestrations.writerows(orch, parentDict={'region': prjRegion})

        if self.paramDatasets.get(KEY_GET_WAITING_JOBS) is True:
            jobs = self.client.syrup.getWaitingAndProcessingJobs()
            self.writer.waiting_jobs.writerows(jobs, parentDict={'region': prjRegion})

        if self.paramDatasets.get(KEY_GET_TOKENS) is True:
            tokens = self.client.storage.getTokens()
            self.writer.tokens.writerows(tokens, parentDict={'region': prjRegion})

    def run(self):

        if self.paramClient == 'master':
            self.client.initManagement(self.paramMasterToken[0]['region'], self.paramMasterToken[0]['#token'],
                                       self.paramMasterToken[0]['org_id'])

            all_projects = self.client.management.getOrganization()['projects']

            for prj in all_projects:

                prj_id = prj['id']
                prj_name = prj['name']
                prj_region = prj['region']
                prj_token_description = prj_name + TOKEN_SUFFIX

                logging.info(f"Downloading data for project {prj_name} in region {prj_region}.")
                prj_token = self.client.management.createStorageToken(prj_id, prj_token_description)
                self.getDataForProject(prj_id, prj_token['token'], prj_region)

        elif self.paramClient == 'storage':

            for prj in self.paramTokens:

                prj_token = prj['#key']
                prj_region = prj['region']
                prj_id = prj_token.split('-')[0]

                logging.info(f"Downloading data for project {prj_id} in region {prj_region}.")
                self.getDataForProject(prj_id, prj_token, prj_region)
