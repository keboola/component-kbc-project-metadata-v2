import dateutil
import logging
import sys
import time
from kbc.env_handler import KBCEnvHandler
from kbc_metadata.client import MetadataClient, StorageClient
from kbc_metadata.result import MetadataWriter

APP_VERSION = '0.0.6'
TOKEN_SUFFIX = '_Telemetry_token'
TOKEN_EXPIRATION_CUSHION = 30 * 60  # 30 minutes

KEY_TOKENS = 'tokens'
KEY_MASTERTOKEN = 'master_token'
KEY_DATASETS = 'datasets'
KEY_INCREMENTAL = 'incremental_load'

MANDATORY_PARAMS = [[KEY_TOKENS, KEY_MASTERTOKEN], KEY_DATASETS]

KEY_GET_ALL_CONFIGURATIONS = 'get_all_configurations'
KEY_GET_TOKENS = 'get_tokens'
KEY_GET_ORCHESTRATIONS = 'get_orchestrations'
KEY_GET_WAITING_JOBS = 'get_waiting_jobs'
KEY_GET_TABLES = 'get_tables'
KEY_GET_TRANSFORMATIONS = 'get_transformations'
KEY_GET_PROJECT_USERS = 'get_project_users'
KEY_GET_ORGANIZATION_USERS = 'get_organization_users'

STORAGE_ENDPOINTS = [KEY_GET_ALL_CONFIGURATIONS, KEY_GET_TOKENS, KEY_GET_ORCHESTRATIONS, KEY_GET_WAITING_JOBS,
                     KEY_GET_TABLES, KEY_GET_TRANSFORMATIONS, KEY_GET_PROJECT_USERS]


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
        self.paramIncremental = bool(self.cfg_params.get(KEY_INCREMENTAL, False))

        self.client = MetadataClient()
        self.paramClient = self.determineToken()
        self.createWriters()

        self.previousTokens = {} if self.get_state_file() is None else self.get_state_file()
        self.newTokens = {}

        # logging.debug("Previous tokens:")
        # logging.debug(self.previousTokens)

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

        if self.paramDatasets.get(KEY_GET_ALL_CONFIGURATIONS) is True:
            self.writer.configurations = MetadataWriter(self.tables_out_path, 'configurations', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TABLES) is True:
            self.writer.tables = MetadataWriter(self.tables_out_path, 'tables', self.paramIncremental)
            self.writer.tables_metadata = MetadataWriter(self.tables_out_path, 'tables-metadata', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TRANSFORMATIONS) is True:
            self.writer.transformations = MetadataWriter(self.tables_out_path, 'transformations', self.paramIncremental)
            self.writer.transformations_buckets = MetadataWriter(self.tables_out_path, 'transformations-buckets',
                                                                 self.paramIncremental)
            self.writer.transformations_inputs = MetadataWriter(self.tables_out_path, 'transformations-inputs',
                                                                self.paramIncremental)
            self.writer.transformations_outputs = MetadataWriter(self.tables_out_path, 'transformations-outputs',
                                                                 self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_PROJECT_USERS) is True:
            self.writer.project_users = MetadataWriter(self.tables_out_path, 'project-users', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_ORGANIZATION_USERS) is True:
            self.writer.organization_users = MetadataWriter(self.tables_out_path, 'organization-users',
                                                            self.paramIncremental)

    def getDataForProject(self, prjId, prjToken, prjRegion):

        self.client.initStorageAndSyrup(prjRegion, prjToken, prjId)
        p_dict = {'region': prjRegion, 'project_id': prjId}

        if self.paramDatasets.get(KEY_GET_PROJECT_USERS) is True:
            users = self.client.management.getProjectUsers(prjId)
            self.writer.project_users.writerows(users, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_ORCHESTRATIONS) is True:
            orch = self.client.syrup.getOrchestrations()
            self.writer.orchestrations.writerows(orch, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_WAITING_JOBS) is True:
            jobs = self.client.syrup.getWaitingAndProcessingJobs()
            self.writer.waiting_jobs.writerows(jobs, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_TOKENS) is True:
            tokens = self.client.storage.getTokens()
            self.writer.tokens.writerows(tokens, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_ALL_CONFIGURATIONS) is True:
            configs = self.client.storage.getAllConfigurations()

            for comp in configs:
                cfg = {}
                cfg['component_id'] = comp['id']
                cfg['component_type'] = comp['type']
                cfg['component_name'] = comp['name']
                cfg = {**cfg, **p_dict}

                self.writer.configurations.writerows(comp['configurations'], parentDict=cfg)

        if self.paramDatasets.get(KEY_GET_TABLES) is True:
            tables = self.client.storage.getAllTables()
            for t in tables:
                t['primaryKey'] = ','.join(t['primaryKey'])
                cfg = {}
                cfg['table_id'] = t['id']
                cfg = {**cfg, **p_dict}

                self.writer.tables_metadata.writerows(t['metadata'], parentDict=cfg)

            self.writer.tables.writerows(tables, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_TRANSFORMATIONS) is True:
            buckets = self.client.storage.getTransformations()
            self.writer.transformations_buckets.writerows(buckets, parentDict=p_dict)

            for transformation in buckets:
                _bucket = {}
                _bucket['bucket_id'] = transformation['id']
                _bucket_parent = {**_bucket, **p_dict}

                _trans = []
                for t in transformation['rows']:
                    t['configuration']['packages'] = ','.join(t['configuration'].get('packages', []))
                    t['configuration']['requires'] = ','.join(t['configuration'].get('requires', []))
                    _trans += [t]

                    _transformation = {'transformation_id': t['id']}
                    _transformation_parent = {**_transformation, **_bucket_parent}

                    _inputs = []
                    for table_input in t['configuration'].get('input', []):
                        table_input['columns'] = ','.join(table_input.get('columns', []))
                        table_input['whereValues'] = ','.join(table_input.get('whereValues', []))
                        table_input['loadType'] = table_input.get('loadType', 'copy')

                        _inputs += [table_input.copy()]

                    self.writer.transformations_inputs.writerows(_inputs, parentDict=_transformation_parent)

                    _outputs = []
                    for table_output in t['configuration'].get('output', []):
                        table_output['primaryKey'] = ','.join(table_output.get('primaryKey', []))
                        table_output['incremental'] = table_output.get('incremental', False)
                        table_output['deleteWhereValues'] = ','.join(table_output.get('deleteWhereValues', []))

                        _outputs += [table_output.copy()]

                    self.writer.transformations_outputs.writerows(_outputs, parentDict=_transformation_parent)

                self.writer.transformations.writerows(_trans, parentDict=_bucket_parent)

    @staticmethod
    def convertIsoFormatToTimestamp(isoDTString: str) -> int:

        if isoDTString == '' or isoDTString is None:
            return None

        else:
            return int(dateutil.parser.parse(isoDTString).timestamp())

    @staticmethod
    def tokenInTreshold(tokenExpiration: int) -> bool:
        if tokenExpiration is None:
            return True

        else:
            current = int(time.time())
            expiration = tokenExpiration - current

            if expiration >= TOKEN_EXPIRATION_CUSHION:
                return True
            else:
                return False

    def isTokenValid(self, token: str, tokenExpiration: int, region: str, project: str) -> bool:

        tokenNotExpired = self.tokenInTreshold(tokenExpiration)
        tokenIsValid = StorageClient(region=region, project=project, token=token, soft=True)._verifyStorageToken()

        if all([tokenIsValid, tokenNotExpired]):
            return True
        else:
            return False

    def run(self):

        if self.paramClient == 'master':
            self.client.initManagement(self.paramMasterToken[0]['region'], self.paramMasterToken[0]['#token'],
                                       self.paramMasterToken[0]['org_id'])

            all_projects = self.client.management.getOrganization()['projects']

            if self.paramDatasets.get(KEY_GET_ORGANIZATION_USERS) is True:
                org_users = self.client.management.getOrganizationUsers()
                self.writer.organization_users.writerows(org_users, parentDict={
                    'organization_id': self.client.management.paramOrganization,
                    'region': self.client.management.paramRegion
                    })

            storage_booolean = [self.paramDatasets.get(key, False) for key in STORAGE_ENDPOINTS]
            if any(storage_booolean) is True:

                for prj in all_projects:

                    prj_id = str(prj['id'])
                    prj_name = prj['name']
                    prj_region = prj['region']
                    prj_token_description = prj_name + TOKEN_SUFFIX
                    prj_token_key = '|'.join([prj_region.replace('-', '_'), prj_id])

                    prj_token_old = self.previousTokens.get(prj_token_key)
                    # logging.debug("Old token:")
                    # logging.debug(prj_token_old)

                    if prj_token_old is None:
                        logging.debug(f"Creating new storage token for project {prj_id} in region {prj_region}.")
                        prj_token_new = self.client.management.createStorageToken(prj_id, prj_token_description)

                        prj_token = {
                            'id': prj_token_new['id'],
                            '#token': prj_token_new['token'],
                            'expires': self.convertIsoFormatToTimestamp(prj_token_new['expires'])
                        }

                    else:
                        valid = self.isTokenValid(prj_token_old['#token'], prj_token_old['expires'], prj_region, prj_id)

                        if valid is True:
                            logging.debug(f"Using token {prj_token_old['id']} from state for project {prj_id}.")
                            prj_token = prj_token_old

                        else:
                            logging.debug(f"Creating new storage token for project {prj_id} in region {prj_region}.")
                            prj_token_new = self.client.management.createStorageToken(prj_id, prj_token_description)

                            prj_token = {
                                'id': prj_token_new['id'],
                                '#token': prj_token_new['token'],
                                'expires': self.convertIsoFormatToTimestamp(prj_token_new['expires'])
                            }

                    logging.info(f"Downloading data for project {prj_name} in region {prj_region}.")
                    self.getDataForProject(prj_id, prj_token['#token'], prj_region)
                    self.newTokens[prj_token_key] = prj_token

        elif self.paramClient == 'storage':

            for prj in self.paramTokens:

                prj_token = prj['#key']
                prj_region = prj['region']
                prj_id = prj_token.split('-')[0]

                logging.info(f"Downloading data for project {prj_id} in region {prj_region}.")
                self.getDataForProject(prj_id, prj_token, prj_region)

        self.write_state_file(self.newTokens)
