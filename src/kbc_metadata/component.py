import dateutil
import logging
import sys
import time
from hashlib import md5
from kbc.env_handler import KBCEnvHandler
from kbc_metadata.client import MetadataClient, StorageClient
from kbc_metadata.result import MetadataWriter
from typing import Dict, List

APP_VERSION = '0.0.20'
TOKEN_SUFFIX = '_Telemetry_token'
TOKEN_EXPIRATION_CUSHION = 30 * 60  # 30 minutes

KEY_TOKENS = 'tokens'
KEY_MASTERTOKEN = 'master_token'
KEY_DATASETS = 'datasets'
KEY_INCREMENTAL = 'incremental_load'

MANDATORY_PARAMS = [[KEY_TOKENS, KEY_MASTERTOKEN], KEY_DATASETS]

KEY_GET_ALL_CONFIGURATIONS = 'get_all_configurations'
KEY_GET_TOKENS = 'get_tokens'
KEY_GET_TOKENS_LAST_EVENTS = 'get_tokens_last_events'
KEY_GET_ORCHESTRATIONS = 'get_orchestrations'
KEY_GET_WAITING_JOBS = 'get_waiting_jobs'
KEY_GET_TABLES = 'get_tables'
KEY_GET_TRANSFORMATIONS = 'get_transformations'
KEY_GET_PROJECT_USERS = 'get_project_users'
KEY_GET_ORGANIZATION_USERS = 'get_organization_users'
KEY_GET_TRIGGERS = 'get_triggers'
KEY_GET_COLUMNS = 'get_columns'

STORAGE_ENDPOINTS = [KEY_GET_ALL_CONFIGURATIONS, KEY_GET_TOKENS, KEY_GET_ORCHESTRATIONS, KEY_GET_WAITING_JOBS,
                     KEY_GET_TABLES, KEY_GET_TRANSFORMATIONS, KEY_GET_TRIGGERS]
MANAGEMENT_ENDPOINTS = [KEY_GET_PROJECT_USERS, KEY_GET_ORGANIZATION_USERS]


class ComponentWriters:
    pass


class MetadataComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='INFO')
        self.validate_config(MANDATORY_PARAMS)
        logging.info(f"Running component version {APP_VERSION}...")

        if self.cfg_params.get('debug', False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

        self.paramTokens = self.cfg_params.get(KEY_TOKENS, [])
        self.paramMasterToken = self.cfg_params.get(KEY_MASTERTOKEN, [])
        self.paramDatasets = self.cfg_params[KEY_DATASETS]
        self.paramIncremental = bool(self.cfg_params.get(KEY_INCREMENTAL, False))

        self.client = MetadataClient()
        self.paramClient = self.determineToken()
        self.checkTokenPermissions()
        self.createWriters()

        self.previousTokens = {} if self.get_state_file() is None else self.get_state_file()
        self.newTokens = {}

        logging.debug(f"Using {self.paramClient} token.")

    def checkTokenPermissions(self):

        if self.paramClient == 'management':
            return

        else:
            configBool = [self.paramDatasets.get(k, False) for k in MANAGEMENT_ENDPOINTS]
            if any(configBool) is True:
                logging.error(f"Management token required to obtain the following options: {MANAGEMENT_ENDPOINTS}.")
                sys.exit(1)

            else:
                pass

    def determineToken(self):

        if len(self.paramMasterToken) != 0:

            if len(self.paramMasterToken) > 1:
                logging.warning("More than 1 master token specified. Only first will be used.")

            _mastToken = self.paramMasterToken[0]

            if ('region' not in _mastToken or 'org_id' not in _mastToken or '#token' not in _mastToken):
                logging.exception("Missing mandatory fields from master token specification.")
                sys.exit(1)

            elif (_mastToken['region'] == '' or _mastToken['#token'] == '' or _mastToken['org_id'] == ''):
                logging.error("Missing parameter specification in master token.")
                sys.exit(1)

            else:
                return 'management'

        elif len(self.paramTokens) != 0:

            for token in self.paramTokens:

                if 'region' not in token or '#key' not in token:
                    logging.exception("Missing mandatory fields for storage token specification.")
                    sys.exit(1)

                elif token['region'] == '' or token['#key'] == '':
                    logging.error("Missing parameters specification for one of the storage tokens.")
                    sys.exit(1)

                else:
                    return 'storage'

        else:
            logging.exception("Neither master, nor storage token specified.")
            sys.exit(1)

    @staticmethod
    def _getObjectFromList(listOfObject: List, searchKey: str, searchKeyValue) -> Dict:

        _evalList = [obj[searchKey] == searchKeyValue for obj in listOfObject]
        _idx = _evalList.index(True)

        return listOfObject[_idx]

    def createWriters(self):

        self.writer = ComponentWriters

        if self.paramDatasets.get(KEY_GET_ORCHESTRATIONS) is True:
            self.writer.orchestrations = MetadataWriter(self.tables_out_path, 'orchestrations', self.paramIncremental)
            self.writer.orchestration_notifications = MetadataWriter(self.tables_out_path,
                                                                     'orchestrations-notifications',
                                                                     self.paramIncremental)
            self.writer.orchestrations_tasks = MetadataWriter(self.tables_out_path, 'orchestrations-tasks',
                                                              self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_WAITING_JOBS) is True:
            self.writer.waiting_jobs = MetadataWriter(self.tables_out_path, 'waiting-jobs', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TOKENS) is True:
            self.writer.tokens = MetadataWriter(self.tables_out_path, 'tokens', self.paramIncremental)

            if self.paramDatasets.get(KEY_GET_TOKENS_LAST_EVENTS) is True:
                self.writer.tokens_last_events = MetadataWriter(self.tables_out_path, 'tokens-last-events',
                                                                self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_ALL_CONFIGURATIONS) is True:
            self.writer.configurations = MetadataWriter(self.tables_out_path, 'configurations', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TABLES) is True:
            self.writer.tables = MetadataWriter(self.tables_out_path, 'tables', self.paramIncremental)
            self.writer.tables_metadata = MetadataWriter(self.tables_out_path, 'tables-metadata', self.paramIncremental)

            if self.paramDatasets.get(KEY_GET_COLUMNS) is True:
                self.writer.columns = MetadataWriter(self.tables_out_path, 'tables-columns', self.paramIncremental)
                self.writer.columns_metadata = MetadataWriter(self.tables_out_path, 'tables-columns-metadata',
                                                              self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TRANSFORMATIONS) is True:
            self.writer.transformations = MetadataWriter(self.tables_out_path, 'transformations', self.paramIncremental)
            self.writer.transformations_buckets = MetadataWriter(self.tables_out_path, 'transformations-buckets',
                                                                 self.paramIncremental)
            self.writer.transformations_inputs = MetadataWriter(self.tables_out_path, 'transformations-inputs',
                                                                self.paramIncremental)

            self.writer.transformations_inputs_md = MetadataWriter(self.tables_out_path,
                                                                   'transformations-inputs-metadata',
                                                                   self.paramIncremental)

            self.writer.transformations_outputs = MetadataWriter(self.tables_out_path, 'transformations-outputs',
                                                                 self.paramIncremental)

            self.writer.transformations_queries = MetadataWriter(self.tables_out_path, 'transformations-queries',
                                                                 self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_PROJECT_USERS) is True:
            self.writer.project_users = MetadataWriter(self.tables_out_path, 'project-users', self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_ORGANIZATION_USERS) is True:
            self.writer.organization_users = MetadataWriter(self.tables_out_path, 'organization-users',
                                                            self.paramIncremental)

        if self.paramDatasets.get(KEY_GET_TRIGGERS) is True:
            self.writer.triggers = MetadataWriter(self.tables_out_path, 'triggers', self.paramIncremental)
            self.writer.triggers_tables = MetadataWriter(self.tables_out_path, 'triggers-tables', self.paramIncremental)

    def getDataForProject(self, prjId, prjToken, prjRegion):

        self.client.initStorageAndSyrup(prjRegion, prjToken, prjId)
        p_dict = {'region': prjRegion, 'project_id': prjId}

        if self.paramDatasets.get(KEY_GET_ORCHESTRATIONS) is True:
            orchestrations = self.client.syrup.getOrchestrations()
            orchestrations_storage = self.client.storage.getOrchestrations()
            self.writer.orchestrations.writerows(orchestrations, parentDict=p_dict)

            for orch in orchestrations:
                orchestration_id = orch['id']
                _orchestration_parent = {**{'orchestration_id': orchestration_id}, **p_dict}

                self.writer.orchestration_notifications.writerows(orch['notifications'],
                                                                  parentDict=_orchestration_parent)

                storage_configuration = self._getObjectFromList(orchestrations_storage, 'id', str(orchestration_id))
                orchestration_tasks = storage_configuration['configuration']['tasks']
                _tasks = []

                for idx, task in enumerate(orchestration_tasks):
                    _tasks += [{**task, **{'api_index': idx}}.copy()]

                self.writer.orchestrations_tasks.writerows(_tasks, parentDict=_orchestration_parent)

        if self.paramDatasets.get(KEY_GET_WAITING_JOBS) is True:
            jobs = self.client.syrup.getWaitingAndProcessingJobs()
            self.writer.waiting_jobs.writerows(jobs, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_TOKENS) is True:
            tokens = self.client.storage.getTokens()
            self.writer.tokens.writerows(tokens, parentDict=p_dict)

            if self.paramDatasets.get(KEY_GET_TOKENS_LAST_EVENTS) is True:

                for token in tokens:
                    token_id = token['id']

                    _last_event = self.client.storage.getTokenLastEvent(token_id)

                    if _last_event == []:
                        continue
                    else:
                        self.writer.tokens_last_events.writerows(_last_event,
                                                                 parentDict={**p_dict, **{'token_id': token_id}})

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

                if self.paramDatasets.get(KEY_GET_COLUMNS) is True:
                    _cols = [{'column': col} for col in t['columns']]
                    self.writer.columns.writerows(_cols, parentDict=cfg)

                    for col in t['columnMetadata']:
                        col_cfg = {**cfg, **{'column': col}}
                        self.writer.columns_metadata.writerows(t['columnMetadata'][col], parentDict=col_cfg)

            self.writer.tables.writerows(tables, parentDict=p_dict)

        if self.paramDatasets.get(KEY_GET_TRANSFORMATIONS) is True:
            buckets = self.client.storage.getTransformations()
            self.writer.transformations_buckets.writerows(buckets, parentDict=p_dict)

            for bucket in buckets:
                _bucket = {}
                _bucket['bucket_id'] = bucket['id']
                _bucket_parent = {**_bucket, **p_dict}

                _trans = []
                for transformation in bucket['rows']:
                    transformation['configuration']['packages'] = ','.join(
                        transformation['configuration'].get('packages', []))
                    transformation['configuration']['requires'] = ','.join(
                        transformation['configuration'].get('requires', []))

                    _transformation_hash = md5('|'.join([transformation['id'], bucket['id']]).encode()).hexdigest()

                    transformation['id_md5'] = _transformation_hash
                    _trans += [transformation]

                    _transformation = {'transformation_id': _transformation_hash}
                    _transformation_parent = {**_transformation, **p_dict}

                    _inputs = []
                    _metadata = []
                    for table_input in transformation['configuration'].get('input', []):
                        table_input['columns'] = ','.join(table_input.get('columns', []))
                        table_input['whereValues'] = ','.join([str(x) for x in table_input.get('whereValues', [])])
                        table_input['loadType'] = table_input.get('loadType', 'copy')

                        _inputs += [table_input.copy()]

                        for column in table_input.get('datatypes', []):
                            _metadata += [{**table_input['datatypes'][column],
                                           **{'source': table_input['source'],
                                              'destination': table_input['destination']}}]

                    self.writer.transformations_inputs.writerows(_inputs, parentDict=_transformation_parent)
                    self.writer.transformations_inputs_md.writerows(_metadata, parentDict=_transformation_parent)

                    _outputs = []
                    for table_output in transformation['configuration'].get('output', []):
                        table_output['primaryKey'] = ','.join(table_output.get('primaryKey', []))
                        table_output['incremental'] = table_output.get('incremental', False)
                        table_output['deleteWhereValues'] = ','.join(table_output.get('deleteWhereValues', []))

                        _outputs += [table_output.copy()]

                    self.writer.transformations_outputs.writerows(_outputs, parentDict=_transformation_parent)

                    _queries = [{'query_index': idx, 'query': q} for idx, q in
                                enumerate(transformation['configuration'].get('queries', []))]
                    self.writer.transformations_queries.writerows(_queries, parentDict=_transformation_parent)

                self.writer.transformations.writerows(_trans, parentDict=_bucket_parent)

        if self.paramDatasets.get(KEY_GET_TRIGGERS) is True:
            triggers = self.client.storage.getTriggers()
            self.writer.triggers.writerows(triggers, parentDict=p_dict)

            for trigger in triggers:
                _trigger_parent = {**{'trigger_id': trigger['id']}, **p_dict}
                self.writer.triggers_tables.writerows(trigger.get('tables', []), parentDict=_trigger_parent)

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

        if self.paramClient == 'management':
            self.client.initManagement(self.paramMasterToken[0]['region'], self.paramMasterToken[0]['#token'],
                                       self.paramMasterToken[0]['org_id'])

            all_projects = self.client.management.getOrganization()['projects']

            if self.paramDatasets.get(KEY_GET_ORGANIZATION_USERS) is True:
                org_users = self.client.management.getOrganizationUsers()
                self.writer.organization_users.writerows(org_users, parentDict={
                    'organization_id': self.client.management.paramOrganization,
                    'region': self.client.management.paramRegion
                })

            if self.paramDatasets.get(KEY_GET_PROJECT_USERS) is True:

                for project in all_projects:
                    p_dict = {'project_id': project['id'], 'region': self.paramMasterToken[0]['region']}
                    users = self.client.management.getProjectUsers(project['id'])
                    self.writer.project_users.writerows(users, parentDict=p_dict)

            storage_booolean = [self.paramDatasets.get(key, False) for key in STORAGE_ENDPOINTS]
            if any(storage_booolean) is True:

                for prj in all_projects:

                    prj_id = str(prj['id'])
                    prj_name = prj['name']
                    prj_region = prj['region']
                    prj_token_description = prj_name + TOKEN_SUFFIX
                    prj_token_key = '|'.join([prj_region.replace('-', '_'), prj_id])

                    prj_token_old = self.previousTokens.get(prj_token_key)

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

            i = 0
            for prj in self.paramTokens:

                i += 1
                prj_token = prj['#key']
                prj_region = prj['region']
                prj_id = prj_token.split('-')[0]

                if prj_token == '':
                    logging.error(f"Token at position {i} is empty.")
                    sys.exit(1)

                logging.info(f"Downloading data for project {prj_id} in region {prj_region}.")
                self.getDataForProject(prj_id, prj_token, prj_region)

        self.write_state_file(self.newTokens)
