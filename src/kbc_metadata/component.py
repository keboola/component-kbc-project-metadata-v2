import logging
import sys
# import time
from kbc.env_handler import KBCEnvHandler
from kbc_metadata.client import MetadataClient
from kbc_metadata.result import MetadataWriter

APP_VERSION = '0.0.3'
TOKEN_SUFFIX = '_Telemetry_token'
TOKEN_EXPIRATION_CUSHION = 30 * 60  # 30 minutes

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
KEY_GET_TABLES = 'get_tables'
KEY_GET_TRANSFORMATIONS = 'get_transformations'


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

        # self.previousTokens = {} if self.get_state_file() is None else self.get_state_file()

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

    def getDataForProject(self, prjId, prjToken, prjRegion):

        self.client.initStorageAndSyrup(prjRegion, prjToken, prjId)
        p_dict = {'region': prjRegion, 'project_id': prjId}

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
                        table_input['whereColumn'] = table_input.get('whereColumn', '')
                        table_input['whereOperator'] = table_input.get('whereOperator', '')
                        table_input['loadType'] = table_input.get('loadType', 'copy')
                        table_input['changedSince'] = table_input.get('changedSince', '')

                        _inputs += [table_input.copy()]

                    self.writer.transformations_inputs.writerows(_inputs, parentDict=_transformation_parent)

                    _outputs = []
                    for table_output in t['configuration'].get('output', []):
                        table_output['primaryKey'] = ','.join(table_output.get('primaryKey', []))
                        table_output['incremental'] = table_output.get('incremental', False)
                        table_output['deleteWhereValues'] = ','.join(table_output.get('deleteWhereValues', []))
                        table_output['deleteWhereOperator'] = table_output.get('deleteWhereOperator', '')
                        table_output['deleteWhereColumn'] = table_output.get('deleteWhereColumn', '')

                        _outputs += [table_output.copy()]

                    self.writer.transformations_outputs.writerows(_outputs, parentDict=_transformation_parent)

                self.writer.transformations.writerows(_trans, parentDict=_bucket_parent)

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
