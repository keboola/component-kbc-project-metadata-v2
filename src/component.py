import dateparser
import dateutil.parser
import logging
import sys
import time

from contextlib import nullcontext
from dataclasses import dataclass
from hashlib import md5
from keboola.component import CommonInterface

from client import Client, StorageClient
from result import Writer
from table_definitions import *  # noqa

# Key for current stack selection
KEY_CURRENT = 'current'

APP_VERSION = '2.0.1'
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
KEY_GET_TRANSFORMATIONS_V2 = 'get_transformations_v2'
KEY_GET_PROJECT_USERS = 'get_project_users'
KEY_GET_ORGANIZATION_USERS = 'get_organization_users'
KEY_GET_TRIGGERS = 'get_triggers'
KEY_GET_COLUMNS = 'get_columns'
KEY_GET_WORKSPACE_LOAD_EVENTS = 'get_workspace_load_events'
KEY_GET_TABLES_LOAD_EVENTS = 'get_tables_load_events'

# Token keys
KEY_MAN_TOKEN = '#token'
KEY_SAP_TOKEN = '#key'
KEY_REGION = 'region'
KEY_ORGANIZATION_ID = 'org_id'

TR_V2_CMP_ID = ['keboola.snowflake-transformation', 'keboola.python-transformation-v2',
                'keboola.synapse-transformation', 'keboola.redshift-transformation',
                'keboola.r-transformation-v2', 'keboola.openrefine-transformation',
                'keboola.oracle-transformation', 'keboola.csas-python-transformation-v2',
                'keboola.databricks-transformation', 'keboola.exasol-transformation',
                'keboola.python-mlflow-transformation']

STORAGE_ENDPOINTS = [KEY_GET_ALL_CONFIGURATIONS, KEY_GET_TOKENS, KEY_GET_ORCHESTRATIONS, KEY_GET_WAITING_JOBS,
                     KEY_GET_TABLES, KEY_GET_TRANSFORMATIONS, KEY_GET_TRIGGERS, KEY_GET_WORKSPACE_LOAD_EVENTS,
                     KEY_GET_TRANSFORMATIONS_V2, KEY_GET_TABLES_LOAD_EVENTS]
MANAGEMENT_ENDPOINTS = [KEY_GET_PROJECT_USERS, KEY_GET_ORGANIZATION_USERS]


class ComponentWriters:
    pass


@dataclass
class Parameters:
    tokens: list
    master_token: list
    datasets: list
    incremental: bool
    current_stack: str


@dataclass
class ManagementToken:
    token: str
    organization_id: str
    region: str


class Component(CommonInterface):

    def __init__(self):

        super().__init__(log_level=logging.INFO)
        self.validate_configuration_parameters(MANDATORY_PARAMS)

        logging.info(f"Running component version {APP_VERSION}...")

        _par = self.configuration.parameters

        if _par.get('debug', False) is True:
            sys.tracebacklimit = 3
            self.set_default_logger(logging.DEBUG)

        self.parameters = Parameters(_par.get(KEY_TOKENS, []), _par.get(KEY_MASTERTOKEN, []),
                                     _par[KEY_DATASETS], bool(_par.get(KEY_INCREMENTAL, False)),
                                     self.environment_variables.stack_id)

        self.client = Client()
        self.writers = ComponentWriters

        self.parameters.client_to_use = self.determine_token()
        self.check_token_permissions()
        # self.createWriters()

        state = self.get_state_file()
        if state is None:
            state = {}

        self.previous_tokens = state.get('tokens', {})
        if isinstance(self.previous_tokens, list):
            self.previous_tokens = {}
        self.new_tokens = {}

        self.last_processed_transformations = state.get('tr_last_processed_id', {})
        if isinstance(self.last_processed_transformations, list):
            self.last_processed_transformations = {}

        self.latest_date = state.get('date', dateparser.parse("7 days ago").strftime("%Y-%m-%d"))
        self.table_definitions = {}

        logging.debug(f"Using {self.parameters.client_to_use} token.")

    def check_token_permissions(self):

        if self.parameters.client_to_use == 'management':
            return

        else:
            config_bool = [self.parameters.datasets.get(k, False) for k in MANAGEMENT_ENDPOINTS]
            if any(config_bool) is True:
                logging.error(f"Management token required to obtain the following options: {MANAGEMENT_ENDPOINTS}.")
                sys.exit(1)

            else:
                pass

    def determine_token(self):

        if len_master := len(self.parameters.master_token) != 0:

            if len_master > 1:
                logging.warning("More than 1 master token specified. Only first will be used.")

            _master_token = self.parameters.master_token[0]

            if ('region' not in _master_token or 'org_id' not in _master_token or '#token' not in _master_token):
                logging.exception("Missing mandatory fields from master token specification.")
                sys.exit(1)

            elif (_master_token['region'].strip() == '' or _master_token['#token'].strip() == ''
                  or _master_token['org_id'].strip() == ''):
                logging.error("Missing parameter specification in master token.")
                sys.exit(1)

            else:
                return 'management'

        elif len(self.parameters.tokens) != 0:

            for token in self.parameters.tokens:

                if 'region' not in token or '#key' not in token:
                    logging.exception("Missing mandatory fields for storage token specification.")
                    sys.exit(1)

                elif token['region'].strip() == '' or token['#key'].strip() == '':
                    logging.error("Missing parameters specification for one of the storage tokens.")
                    sys.exit(1)

                else:
                    return 'storage'

        else:
            logging.exception("Neither master, nor storage token specified.")
            sys.exit(1)

    @staticmethod
    def _get_object_from_list(object_list: list, search_key: str, search_key_value) -> dict:

        _eval_list = [obj[search_key] == search_key_value for obj in object_list]
        _idx = _eval_list.index(True)

        return object_list[_idx]

    @staticmethod
    def convert_iso_format_to_epoch_timestamp(iso_dt_string: str) -> int:

        if iso_dt_string == '' or iso_dt_string is None:
            return None

        else:
            return int(dateutil.parser.parse(iso_dt_string).timestamp())

    @staticmethod
    def is_token_in_treshold(token_expiration: int) -> bool:
        if token_expiration is None:
            return True

        else:
            current = int(time.time())
            expiration = token_expiration - current

            if expiration >= TOKEN_EXPIRATION_CUSHION:
                return True
            else:
                return False

    def is_token_valid(self, token: str, token_expiration: int, region: str, project: str) -> bool:

        is_token_expired = self.is_token_in_treshold(token_expiration)
        is_token_valid = StorageClient(region=region, project=project, token=token).verify_storage_token()

        if all([is_token_valid, is_token_expired]):
            return True
        else:
            return False

    def determine_stack(self, region: str):

        if region == 'us-east-1':
            return 'keboola.com'
        elif region == 'eu-central-1':
            return 'eu-central-1.keboola.com'
        elif region == KEY_CURRENT:
            return self.parameters.current_stack.replace('connection.', '')
        else:
            return region

    def build_table_definition(self, table_name: str):

        if table_name in self.table_definitions:
            return self.table_definitions[table_name]

        else:
            raw_cols = eval(f'FIELDS_{table_name.upper().replace("-", "_")}')
            kbc_cols = eval(f'FIELDS_R_{table_name.upper().replace("-", "_")}')
            json_cols = eval(f'JSON_{table_name.upper().replace("-", "_")}')
            pk = eval(f'PK_{table_name.upper().replace("-", "_")}')

            tdf = self.create_out_table_definition(name=table_name, primary_key=pk, columns=kbc_cols,
                                                   incremental=self.parameters.incremental)
            tdf.writer_columns = raw_cols
            tdf.json_columns = json_cols

            self.table_definitions[table_name] = tdf

            return tdf

    def download_organization_data(self, project_ids: list):

        if self.parameters.datasets.get(KEY_GET_ORGANIZATION_USERS):

            logging.info(f"Downloading organization users for organization {self.management_token.organization_id} in "
                         f"stack {self.parameters.region}.")
            _org_pdict = {
                'organization_id': self.management_token.organization_id,
                'region': self.parameters.region
            }
            _org_users_tdf = self.build_table_definition('organization-users')

            with Writer(_org_users_tdf) as wrt:
                org_users = self.client.management.get_organization_users()
                wrt.write_rows(org_users, parent_dict=_org_pdict)

        if self.parameters.datasets.get(KEY_GET_PROJECT_USERS):

            logging.info(f"Downloading project users for organization {self.management_token.organization_id} in "
                         f"stack {self.parameters.region}.")

            _prj_users_tdf = self.build_table_definition('project-users')

            with Writer(_prj_users_tdf) as wrt:
                for prj_id in project_ids:
                    _pdict = {'project_id': prj_id, 'region': self.parameters.region}
                    users = self.client.management.get_project_users(prj_id)
                    wrt.write_rows(users, _pdict)

    def get_waiting_jobs(self, parent_dict: dict):

        _waiting_jobs_tdf = self.build_table_definition('waiting-jobs')
        jobs = self.client.syrup.get_waiting_and_processing_jobs()

        with Writer(_waiting_jobs_tdf) as wrt:
            wrt.write_rows(jobs, parent_dict)

    def get_tokens_and_events(self, parent_dict: dict):

        _tokens_tdf = self.build_table_definition('tokens')
        tokens = self.client.storage.get_tokens()

        with Writer(_tokens_tdf) as wrt:
            wrt.write_rows(tokens, parent_dict)

        if self.parameters.datasets.get(KEY_GET_TOKENS_LAST_EVENTS):

            _tokens_le_tdf = self.build_table_definition('tokens-last-events')
            with Writer(_tokens_le_tdf) as wrt:
                for token in tokens:
                    token_id = token['id']
                    _last_event = self.client.storage.get_tokens_last_events(token_id)

                    if _last_event != []:
                        wrt.write_rows(_last_event, {**parent_dict, **{'token_id': token_id}})

    def get_all_configurations(self, parent_dict: dict):

        _all_configs_tdf = self.build_table_definition('configurations')
        configs = self.client.storage.get_all_configurations()

        with Writer(_all_configs_tdf) as wrt:
            for component in configs:
                comp = {}
                comp['component_id'] = component['id']
                comp['component_type'] = component['type']
                comp['component_name'] = component['name']

                comp = {**comp, **parent_dict}

                wrt.write_rows(component['configurations'], comp)

    def get_tables(self, parent_dict: dict):

        tables = self.client.storage.get_all_tables()

        _tables_tdf = self.build_table_definition('tables')
        _tables_md_tdf = self.build_table_definition('tables-metadata')
        wrt_tables = Writer(_tables_tdf)
        wrt_tables_md = Writer(_tables_md_tdf)

        write_column_data = self.parameters.datasets.get(KEY_GET_COLUMNS)

        if write_column_data:
            _columns_tdf = self.build_table_definition('tables-columns')
            _columns_md_tdf = self.build_table_definition('tables-columns-metadata')
            wrt_columns = Writer(_columns_tdf)
            wrt_columns_md = Writer(_columns_md_tdf)
        else:
            wrt_columns = nullcontext()
            wrt_columns_md = nullcontext()

        with wrt_tables, wrt_tables_md, wrt_columns, wrt_columns_md:

            for t in tables:
                t['primaryKey'] = ','.join(t['primaryKey'])
                cfg = {}
                cfg['table_id'] = t['id']
                cfg = {**cfg, **parent_dict}

                wrt_tables_md.write_rows(t['metadata'], parent_dict=cfg)

                if write_column_data:

                    _cols = [{'column': col} for col in t['columns']]
                    wrt_columns.write_rows(_cols, parent_dict=cfg)

                    for col in t['columnMetadata']:
                        col_cfg = {**cfg, **{'column': col}}
                        wrt_columns_md.write_rows(t['columnMetadata'][col], col_cfg)

            wrt_tables.write_rows(tables, parent_dict)

    def get_orchestrations(self, parent_dict: dict):

        _orch_tdf = self.build_table_definition('orchestrations')
        orchestrations = self.client.syrup.get_orchestrations()
        orchestrations_sapi = self.client.storage.get_orchestrations()

        with Writer(_orch_tdf) as wrt:
            wrt.write_rows(orchestrations, parent_dict)

        _orch_notif_tdf = self.build_table_definition('orchestrations-notifications')
        _orch_tasks_tdf = self.build_table_definition('orchestrations-tasks')

        with Writer(_orch_notif_tdf) as wrt_notif, Writer(_orch_tasks_tdf) as wrt_tasks:
            for orch in orchestrations:
                orch_id = orch['id']
                _orch_pdict = {**{'orchestration_id': orch_id}, **parent_dict}

                wrt_notif.write_rows(orch['notifications'], _orch_pdict)

                sapi_config = self._get_object_from_list(orchestrations_sapi, 'id', str(orch_id))
                orch_tasks = sapi_config['configuration']['tasks']

                for idx, task in enumerate(orch_tasks):
                    wrt_tasks.write_row({**task, **{'api_index': idx}}, _orch_pdict)

    def get_triggers(self, parent_dict: dict):

        _triggers_tdf = self.build_table_definition('triggers')
        _triggers_tables_tdf = self.build_table_definition('triggers-tables')
        triggers = self.client.storage.get_triggers()

        wrt_triggers = Writer(_triggers_tdf)
        wrt_triggers_tables = Writer(_triggers_tables_tdf)

        with wrt_triggers, wrt_triggers_tables:
            wrt_triggers.write_rows(triggers, parent_dict)

            for trigger in triggers:
                _trigg_pdict = {**{'trigger_id': trigger['id']}, **parent_dict}
                wrt_triggers_tables.write_rows(trigger.get('tables', []), _trigg_pdict)

    def get_workspace_load_events(self, parent_dict: dict, project_key: str):

        _ws_load_events_tdf = self.build_table_definition('workspace-table-loads')
        last_processed_job_id = self.last_processed_transformations.get(project_key)
        transformation_jobs = self.client.syrup.get_transformation_jobs(last_processed_job_id)

        transformation_jobs.reverse()
        encountered_processing = False

        with Writer(_ws_load_events_tdf) as wrt:
            for job in transformation_jobs:
                if job['status'] in ('processing', 'waiting', 'terminating'):
                    encountered_processing = True

                if encountered_processing is False:
                    last_processed_job_id = job['id']

                run_id = job['runId']
                storage_events = self.client.storage.get_workspace_load_events(runId=run_id)
                wrt.write_rows(storage_events, parent_dict)

        self.last_processed_transformations[project_key] = last_processed_job_id

    def get_transformations_v1(self, parent_dict: dict):

        _tr_tdf = self.build_table_definition('transformations')
        _tr_buckets_tdf = self.build_table_definition('transformations-buckets')
        _tr_inputs_tdf = self.build_table_definition('transformations-inputs')
        _tr_inputs_md_tdf = self.build_table_definition('transformations-inputs-metadata')
        _tr_outputs_tdf = self.build_table_definition('transformations-outputs')
        _tr_queries_tdf = self.build_table_definition('transformations-queries')

        buckets = self.client.storage.get_transformations_v1()

        with Writer(_tr_buckets_tdf) as wrt_buckets:
            wrt_buckets.write_rows(buckets, parent_dict)

        wrt_tr = Writer(_tr_tdf)
        wrt_inputs = Writer(_tr_inputs_tdf)
        wrt_inputs_md = Writer(_tr_inputs_md_tdf)
        wrt_outputs = Writer(_tr_outputs_tdf)
        wrt_queries = Writer(_tr_queries_tdf)

        with wrt_tr, wrt_inputs, wrt_inputs_md, wrt_outputs, wrt_queries:

            for bucket in buckets:
                _bucket = {}
                _bucket['bucket_id'] = bucket['id']
                _bucket_parent = {**_bucket, **parent_dict}

                for transformation in bucket['rows']:
                    if transformation['configuration'].get('backend') == 'mysql':
                        continue

                    transformation['configuration']['packages'] = ','.join(transformation['configuration']
                                                                           .get('packages', []))
                    transformation['configuration']['requires'] = ','.join(transformation['configuration']
                                                                           .get('requires', []))

                    _tr_hash = md5('|'.join([transformation['id'], bucket['id']]).encode()).hexdigest()
                    transformation['id_md5'] = _tr_hash
                    wrt_tr.write_row(transformation, _bucket_parent)

                    _tr_parent = {**{'transformation_id': _tr_hash}, **parent_dict}

                    for table_input in transformation['configuration'].get('input', []):
                        table_input['columns'] = ','.join(table_input.get('columns', []))
                        table_input['whereValues'] = ','.join([str(x) for x in table_input.get('whereValues', [])])
                        table_input['loadType'] = table_input.get('loadType', 'copy')

                        wrt_inputs.write_row(table_input, _tr_parent)

                        if transformation['configuration'].get('backend') != 'redshift':
                            for column in table_input.get('datatypes', []):
                                _dt = table_input['datatypes'][column]
                                if _dt is None:
                                    continue
                                _metadata = {**_dt, **{'source': table_input['source'],
                                                       'destination': table_input['destination']}}

                                wrt_inputs_md.write_row(_metadata, _tr_parent)

                    for table_output in transformation['configuration'].get('output', []):
                        table_output['primaryKey'] = ','.join(table_output.get('primaryKey', []))
                        table_output['incremental'] = table_output.get('incremental', False)
                        table_output['deleteWhereValues'] = ','.join(table_output.get('deleteWhereValues', []))

                        wrt_outputs.write_row(table_output, _tr_parent)

                    _queries = [{'query_index': idx, 'query': q} for idx, q in
                                enumerate(transformation['configuration'].get('queries', []))]
                    wrt_queries.write_rows(_queries, _tr_parent)

    def get_transformations_v2(self, parent_dict: dict):

        _tr_tdf = self.build_table_definition('transformations-v2')
        _tr_inputs_tdf = self.build_table_definition('transformations-v2-inputs')
        _tr_inputs_md_tdf = self.build_table_definition('transformations-v2-inputs-metadata')
        _tr_outputs_tdf = self.build_table_definition('transformations-v2-outputs')
        _tr_codes_tdf = self.build_table_definition('transformations-v2-codes')
        wrt_tr = Writer(_tr_tdf)
        wrt_tr_inputs = Writer(_tr_inputs_tdf)
        wrt_tr_inputs_md = Writer(_tr_inputs_md_tdf)
        wrt_tr_outputs = Writer(_tr_outputs_tdf)
        wrt_tr_codes = Writer(_tr_codes_tdf)

        with wrt_tr, wrt_tr_inputs, wrt_tr_inputs_md, wrt_tr_outputs, wrt_tr_codes:

            for tr_cmp_id in TR_V2_CMP_ID:
                tr_configs = self.client.storage.get_component_configurations(tr_cmp_id)

                _cmp_pdict = {**{'component_id': tr_cmp_id}, **parent_dict}

                for tr in tr_configs:
                    tr_id = tr['id']
                    tr['packages'] = ','.join(tr['configuration'].get('parameters', {}).get('packages', []))
                    tr['variables_id'] = tr['configuration'].get('variables_id', '')
                    tr['variables_values_id'] = tr['configuration'].get('variables_values_id', '')

                    wrt_tr.write_row(tr, _cmp_pdict)

                    _tr_pdict = {**{'transformation_id': tr_id}, **_cmp_pdict}
                    io = tr['configuration'].get('storage', {})

                    for ti in io.get('input', {}).get('tables', []):
                        wrt_tr_inputs.write_row(ti, _tr_pdict)

                        _ti_pdict = {**{'table_source': ti['source'],
                                        'table_destination': ti['destination']},
                                     **_tr_pdict}

                        wrt_tr_inputs_md.write_rows(ti.get('column_types', []), _ti_pdict)

                    for to in io.get('output', {}).get('tables', []):
                        wrt_tr_outputs.write_row(to, _tr_pdict)

                    code_blocks = tr['configuration'].get('parameters', {}).get('blocks', [])

                    for cb_idx, cb in enumerate(code_blocks):
                        _cb_parent = {**{'block_name': cb['name'], 'block_index': cb_idx}, **_tr_pdict}

                        for c_idx, c in enumerate(cb.get('codes', [])):
                            _c_parent = {**_cb_parent, **{'code_name': c['name'], 'code_index': c_idx}}

                            for sc_idx, sc in enumerate(c.get('script', [])):
                                wrt_tr_codes.write_row({'script': sc, 'script_index': sc_idx}, _c_parent)

    def get_table_load_events(self, parent_dict: dict):

        _table_events_tdf = self.build_table_definition('tables-load-events')
        wrt = Writer(_table_events_tdf)

        table_ids = [t['id'] for t in self.client.storage.get_all_tables(include=False)]

        with wrt:
            for table in table_ids:
                load_events = self.client.storage.get_table_load_events(table, self.latest_date)
                wrt.write_rows(load_events, parent_dict)

    def get_project_data(self, project_id: str, project_token: str, project_key: str):

        self.client.init_storage_and_syrup_clients(self.parameters.region, project_token, project_id)
        _p_dict = {'region': self.parameters.region, 'project_id': project_id}

        if self.parameters.datasets.get(KEY_GET_WAITING_JOBS):
            self.get_waiting_jobs(_p_dict)

        if self.parameters.datasets.get(KEY_GET_TOKENS):
            self.get_tokens_and_events(_p_dict)

        if self.parameters.datasets.get(KEY_GET_ALL_CONFIGURATIONS):
            self.get_all_configurations(_p_dict)

        if self.parameters.datasets.get(KEY_GET_TABLES):
            self.get_tables(_p_dict)

        if self.parameters.datasets.get(KEY_GET_ORCHESTRATIONS):
            self.get_orchestrations(_p_dict)

        if self.parameters.datasets.get(KEY_GET_TRIGGERS):
            self.get_triggers(_p_dict)

        if self.parameters.datasets.get(KEY_GET_WORKSPACE_LOAD_EVENTS):
            self.get_workspace_load_events(_p_dict, project_key)

        if self.parameters.datasets.get(KEY_GET_TRANSFORMATIONS):
            self.get_transformations_v1(_p_dict)

        if self.parameters.datasets.get(KEY_GET_TRANSFORMATIONS_V2):
            self.get_transformations_v2(_p_dict)

        if self.parameters.datasets.get(KEY_GET_TABLES_LOAD_EVENTS):
            self.get_table_load_events(_p_dict)
            self.latest_date = dateparser.parse('today').strftime('%Y-%m-%d')

    def run(self):

        if self.parameters.client_to_use == 'management':

            _man_token = self.parameters.master_token[0]
            self.management_token = ManagementToken(_man_token[KEY_MAN_TOKEN], _man_token[KEY_ORGANIZATION_ID],
                                                    _man_token[KEY_REGION])

            self.parameters.region = self.determine_stack(self.management_token.region)

            self.client.init_management_client(self.parameters.region, self.management_token.token,
                                               self.management_token.organization_id)

            all_projects = self.client.management.get_organization()['projects']
            all_project_ids = [prj['id'] for prj in all_projects]

            self.download_organization_data(all_project_ids)

            storage_data_bool = [self.parameters.datasets.get(key, False) for key in STORAGE_ENDPOINTS]
            if any(storage_data_bool):

                for prj in all_projects:

                    prj_id = str(prj['id'])
                    prj_name = prj['name']
                    prj_region = self.parameters.region
                    prj_token_description = prj_name + TOKEN_SUFFIX
                    prj_token_key = '|'.join([prj_region.replace('-', '_'), prj_id])

                    prj_token_old = self.previous_tokens.get(prj_token_key)

                    if not prj_token_old:
                        logging.debug(f"Creating new storage token for project {prj_id} in stack {prj_region}.")
                        prj_token_new = self.client.management.create_storage_token(prj_id, prj_token_description)
                        prj_token = {
                            'id': prj_token_new['id'],
                            '#token': prj_token_new['token'],
                            'expires': self.convert_iso_format_to_epoch_timestamp(prj_token_new['expires'])
                        }

                    else:

                        valid = self.is_token_valid(prj_token_old['#token'], prj_token_old['expires'],
                                                    prj_region, prj_id)

                        if valid:
                            logging.debug(f"Using token {prj_token_old['id']} from state for project {prj_id} in "
                                          f"stack {prj_region}.")
                            prj_token = prj_token_old

                        else:
                            logging.debug(f"Creating new storage token for project {prj_id} in stack {prj_region}.")
                            prj_token_new = self.client.management.create_storage_token(prj_id, prj_token_description)

                            prj_token = {
                                'id': prj_token_new['id'],
                                '#token': prj_token_new['token'],
                                'expires': self.convert_iso_format_to_epoch_timestamp(prj_token_new['expires'])
                            }

                    logging.info(f"Downloading data for project {prj_name} in stack {prj_region}.")
                    self.get_project_data(prj_id, prj_token['#token'], prj_token_key)
                    self.new_tokens[prj_token_key] = prj_token

        else:
            for idx, prj in enumerate(self.parameters.tokens):

                prj_token = prj[KEY_SAP_TOKEN]
                self.parameters.region = self.determine_stack(prj[KEY_REGION])
                prj_id = prj_token.split('-')[0]
                prj_token_key = '|'.join([self.parameters.region.replace('-', '_'), prj_id])

                if prj_token.strip() == '':
                    logging.error(f"Token as position {idx} is empty. Skipping.")
                    continue

                logging.info(f"Downloading data for project {prj_id} in stack {self.parameters.region}.")
                self.get_project_data(prj_id, prj_token, prj_token_key)

        new_state = {
            'tokens': self.new_tokens,
            'tr_last_processed_id': self.last_processed_transformations,
            'date': self.latest_date
        }

        self.write_state_file(new_state)
        self.write_manifests(self.table_definitions.values())


if __name__ == '__main__':

    m = Component()
    m.run()
