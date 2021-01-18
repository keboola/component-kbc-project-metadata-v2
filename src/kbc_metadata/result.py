import os
import csv
import json

FIELDS_ORCHESTRATIONS = ['id', 'region', 'project_id', 'name', 'crontabRecord', 'crontabTimezone', 'createdTime',
                         'lastScheduledTime', 'nextScheduledTime', 'token_id', 'token_description', 'active',
                         'lastExecutedJob_id', 'lastExecutedJob_status', 'lastExecutedJob_createdTime',
                         'lastExecutedJob_startTime', 'lastExecutedJob_endTime']
FIELDS_R_ORCHESTRATIONS = ['id', 'region', 'project_id', 'name', 'crontab_record', 'crontab_timezone', 'created_time',
                           'last_scheduled_time', 'next_scheduled_time', 'token_id', 'token_description', 'active',
                           'last_executed_job_id', 'last_executed_job_status', 'last_executed_job_created_time',
                           'last_executed_job_start_time', 'last_executed_job_end_time']

PK_ORCHESTRATIONS = ['id', 'region']
JSON_ORCHESTRATIONS = []

FIELDS_ORCHESTRATIONS_TASKS = ['id', 'orchestration_id', 'region', 'component', 'action', 'actionParameters',
                               'timeoutMinutes', 'active', 'continueOnFailure', 'phase', 'api_index']
FIELDS_R_ORCHESTRATIONS_TASKS = ['id', 'orchestration_id', 'region', 'component_id', 'action', 'action_parameters',
                                 'timeout_minutes', 'is_active', 'continue_on_failure', 'phase', 'api_index']
PK_ORCHESTRATIONS_TASKS = ['id', 'region']
JSON_ORCHESTRATIONS_TASKS = ['actionParameters']

FIELDS_ORCHESTRATIONS_NOTIFICATIONS = ['orchestration_id', 'region', 'email', 'channel', 'parameters']
FIELDS_R_ORCHESTRATIONS_NOTIFICATIONS = FIELDS_ORCHESTRATIONS_NOTIFICATIONS
PK_ORCHESTRATIONS_NOTIFICATIONS = ['orchestration_id', 'region', 'email', 'channel']
JSON_ORCHESTRATIONS_NOTIFICATIONS = ['parameters']

FIELDS_WAITING_JOBS = ['id', 'region', 'runId', 'project_id', 'project_name', 'token_id', 'token_description',
                       'component', 'status', 'createdTime', 'startTime', 'endTime', 'params_config',
                       'params_configBucketId']
FIELDS_R_WAITING_JOBS = ['id', 'region', 'run_id', 'project_id', 'project_name', 'token_id', 'token_description',
                         'component', 'status', 'created_time', 'start_time', 'end_time', 'params_configuration_id',
                         'params_configuration_bucket_id']
PK_WAITING_JOBS = ['id', 'region']
JSON_WAITING_JOBS = []

FIELDS_TOKENS = ['id', 'region', 'project_id', 'created', 'refreshed', 'description', 'isMasterToken',
                 'canManageBuckets', 'canManageTokens', 'canReadAllFileUploads', 'canPurgeTrash', 'expires',
                 'isExpired', 'isDisabled', 'dailyCapacity', 'creatorToken_id', 'creatorToken_description',
                 'admin_id', 'admin_name']
FIELDS_R_TOKENS = ['id', 'region', 'project_id', 'created', 'refreshed', 'description', 'is_master_token',
                   'can_manage_buckets', 'can_manage_tokens', 'can_read_all_file_uploads', 'can_purge_trash', 'expires',
                   'is_expired', 'is_disabled', 'daily_capacity', 'creator_token_id', 'creator_token_description',
                   'admin_id', 'admin_name']
PK_TOKENS = ['id', 'region']
JSON_TOKENS = []

FIELDS_TOKENS_LAST_EVENTS = ['token_id', 'region', 'project_id', 'id', 'event', 'component', 'message',
                             'description', 'type', 'created', 'configurationId', 'objectId', 'objectName',
                             'objectType', 'uri']
FIELDS_R_TOKENS_LAST_EVENTS = ['token_id', 'region', 'project_id', 'event_id', 'event', 'component', 'message',
                               'description', 'type', 'event_created', 'configuration_id', 'object_id',
                               'object_name', 'object_type', 'uri']
PK_TOKENS_LAST_EVENTS = ['token_id', 'region', 'project_id']
JSON_TOKENS_LAST_EVENTS = []

FIELDS_CONFIGURATIONS = ['id', 'region', 'project_id', 'name', 'created', 'creatorToken_id', 'creatorToken_description',
                         'component_id', 'component_name', 'component_type',
                         'version', 'isDeleted', 'currentVersion_created', 'currentVersion_creatorToken_id',
                         'currentVersion_creatorToken_description', 'currentVersion_changeDescription', 'description']
FIELDS_R_CONFIGURATIONS = ['id', 'region', 'project_id', 'name', 'created', 'creator_token_id',
                           'creator_token_description', 'component_id', 'component_name', 'component_type',
                           'version', 'is_deleted', 'current_version_created', 'current_version_creator_token_id',
                           'current_version_creator_token_description', 'current_version_change_description',
                           'description']
PK_CONFIGURATIONS = ['id', 'region']
JSON_CONFIGURATIONS = []

FIELDS_TABLES = ['id', 'region', 'project_id', 'name', 'primaryKey', 'created', 'lastImportDate', 'lastChangeDate',
                 'rowsCount', 'dataSizeBytes', 'isAlias', 'isAliasable', 'bucket_id', 'bucket_name', 'bucket_stage',
                 'bucket_created', 'bucket_lastChangeDate', 'bucket_isReadOnly', 'bucket_sharing',
                 'bucket_sharedBy_id', 'bucket_sharedBy_name', 'bucket_sharedBy_date', 'sourceTable_id',
                 'sourceTable_project_id']
FIELDS_R_TABLES = ['id', 'region', 'project_id', 'name', 'primary_key', 'created', 'last_import_date',
                   'last_change_date', 'rows_count', 'data_size_bytes', 'is_alias', 'is_aliasable', 'bucket_id',
                   'bucket_name', 'bucket_stage', 'bucket_created', 'bucket_last_change_date', 'bucket_is_read_only',
                   'sharing', 'shared_by_id', 'shared_by_name', 'shared_by_date', 'source_table_id',
                   'source_table_project_id']
PK_TABLES = ['id', 'region', 'project_id']
JSON_TABLES = []

FIELDS_TABLES_METADATA = ['table_id', 'region', 'project_id', 'id', 'key', 'value', 'provider', 'timestamp']
FIELDS_R_TABLES_METADATA = FIELDS_TABLES_METADATA
PK_TABLES_METADATA = ['id', 'table_id', 'region', 'project_id']
JSON_TABLES_METADATA = []

FIELDS_TABLES_COLUMNS = ['table_id', 'region', 'project_id', 'column']
FIELDS_R_TABLES_COLUMNS = FIELDS_TABLES_COLUMNS
PK_TABLES_COLUMNS = ['table_id', 'region', 'project_id', 'column']
JSON_TABLES_COLUMNS = []

FIELDS_TABLES_COLUMNS_METADATA = ['table_id', 'region', 'project_id', 'column', 'id', 'key',
                                  'value', 'provider', 'timestamp']
FIELDS_R_TABLES_COLUMNS_METADATA = FIELDS_TABLES_COLUMNS_METADATA
PK_TABLES_COLUMNS_METADATA = ['id']
JSON_TABLES_COLUMNS_METADATA = []

FIELDS_TRANSFORMATIONS_BUCKETS = ['id', 'region', 'project_id', 'name', 'description', 'version', 'created',
                                  'creatorToken_id', 'creatorToken_description', 'changeDescription',
                                  'currentVersion_created', 'currentVersion_creatorToken_id',
                                  'currentVersion_creatorToken_description']
FIELDS_R_TRANSFORMATIONS_BUCKETS = ['id', 'region', 'project_id', 'name', 'description', 'version', 'created',
                                    'creator_token_id', 'creator_token_description', 'change_description',
                                    'current_version_created', 'current_version_creator_token_id',
                                    'current_version_creator_token_description']
PK_TRANSFORMATIONS_BUCKETS = ['id', 'region', 'project_id']
JSON_TRANSFORMATIONS_BUCKETS = []

FIELDS_TRANSFORMATIONS = ['id_md5', 'id', 'region', 'project_id', 'bucket_id', 'name', 'description',
                          'configuration_packages', 'configuration_requires', 'configuration_backend',
                          'configuration_type', 'configuration_phase', 'configuration_disabled', 'version', 'created',
                          'creatorToken_id', 'creatorToken_description', 'changeDescription']
FIELDS_R_TRANSFORMATIONS = ['id', 'number', 'region', 'project_id', 'bucket_id', 'name', 'description',
                            'packages', 'requires', 'backend', 'type', 'phase', 'disabled', 'version', 'created',
                            'creator_token_id', 'creator_token_description', 'change_description']
PK_TRANSFORMATIONS = ['id', 'region', 'project_id']
JSON_TRANSFORMATIONS = []

FIELDS_TRANSFORMATIONS_INPUTS = ['transformation_id', 'region', 'source', 'destination', 'loadType',
                                 'whereColumn', 'whereValues', 'whereOperator', 'changedSince', 'columns']
FIELDS_R_TRANSFORMATIONS_INPUTS = ['transformation_id', 'region', 'source', 'destination', 'load_type',
                                   'filter_where_column', 'filter_where_values', 'filter_where_operator',
                                   'filter_changed_since', 'input_columns']
PK_TRANSFORMATIONS_INPUTS = ['transformation_id', 'region', 'source', 'destination']
JSON_TRANSFORMATIONS_INPUTS = []

FIELDS_TRANSFORMATIONS_INPUTS_METADATA = ['transformation_id', 'region', 'source', 'destination', 'column', 'type',
                                          'length', 'convertEmptyValuesToNull']
FIELDS_R_TRANSFORMATIONS_INPUTS_METADATA = ['transformation_id', 'region', 'source', 'destination', 'column',
                                            'datatype', 'length', 'convert_empty_values_to_null']
PK_TRANSFORMATIONS_INPUTS_METADATA = ['transformation_id', 'region', 'source', 'destination', 'column']
JSON_TRANSFORMATIONS_INPUTS_METADATA = []

FIELDS_TRANSFORMATIONS_OUTPUTS = ['transformation_id', 'region', 'destination', 'source', 'primaryKey',
                                  'incremental', 'deleteWhereColumn', 'deleteWhereOperator', 'deleteWhereValues']
FIELDS_R_TRANSFORMATIONS_OUTPUTS = ['transformation_id', 'region', 'destination', 'source', 'primary_key',
                                    'incremental_load', 'delete_where_column', 'delete_where_operator',
                                    'delete_where_values']
PK_TRANSFORMATIONS_OUTPUTS = ['transformation_id', 'region', 'destination', 'source']
JSON_TRANSFORMATIONS_OUTPUTS = []

FIELDS_PROJECT_USERS = ['id', 'region', 'project_id', 'name', 'email', 'mfaEnabled', 'canAccessLogs', 'isSuperAdmin',
                        'expires', 'created', 'reason', 'role', 'status', 'invitor_id', 'invitor_name', 'invitor_email',
                        'approver_id', 'approver_name', 'approver_email']
FIELDS_R_PROJECT_USERS = ['id', 'region', 'project_id', 'name', 'email', 'mfa_enabled', 'can_access_logs',
                          'is_super_admin', 'expires', 'created', 'reason', 'role', 'status', 'invitor_id',
                          'invitor_name', 'invitor_email', 'approver_id', 'approver_name', 'approver_email']
PK_PROJECT_USERS = ['id', 'region', 'project_id']
JSON_PROJECT_USERS = []

FIELDS_ORGANIZATION_USERS = ['id', 'region', 'organization_id', 'name', 'email', 'mfaEnabled', 'canAccessLogs',
                             'isSuperAdmin', 'created', 'invitor_id', 'invitor_name', 'invitor_email']
FIELDS_R_ORGANIZATION_USERS = ['id', 'region', 'organization_id', 'name', 'email', 'mfa_enabled', 'can_access_logs',
                               'is_super_admin', 'created', 'invitor_id', 'invitor_name', 'invitor_email']
PK_ORGANIZATION_USERS = ['id', 'region', 'organization_id']
JSON_ORGANIZATION_USERS = []

FIELDS_TRANSFORMATIONS_QUERIES = ['transformation_id', 'region', 'query_index', 'query', 'bucket_id']
FIELDS_R_TRANSFORMATIONS_QUERIES = FIELDS_TRANSFORMATIONS_QUERIES
PK_TRANSFORMATIONS_QUERIES = ['transformation_id', 'region', 'query_index']
JSON_TRANSFORMATIONS_QUERIES = []

FIELDS_TRIGGERS = ['id', 'region', 'project_id', 'runWithTokenId', 'component', 'configurationId', 'lastRun',
                   'creatorToken_id', 'creatorToken_description', 'coolDownPeriodMinutes']
FIELDS_R_TRIGGERS = ['id', 'region', 'project_id', 'run_with_token_id', 'component', 'configuration_id', 'last_run',
                     'creator_token_id', 'creator_token_description', 'cooldown_period_minutes']
PK_TRIGGERS = ['id', 'region', 'project_id']
JSON_TRIGGERS = []

FIELDS_TRIGGERS_TABLES = ['trigger_id', 'region', 'project_id', 'tableId']
FIELDS_R_TRIGGERS_TABLES = ['trigger_id', 'region', 'project_id', 'table_id']
PK_TRIGGERS_TABLES = ['trigger_id', 'region', 'project_id']
JSON_TRIGGERS_TABLES = []


class MetadataWriter:

    def __init__(self, tableOutPath, tableName, incremental):

        self.paramPath = tableOutPath
        self.paramTableName = tableName
        self.paramTable = tableName + '.csv'
        self.paramTablePath = os.path.join(self.paramPath, self.paramTable)
        self.paramFields = eval(f'FIELDS_{tableName.upper().replace("-", "_")}')
        self.paramJsonFields = eval(f'JSON_{tableName.upper().replace("-", "_")}')
        self.paramPrimaryKey = eval(f'PK_{tableName.upper().replace("-", "_")}')
        self.paramFieldsRenamed = eval(f'FIELDS_R_{tableName.upper().replace("-", "_")}')
        self.paramIncremental = incremental

        self.createManifest()
        self.createWriter()

    def createManifest(self):

        template = {
            'incremental': self.paramIncremental,
            'primary_key': self.paramPrimaryKey,
            'columns': self.paramFieldsRenamed
        }

        path = self.paramTablePath + '.manifest'

        with open(path, 'w') as manifest:

            json.dump(template, manifest)

    def createWriter(self):

        self.writer = csv.DictWriter(open(self.paramTablePath, 'w'), fieldnames=self.paramFields,
                                     restval='', extrasaction='ignore', quotechar='\"', quoting=csv.QUOTE_ALL)

    def writerows(self, listToWrite, parentDict=None):

        for row in listToWrite:

            row_f = self.flatten_json(x=row)

            if self.paramJsonFields != []:
                for field in self.paramJsonFields:
                    row_f[field] = json.dumps(row[field])

            _dictToWrite = {}

            for key, value in row_f.items():

                if key in self.paramFields:
                    _dictToWrite[key] = value

                else:
                    continue

            if parentDict is not None:
                _dictToWrite = {**_dictToWrite, **parentDict}

            self.writer.writerow(_dictToWrite)

    def flatten_json(self, x, out=None, name=''):
        if out is None:
            out = dict()

        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '_')
        else:
            out[name[:-1]] = x

        return out
