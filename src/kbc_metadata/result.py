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
                 'isExpired', 'isDisabled', 'dailyCapacity', 'creatorToken_id', 'creatorToken_description']
FIELDS_R_TOKENS = ['id', 'region', 'project_id', 'created', 'refreshed', 'description', 'is_master_token',
                   'can_manage_buckets', 'can_manage_tokens', 'can_read_all_file_uploads', 'can_purge_trash', 'expires',
                   'is_expired', 'is_disabled', 'daily_capacity', 'creator_token_id', 'creator_token_description']
PK_TOKENS = ['id', 'region']
JSON_TOKENS = []

FIELDS_CONFIGURATIONS = ['id', 'region', 'project_id', 'name', 'created', 'creatorToken_id', 'creatorToken_description',
                         'component_id', 'component_name', 'component_type',
                         'version', 'isDeleted', 'currentVersion_created', 'currentVersion_creatorToken_id',
                         'currentVersion_creatorToken_description', 'currentVersion_changeDescription']
FIELDS_R_CONFIGURATIONS = ['id', 'region', 'project_id', 'name', 'created', 'creator_token_id',
                           'creator_token_description', 'component_id', 'component_name', 'component_type',
                           'version', 'is_deleted', 'current_version_created', 'current_version_creator_token_id',
                           'current_version_creator_token_description', 'current_version_change_description']
PK_CONFIGURATIONS = ['id', 'region']
JSON_CONFIGURATIONS = []

FIELDS_TABLES = ['id', 'region', 'project_id', 'name', 'primaryKey', 'created', 'lastImportDate', 'lastChangeDate',
                 'rowsCount', 'dataSizeBytes', 'isAlias', 'isAliasable', 'bucket_id', 'bucket_name', 'bucket_stage',
                 'bucket_created', 'bucket_lastChangeDate', 'bucket_isReadOnly']
FIELDS_R_TABLES = ['id', 'region', 'project_id', 'name', 'primary_key', 'created', 'last_import_date',
                   'last_change_date', 'rows_count', 'data_size_bytes', 'is_alias', 'is_aliasable', 'bucket_id',
                   'bucket_name', 'bucket_stage', 'bucket_created', 'bucket_last_change_date', 'bucket_is_read_only']
PK_TABLES = ['id', 'region', 'project_id']
JSON_TABLES = []

FIELDS_TABLES_METADATA = ['table_id', 'region', 'project_id', 'id', 'key', 'value', 'provider', 'timestamp']
FIELDS_R_TABLES_METADATA = FIELDS_TABLES_METADATA
PK_TABLES_METADATA = ['id', 'table_id', 'region', 'project_id']
JSON_TABLES_METADATA = []

FIELDS_TRANSFORMATIONS_BUCKETS = ['id', 'region', 'project_id', 'name', 'version', 'created', 'creatorToken_id',
                                  'creatorToken_description', 'changeDescription', 'currentVersion_created',
                                  'currentVersion_creatorToken_id', 'currentVersion_creatorToken_description']
FIELDS_R_TRANSFORMATIONS_BUCKETS = ['id', 'region', 'project_id', 'name', 'version', 'created', 'creator_token_id',
                                    'creator_token_description', 'change_description', 'current_version_created',
                                    'current_version_creator_token_id', 'current_version_creator_token_description']
PK_TRANSFORMATIONS_BUCKETS = ['id', 'region', 'project_id']
JSON_TRANSFORMATIONS_BUCKETS = []

FIELDS_TRANSFORMATIONS = ['id', 'region', 'project_id', 'bucket_id', 'name', 'configuration_packages',
                          'configuration_requires', 'configuration_backend', 'configuration_type',
                          'configuration_phase', 'configuration_disabled', 'version', 'created',
                          'creatorToken_id', 'creatorToken_description', 'changeDescription']
FIELDS_R_TRANSFORMATIONS = ['id', 'region', 'project_id', 'bucket_id', 'name', 'packages', 'requires', 'backend',
                            'type', 'phase', 'disabled', 'version', 'created', 'creator_token_id',
                            'creator_token_description', 'change_description']
PK_TRANSFORMATIONS = ['id', 'region', 'project_id']
JSON_TRANSFORMATIONS = []

FIELDS_TRANSFORMATIONS_INPUTS = ['transformation_id', 'region', 'source', 'destination', 'loadType',
                                 'whereColumn', 'whereValues', 'whereOperator', 'changedSince', 'columns']
FIELDS_R_TRANSFORMATIONS_INPUTS = ['transformation_id', 'region', 'source', 'destination', 'load_type',
                                   'filter_where_column', 'filter_where_values', 'filter_where_operator',
                                   'filter_changed_since', 'input_columns']
PK_TRANSFORMATIONS_INPUTS = ['transformation_id', 'region', 'source', 'destination']
JSON_TRANSFORMATIONS_INPUTS = []

FIELDS_TRANSFORMATIONS_OUTPUTS = ['transformation_id', 'region', 'destination', 'source', 'primaryKey',
                                  'incremental', 'deleteWhereColumn', 'deleteWhereOperator', 'deleteWhereValues']
FIELDS_R_TRANSFORMATIONS_OUTPUTS = ['transformation_id', 'region', 'destination', 'source', 'primary_key',
                                    'incremental_load', 'delete_where_column', 'delete_where_operator',
                                    'delete_where_values']
PK_TRANSFORMATIONS_OUTPUTS = ['transformation_id', 'region', 'destination', 'source']
JSON_TRANSFORMATIONS_OUTPUTS = []


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

            row = self.flatten_json(row)
            _dictToWrite = {}

            for key, value in row.items():

                if key in self.paramJsonFields:
                    _dictToWrite[key] = json.dumps(value)

                elif key in self.paramFields:
                    _dictToWrite[key] = value

                else:
                    continue

            if parentDict is not None:
                _dictToWrite = {**_dictToWrite, **parentDict}

            self.writer.writerow(_dictToWrite)

    def flatten_json(self, x, out={}, name=''):
        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '_')
        else:
            out[name[:-1]] = x

        return out
