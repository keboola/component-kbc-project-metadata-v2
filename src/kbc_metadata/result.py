import os
import csv
import json

FIELDS_COMPONENTS = ['id', 'type', 'name', 'flags']
PK_COMPONENTS = ['id']
JSON_COMPONENTS = ['flags']

FIELDS_ORCHESTRATIONS = ['id', 'region', 'name', 'crontabRecord', 'crontabTimezone', 'createdTime', 'lastScheduledTime',
                         'nextScheduledTime', 'token_id', 'token_description', 'active', 'lastExecutedJob_id',
                         'lastExecutedJob_status', 'lastExecutedJob_createdTime', 'lastExecutedJob_startTime',
                         'lastExecutedJob_endTime']
FIELDS_R_ORCHESTRATIONS = ['id', 'region', 'name', 'crontab_record', 'crontab_timezone', 'created_time',
                           'last_scheduled_time', 'next_scheduled_time', 'token_id', 'token_description', 'active',
                           'last_executed_job_id', 'last_executed_job_status', 'last_executed_job_created_time',
                           'last_executed_job_start_time', 'last_executed_job_end_time']

PK_ORCHESTRATIONS = ['id', 'region']
JSON_ORCHESTRATIONS = []

FIELDS_WAITING_JOBS = ['id', 'region', 'runId', 'project_id', 'project_name', 'token_id', 'token_description',
                       'component', 'status', 'createdTime', 'startTime', 'endTime']
FIELDS_R_WAITING_JOBS = ['id', 'region', 'run_id', 'project_id', 'project_name', 'token_id', 'token_description',
                         'component', 'status', 'created_time', 'start_time', 'end_time']
PK_WAITING_JOBS = ['id', 'region']
JSON_WAITING_JOBS = []

FIELDS_TOKENS = ['id', 'region', 'created', 'refreshed', 'description', 'isMasterToken', 'canManageBuckets',
                 'canManageTokens', 'canReadAllFileUploads', 'canPurgeTrash', 'expires', 'isExpired', 'isDisabled',
                 'dailyCapacity', 'creatorToken_id', 'creatorToken_description']
FIELDS_R_TOKENS = ['id', 'region', 'created', 'refreshed', 'description', 'is_master_token', 'can_manage_buckets',
                   'can_manage_tokens', 'can_read_all_file_uploads', 'can_purge_trash', 'expires', 'is_expired',
                   'is_disabled', 'daily_capacity', 'creator_token_id', 'creator_token_description']
PK_TOKENS = ['id', 'region']
JSON_TOKENS = []


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
