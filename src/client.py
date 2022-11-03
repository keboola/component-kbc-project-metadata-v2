import logging
import sys
from dataclasses import dataclass
from json import JSONDecodeError

import requests
from keboola.http_client import HttpClient

DEFAULT_TOKEN_EXPIRATION = 26 * 60 * 60  # Default token expiration set to 26 hours

KEBOOLA_API_URLS = {
    'syrup': 'https://syrup.{REGION}',
    'storage': 'https://connection.{REGION}/v2/storage',
    'management': 'https://connection.{REGION}/manage',
}


@dataclass
class SAPIParameters:
    token: str
    region: str
    project: str


@dataclass
class ManAPIParameters:
    token: str
    region: str
    organization: str


def response_splitter(response: requests.models.Response):
    if not isinstance(response, requests.models.Response):
        raise TypeError(f"Expecting type requests.models.Response, received type {type(response)}")

    else:
        try:
            return response.status_code, response.json()
        except JSONDecodeError as e:
            logging.error(f"Cannot parse response : {response.text} from: {response.request.path_url}")
            raise e


class StorageClient(HttpClient):
    LIMIT = 100

    def __init__(self, region: str, token: str, project: str):

        _default_header = {'x-storageapi-token': token}
        _url = KEBOOLA_API_URLS['storage'].format(REGION=region)

        logging.debug(f"Storage URL set to: {_url}")

        super().__init__(base_url=_url, default_http_header=_default_header)
        self.parameters = SAPIParameters(token, region, project)

    def verify_storage_token(self) -> bool:

        rsp_verify = self.get_raw('tokens/verify')
        sc_verify, js_verify = response_splitter(rsp_verify)

        if sc_verify == 200:
            return True

        elif sc_verify in (400, 401):
            return False

        else:
            logging.exception(f"Could not verify storage token validity.\nReceived: {sc_verify} - {js_verify}.")
            sys.exit(1)

    def get_tokens(self) -> list:

        rsp_tokens = self.get_raw('tokens')
        sc_tokens, js_tokens = response_splitter(rsp_tokens)

        if sc_tokens == 200:
            return js_tokens

        else:
            logging.error(f"Could not download tokens for project {self.parameters.project} for stack "
                          f"{self.parameters.region}.\nReceived: {sc_tokens} - {js_tokens}.")
            sys.exit(1)

    def get_component_configurations(self, component_id: str) -> list:

        rsp_configs = self.get_raw(f'components/{component_id}/configs')
        sc_configs, js_configs = response_splitter(rsp_configs)

        if sc_configs == 200:
            return js_configs
        else:
            logging.error(f"Could not download configurations of component {component_id} for project "
                          f"{self.parameters.project} in"
                          f"stack {self.parameters.region}.\nReceived: {sc_configs} - {js_configs}.")
            sys.exit(1)

    def get_all_configurations(self) -> list:

        par_configs = {'include': 'configuration,rows'}

        rsp_configs = self.get_raw('components', params=par_configs)
        sc_configs, js_configs = response_splitter(rsp_configs)

        if sc_configs == 200:
            return js_configs

        else:
            logging.error(f"Could not download configurations for project {self.parameters.project} in"
                          f"stack {self.parameters.region}.\nReceived: {sc_configs} - {js_configs}.")
            sys.exit(1)

    def get_orchestrations(self) -> list:

        return self.get_component_configurations('orchestrator')

    def get_transformations_v1(self) -> list:

        return self.get_component_configurations('transformation')

    def get_storage_buckets(self) -> list:

        rsp_buckets = self.get_raw('buckets')
        sc_buckets, js_buckets = response_splitter(rsp_buckets)

        if sc_buckets == 200:
            return js_buckets

        else:
            logging.error(f"Could not download storage buckets for project {self.parameters.project} in stack "
                          f"{self.parameters.region}.\nReceived: {sc_buckets} - {js_buckets}.")
            sys.exit(1)

    def get_all_tables(self, include: bool = True) -> list:

        if include:
            par_tables = {'include': 'metadata,buckets,columns,columnMetadata'}
        else:
            par_tables = {}

        rsp_tables = self.get_raw('tables', params=par_tables)
        sc_tables, js_tables = response_splitter(rsp_tables)

        if sc_tables == 200:
            return js_tables

        else:
            logging.error(f"Could not download storage buckets for project {self.parameters.project} in stack "
                          f"{self.parameters.region}.\nReceived: {sc_tables} - {js_tables}.")
            sys.exit(1)

    def get_triggers(self) -> list:

        rsp_triggers = self.get_raw('triggers')
        sc_triggers, js_triggers = response_splitter(rsp_triggers)

        if sc_triggers == 200:
            return js_triggers

        else:
            logging.error(f"Could not download triggers for project {self.parameters.project} "
                          f"in stack {self.parameters.region}.\nReceived: {sc_triggers} - {js_triggers}.")
            sys.exit(1)

    def get_tokens_last_events(self, token_id: str) -> list:

        par_events = {'limit': 1}

        rsp_events = self.get_raw(f'tokens/{token_id}/events', params=par_events)
        sc_events, js_events = response_splitter(rsp_events)

        if sc_events == 200:
            return js_events

        else:
            logging.error(f"Could not download last token event for token {token_id} in project "
                          f"{self.parameters.project} in stack {self.parameters.region}.\n"
                          f"Received: {sc_events} - {js_events}.")
            return []

    def get_workspace_load_events(self, **kwargs):

        kwargs['component'] = 'storage'
        kwargs['q'] = 'event:storage.workspaceLoaded'

        return self._get_paged_events('events', **kwargs)

    def get_table_load_events(self, table_id: str, date: str, **kwargs):

        TABLE_LOAD_EVENTS = ['storage.tableExported', 'storage.tableImportError', 'storage.tableImportStarted',
                             'storage.tableImportDone', 'storage.workspaceLoaded', 'storage.workspaceTableCloned']

        kwargs['component'] = 'storage'
        kwargs['q'] = f"({' OR '.join([f'event:{e}' for e in TABLE_LOAD_EVENTS])}) AND created:>={date}"

        return self._get_paged_events(f'tables/{table_id}/events', **kwargs)

    def _get_paged_events(self, url: str, **kwargs):

        par_events = kwargs
        par_events['limit'] = 1000

        offset = 0
        is_complete = False
        all_events = []
        max_id = 0

        while not is_complete:
            par_events['offset'] = 0
            if max_id:
                par_events.pop('offset', None)
                par_events['maxId'] = offset

            rsp_events = self.get_raw(url, params=par_events)
            sc_events, js_events = response_splitter(rsp_events)

            if sc_events == 200:

                all_events += js_events
                if js_events and max_id != js_events[-1:][0]['id']:
                    max_id = js_events[-1:][0]['id']

                if not js_events or max_id == js_events[-1:][0]['id']:
                    is_complete = True
                    return all_events

            else:
                logging.error(f"Could not download events for url {url} in project {self.parameters.project} ",
                              f"in stack {self.parameters.region}.\nReceived: {sc_events} - {js_events}.")
                sys.exit(1)


class SyrupClient(HttpClient):
    LIMIT = 1000

    def __init__(self, region: str, token: str, project: str):

        _default_header = {'x-storageapi-token': token}
        _url = KEBOOLA_API_URLS['syrup'].format(REGION=region)

        logging.debug(f"Syrup URL set to: {_url}")

        super().__init__(base_url=_url, default_http_header=_default_header)
        self.parameters = SAPIParameters(token, region, project)

    def get_waiting_and_processing_jobs(self) -> list:

        par_jobs = {'q': 'status:waiting OR status:processing'}
        return self._get_paged_jobs(**par_jobs)

    def get_transformation_jobs(self, last_job_id: str = None, **kwargs) -> list:

        q = '(component:transformation OR params.component:transformation)'
        # ' AND -(status:processing OR status:waiting OR status:terminating)'

        if last_job_id is not None:
            q += f' AND id:>{last_job_id}'
            logging.debug(f"Downloading transformations jobs since last job id {last_job_id}.")
        else:
            q += ' AND createdTime:>now-7d'
            logging.debug("Downloading transformations jobs created in the last 7 days.")

        kwargs['q'] = q

        return self._get_paged_jobs(**kwargs)

    def _get_paged_jobs(self, **kwargs) -> list:

        par_jobs = kwargs
        par_jobs['limit'] = self.LIMIT

        offset = 0
        is_complete = False
        all_jobs = []

        while is_complete is False:
            par_jobs['offset'] = offset

            rsp_jobs = self.get_raw('queue/jobs', params=par_jobs)
            sc_jobs, js_jobs = response_splitter(rsp_jobs)

            if sc_jobs == 200:
                all_jobs += js_jobs

                if len(js_jobs) < self.LIMIT:
                    is_complete = True
                    return all_jobs

                else:
                    offset += self.LIMIT

            else:
                logging.error(f"Could not download jobs for project {self.parameters.project} in stack "
                              f"{self.parameters.region}.\nReceived: {sc_jobs} - {js_jobs}.")
                sys.exit(1)

    def get_orchestrations(self) -> list:

        rsp_orch = self.get_raw('orchestrator/orchestrations')
        sc_orch, js_orch = response_splitter(rsp_orch)

        if sc_orch == 200:
            return js_orch

        else:
            logging.error(f"Could not list orchestrations for project {self.parameters.project} in stack "
                          f"{self.parameters.region}.\nReceived: {sc_orch} - {js_orch}.")
            sys.exit(1)

    def get_orchestration_tasks(self, orchestration_id: str) -> list:

        rsp_tasks = self.get_raw(f'orchestrator/orchestrations/{orchestration_id}/tasks')
        sc_tasks, js_tasks = response_splitter(rsp_tasks)

        if sc_tasks == 200:
            return js_tasks

        else:
            logging.error(f"Could not list orchestration tasks for orchestration {orchestration_id} in "
                          f"project {self.parameters.project} in stack {self.parameters.region}.\n"
                          f"Received: {sc_tasks} - {js_tasks}.")
            sys.exit(1)


class ManagementClient(HttpClient):

    def __init__(self, region: str, token: str, organization: str):

        _default_header = {'X-KBC-ManageApiToken': token}
        _url = KEBOOLA_API_URLS['management'].format(REGION=region)

        super().__init__(base_url=_url, default_http_header=_default_header)
        self.parameters = ManAPIParameters(token, region, organization)
        self.verify_token()

    def verify_token(self) -> None:

        rsp_verify = self.get_raw('tokens/verify')
        sc_verify, js_verify = response_splitter(rsp_verify)

        if sc_verify == 200:
            return

        else:
            logging.error(f"Provided management token could not be verified.\nReceived: {sc_verify} - {js_verify}.")
            sys.exit(1)

    def get_organization(self) -> dict:

        rsp_org = self.get_raw(f'organizations/{self.parameters.organization}')
        sc_org, js_org = response_splitter(rsp_org)

        if sc_org == 200:
            return js_org

        elif sc_org == 403:
            logging.error(f"User does not have access to organization {self.parameters.organization} ",
                          f"in stack {self.parameters.region}.")
            sys.exit(1)

        else:
            logging.error(f"Could not access organization {self.parameters.organization} in stack "
                          f"{self.parameters.region}.\nReceived: {sc_org} - {js_org}.")
            sys.exit(1)

    def get_project_users(self, project_id: str) -> list:

        rsp_users = self.get_raw(f'projects/{project_id}/users')
        sc_users, js_users = response_splitter(rsp_users)

        if sc_users == 200:
            return js_users

        else:
            logging.error(f"Could not download users for project {project_id} in stack {self.parameters.region}.\n"
                          f"Received: {sc_users} - {js_users}.")
            sys.exit(1)

    def create_storage_token(self, project_id: str, description: str,
                             expiration: int = DEFAULT_TOKEN_EXPIRATION) -> dict:

        hdr_token = {
            'content-type': 'application/json',
            'accept': 'application/json'
        }

        data_token = {
            'description': description,
            'expiresIn': expiration,
            'canManageBuckets': True,
            'canReadAllFileUploads': False,
            'canPurgeTrash': False,
            'canManageTokens': True,
            'componentAccess': ['componentAccess'],
            'bucketPermissions': {"*": "read"}
        }

        rsp_token = self.post_raw(f'projects/{project_id}/tokens', headers=hdr_token, json=data_token)
        sc_token, js_token = response_splitter(rsp_token)

        if sc_token == 201:
            return js_token

        else:
            logging.error(f"Unable to create storage token in project {project_id} in stack {self.parameters.region}."
                          f"\nReceived: {sc_token} - {js_token}.")
            sys.exit(1)

    def get_organization_users(self) -> list:

        rsp_org_users = self.get_raw(f'organizations/{self.parameters.organization}/users')
        sc_org_users, js_org_users = response_splitter(rsp_org_users)

        if sc_org_users == 200:
            return js_org_users

        else:
            logging.error(f"Could not download organization users for organization {self.parameters.organization} "
                          f"in stack {self.parameters.region}.\nReceived: {sc_org_users} - {js_org_users}.")
            sys.exit(1)


class Client:

    def __init__(self):
        self.management = None
        self.syrup = None
        self.storage = None

    def init_storage_and_syrup_clients(self, region, token, project):
        self.storage = StorageClient(region, token, project)
        self.syrup = SyrupClient(region, token, project)

    def init_management_client(self, region, token, organization):
        self.management = ManagementClient(region, token, organization)
