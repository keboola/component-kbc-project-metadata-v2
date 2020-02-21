import logging
import requests
import sys
from typing import Dict, List
from urllib.parse import urljoin
from kbc.client_base import HttpClientBase

DEFAULT_TOKEN_EXPIRATION = 26 * 60 * 60  # Default token expiration set to 26 hours

BASE_URL = {
    'eu-central-1': 'https://connection.eu-central-1.keboola.com',
    'us-east-1': 'https://connection.keboola.com'
}

SYRUP_URL = {
    'eu-central-1': 'https://syrup.eu-central-1.keboola.com',
    'us-east-1': 'https://syrup.keboola.com'
}

ApiResponse = List[Dict]


class StorageClient(HttpClientBase):

    def __init__(self, region: str, token: str, project: str, soft: bool = False) -> None:

        if region not in BASE_URL.keys():
            logging.exception(f"Unsupported region {region}.")
            sys.exit(1)

        defHeaders = {
            'x-storageapi-token': token
        }

        super().__init__(base_url=urljoin(BASE_URL[region], 'v2/storage/'), default_http_header=defHeaders)
        self.paramToken = token
        self.paramRegion = region
        self.paramProject = project
        self.paramSoft = soft

        if self.paramSoft is False:
            self._verifyStorageToken()

    def _verifyStorageToken(self) -> bool:

        urlVerify = urljoin(self.base_url, 'tokens/verify')

        rspVerify = self.get_raw(url=urlVerify)
        scVerify, jsVerify = Utils.responseSplitter(rspVerify)

        if scVerify == 200:
            return True

        elif scVerify in (400, 401):
            if self.paramSoft is False:
                logging.exception(f"Token verification failed for {self.paramProject} in region {self.paramRegion}.")
                logging.debug(jsVerify)
                sys.exit(1)

            else:
                return False

        else:
            logging.exception(f"Unknown exception. Received: {scVerify} - {jsVerify}.")
            sys.exit(2)

    def getTokens(self) -> ApiResponse:

        urlTokens = urljoin(self.base_url, 'tokens')

        rspTokens = self.get_raw(url=urlTokens)
        scTokens, jsTokens = Utils.responseSplitter(rspTokens)

        if scTokens == 200:
            return jsTokens

        else:
            logging.error(f"Could not obtain tokens for project {self.paramProject} in {self.paramRegion}.")
            logging.exception(f"Received: {scTokens} - {jsTokens}.")
            sys.exit(1)

    def getTransformations(self) -> ApiResponse:

        urlTransformations = urljoin(self.base_url, 'components/transformation/configs')

        rspTransformations = self.get_raw(url=urlTransformations)
        scTransformations, jsTransformations = Utils.responseSplitter(rspTransformations)

        if scTransformations == 200:
            return jsTransformations

        else:
            logging.error(f"Could not obtain transformations for project {self.paramProject} in {self.paramRegion}.")
            logging.exception(f"Received: {scTransformations} - {jsTransformations}.")
            sys.exit(1)

    def getAllConfigurations(self) -> ApiResponse:

        urlConfigs = urljoin(self.base_url, 'components')

        rspConfigs = self.get_raw(urlConfigs)
        scConfigs, jsConfigs = Utils.responseSplitter(rspConfigs)

        if scConfigs == 200:
            return jsConfigs

        else:
            logging.error(''.join([f"Could not obtain configurations for project {self.paramProject} in ",
                                   f"region {self.paramRegion}."]))
            logging.exception(f"Received: {scConfigs} - {jsConfigs}.")
            sys.exit(1)

    def getStorageBuckets(self) -> ApiResponse:

        urlBuckets = urljoin(self.base_url, 'buckets')

        rspBuckets = self.get_raw(urlBuckets)
        scBuckets, jsBuckets = Utils.responseSplitter(rspBuckets)

        if scBuckets == 200:
            return jsBuckets

        else:
            logging.error(f"Could not obtain buckets for project {self.paramProject} in region {self.paramRegion}.")
            logging.exception(f"Received: {scBuckets} - {jsBuckets}.")
            sys.exit(1)

    def getAllTables(self) -> ApiResponse:

        urlTables = urljoin(self.base_url, 'tables')
        paramsTables = {
            'include': 'metadata,buckets'
        }

        rspTables = self.get_raw(urlTables, params=paramsTables)
        scTables, jsTables = Utils.responseSplitter(rspTables)

        if scTables == 200:
            return jsTables

        else:
            logging.error(f"Could not obtain tables for project {self.paramProject} in region {self.paramRegion}.")
            logging.exception(f"Received: {scTables} - {jsTables}.")
            sys.exit(1)


class SyrupClient(HttpClientBase):

    def __init__(self, region: str, token: str, project: str) -> None:

        defHeaders = {
            'x-storageapi-token': token
        }

        super().__init__(base_url=SYRUP_URL[region], default_http_header=defHeaders)
        self.paramRegion = region
        self.paramProject = project

    def getWaitingAndProcessingJobs(self) -> ApiResponse:

        urlJobs = urljoin(self.base_url, 'queue/jobs')
        paramsJobs = {
            'q': 'status:waiting OR status:processing'
        }

        rspJobs = self.get_raw(url=urlJobs, params=paramsJobs)
        scJobs, jsJobs = Utils.responseSplitter(rspJobs)

        if scJobs == 200:
            return jsJobs

        else:
            logging.error(f"Could not obtain jobs for project {self.paramProject} in region {self.paramRegion}.")
            logging.exception(f"Received: {scJobs} - {jsJobs}.")
            sys.exit(1)

    def getOrchestrations(self) -> ApiResponse:

        urlOrch = urljoin(self.base_url, 'orchestrator/orchestrations')

        rspOrch = self.get_raw(url=urlOrch)
        scOrch, jsOrch = Utils.responseSplitter(rspOrch)

        if scOrch == 200:
            return jsOrch

        else:
            logging.error(f"Could not list orchestrations for {self.paramProject} in {self.paramRegion}.")
            logging.exception(f"Received: {scOrch} - {jsOrch}.")
            sys.exit(1)

    def getOrchestrationTasks(self, orchestrationId: str) -> ApiResponse:

        urlOrchTasks = urljoin(self.base_url, f'orchestrator/orchestrations/{orchestrationId}/tasks')

        rspOrchTasks = self.get_raw(url=urlOrchTasks)
        scOrchTasks, jsOrchTasks = Utils.responseSplitter(rspOrchTasks)

        if scOrchTasks == 200:
            return jsOrchTasks

        else:
            logging.error(''.join([f"Could not list orchestration tasks for orchestration {orchestrationId} in ",
                                   "{self.paramProject} in {self.paramRegion}."]))
            logging.exception(f"Received: {scOrchTasks} - {jsOrchTasks}.")
            sys.exit(1)


class ManagementClient(HttpClientBase):

    def __init__(self, region: str, token: str, organization: str):

        if region not in BASE_URL.keys():
            logging.exception(f"Unsupported region {region}.")
            sys.exit(1)

        defHeaders = {
            'x-kbc-manageapitoken': token,
            'accept': 'application/json'
        }

        super().__init__(base_url=urljoin(BASE_URL[region], 'manage/'), default_http_header=defHeaders)
        self.paramToken = token
        self.paramRegion = region
        self.paramOrganization = organization
        self._verifyManagementToken()

    def _verifyManagementToken(self) -> None:

        urlVerify = urljoin(self.base_url, 'tokens/verify')
        logging.debug(urlVerify)

        rspVerify = self.get_raw(urlVerify)
        scVerify, jsVerify = Utils.responseSplitter(rspVerify)

        if scVerify == 200:
            return

        else:
            logging.error("Could not verify management token.")
            logging.exception(f"Received: {scVerify} - {jsVerify}.")
            sys.exit(1)

    def getOrganization(self) -> Dict:

        urlOrg = urljoin(self.base_url, f"organizations/{self.paramOrganization}")

        rspOrg = self.get_raw(url=urlOrg)
        scOrg, jsOrg = Utils.responseSplitter(rspOrg)

        if scOrg == 200:
            return jsOrg

        elif scOrg == 403:
            logging.error(''.join([f"User does not have access to organization {self.paramOrganization} ",
                                   f"in region {self.paramRegion}."]))
            sys.exit(1)

        else:
            logging.error(f"Could not access organization {self.paramOrganization} in region {self.paramRegion}.")
            logging.exception(f"Received: {scOrg} - {jsOrg}.")
            sys.exit(1)

    def getProjectUsers(self, projectId) -> ApiResponse:

        urlUsers = urljoin(self.base_url, f'projects/{projectId}/users')

        rspUsers = self.get_raw(url=urlUsers)
        scUsers, jsUsers = Utils.responseSplitter(rspUsers)

        if scUsers == 200:
            return jsUsers

        else:
            logging.error(f"Could not download users for project {projectId} in region {self.paramRegion}.")
            logging.exception(f"Received: {scUsers} - {jsUsers}.")
            sys.exit(1)

    def createStorageToken(self, projectId: str, description: str, expiration: int = DEFAULT_TOKEN_EXPIRATION) -> Dict:

        urlToken = urljoin(self.base_url, f'projects/{projectId}/tokens')
        headersToken = {
            'content-type': 'application/json',
            'accept': 'application/json'
        }

        dataToken = {
            'description': description,
            'expiresIn': expiration,
            'canManageBuckets': True,
            'canReadAllFileUploads': False,
            'canPurgeTrash': False,
            'canManageTokens': True,
            'componentAccess': ['componentAccess'],
            'bucketPermissions': {"*": "read"}
        }

        rspToken = self.post_raw(url=urlToken, headers=headersToken, json=dataToken)
        scToken, jsToken = Utils.responseSplitter(rspToken)

        if scToken == 201:
            return jsToken

        else:
            logging.error(f"Unable to create storage token in {projectId} in region {self.paramRegion}.")
            logging.exception(f"Received: {scToken} - {jsToken}.")
            sys.exit(1)


class MetadataClient:

    def __init__(self):

        self.management = None
        self.syrup = None
        self.storage = None

    def initStorageAndSyrup(self, region, token, project):

        self.storage = StorageClient(region, token, project)
        self.syrup = SyrupClient(region, token, project)

    def initManagement(self, region, token, organization):

        self.management = ManagementClient(region, token, organization)


class Utils:

    def __init__(self):
        pass

    @staticmethod
    def responseSplitter(response: requests.models.Response):

        if not isinstance(response, requests.models.Response):
            raise TypeError(f"Expecting type requests.models.Response, received type {type(response)}")

        else:
            return response.status_code, response.json()


if __name__ == '__main__':

    syr = SyrupClient('us-east-1', '4390-244910-06tQAVYel1LW6xnoBQJyFqZdF0qGiUANxid7DOpK', '4390')
    x = syr.getWaitingAndProcessingJobs()
