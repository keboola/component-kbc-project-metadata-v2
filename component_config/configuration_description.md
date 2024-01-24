**⚠ NOTE** This extractor is being decomisioned and replaced by the official [Temelemtry Extractor](https://components.keboola.com/components/keboola.ex-telemetry-data) which provides the same data and is actively maintained.

To configure the extractor, either [a management token](https://help.keboola.com/management/account/#tokens) is needed or an [array of storage tokens](https://help.keboola.com/management/project/tokens/) for all projects, for which metadata should be downloaded. Additionally, you'll need to be able to specify the ID of your organization and region, where your projects are located.

A sample of the configuration can be found in the [component repository](https://bitbucket.org/kds_consulting_team/kds-team.ex-kbc-project-metadata-v2/src/master/component_config/sample-config/).

### Token specification

The application accepts either one of the two options:

1. one management token,
2. any number of storage tokens.

If both options are specified, management token is prioritized and storage tokens are disregarded.

#### Authenticating with management token

Specifying management token allows to download data for all projects in the specified organization. The user, whose access token is used in the configuration, **must be part of the organization**, for which the data should be downloaded. If user is not part of the organization, the extractor will not be able to access the organization data and hence won't be able to download necessary metadata.

The management token automatically accesses all projects within the organization and creates temporary storage tokens to download the necessary data. These tokens can be distinguished by their name, which follows `[PROJECT_NAME]_Telemetry_token` naming convention. All of the automatically created tokens have an expiration of 26 hours and are **re-used by the extractor**, if the extractor is ran multiple times a day.

Along with a management token, an ID of the organization must be provided as well as region, where the organization is located. The ID of the organization can be found in the URL of the organization page - follow our [help page article](https://help.keboola.com/management/organization/) on how to access it - e.g. in URL [https://connection.keboola.com/admin/organizations/1234](https://connection.keboola.com/admin/organizations/1234), `1234` is the ID of the organization.

#### Authenticating with storage tokens

Storage tokens provide direct access to the project, in which they were created. Extractor uses these tokens to directly download data from within the project. It's therefore crucial, that these tokens **do not have any limited functionality** (such as restricted to only some tables or components), otherwise the tokens will not be able to download all the necessary data.

### Available data

The metadata extractor allows to download an extended set of objects from the Keboola's APIs. All of the options and their requirements will be discussed here.

- Get Tokens (`get_tokens`)
    - **description**: the option downloads data about all storage tokens present in the project
    - **table(s)**: `tokens`
    - **requirements**: this option requires either a management token, a master storage token or a regular storage token with `canManageTokens` permissions
    - **use case**: monitor all tokens in the project to prevent security breaches
- Get Tokens Last Events (`get_tokens_last_events`)
    - **description**: the option downloads data about last one event for each of the tokens in the project
    - **table(s)**: `tokens-last-events`
    - **requirements**: the option requires the `get_tokens` option to be active, otherwise no events will be downloaded
    - **use case**: determine inactive tokens and remove them
- Get Orchestrations (`get_orchestrations`)
    - **description**: the option downloads information about all orchestrations in the project, along with orchestration tasks and notifications
    - **table(s)**: `orchestrations`, `orchestrations-tasks` and `orchestrations-notifications`
    - **requirements**: none
    - **use case**: find orchestrations without error notifications set up, find orchestrations with inactive tasks
- Get Orchestration Triggers (`get_triggers`)
    - **description**: downloads data about event triggers for event triggered orchestrations
    - **table(s)**: `triggers` and `triggers-tables`
    - **requirements**: none
    - **use case**: determine tables which are necessary to run your event triggered orchestrations
- Get Waiting Jobs (`get_waiting_jobs`)
    - **description**: downloads all currently waiting jobs in the project
    - **table(s)**: `waiting-jobs`
    - **requirements**: none
    - **use case**: monitor all waiting or processing jobs at any given time
- Get Tables (`get_tables`)
    - **description**: downloads information about all tables in storage as well as their metadata (descriptions, last updates, etc.)
    - **table(s)**: `tables` and `tables-metadata`
    - **requirements**: a storage token with no limited access to storage
    - **use case**: find tables without descriptions, find tables which were not updated in a certain period of time
- Get Columns (`get_columns`)
    - **description**: downloads information about all of the columns for each table
    - **table(s)**: `tables-columns` and `tables-columns-metadata`
    - **requirements**: `get_tables` option must be checked
    - **use case**: find columns without descriptions, find dropped columns with snapshotting
- Get All Configurations (`get_all_configurations`)
    - **description**: downloads information about all configurations present in the project
    - **table(s)** `configurations`
    - **requirements**: a storage token with unlimited access to all components
    - **use case**: find all configurations without descriptions, find all configurations that don't follow naming conventions
- Get Transformations and Their Details (`get_transformations`)
    - **description**: downloads information about transformations, input, outputs and queries used
    - **table(s)**: `transformations`, `transformations-buckets`, `transformations-inputs`, `transformations-inputs-metadata`, `transformations-outputs` and `transformations-queries`
    - **requirements**: a storage token with access to transformations
    - **use case**: analyze SQL queries for extra inputs, find transformations which are processing same output with full load
- Get Project Users (`get_project_users`)
    - **description**: downloads data about users and their access to projects
    - **table(s)**: `project-users`
    - **requirements**: a management access token
    - **use case**: monitor users' access to projects
- Get Organization Users (`get_organization_users`)
    - **description**: downloads data about users belonging to the specified organization
    - **table(s)**: `organization-users`
    - **requirements**: a management token
    - **use case**: monitor users' access to organization
- Get Tables Load Events (`get_table_load_events`)
    - **description**: downloads all import/export events of table. First load will backfill all available (6month back), each consecutive will bring in new ones.
    - **table(s)**: `table-load-events`
    - **requirements**:  a storage token with unlimited access to all buckets
    - **use case**: monitor table activity
- Get Notifications (`get_notifications`)
    - **description**: downloads all notifications for all components in QueueV2
    - **table(s)**: `notifications`
    - **requirements**: a storage token with read access to all components
    - **use case**: monitor notification
- Get Schedules (`get_schedules`)
    - **description**: downloads data of all scheduling of components and orchestrations
    - **table(s)**: `schedules`
    - **requirements**:  a storage token with unlimited access to all components
    - **use case**: monitor scheduling of orchestration and components