To configure an extractor, either a master token or a list of storage tokens is required, and it's required to configure data which will be downloaded.

### Token specification

Users can specify either a master token or a list of storage tokens. If a master token is specified, it takes precedence over storage tokens.

Along with **master token**, an organization ID and region is required as well. Token will then access each project in the organization and create temporary storage tokens to download data. These temporary tokens are always named as `[PROJECT_NAME]_Telemetry_token` and have an expiration of 26 hours. Created storage tokens are saved in configurations state file and are re-used at later runs if they're still valid. **In order to download data for all projects in an organization, user whose master token is specified must be art of the organization.**

If a list of **storage tokens** is specified, extractor will use these tokens to access the project and no extra tokens are required.

If any of the tokens in the configuration is not valid, the extractor will fail.

### Data download

The metadata extractor offers multiple available data objects, which can be downloaded.

#### 1. Get Tokens

The option downloads all tokens that are present in the project at the time of a download. Result is the table `tokens`, which contains all available information about tokens in each project. This option requires a master token to be specified or a storage token with `canManageTokens` permissions.

#### 2. Get Tokens Last Events

The option downloads last events for each token fetched with `Get Tokens` option. It is therefore required, that `Get Tokens` is checked as well for this option to download latest events of tokens. Result is table `tokens-last-events`, which has information about last events of each token.

#### 3. Get Orchestrations

The option downloads orchestrations in each project, as well as their tasks and notifications. The option results in 3 tables: `orchestrations`, `orchestrations-tasks` and `orchestrations-notifications`.

#### 4. Get Orchestration Triggers

The options downloads data about event triggers for event triggered orchestrations as well as tables, for which a trigger is set. Results in `triggers` and `triggers-tables` tables.

#### 5. Get Waiting Jobs

The option downloads jobs, which are currently processing or waiting in the projects. Results in `waiting-jobs`.

#### 6. Get Tables

The option downloads metadata about all tables present in project's storage, as well as metadata associated with that table. Results in `tables` and `tables-metadata`.

#### 7. Get All Configurations

Downloads information about all configurations in the project, their names, descriptions and ids, as well as component types. Results in `configurations` table.

#### 8. Get Transformations and Their Details

Downloads information about all transformations present in the projects. Results in 5 tables:

- `transformations-buckets` - has information about all transformation buckets
- `transformations` - has information about all transformation in all buckets
- `transformations-inputs` - information about input mappings for all transformations
- `transformations-outputs` - information about output mappings for all transformations
- `transformations-queries` - information about all queries/scripts for all transformations

#### 9. Get Project Users

Downloads information about project users and their belonging to a project. Results in `project-users` table. This option requires a master token.

#### 10. Get Organization Users

Downloads information about organization users. Results in table `organization-users`. This option requires a master token.