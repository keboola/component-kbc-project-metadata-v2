# KBC Metadata extractor

The Keboola metadata extractor downloads information from Keboola's APIs about various objects, users, etc. The metadata obtained by this extractor can be used in addition with the default telemetry data about Keboola projects to bring even more insights into the telemetry of your organization.

The extractor utilizes folllowing Keboola APIs:

1. [Storage API](https://keboola.docs.apiary.io/#),
2. [Management API](https://keboolamanagementapi.docs.apiary.io/#),
3. [Orchestrator API](https://keboolaorchestratorv2api.docs.apiary.io/#),
4. [Queue API](https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.0.0#/);

and allows to download information about:

1. tokens and their last events,
2. orchestrations,
3. waiting jobs,
4. tables and columns,
5. configurations,
6. transformations,
7. project and organization users.

A sample of the configuration can be found in the [component repository](https://bitbucket.org/kds_consulting_team/kds-team.ex-kbc-project-metadata-v2/src/master/component_config/sample-config/).