# Dagster Infra Overview

A Dagster OSS [deployment](https://docs.dagster.io/deployment/oss/oss-deployment-architecture) is made of four parts
1. dagster-webserver
2. dagster-daemon
3. code locations
4. a database
5. executors (compute backend)

## dagster-webserver

This is the UI of the instance where users can monitor and operate workflows.

Our dagster-webserver is running as a Container App named [dagster](https://portal.azure.com/#@ext.cdc.gov/resource/subscriptions/ef340bd6-2809-4635-b18b-7e6583a8803b/resourceGroups/EXT-EDAV-CFA-PRD/providers/Microsoft.App/containerApps/dagster/containerapp) and is visible from the VAP at [http://dagster.apps.edav.ext.cdc.gov].
A Container App is ideal for this use case since it gives us the ability to perform seamless deployments and rollbacks and can maintain a stable DNS name.

## dagster-daemon

The dagster daemon is a process that polls for available jobs that have been scheduled by the UI.

Our dagster-daemon is running as a Container Instance named [dagster-daemon](https://portal.azure.com/#@ext.cdc.gov/resource/subscriptions/ef340bd6-2809-4635-b18b-7e6583a8803b/resourceGroups/ext-edav-cfa-prd/providers/Microsoft.ContainerInstance/containerGroups/dagster-daemon/overview). A Container Instance is good for this use case since they have integrated logs and CPU/RAM metrics and can be scaled up easy if need be.

## Code locations

Code locations are the source of Dagster workflows. 

Our code locations are currently stored on an Azure File Share named [cfadagsterfs](https://portal.azure.com/#@ext.cdc.gov/resource/subscriptions/ef340bd6-2809-4635-b18b-7e6583a8803b/resourceGroups/EXT-EDAV-CFA-PRD/providers/Microsoft.Storage/storageAccounts/cfadagsterfs/fileList). Each code location is represented by a python file with dependencies specified in PEP 723 format. Using a File Share allows us to store the code locations and their dependencies separately from the dagster-webserver and dagster-daemon in a resilient, cost-effective manner.

The code locations live under /opt/dagster/code_location, with Dagster config under /opt/dagster/dagster_home/

/opt/dagster$ tree . -L 2
.
├── code_location
│   ├── cfa-dagster
│   └── cfa-dagster-sandbox
└── dagster_home
    ├── gh_app.pem
    ├── dagster.yaml
    └── workspace.yaml

## The Database

A database is required to store all the metadata Dagster uses to orchestrate and monitor runs.

We have one Azure PostgreSQL database for development named [cfa-pg-dagster-dev](https://portal.azure.com/#@ext.cdc.gov/resource/subscriptions/ef340bd6-2809-4635-b18b-7e6583a8803b/resourceGroups/EXT-EDAV-CFA-PRD/providers/Microsoft.DBforPostgreSQL/flexibleServers/cfa-pg-dagster-dev/overview) and another named [cfa-pg-dagster](https://portal.azure.com/#@ext.cdc.gov/resource/subscriptions/ef340bd6-2809-4635-b18b-7e6583a8803b/resourceGroups/EXT-EDAV-CFA-PRD/providers/Microsoft.DBforPostgreSQL/flexibleServers/cfa-pg-dagster/overview) for production.
Azure PostgreSQL is robust and offers the flexibility to size up the database if development or production needs exceed current capacity
