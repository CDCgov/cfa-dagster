#!/usr/bin/env -S uv run --script
# PEP 723 dependency definition: https://peps.python.org/pep-0723/
# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = [
#    "dagster-azure>=0.27.4",
#    "dagster-docker>=0.27.4",
#    "dagster-postgres>=0.27.4",
#    "dagster-webserver==1.12.2",
#    "dagster==1.12.2",
#  # pydantic 2.12.X contains breaking changes for Dagster
#    "pydantic==2.11.9",
#    "cfa-dagster @ git+https://github.com/cdcgov/cfa-dagster.git",
#    "pyyaml>=6.0.2",
# ]
# ///
import json
import os
import subprocess

import dagster as dg
from dagster_azure.blob import (
    AzureBlobStorageDefaultCredential,
    AzureBlobStorageResource,
)

from cfa_dagster.azure_adls2.io_manager import ADLS2PickleIOManager
from cfa_dagster.utils import bootstrap_dev, collect_definitions
from cfa_dagster.azure_batch.executor import azure_batch_executor
from cfa_dagster.azure_container_app_job.executor import (
    azure_container_app_job_executor as azure_caj_executor,
)
from cfa_dagster.docker.executor import docker_executor

# function to start the dev server
bootstrap_dev()

user = os.getenv("DAGSTER_USER")


@dg.asset(
    kinds={"azure_blob"},
    description="An asset that downloads a file from Azure Blob Storage",
)
def basic_blob_asset(azure_blob_storage: AzureBlobStorageResource):
    """
    An asset that downloads a config file from Azure Blob
    """
    container_name = "cfadagsterdev"
    with azure_blob_storage.get_client() as blob_storage_client:
        container_client = blob_storage_client.get_container_client(container_name)
    downloader = container_client.download_blob("test-files/test_config.json")
    print("Downloaded file from blob!")
    return downloader.readall().decode("utf-8")


@dg.asset(
    description="An asset that runs R code",
)
def basic_r_asset(basic_blob_asset):
    subprocess.run("Rscript hello.R", shell=True, check=True)

    # Read the random number from output.txt
    with open("output.txt", "r") as f:
        random_number = f.read().strip()

    return dg.MaterializeResult(
        metadata={
            # add metadata from upstream asset
            "config": dg.MetadataValue.json(json.loads(basic_blob_asset)),
            # Dagster will plot numeric values as you repeat runs
            "output_value": dg.MetadataValue.int(int(random_number)),
        }
    )


disease_partitions = dg.StaticPartitionsDefinition(["COVID", "FLU", "RSV"])


@dg.asset(
    description="A partitioned asset that runs R code for different diseases",
    partitions_def=disease_partitions,
)
def partitioned_r_asset(context: dg.OpExecutionContext):
    disease = context.partition_key
    subprocess.run(f"Rscript hello.R {disease}", shell=True, check=True)


# this should match your Dockerfile WORKDIR
workdir = "/app"

# configuring an executor to run workflow steps on Docker
# add this to a job or the Definitions class to use it
docker_executor_configured = docker_executor.configured(
    {
        # specify a default image
        "image": "basic-r-asset",
        # set env vars here
        # "env_vars": [f"DAGSTER_USER={user}"],
        "container_kwargs": {
            "volumes": [
                # bind the ~/.azure folder for optional cli login
                f"/home/{user}/.azure:/root/.azure",
                # bind current file so we don't have to rebuild
                # the container image for workflow changes
                f"{__file__}:{workdir}/{os.path.basename(__file__)}",
            ]
        },
    }
)

# configuring an executor to run workflow steps on Azure Container App Jobs
# add this to a job or the Definitions class to use it
azure_caj_executor_configured = azure_caj_executor.configured(
    {
        "container_app_job_name": "cfa-dagster",
        # specify a default image
        "image": f"cfaprdbatchcr.azurecr.io/cfa-dagster:{user}",
        # set env vars here
        # "env_vars": [f"DAGSTER_USER={user}"],
    }
)

# configuring an executor to run workflow steps on Azure Batch 4CPU 16GB RAM pool
# add this to a job or the Definitions class to use it
azure_batch_executor_configured = azure_batch_executor.configured(
    {
        # change the pool_name to your existing pool name
        "pool_name": "cfa-dagster",
        # specify a default image
        "image": f"cfaprdbatchcr.azurecr.io/cfa-dagster:{user}",
        # set env vars here
        # "env_vars": [f"DAGSTER_USER={user}"],
        "container_kwargs": {
            # set the working directory to match your Dockerfile
            # required for Azure Batch
            "working_dir": workdir,
            # mount config if your existing Batch pool already has Blob mounts
            # "volumes": [
            #     "nssp-etl:nssp-etl",
            # ]
        },
    }
)

# jobs are used to materialize assets with a given configuration
basic_r_asset_job = dg.define_asset_job(
    name="basic_r_asset_job",
    # specify an executor including docker, Azure Container App Job, or
    # the future Azure Batch executor
    executor_def=docker_executor_configured,
    # uncomment the below to switch to run on Azure Container App Jobs.
    # remember to rebuild and push your image if you made any workflow changes
    # executor_def=azure_caj_executor_configured,
    selection=dg.AssetSelection.assets(basic_r_asset),
    # tag the run with your user to allow for easy filtering in the Dagster UI
    tags={"user": user},
)

partitioned_r_asset_job = dg.define_asset_job(
    name="partitioned_r_asset_job",
    executor_def=docker_executor_configured,
    # uncomment the below to switch to run on Azure Container App Jobs.
    # remember to rebuild and push your image if you made any workflow changes
    # executor_def=azure_caj_executor_configured,
    selection=dg.AssetSelection.assets(partitioned_r_asset),
    # tag the run with your user to allow for easy filtering in the Dagster UI
    tags={"user": user},
)

# schedule the job to run weekly
schedule_every_wednesday = dg.ScheduleDefinition(
    name="weekly_cron",
    cron_schedule="0 9 * * 3",
    job=basic_r_asset_job
)

# env variable set by Dagster CLI
is_production = not os.getenv("DAGSTER_IS_DEV_CLI")
# change storage accounts between dev and prod
storage_account = "cfadagster" if is_production else "cfadagsterdev"

# collect Dagster definitions from the current file
collected_defs = collect_definitions(globals())

# Create Definitions object
defs = dg.Definitions(
    assets=collected_defs["assets"],
    asset_checks=collected_defs["asset_checks"],
    jobs=collected_defs["jobs"],
    sensors=collected_defs["sensors"],
    schedules=collected_defs["schedules"],
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # an example storage account
        "azure_blob_storage": AzureBlobStorageResource(
            account_url=f"{storage_account}.blob.core.windows.net",
            credential=AzureBlobStorageDefaultCredential(),
        ),
    },
    # setting Docker as the default executor. comment this out to use
    # the default executor that runs directly on your computer
    executor=docker_executor_configured,
    # executor=azure_caj_executor_configured,
    # executor=azure_batch_executor_configured,
)

