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

# ruff: noqa: F401
from cfa_dagster import (
    ADLS2PickleIOManager,
    AzureContainerAppJobRunLauncher,
    ExecutionConfig,
    SelectorConfig,
    azure_batch_executor,
    collect_definitions,
    docker_executor,
    dynamic_executor,
    is_production,
    start_dev_env,
)
from cfa_dagster import (
    azure_container_app_job_executor as azure_caj_executor,
)

# function to start the dev server
start_dev_env(__name__)

user = os.getenv("DAGSTER_USER")

IMAGE_REGISTRY = "cfaprdbatchcr"
image_tag = "latest" if is_production() else user
image = f"{IMAGE_REGISTRY}.azurecr.io/cfa-dagster:{image_tag}"


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
        container_client = blob_storage_client.get_container_client(
            container_name
        )
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


# partitions are parameters to your asset function that trigger parallel compute
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

# image = f"cfaprdbatchcr.azurecr.io/cfa-dagster:{user}"

# this is the default run config that launches the job in your local shell
# and executes each step in a separate system process
default_config = ExecutionConfig(
    launcher=SelectorConfig(class_name=dg.DefaultRunLauncher.__name__),
    executor=SelectorConfig(class_name=dg.multiprocess_executor.__name__),
)

# configuring an executor to run each workflow step in a new Docker container
# add this to a job or the Definitions class to use it
docker_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=docker_executor.__name__,
        config={
            # specify a default image
            "image": image,
            # set env vars here
            # "env_vars": [f"DAGSTER_USER"],
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    f"{__file__}:{workdir}/{os.path.basename(__file__)}",
                ]
            },
        },
    )
)


# configuring an executor to run each workflow step in a new Azure Container
# App Job execution
# add this to a job or the Definitions class to use it
azure_caj_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_caj_executor.__name__,
        config={
            "container_app_job_name": "cfa-dagster",
            # specify a default image
            "image": image,
            # set env vars here
            # "env_vars": [f"DAGSTER_USER"],
        },
    )
)

# configuring a run launcher to launch each run in an Azure Container App Job
# and configuring an executor to run each workflow steps in a new Azure Batch
# task for maximum scale
# add this to a job or the Definitions class to use it
azure_batch_config = ExecutionConfig(
    launcher=SelectorConfig(
        class_name=AzureContainerAppJobRunLauncher.__name__
    ),
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            # change the pool_name to your existing pool name
            "pool_name": "cfa-dagster",
            # specify a default image
            "image": image,
            # set env vars here
            "env_vars": ["CFA_DAGSTER_LOG_LEVEL=debug"],
            "container_kwargs": {
                # set the working directory to match your Dockerfile
                # required for Azure Batch
                "working_dir": workdir,
                # mount config if your existing Batch pool already has Blob mounts
                # "volumes": [
                #     "nssp-etl:nssp-etl",
                # ]
            },
        },
    ),
)

# jobs are used to materialize assets with a given configuration
basic_r_asset_job = dg.define_asset_job(
    name="basic_r_asset_job",
    selection=dg.AssetSelection.assets(basic_r_asset),
    # tag the run with your user to allow for easy filtering in the Dagster UI
    tags={"user": user},
    config=dg.RunConfig(
        # try switching to Azure compute after pushing your image
        execution=docker_config.to_run_config()
        # execution=azure_batch_config.to_run_config()
        # execution=azure_caj_config.to_run_config()
    ),
    executor_def=dynamic_executor(),
)

partitioned_r_asset_job = dg.define_asset_job(
    name="partitioned_r_asset_job",
    selection=dg.AssetSelection.assets(partitioned_r_asset),
    # tag the run with your user to allow for easy filtering in the Dagster UI
    tags={"user": user},
    config=dg.RunConfig(
        # try switching to Azure compute after pushing your image
        execution=docker_config.to_run_config()
        # execution=azure_batch_config.to_run_config()
        # execution=azure_caj_config.to_run_config()
    ),
    executor_def=dynamic_executor(),
)


@dg.op
def build_image(context: dg.OpExecutionContext, should_push: bool):
    cmd = f"docker build -t {image} ."

    if should_push:
        subprocess.run(
            f"az login --identity && az acr login -n {IMAGE_REGISTRY}",
            check=True,
            shell=True,
        )
        cmd += " --push"

    context.log.debug(f"Running {cmd}")
    subprocess.run(cmd, check=True, shell=True)


@dg.job(
    config=dg.RunConfig(
        ops={"build_image": {"inputs": {"should_push": False}}},
        # configure this job to run on your computer
        execution=default_config.to_run_config(),
    ),
    executor_def=dynamic_executor(),
)
def build_image_job():
    build_image()


# schedule a job to run weekly
schedule_every_wednesday = dg.ScheduleDefinition(
    name="weekly_cron", cron_schedule="0 9 * * 3", job=basic_r_asset_job
)


# change storage accounts between dev and prod
storage_account = "cfadagster" if is_production() else "cfadagsterdev"

collected_defs = collect_definitions(globals())


# Create Definitions object
defs = dg.Definitions(
    **collected_defs,
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
    executor=dynamic_executor(
        # try switching to Azure compute after pushing your image
        # default_config=default_config
        default_config=docker_config
        # default_config=azure_caj_config
        # default_config=azure_batch_config
    ),
)
