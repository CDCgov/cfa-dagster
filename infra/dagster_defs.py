#!/usr/bin/env -S uv run --script
import os
from datetime import datetime, timedelta, timezone
from typing import List

import dagster as dg
import requests
import yaml
from azure.batch import BatchServiceClient
from azure.batch.models import (
    BatchErrorException,
    JobListOptions,
    TaskListOptions,
)
from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.msi import ManagedServiceIdentityClient
from msrest.authentication import BasicTokenAuthentication

from cfa_dagster import (
    ADLS2PickleIOManager,
    ExecutionConfig,
    SelectorConfig,
    collect_definitions,
    dynamic_executor,
    start_dev_env,
)

# Start the Dagster UI and set necessary env vars
start_dev_env(__name__)


# get the user from the environment, throw an error if variable is not set
user = os.environ["DAGSTER_USER"]


CODE_LOCATION_DIR = "/opt/dagster/code_location"
TMP_VENV_DIR = "/tmp/venv"
WORKSPACE_YAML_PATH = "/opt/dagster/dagster_home/workspace.yaml"
GRAPHQL_URL = "http://dagster.apps.edav.ext.cdc.gov/graphql"


def find_stale_dagster_jobs(
    batch_client: BatchServiceClient,
    idle_threshold: timedelta,
) -> List[str]:
    stale_job_ids: List[str] = []

    jobs = batch_client.job.list(
        job_list_options=JobListOptions(
            filter="startswith(id,'dagster-') and state eq 'active'",
            select="id",
        ),
    )

    cutoff = (datetime.now(timezone.utc) - idle_threshold).isoformat()

    for job in jobs:
        job_id = job.id

        # 1️⃣ Any active tasks? → skip
        active_tasks = batch_client.task.list(
            job_id,
            task_list_options=TaskListOptions(
                filter=(
                    "state eq 'active' or "
                    "state eq 'running' or "
                    "state eq 'preparing'"
                ),
                max_results=1,
                select="id",
            ),
        )

        if any(True for _ in active_tasks):
            print(f"Job '{job_id}' has active tasks, skipping...")
            continue

        # 2️⃣ Any recently-created tasks? → skip
        recent_tasks = batch_client.task.list(
            job_id,
            task_list_options=TaskListOptions(
                filter=f"creationTime ge {cutoff}",
                max_results=1,
                select="id",
            ),
        )

        if any(True for _ in recent_tasks):
            print(f"Job '{job_id}' has recent tasks, skipping...")
            continue

        # No active tasks + no recent tasks → stale
        stale_job_ids.append(job_id)

    return stale_job_ids


@dg.op(required_resource_keys={"batch_client"})
def cleanup_stale_batch_jobs(
    context: dg.OpExecutionContext,
):
    batch_client = context.resources.batch_client
    idle_threshold = timedelta(hours=1)

    stale_jobs = find_stale_dagster_jobs(
        batch_client=batch_client,
        idle_threshold=idle_threshold,
    )

    if not stale_jobs:
        context.log.info("No stale dagster Batch jobs found.")
        return

    for job_id in stale_jobs:
        try:
            context.log.info(f"Terminating idle Batch job: {job_id}")
            batch_client.job.terminate(job_id)
        except BatchErrorException as err:
            context.log.warning(
                f"Failed to terminate job {job_id}: "
                f"{err.error.code if err.error else err}"
            )


class AzureIdentityCredentialAdapter(BasicTokenAuthentication):
    def __init__(self, credential: TokenCredential, scope: str):
        super().__init__(None)
        self._credential = credential
        self._scope = scope

    def signed_session(self, session):
        token = self._credential.get_token(self._scope)
        session.headers["Authorization"] = f"Bearer {token.token}"
        return session


@dg.resource
def batch_client_resource():
    credential = DefaultAzureCredential()

    adapter = AzureIdentityCredentialAdapter(
        credential=credential,
        scope="https://batch.core.windows.net/.default",
    )

    return BatchServiceClient(
        credentials=adapter,
        batch_url="https://cfaprdba.eastus.batch.azure.com",
    )


@dg.job(resource_defs={"batch_client": batch_client_resource})
def cleanup_dagster_batch_jobs():
    cleanup_stale_batch_jobs()


@dg.op(out={"registry_image": dg.Out(str), "code_location_name": dg.Out(str)})
def get_code_location_name(
    context: dg.OpExecutionContext, registry_image: str
) -> tuple[str, str]:
    image_name_with_tag = registry_image.split("/")[-1]
    image_name = image_name_with_tag.split(":")[0]
    code_location_name = image_name.replace("_", "-")
    context.log.info(f"code_location_name: '{code_location_name}'")
    return (registry_image, code_location_name)


@dg.op(
    out={
        "code_location_name": dg.Out(str),
        "grpc_host": dg.Out(str),
        "grpc_port": dg.Out(int),
    }
)
def create_or_update_code_location_aca(
    context,
    registry_image: str,
    code_location_name: str,
) -> tuple[str, str, int]:
    """
    Creates or updates an Azure Container App running the given image.
    """

    credential = DefaultAzureCredential()

    subscription_id = next(
        SubscriptionClient(credential).subscriptions.list()
    ).subscription_id

    resource_group_name = "ext-edav-cfa-prd"
    location = "eastus"

    containerapp_env_name = "ext-edav-cfa-cae-prd"
    managed_identity_name = "dagster-daemon-mi"

    msi_client = ManagedServiceIdentityClient(
        credential,
        subscription_id,
    )

    managed_identity = msi_client.user_assigned_identities.get(
        resource_group_name,
        managed_identity_name,
    )

    managed_identity_client_id = managed_identity.client_id

    managed_identity_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/"
        f"{resource_group_name}/providers/Microsoft.ManagedIdentity/"
        f"userAssignedIdentities/{managed_identity_name}"
    )

    containerapp_client = ContainerAppsAPIClient(credential, subscription_id)

    app_name = f"dcl-{code_location_name}"
    grpc_port = 4000
    deploy_date = datetime.now(timezone.utc).isoformat()
    container_app_def = {
        "location": location,
        "identity": {
            "type": "UserAssigned",
            "userAssignedIdentities": {managed_identity_id: {}},
        },
        "properties": {
            "environmentId": (
                f"/subscriptions/{subscription_id}/resourceGroups/"
                f"{resource_group_name}/providers/Microsoft.App/"
                f"managedEnvironments/{containerapp_env_name}"
            ),
            "configuration": {
                "ingress": {
                    "external": False,
                    "targetPort": grpc_port,
                    "transport": "tcp",
                },
                "registries": [
                    {
                        "server": "cfaprdbatchcr.azurecr.io",
                        "identity": "system-environment",
                    }
                ],
            },
            "template": {
                "containers": [
                    {
                        "name": app_name,
                        "image": registry_image,
                        "resources": {
                            "cpu": 0.5,
                            "memory": "1Gi",
                        },
                        "command": [
                            "dagster",
                            "code-server",
                            "start",
                            "-h",
                            "0.0.0.0",
                            "-p",
                            str(grpc_port),
                            "-f",
                            "dagster_defs.py",
                            "--container-image",
                            registry_image,
                        ],
                        "env": [
                            {"name": "DAGSTER_USER", "value": "prod"},
                            {"name": "CFA_DAGSTER_ENV", "value": "prod"},
                            {"name": "DEPLOY_DATE", "value": deploy_date},
                            {
                                "name": "AZURE_CLIENT_ID",
                                "value": managed_identity_client_id,
                            },
                        ],
                    }
                ],
                "scale": {
                    "minReplicas": 1,
                    "maxReplicas": 1,
                },
            },
        },
    }

    context.log.info(f"Creating container app '{app_name}'...")

    poller = containerapp_client.container_apps.begin_create_or_update(
        resource_group_name,
        app_name,
        container_app_def,
    )

    app = poller.result()

    context.log.info(
        f"Container app '{app_name}' created with state {app.provisioning_state}"
    )

    return (code_location_name, app_name, grpc_port)


@dg.op
def update_dagster_aca(context):
    credential = DefaultAzureCredential()

    subscription_id = next(
        SubscriptionClient(credential).subscriptions.list()
    ).subscription_id

    resource_group_name = "ext-edav-cfa-prd"
    location = "eastus"
    containerapp_env_name = "ext-edav-cfa-cae-prd"

    managed_identity_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/"
        f"{resource_group_name}/providers/Microsoft.ManagedIdentity/"
        "userAssignedIdentities/dagster-daemon-mi"
    )

    containerapp_client = ContainerAppsAPIClient(credential, subscription_id)

    app_name = "dagster"
    deploy_date = datetime.now(timezone.utc).isoformat()

    # -----------------------------
    # Shared container configuration
    # -----------------------------
    shared_container_args = {
        "image": "ghcr.io/cdcgov/cfa-dagster:latest",
        "env": [
            {
                "name": "DAGSTER_POSTGRES_HOST",
                "secretRef": "cfa-pg-dagster-host",  # pragma: allowlist secret
            },
            {
                "name": "DAGSTER_POSTGRES_USER",
                "secretRef": "cfa-pg-dagster-admin-username",  # pragma: allowlist secret
            },
            {
                "name": "DAGSTER_POSTGRES_PASSWORD",
                "secretRef": "cfa-pg-dagster-admin-password",  # pragma: allowlist secret
            },
            {"name": "DAGSTER_POSTGRES_DB", "value": "postgres"},
            {"name": "DAGSTER_USER", "value": "prod"},
            {
                "name": "AZURE_CLIENT_ID",
                "value": "bdca4f03-d071-4928-914e-def0dcdbe460",
            },
            {"name": "CFA_DAGSTER_LOG_LEVEL", "value": "INFO"},
            {"name": "CFA_DAGSTER_ENV", "value": "prod"},
            {"name": "DEPLOY_DATE", "value": deploy_date},
            # daemon-specific var included for both (harmless for web)
            {"name": "DAGSTER_HOME", "value": "/opt/dagster/dagster_home"},
        ],
        "volumeMounts": [
            {
                "volumeName": "cfadagster",
                "mountPath": "/opt/dagster",
            }
        ],
        "resources": {
            "cpu": 1,
            "memory": "2Gi",
        },
    }
    # -----------------------------
    # Container App definition
    # -----------------------------
    container_app_def = {
        "location": location,
        "identity": {
            "type": "UserAssigned",
            "userAssignedIdentities": {managed_identity_id: {}},
        },
        "properties": {
            "environmentId": (
                f"/subscriptions/{subscription_id}/resourceGroups/"
                f"{resource_group_name}/providers/Microsoft.App/"
                f"managedEnvironments/{containerapp_env_name}"
            ),
            "configuration": {
                "secrets": [
                    {
                        "name": "cfa-pg-dagster-admin-password",
                        "keyVaultUrl": "https://cfa-tools.vault.azure.net/secrets/cfa-pg-dagster-admin-password",
                        "identity": managed_identity_id,
                    },
                    {
                        "name": "cfa-pg-dagster-admin-username",
                        "keyVaultUrl": "https://cfa-tools.vault.azure.net/secrets/cfa-pg-dagster-admin-username",
                        "identity": managed_identity_id,
                    },
                    {
                        "name": "cfa-pg-dagster-host",
                        "keyVaultUrl": "https://cfa-tools.vault.azure.net/secrets/cfa-pg-dagster-host",
                        "identity": managed_identity_id,
                    },
                ],
                "registries": [
                    {
                        "server": "cfaprdbatchcr.azurecr.io",
                        "identity": "system-environment",
                    }
                ],
                "ingress": {
                    "external": True,
                    "targetPort": 3000,
                    "allowInsecure": True,
                },
            },
            "template": {
                "containers": [
                    {
                        "name": "dagster",
                        "command": ["dagster-webserver"],
                        "args": ["-h", "0.0.0.0", "-p", "3000"],
                        **shared_container_args,
                    },
                    {
                        "name": "dagster-daemon",
                        "command": ["dagster-daemon"],
                        "args": ["run"],
                        **shared_container_args,
                    },
                ],
                "scale": {
                    "minReplicas": 0,
                    "maxReplicas": 1,
                },
                "volumes": [
                    {
                        "name": "cfadagster",
                        "storageType": "AzureFile",
                        "storageName": "cfadagster",
                    }
                ],
            },
        },
    }

    context.log.info(f"Creating container app '{app_name}'...")

    poller = containerapp_client.container_apps.begin_create_or_update(
        resource_group_name,
        app_name,
        container_app_def,
    )

    app = poller.result()

    context.log.info(
        f"Container app '{app_name}' created with state {app.provisioning_state}"
    )

    return app


@dg.op
def update_workspace_yaml(
    context: dg.OpExecutionContext,
    code_location_name: str,
    grpc_host: str,
    grpc_port: int,
) -> bool:
    """
    Adds a new python_file entry to the load_from list in a Dagster
     workspace YAML file.
    """
    # Read the existing YAML content
    with open(WORKSPACE_YAML_PATH, "r") as f:
        data = yaml.safe_load(f)

    # Ensure load_from exists and is a list
    if "load_from" not in data or not isinstance(data["load_from"], list):
        data["load_from"] = []

    did_update = False
    # Check if location_name already exists
    for entry in data["load_from"]:
        grpc_server: dict = entry.get("grpc_server", {})
        if grpc_server.get("location_name") == code_location_name:
            grpc_server["host"] = grpc_host
            grpc_server["port"] = grpc_port
            context.log.info(
                f"Updated code location '{code_location_name}' "
                f"in {WORKSPACE_YAML_PATH}"
            )
            did_update = True
            break

    if not did_update:
        # Append the new code location
        data["load_from"].append(
            {
                "grpc_server": {
                    "host": f"{grpc_host}",
                    "port": grpc_port,
                    "location_name": code_location_name,
                }
            }
        )
        context.log.info(
            f"Added code location '{code_location_name}' to {WORKSPACE_YAML_PATH}"
        )

    # Write the updated YAML back to the file
    with open(WORKSPACE_YAML_PATH, "w") as f:
        yaml.dump(data, f, default_flow_style=False)

    return True


@dg.op(ins={"should_run": dg.In(dg.Nothing)})
def restart_dagster_webserver(context: dg.OpExecutionContext):
    RESOURCE_GROUP = "ext-edav-cfa-prd"
    CONTAINER_APP = "dagster"
    credential = DefaultAzureCredential()

    # Get first subscription for logged-in credential
    first_subscription_id = (
        SubscriptionClient(credential)
        .subscriptions.list()
        .next()
        .subscription_id
    )

    client = ContainerAppsAPIClient(
        credential=credential, subscription_id=first_subscription_id
    )

    # Find active revision
    revisions = list(
        client.container_apps_revisions.list_revisions(
            RESOURCE_GROUP, CONTAINER_APP
        )
    )
    active_revision = next((r for r in revisions if r.active), None)

    if not active_revision:
        raise RuntimeError("No active revision found!")

    rev_name = active_revision.name
    client.container_apps_revisions.restart_revision(
        resource_group_name=RESOURCE_GROUP,
        container_app_name=CONTAINER_APP,
        revision_name=rev_name,
    )

    context.log.info(f"Restarting container app: {CONTAINER_APP}")


# using the Nothing type to force synchronous execution
@dg.op(ins={"should_run": dg.In(dg.Nothing)})
def reload_dagster_workspace(context: dg.OpExecutionContext):
    query = """
    mutation reload_workspace {
      reloadWorkspace {
        __typename
        ... on Workspace {
          id
          locationEntries {
            id
            name
            loadStatus
            locationOrLoadError {
              __typename
              ... on PythonError {
                message
                stack
              }
            }
          }
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
    """
    res = requests.post(GRAPHQL_URL, json={"query": query})
    context.log.debug(res.text)
    res.raise_for_status()
    context.log.info("Reloaded workspace")


@dg.job(tags={"concurrency": "single"})
def update_code_location():
    registry_image, code_location_name = get_code_location_name()
    code_location_name, grpc_host, grpc_port = (
        create_or_update_code_location_aca(registry_image, code_location_name)
    )
    did_update = update_workspace_yaml(
        code_location_name, grpc_host, grpc_port
    )
    reload_dagster_workspace(did_update)


@dg.job
def update_dagster_containerapp():
    update_dagster_aca()


@dg.job
def restart_webserver():
    restart_dagster_webserver()


@dg.job
def reload_workspace():
    reload_dagster_workspace()


cleanup_batch_schedule = dg.ScheduleDefinition(
    job=cleanup_dagster_batch_jobs,
    cron_schedule="0 */3 * * *",
    execution_timezone="America/Los_Angeles",
)

# collect Dagster definitions from the current file
collected_defs = collect_definitions(globals())

# Create Definitions object
defs = dg.Definitions(
    **collected_defs,
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
    },
    executor=dynamic_executor(
        ExecutionConfig(
            launcher=SelectorConfig(class_name="DefaultRunLauncher"),
            executor=SelectorConfig(class_name="multiprocess_executor"),
        )
    ),
)
