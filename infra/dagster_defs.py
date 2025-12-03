#!/usr/bin/env -S uv run --script
import os

import dagster as dg
import requests
import yaml
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.loganalytics import LogAnalyticsManagementClient
from azure.mgmt.resource.subscriptions import SubscriptionClient

from cfa_dagster.azure_adls2.io_manager import ADLS2PickleIOManager
from cfa_dagster.utils import collect_definitions, start_dev_env

# Start the Dagster UI and set necessary env vars
start_dev_env()


# get the user from the environment, throw an error if variable is not set
user = os.environ["DAGSTER_USER"]


CODE_LOCATION_DIR = "/opt/dagster/code_location"
TMP_VENV_DIR = "/tmp/venv"
WORKSPACE_YAML_PATH = "/opt/dagster/dagster_home/workspace.yaml"
GRAPHQL_URL = "http://dagster.apps.edav.ext.cdc.gov/graphql"


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
def create_or_update_code_location_aci(
    context: dg.OpExecutionContext,
    registry_image: str,
    code_location_name: str,
) -> tuple[str, str, int]:
    """
    Launches an Azure Container Instance with the given image.
    """
    credential = DefaultAzureCredential()
    first_subscription_id = (
        SubscriptionClient(credential)
        .subscriptions.list()
        .next()
        .subscription_id
    )
    subscription_id = first_subscription_id

    resource_group_name = "ext-edav-cfa-prd"

    aci_subnet_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/"
        "EXT-EDAV-CFA-Network-PRD/providers/Microsoft.Network/virtualNetworks/"
        "EXT_EDAV_CFA_VNET_PRD/subnets/EXT_EDAV_CFA_CONTAINER_INSTANCE_PRD"
    )

    managed_identity_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/"
        f"{resource_group_name}/providers/Microsoft.ManagedIdentity/"
        "userAssignedIdentities/dagster-daemon-mi"
    )

    la_resource_group = "DefaultResourceGroup-EUS"
    log_analytics_workspace_name = f"DefaultWorkspace-{subscription_id}-EUS"

    log_analytics_client = LogAnalyticsManagementClient(
        credential, subscription_id
    )
    workspace = log_analytics_client.workspaces.get(
        la_resource_group, log_analytics_workspace_name
    )
    log_analytics_workspace_id = workspace.customer_id

    shared_keys = log_analytics_client.shared_keys.get_shared_keys(
        la_resource_group, log_analytics_workspace_name
    )
    log_analytics_workspace_key = shared_keys.primary_shared_key

    aci_client = ContainerInstanceManagementClient(credential, subscription_id)

    container_group_name = f"dcl-{code_location_name}"

    container_resource_requests = {"memory_in_gb": 1.0, "cpu": 0.5}
    grpc_port = 4000

    container_group = {
        "location": "eastus",
        "identity": {
            "type": "UserAssigned",
            "user_assigned_identities": {managed_identity_id: {}},
        },
        "imageRegistryCredentials": [
            {
                "server": "cfaprdbatchcr.azurecr.io",
                "identity": managed_identity_id,
            }
        ],
        "containers": [
            {
                "name": container_group_name,
                "image": registry_image,
                "resources": {
                    "requests": container_resource_requests,
                },
                "command": [
                    "dagster",
                    "code-server",
                    "start",
                    "-h",
                    "0.0.0.0",
                    "-p",
                    f"{grpc_port}",
                    "-f",
                    "dagster_defs.py",
                ],
                "ports": [{"port": grpc_port, "protocol": "TCP"}],
                "environment_variables": [
                    {"name": "DAGSTER_USER", "value": "prod"},
                ],
            }
        ],
        "os_type": "Linux",
        "restart_policy": "OnFailure",
        "ip_address": {
            "type": "Private",
            "ports": [{"port": grpc_port, "protocol": "TCP"}],
        },
        "subnet_ids": [{"id": aci_subnet_id}],
        "dns_config": {"name_servers": ["172.45.0.36", "172.45.0.37"]},
        "diagnostics": {
            "log_analytics": {
                "workspace_id": log_analytics_workspace_id,
                "workspace_key": log_analytics_workspace_key,
            }
        },
    }

    context.log.info(f"Creating container group '{container_group_name}'...")
    poller = aci_client.container_groups.begin_create_or_update(
        resource_group_name,
        container_group_name,
        container_group,
    )
    new_cg = poller.result()
    context.log.info(
        f"Container group '{new_cg.name}' "
        f"created with state '{new_cg.provisioning_state}'."
    )
    context.log.info(
        f"Container group '{new_cg.name}' "
        f"has IP address '{new_cg.ip_address.ip}'."
    )

    return (code_location_name, new_cg.ip_address.ip, grpc_port)


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


@dg.job()
def update_code_location():
    registry_image, code_location_name = get_code_location_name()
    code_location_name, grpc_host, grpc_port = (
        create_or_update_code_location_aci(registry_image, code_location_name)
    )
    did_update = update_workspace_yaml(
        code_location_name, grpc_host, grpc_port
    )
    reload_dagster_workspace(did_update)


@dg.job
def restart_webserver():
    restart_dagster_webserver()


@dg.job
def reload_workspace():
    reload_dagster_workspace()


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
    },
    executor=dg.in_process_executor,
)
