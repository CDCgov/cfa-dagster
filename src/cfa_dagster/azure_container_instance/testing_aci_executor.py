### TESTING

import os
from unittest.mock import MagicMock

from dagster_docker.container_context import DockerContainerContext

from cfa_dagster.azure_container_instance.executor import (
    AzureContainerInstanceStepHandler,
)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DOCKER_IMAGE = "cfaprdbatchcr.azurecr.io/cfa-county-rt:latest"

# Optional: attach this managed identity to the ACI container group.
# Set to None to test without a managed identity.
IDENTITY_NAME = "ext-edav-cfa-batch-account"

os.environ["DAGSTER_USER"] = "zqm6"

# ---------------------------------------------------------------------
# Construct the real handler
# ---------------------------------------------------------------------

handler = AzureContainerInstanceStepHandler(
    image=DOCKER_IMAGE,
    identity_name=IDENTITY_NAME,
    container_context=DockerContainerContext(
        env_vars=[],
        networks=[],
        container_kwargs={},
    ),
    cpu=1.0,
    memory=1.5,
)

print(handler)
print(handler.__dict__)

print(handler._subscription_id)
print(handler._location)
print(handler._resource_group)

# Verify the managed identity was resolved correctly.
print(handler._identity)

if handler._identity:
    print(handler._identity.name)
    print(handler._identity.id)
    print(handler._identity.client_id)

# ---------------------------------------------------------------------
# Minimal fake StepHandlerContext
# ---------------------------------------------------------------------

ctx = MagicMock()

ctx.execute_step_args.step_keys_to_execute = [
    "interactive-test",
]

ctx.dagster_run.remote_job_origin = None
ctx.dagster_run.job_name = "interactive-test-job"
ctx.dagster_run.run_id = (
    "11111111-2222-3333-4444-555555555555"
)
ctx.dagster_run.tags = {
    "cfa_dagster/run_ts": "2026-08-07T12:00:00+00:00",
}

# ---------------------------------------------------------------------
# Build the ACI model only
# ---------------------------------------------------------------------

group = handler._build_container_group(ctx)

print(group.as_dict())

# Inspect the attached identity
print(group.identity)

# Inspect registry credentials
print(group.image_registry_credentials)

# ---------------------------------------------------------------------
# Submit manually (recommended while developing)
# ---------------------------------------------------------------------

container_group_name = handler._get_container_group_id(ctx)

poller = handler._azure_client.container_groups.begin_create_or_update(
    resource_group_name=handler._resource_group,
    container_group_name=container_group_name,
    container_group=group,
)

result = poller.result()

print(result.provisioning_state)

# ---------------------------------------------------------------------
# Inspect what Azure actually created
# ---------------------------------------------------------------------

created_group = handler._azure_client.container_groups.get(
    resource_group_name=handler._resource_group,
    container_group_name=container_group_name,
)

print(created_group.as_dict())

# ---------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------

handler._azure_client.container_groups.begin_delete(
    resource_group_name=handler._resource_group,
    container_group_name=container_group_name,
).result()