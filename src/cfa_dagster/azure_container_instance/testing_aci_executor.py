### TESTING
import os
from cfa_dagster.azure_container_instance.executor import (
     AzureContainerInstanceStepHandler,
)
from unittest.mock import MagicMock

from dagster_docker.container_context import DockerContainerContext

DOCKER_IMAGE = "cfaprdbatchcr.azurecr.io/cfa-county-rt:latest"

os.environ["DAGSTER_USER"] = "zqm6"
handler = AzureContainerInstanceStepHandler(
    image=DOCKER_IMAGE,
    container_context=DockerContainerContext(
        env_vars=[],
        networks=[],
        container_kwargs={
            "working_dir": "/",
        },
    ),
    cpu=1.0,
    memory=1.5,
)

handler._subscription_id
handler._location

# Mock some necessary parameters
ctx = MagicMock()
ctx.execute_step_args.step_keys_to_execute = ["interactive-test"]
ctx.dagster_run.tags = {
    "cfa_dagster/run_ts": "2026-08-06T14:00:00+00:00"
}
ctx.dagster_run.remote_job_origin = None
ctx.dagster_run.job_name = "interactive-test-job"
ctx.dagster_run.run_id = "11111111-2222-3333-4444-555555555555"
ctx.dagster_run.tags = {
    "cfa_dagster/run_ts": "2026-08-06T14:00:00+00:00",
}

generator = handler.launch_step(ctx)
group = handler._build_container_group(ctx)
group.as_dict()

run = ctx.dagster_run

print(run.remote_job_origin)
print(run.job_name)
print(run.tags)

# submit manually for testing now
group = handler._build_container_group(ctx)

poller = handler._azure_client.container_groups.begin_create_or_update(
    resource_group_name=handler._resource_group,
    container_group_name=handler._get_container_group_id(ctx),
    container_group=group,
)

result = poller.result()
### 