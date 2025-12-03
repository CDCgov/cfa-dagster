from dagster._utils.merger import merge_dicts
from dagster_docker import docker_executor as base_docker_executor
from dagster import Field, Float, StringSource
from azure.mgmt.appcontainers import ContainerAppsAPIClient
import logging

log = logging.getLogger(__name__)

CAJ_CONFIG_SCHEMA = merge_dicts(
        base_docker_executor.config_schema.__dict__,
        {
            "container_app_job_name": Field(
                StringSource,
                is_required=True,
                description="The name of the Container App Job",
            ),
            "cpu": Field(
                Float,
                is_required=False,
                description=(
                    "Required CPU in cores. Min: 0.25 Max: 4.0. "
                    "CPU value must be half memory e.g. 0.25 cpu 0.5 memory"
                ),
            ),
            "memory": Field(
                Float,
                is_required=False,
                description=(
                    "Required memory in GB from Min: 0.5 Max: 8.0"
                    "Memory value must be double CPU e.g. 0.25 cpu 0.5 memory"
                ),
            ),
        },
    )


def start_caj(
    client: ContainerAppsAPIClient,
    resource_group: str,
    container_app_job_name: str,
    image: str,
    env_vars: list[str],
    command: str,
    cpu: float,
    memory: float,
) -> str:
    job_template = client.jobs.get(
        resource_group_name=resource_group,
        job_name=container_app_job_name,
    ).template
    container = job_template.containers[0]
    container.image = image
    container.env = (
        (container.env or []) +
        [{"name": k, "value": v} for k, v in env_vars.items()]
    )
    container.command = command
    if cpu is not None:
        container.resources.cpu = cpu
    if memory is not None:
        container.resources.memory = f"{memory}Gi"
    log.debug(f"container.image: '{container.image}'")
    log.debug(f"container.env: '{container.env}'")
    log.debug(f"container.command: '{container.command}'")
    log.debug(f"container.resources.cpu: '{container.resources.cpu}'")
    log.debug(f"container.resources.memory: '{container.resources.memory}'")

    job_execution = client.jobs.begin_start(
        resource_group_name=resource_group,
        job_name=container_app_job_name,
        template=job_template,
    ).result()
    job_execution_id = job_execution.id.split("/").pop()
    log.debug(f"Started container app job with id: '{job_execution_id}'")
    return job_execution_id


def stop_caj(
    client: ContainerAppsAPIClient,
    resource_group: str,
    container_app_job_name: str,
    job_execution_id: str,
) -> bool:
    log.debug(f"job_execution_id: '{job_execution_id}'")

    # TODO: handle this error:
    """
azure.core.exceptions.HttpResponseError: Operation returned an invalid status 'Bad Request'
Content: "Reason: Not Found. Body: {\"error\":\"Requested job execution cfa-dagster-l70spvu not found\",\"success\":false}"
    """
    client.jobs.begin_stop_execution(
        resource_group, container_app_job_name, job_execution_id
    )

    return True


def get_status_caj(
    client: ContainerAppsAPIClient,
    resource_group: str,
    container_app_job_name: str,
    job_execution_id: str,
):
    execution = client.jobs_executions.list(
        resource_group_name=resource_group,
        job_name=container_app_job_name,
        filter=f"Name eq '{job_execution_id}'",
    ).next()  # only expecting one execution since we have the exact name

    # Status represented by enum, but property acces converts to Capital case
    # https://learn.microsoft.com/en-us/python/api/azure-mgmt-appcontainers/azure.mgmt.appcontainers.models.jobexecutionrunningstate?view=azure-python
    return execution if not execution else execution.status
