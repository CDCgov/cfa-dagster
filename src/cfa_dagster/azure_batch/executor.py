from collections.abc import Iterator
from datetime import datetime as date
from typing import TYPE_CHECKING, Optional, cast

import dagster._check as check
from azure.mgmt.resource.subscriptions import SubscriptionClient
from azure.batch import BatchServiceClient
from azure.batch.models import (
    ContainerRegistry,
    ComputeNodeIdentityReference,
    BatchErrorException,
    CloudTask,
    JobAddParameter,
    PoolInformation,
    TaskAddParameter,
    TaskConstraints,
    TaskContainerSettings,
    UserIdentity,
    AutoUserSpecification,
    AutoUserScope,
    ElevationLevel,
)
from azure.identity import DefaultAzureCredential
from msrest.authentication import BasicTokenAuthentication
from dagster import Field, IntSource, StringSource, executor
from dagster._core.definitions.executor_definition import (
    multiple_process_executor_requirements,
)
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import StepDelegatingExecutor
from dagster._core.executor.step_delegating.step_handler.base import (
    CheckStepHealthResult,
    StepHandler,
    StepHandlerContext,
)
from dagster._core.utils import parse_env_var
from dagster._utils.merger import merge_dicts
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    DOCKER_CONFIG_SCHEMA,
    validate_docker_config,
    validate_docker_image,
)

if TYPE_CHECKING:
    from dagster._core.origin import JobPythonOrigin


@executor(
    name="azure_batch",
    config_schema=merge_dicts(
        DOCKER_CONFIG_SCHEMA,
        {
            "pool_name": Field(
                StringSource,
                is_required=False,
                default_value="cfa-dagster",
                description=(
                    "The name of the Azure Batch Pool. Defaults "
                    "to the cfa-dagster test pool with 4 CPU 16 GB RAM"
                )
            ),
            "retries": get_retries_config(),
            "max_concurrent": Field(
                IntSource,
                is_required=False,
                description=(
                    "Limit on the number of tasks that will run concurrently within the scope "
                    "of a Dagster run. Note that this limit is per run, not global."
                ),
            ),
            "tag_concurrency_limits": get_tag_concurrency_limits_config(),
        },
    ),
    requirements=multiple_process_executor_requirements(),
)
def azure_batch_executor(
    init_context: InitExecutorContext,
) -> Executor:
    """Executor which launches steps as Azure Batch tasks.

    To use the `azure_batch_executor`, set it as the `executor_def` when defining a job:

    .. code-block:: python
        some_job = dg.define_asset_job(
            name="some_job",
            executor_def=azure_container_app_job_executor,
            ..
        )

    Then you can configure the executor with run config as follows:

    .. code-block:: YAML

        execution:
          config:
            pool_name: ...
            image: ...
            env_vars: ...
            container_kwargs: ...

    """
    config = init_context.executor_config
    image = check.opt_str_elem(config, "image")
    registry = check.opt_dict_elem(config, "registry", key_type=str)
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    network = check.opt_str_elem(config, "network")
    networks = check.opt_list_elem(config, "networks", of_type=str)
    container_kwargs = check.opt_dict_elem(
        config, "container_kwargs", key_type=str
    )
    retries = check.dict_elem(config, "retries", key_type=str)
    max_concurrent = check.opt_int_elem(config, "max_concurrent")
    tag_concurrency_limits = check.opt_list_elem(
        config, "tag_concurrency_limits"
    )

    validate_docker_config(network, networks, container_kwargs)

    if network and not networks:
        networks = [network]

    container_context = DockerContainerContext(
        registry=registry,
        env_vars=env_vars or [],
        networks=networks or [],
        container_kwargs=container_kwargs,
    )

    pool_name = check.opt_str_elem(config, "pool_name")

    return StepDelegatingExecutor(
        AzureBatchStepHandler(image, container_context, pool_name),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
    )


class AzureBatchStepHandler(StepHandler):
    def __init__(
        self,
        image: Optional[str],
        container_context: DockerContainerContext,
        pool_name: Optional[str],
    ):
        super().__init__()
        self._step_job_ids = {}
        # self._pool_id = "cfa-dagster"
        self._pool_id = pool_name
        print(f"Launching a new {self.name}")
        credential_v2 = DefaultAzureCredential()
        token = {
            "access_token": credential_v2.get_token(
                "https://batch.core.windows.net/.default"
            ).token
        }
        credential_v1 = BasicTokenAuthentication(token)

        batch_url = f"https://cfaprdba.eastus.batch.azure.com"

        self._subscription_id = (
            SubscriptionClient(credential_v2)
            .subscriptions.list()
            .next()
            .subscription_id
        )

        self._batch_client = BatchServiceClient(
            credentials=credential_v1, batch_url=batch_url
        )

        self._image = check.opt_str_param(image, "image")
        self._container_context = check.inst_param(
            container_context, "container_context", DockerContainerContext
        )

    def _get_image(self, step_handler_context: StepHandlerContext):
        step_key = self._get_step_key(step_handler_context)
        step_context = step_handler_context.get_step_context(step_key)
        image = (
            step_context.run_config
                        .get("ops", {})
                        .get(step_key, {})
                        .get("config", {})
                        .get("image")
        )
        if not image:
            image = self._image

        if not image:
            raise Exception("No docker image specified by the executor or run config")

        return image

    def _get_docker_container_context(
        self, step_handler_context: StepHandlerContext
    ):
        from dagster_docker.docker_run_launcher import DockerRunLauncher

        run_launcher = step_handler_context.instance.run_launcher
        run_target = DockerContainerContext.create_for_run(
            step_handler_context.dagster_run,
            run_launcher
            if isinstance(run_launcher, DockerRunLauncher)
            else None,
        )

        merged_container_context = run_target.merge(self._container_context)

        validate_docker_config(
            network=None,
            networks=merged_container_context.networks,
            container_kwargs=merged_container_context.container_kwargs,
        )

        return merged_container_context

    @property
    def name(self) -> str:
        return "AzureBatchStepHandler"

    def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
        check.not_none(
            step_handler_context.execute_step_args.step_keys_to_execute
        )
        step_keys_to_execute = cast(
            "list[str]",
            step_handler_context.execute_step_args.step_keys_to_execute,
        )
        assert len(step_keys_to_execute) == 1, (
            "Launching/Terminating multiple steps is not currently supported"
        )
        return step_keys_to_execute[0]

    def launch_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        container_context = self._get_docker_container_context(
            step_handler_context
        )
        step_image = self._get_image(step_handler_context)
        validate_docker_image(step_image)
        step_key = self._get_step_key(step_handler_context)
        execute_step_args = step_handler_context.execute_step_args

        job_id = f"dagster-job-{date.now().strftime('%Y-%m-%d-%H_%M_%S')}-{step_key}"
        self._step_job_ids[step_key] = job_id

        pool_info = PoolInformation(pool_id=self._pool_id)
        job = JobAddParameter(id=job_id, pool_info=pool_info)
        try:
            self._batch_client.job.add(job)
        except BatchErrorException as err:
            if err.error.code != "JobExists":
                raise
            else:
                print(f"Job {job_id} already exists.")

        env_vars = dict(
            [parse_env_var(env_var) for env_var in container_context.env_vars]
        )
        env_vars["DAGSTER_RUN_JOB_NAME"] = (
            step_handler_context.dagster_run.job_name
        )
        env_vars["DAGSTER_RUN_STEP_KEY"] = step_key

        command = execute_step_args.get_command_args()

        resource_group_name = "ext-edav-cfa-network-prd"
        user_assigned_identity_name = "ext-edav-cfa-batch-account"
        resource_id = f"/subscriptions/{self._subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{user_assigned_identity_name}"

        container_registry = ContainerRegistry(
            registry_server="cfaprdbatchcr.azurecr.io",
            identity_reference=ComputeNodeIdentityReference(
                resource_id=resource_id
            ),
        )

        volumes = container_context.container_kwargs.get("volumes", [])
        mount_options = ""
        for volume in volumes:
            source, target = volume.split(":", 1)
            mount_options += f" --mount type=bind,source=$AZ_BATCH_NODE_MOUNTS_DIR/{source},target={target}"

        workdir = container_context.container_kwargs.get("working_dir", "/app")

        container_settings = TaskContainerSettings(
            image_name=step_image,
            container_run_options=f"--rm --workdir {workdir} {mount_options}",
            registry=container_registry,
        )

        # Run task as admin to be able to read/write to mounted drives
        user_identity = UserIdentity(
            auto_user=AutoUserSpecification(
                scope=AutoUserScope.pool,
                elevation_level=ElevationLevel.admin,
            )
        )

        task = TaskAddParameter(
            id=f"task-{step_key}",
            command_line=f"/bin/bash -c \'{" ".join(command)}\'",
            container_settings=container_settings,
            environment_settings=[{"name": k, "value": v} for k, v in env_vars.items()],
            user_identity=user_identity,
        )

        self._batch_client.task.add(job_id=job_id, task=task)

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=f"Launching step in Azure Batch job: {job_id}.",
            metadata={
                "Azure Batch Job ID": job_id,
                "Azure Batch Pool ID": self._pool_id,
            },
        )

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        step_key = self._get_step_key(step_handler_context)
        job_id = self._step_job_ids[step_key]
        task_id = f"dagster-step-{step_handler_context.dagster_run.run_id[:8]}-{step_key}"

        try:
            task = self._batch_client.task.get(job_id, task_id)
            if task.state in ("active", "preparing", "running"):
                return CheckStepHealthResult.healthy()
            elif task.state == "completed":
                if task.execution_info.exit_code == 0:
                    return CheckStepHealthResult.healthy() # Consider it healthy and let the framework handle completion
                else:
                    return CheckStepHealthResult.unhealthy(
                        reason=f"Azure Batch task {task_id} in job {job_id} failed with exit code {task.execution_info.exit_code}."
                    )
            else:
                return CheckStepHealthResult.unhealthy(
                    reason=f"Azure Batch task {task_id} in job {job_id} has unexpected state {task.state}."
                )
        except BatchErrorException as err:
            return CheckStepHealthResult.unhealthy(
                reason=f"Error checking Azure Batch task status: {err}"
            )

    def terminate_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        job_id = self._step_job_ids[step_key]

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=f"Terminating Azure Batch job {job_id} for step.",
            event_specific_data=EngineEventData(),
        )
        self._batch_client.job.terminate(job_id)
