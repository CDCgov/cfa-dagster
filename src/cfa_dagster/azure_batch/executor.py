import hashlib
import logging
import os
import re
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional, cast

import dagster._check as check
from azure.batch import BatchServiceClient
from azure.batch.models import (
    AutoUserScope,
    AutoUserSpecification,
    BatchErrorException,
    ComputeNodeIdentityReference,
    ContainerRegistry,
    ElevationLevel,
    JobAddParameter,
    PoolInformation,
    TaskAddParameter,
    TaskContainerSettings,
    UserIdentity,
)
from azure.identity import DefaultAzureCredential
from azure.mgmt.subscription import SubscriptionClient
from dagster import Field, StringSource, executor
from dagster._core.definitions.executor_definition import (
    multiple_process_executor_requirements,
)
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.retries import RetryMode
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
from dagster_docker import docker_executor as base_docker_executor
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    validate_docker_config,
    validate_docker_image,
)
from msrest.authentication import BasicTokenAuthentication

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


@executor(
    name="azure_batch",
    config_schema=merge_dicts(
        base_docker_executor.config_schema.config_type.fields,
        {
            "pool_name": Field(
                StringSource,
                is_required=True,
                description="The name of the Azure Batch Pool.",
            ),
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
    working_dir = container_kwargs.get("working_dir")
    if not working_dir:
        raise ValueError(
            (
                "Missing property 'container_kwargs.working_dir' "
                "is required and must match your Dockerfile WORKDIR"
            )
        )
    retries = check.dict_elem(config, "retries", key_type=str)
    max_concurrent = check.opt_int_elem(config, "max_concurrent")
    tag_concurrency_limits = check.opt_list_elem(
        config, "tag_concurrency_limits"
    )

    # propagate user & dev env vars
    req_vars = [
        "DAGSTER_USER",
        "CFA_DAGSTER_ENV",
        "DAGSTER_IS_DEV_CLI",
        "CFA_DG_PG_HOSTNAME",
        "CFA_DG_PG_USERNAME",
        "CFA_DG_PG_PASSWORD",
    ]
    for env_var in req_vars:
        if os.getenv(env_var) and env_var not in env_vars:
            env_vars.append(env_var)

    validate_docker_config(network, networks, container_kwargs)

    if network and not networks:
        networks = [network]

    container_context = DockerContainerContext(
        registry=registry,
        env_vars=env_vars,
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
        # self._pool_id = "cfa-dagster"
        self._pool_id = pool_name
        log.debug(f"Launching a new {self.name}")
        credential_v2 = DefaultAzureCredential()
        token = {
            "access_token": credential_v2.get_token(
                "https://batch.core.windows.net/.default"
            ).token
        }
        credential_v1 = BasicTokenAuthentication(token)

        batch_url = "https://cfaprdba.eastus.batch.azure.com"

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
            step_context.run_config.get("ops", {})
            .get(step_key, {})
            .get("config", {})
            .get("image")
        )
        if not image:
            image = self._image

        if not image:
            raise Exception(
                "No docker image specified by the executor or run config"
            )

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

    def _get_job_id(self, step_handler_context: StepHandlerContext):
        """
        Creates a unique job id for Azure Batch

        The job id is a uuidv5 generated based on the DAGSTER_USER env
        variable, the pool_id, the code location name, and the hour the run was created.

        This ensures tasks are logically grouped into jobs without running into
        the max active job limit imposed by Batch. Since the job id is scoped to
        the run creation hour, jobs without active tasks can safely be cleaned up by a
        background process.
        """
        run = step_handler_context.dagster_run

        location_name = run.remote_job_origin.repository_origin.code_location_origin.location_name
        dagster_user = os.getenv("DAGSTER_USER")
        run_record = step_handler_context.instance.get_run_record_by_id(
            run.run_id
        )
        if not run_record:
            raise RuntimeError(f"No run record for run id: {run.run_id}")

        run_creation_hour = run_record.create_timestamp.strftime("%Y-%m-%dT%H")
        log.debug(f"dagster_user: '{dagster_user}'")
        log.debug(f"pool_id: '{self._pool_id}'")
        log.debug(f"location_name: '{location_name}'")
        log.debug(f"run_creation_hour: '{run_creation_hour}'")
        base_id = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            ":".join(
                [
                    dagster_user,
                    self._pool_id,
                    location_name,
                    run_creation_hour,
                ]
            ),
        )

        return f"dagster-{base_id}"

    def _clamp_with_hash(self, value: str, max_len: int) -> str:
        if len(value) <= max_len:
            return value

        # Reserve 6 chars for hash + "-"
        hash_len = 6
        keep_len = max_len - hash_len - 1
        digest = hashlib.sha1(value.encode()).hexdigest()[:hash_len]
        return f"{value[:keep_len]}-{digest}"

    def _get_task_id(self, step_handler_context: StepHandlerContext) -> str:
        MAX_ID_LEN = 64  # max length of azure batch task id
        PREFIX = "dg-"
        SHORT_RUN_ID_LEN = 8
        run = step_handler_context.dagster_run

        short_run_id = run.run_id.split("-", 1)[0]  # always 8 chars

        step_key = self._get_step_key(step_handler_context)

        partition_key = run.tags.get("dagster/partition") or ""
        if partition_key:
            partition_key = re.sub(r"[^0-9A-Za-z]+", "_", partition_key)

        if step_handler_context.execute_step_args.known_state:
            retry_count = step_handler_context.execute_step_args.known_state.get_retry_state().get_attempt_count(
                step_key
            )
        else:
            retry_count = 0

        retry_str = str(retry_count)

        # ---- budget calculation ----
        fixed_len = (
            len(PREFIX) + SHORT_RUN_ID_LEN + len(retry_str) + 3
            if partition_key
            else 2  # separators: step-part-run-retry
        )

        remaining = MAX_ID_LEN - fixed_len
        if remaining <= 0:
            raise ValueError("Fixed ID components exceed max length")

        # split remaining budget evenly
        step_budget = remaining // 2
        part_budget = remaining - step_budget

        step_key = self._clamp_with_hash(step_key, step_budget)
        if partition_key:
            partition_key = self._clamp_with_hash(partition_key, part_budget)

        # ---- final assembly ----
        if partition_key:
            return f"{PREFIX}{step_key}-{partition_key}-{short_run_id}-{retry_str}"
        else:
            return f"{PREFIX}{step_key}-{short_run_id}-{retry_str}"

    def _get_or_create_job(self, batch_client, job_id: str, pool_id: str):
        pool_info = PoolInformation(pool_id=pool_id)

        job = JobAddParameter(
            id=job_id,
            pool_info=pool_info,
        )

        try:
            batch_client.job.add(job)
            log.info(f"Created Batch job {job_id}")
            return batch_client.job.get(job_id)

        except BatchErrorException as err:
            if err.error.code != "JobExists":
                raise

            # Job was created by another thread/process
            log.debug(f"Batch job {job_id} already exists")

            existing_job = batch_client.job.get(job_id)

            # Should never happen since pool_id is factored into job_id hash
            if existing_job.pool_info.pool_id != pool_id:
                raise RuntimeError(
                    f"Batch job {job_id} exists but is bound to pool "
                    f"{existing_job.pool_info.pool_id}, expected {pool_id}"
                )

            return existing_job

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

        job_id = self._get_job_id(step_handler_context)
        log.debug(f"job_id: '{job_id}'")

        self._get_or_create_job(self._batch_client, job_id, self._pool_id)

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

        workdir = container_context.container_kwargs.get(
            "working_dir", "/app"
        )  # TODO: validate this

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
        task_id = self._get_task_id(step_handler_context)

        task = TaskAddParameter(
            id=task_id,
            command_line=f"/bin/bash -c '{' '.join(command)}'",
            container_settings=container_settings,
            environment_settings=[
                {"name": k, "value": v} for k, v in env_vars.items()
            ],
            user_identity=user_identity,
        )

        self._batch_client.task.add(job_id=job_id, task=task)

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=(
                f"Launching step in task {task_id}, Azure Batch job: {job_id}."
            ),
            metadata={
                "Azure Batch Job ID": job_id,
                "Azure Batch Pool ID": self._pool_id,
            },
        )

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        job_id = self._get_job_id(step_handler_context)
        task_id = self._get_task_id(step_handler_context)

        try:
            task = self._batch_client.task.get(job_id, task_id)
            if task.state in ("active", "preparing", "running"):
                return CheckStepHealthResult.healthy()
            elif task.state == "completed":
                if task.execution_info.exit_code == 0:
                    return CheckStepHealthResult.healthy()  # Consider it healthy and let the framework handle completion
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
        job_id = self._get_job_id(step_handler_context)
        task_id = self._get_task_id(step_handler_context)

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=f"Terminating Azure Batch task {task_id} in job {job_id} for step {step_key}.",
            event_specific_data=EngineEventData(),
        )
        self._batch_client.task.terminate(job_id=job_id, task_id=task_id)
