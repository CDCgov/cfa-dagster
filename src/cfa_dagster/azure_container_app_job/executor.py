import logging
import os
from collections.abc import Iterator
from typing import Optional, cast

import dagster._check as check
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.subscription import SubscriptionClient
from dagster import executor
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
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    validate_docker_config,
    validate_docker_image,
)

from .utils import CAJ_CONFIG_SCHEMA, get_status_caj, start_caj, stop_caj

log = logging.getLogger(__name__)


@executor(
    name="azure_container_app_job",
    config_schema=CAJ_CONFIG_SCHEMA,
    requirements=multiple_process_executor_requirements(),
)
def azure_container_app_job_executor(
    init_context: InitExecutorContext,
) -> Executor:
    """Executor which launches steps as Container App Job executions.

    To use the `azure_container_app_job_executor`, set it as the `executor_def` when defining a job:

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
            container_app_job_name: ...
            cpu: ...
            ram: ...
            image: ...
            env_vars: ...

    If you're using the DockerRunLauncher, configuration set on the containers created by the run
    launcher will also be set on the containers that are created for each step.
    """
    config = init_context.executor_config
    log.debug(f"init_context: '{init_context}'")

    # this is the config from the Launchpad
    log.debug(f"config: '{config}'")
    container_app_job_name = check.opt_str_elem(
        config, "container_app_job_name"
    )
    cpu = check.opt_float_elem(config, "cpu")
    memory = check.opt_float_elem(config, "memory")
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

    # propagate user & dev env vars
    req_vars = ["DAGSTER_USER", "CFA_DAGSTER_ENV", "DAGSTER_IS_DEV_CLI"]
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

    return StepDelegatingExecutor(
        AzureContainerAppJobStepHandler(
            image, container_context, container_app_job_name, cpu, memory
        ),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
    )


class AzureContainerAppJobStepHandler(StepHandler):
    def __init__(
        self,
        image: Optional[str],
        container_context: DockerContainerContext,
        container_app_job_name: str,
        cpu: float,
        memory: float,
    ):
        super().__init__()
        log.debug(f"Launching a new {self.name}")
        self._step_container_ids = {}
        self._step_caj_execution_ids = {}

        self._container_app_job_name = container_app_job_name
        self._cpu = cpu
        self._memory = memory
        self._resource_group = "ext-edav-cfa-prd"  # TODO: move to config?
        credential = DefaultAzureCredential()

        # Get first subscription for logged-in credential
        first_subscription_id = (
            SubscriptionClient(credential)
            .subscriptions.list()
            .next()
            .subscription_id
        )

        self._azure_caj_client = ContainerAppsAPIClient(
            credential=credential, subscription_id=first_subscription_id
        )

        self._image = check.opt_str_param(image, "image")
        self._container_context = check.inst_param(
            container_context, "container_context", DockerContainerContext
        )

    def _get_image(self, step_handler_context: StepHandlerContext):
        # get image from step config
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
        # This doesn't vary per step: would be good to have a hook where it can be set once
        # for the whole StepHandler but we need access to the DagsterRun for that

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
        return "AzureContainerAppJobStepHandler"

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

    def _create_step_container(
        self,
        client,
        container_context,
        step_image,
        step_handler_context: StepHandlerContext,
    ):
        execute_step_args = step_handler_context.execute_step_args
        # step_keys_to_execute = check.not_none(execute_step_args.step_keys_to_execute)
        # assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        step_key = self._get_step_key(step_handler_context)

        container_kwargs = {**container_context.container_kwargs}
        container_kwargs.pop("stop_timeout", None)

        env_vars = dict(
            [parse_env_var(env_var) for env_var in container_context.env_vars]
        )
        env_vars["DAGSTER_RUN_JOB_NAME"] = (
            step_handler_context.dagster_run.job_name
        )
        env_vars["DAGSTER_RUN_STEP_KEY"] = step_key

        job_execution_id = start_caj(
            self._azure_caj_client,
            resource_group=self._resource_group,
            container_app_job_name=self._container_app_job_name,
            image=step_image,
            env_vars=env_vars,
            command=execute_step_args.get_command_args(),
            cpu=self._cpu,
            memory=self._memory,
        )
        self._step_caj_execution_ids[step_key] = job_execution_id
        return job_execution_id

    def launch_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        container_context = self._get_docker_container_context(
            step_handler_context
        )

        step_image = self._get_image(step_handler_context)

        validate_docker_image(step_image)

        step_key = self._get_step_key(step_handler_context)

        job_execution_id = self._create_step_container(
            self._azure_caj_client,
            container_context,
            step_image,
            step_handler_context,
        )

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=f"Launching step in Container App Job with id: {job_execution_id}.",
            metadata={
                "Azure Container App Job execution id": job_execution_id,
            },
        )

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        step_key = self._get_step_key(step_handler_context)
        job_execution_id = self._step_caj_execution_ids[step_key]
        log.debug(f"job_execution_id: '{job_execution_id}'")
        # return CheckStepHealthResult.healthy()

        status = get_status_caj(
            self._azure_caj_client,
            resource_group=self._resource_group,
            container_app_job_name=self._container_app_job_name,
            job_execution_id=job_execution_id,
        )

        match status:
            case None:
                return CheckStepHealthResult.unhealthy(
                    reason=f"Container App Job {job_execution_id} not found!"
                )
            case (
                "Running" | "Succeeded" | "Processing" | "Stopped" | "Unknown"
            ):
                return CheckStepHealthResult.healthy()
            case "Failed" | "Degraded":
                return CheckStepHealthResult.unhealthy(
                    reason=f"Container App Job {job_execution_id} status is {status} for step {step_key}."
                )
            case _:
                return CheckStepHealthResult.unhealthy(
                    reason=f"Container app execution {job_execution_id} for step {step_key} could not be found."
                )

    def terminate_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        job_execution_id = self._step_caj_execution_ids[step_key]
        log.debug(f"job_execution_id: '{job_execution_id}'")

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=f"Stopping container app job execution {job_execution_id} for step.",
            event_specific_data=EngineEventData(),
        )

        return stop_caj(
            self._azure_caj_client,
            self._resource_group,
            self._container_app_job_name,
            job_execution_id,
        )
