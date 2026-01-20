import logging
import json
import yaml
import os
from collections.abc import Iterator
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from typing import cast
from dagster._serdes.config_class import ConfigurableClassData

from dagster import Field, IntSource, executor
import dagster._check as check
from dagster import executor
from dagster._config import process_config
from dagster._core.definitions.executor_definition import (
    multiple_process_executor_requirements,
)

from dagster._core.execution.step_dependency_config import (
    StepDependencyConfig,
    get_step_dependency_config_field,
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
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.utils import parse_env_var
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    validate_docker_config,
    validate_docker_image,
)
from dagster import (
    in_process_executor,
    multiprocess_executor,
    ExecutorDefinition
)
from cfa_dagster import (
    azure_container_app_job_executor,
    azure_batch_executor,
    docker_executor
)


log = logging.getLogger(__name__)


@executor(
    name="dynamic_executor",
    config_schema={
        "retries": get_retries_config(),
        "max_concurrent": Field(
            IntSource,
            is_required=False,
            description=(
                "Limit on the number of containers that will run concurrently within the scope "
                    "of a Dagster run. Note that this limit is per run, not global."
            ),
        ),
        "tag_concurrency_limits": get_tag_concurrency_limits_config(),
        "step_dependency_config": get_step_dependency_config_field(),
    }
    ,
    requirements=multiple_process_executor_requirements(),
)
def dynamic_executor(
    init_context: InitExecutorContext,
) -> Executor:
    """Dynamic executor that chooses an executor based on the `cfa_dagster/executor` tag"""
    config = init_context.executor_config
    log.debug(f"init_context: '{init_context}'")

    # this is the config from the Launchpad
    log.debug(f"config: '{config}'")
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    retries = check.dict_elem(config, "retries", key_type=str)
    max_concurrent = check.opt_int_elem(config, "max_concurrent")
    tag_concurrency_limits = check.opt_list_elem(
        config, "tag_concurrency_limits"
    )
    # propagate user & dev env vars
    env_vars.append("DAGSTER_USER")
    if os.getenv("DAGSTER_IS_DEV_CLI"):
        env_vars.append("DAGSTER_IS_DEV_CLI")

    return StepDelegatingExecutor(
        DynamicStepHandler(init_context),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
    )


EXECUTOR_CONFIG_KEY = "cfa_dagster/executor"


class DynamicStepHandler(StepHandler):
    def __init__(
        self,
        init_context: InitExecutorContext
    ):
        super().__init__()
        self._init_context = init_context # TODO: add env vars
        self._executor = None
        log.debug(f"Launching a new {self.name}")

    @property
    def name(self) -> str:
        return "DynamicStepHandler"

    def _create_executor(self, executor_config: dict) -> StepDelegatingExecutor:
        is_production = not os.getenv("DAGSTER_IS_DEV_CLI")
        executor_class_name = executor_config.get("class")
        match (is_production, executor_class_name):
            case (True, docker_executor.__name__):
                raise RuntimeError(
                    f"You can't use {executor_class_name} in production!"
                )

        try:
            executor_class: ExecutorDefinition = globals()[executor_class_name]
        except KeyError:
            valid_executors = [
                c.__name__
                for c in [
                    in_process_executor,
                    multiprocess_executor,
                    docker_executor,
                    azure_batch_executor,
                    azure_container_app_job_executor,
                ]
            ]
            raise RuntimeError(
                f"Invalid executor class specified: '{executor_class_name}'. "
                "Must be one of: "
                f"{valid_executors}"
            )

        default_config = process_config(executor_class.config_schema, {}).value
        log.debug(f"default_config: '{default_config}'")
        config = executor_config.get(
            "config",
            default_config
        )

        # default executors throw an error for env vars
        if (executor_class_name != in_process_executor and
                executor_class_name != multiprocess_executor):
            env_vars = config.get("env_vars", [])
            # Need to check if env vars are present first or
            # each run will append them again
            if "DAGSTER_USER" not in env_vars:
                env_vars.append("DAGSTER_USER")
            if "DAGSTER_IS_DEV_CLI" not in env_vars and os.getenv(
                "DAGSTER_IS_DEV_CLI"
            ):
                env_vars.append("DAGSTER_IS_DEV_CLI")
            config["env_vars"] = env_vars

        updated_context = self._init_context._replace(executor_config=config)
        run_executor = executor_class.executor_creation_fn(updated_context)
        log.debug(f"run_executor: '{run_executor}'")
        return run_executor

    def _get_executor_config_from_tags(self, tags: dict):
        executor_config_str = tags.get(EXECUTOR_CONFIG_KEY, "{}")
        try:
            return json.loads(executor_config_str)
        except json.decoder.JSONDecodeError:
            raise RuntimeError(
                f"Invalid JSON for '{EXECUTOR_CONFIG_KEY}'. "
                f"Received: '{executor_config_str}'"
            )

    def _get_step_handler(self, step_handler_context: StepHandlerContext) -> StepHandler:
        if not self._executor:
            tags = step_handler_context.dagster_run.tags
            executor_config = self._get_executor_config_from_tags(tags)
            self._executor = self._create_executor(executor_config)

        return self._executor._step_handler

    def launch_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_handler = self._get_step_handler(step_handler_context)
        yield step_handler.launch_step(step_handler_context)

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        step_handler = self._get_step_handler(step_handler_context)
        return step_handler.check_step_health(step_handler_context)

    def terminate_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_handler = self._get_step_handler(step_handler_context)
        return step_handler.terminate_step(step_handler_context)
