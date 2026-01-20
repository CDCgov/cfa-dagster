import logging
import json
from dagster._utils.merger import merge_dicts
from dagster._core.execution.context.system import PlanOrchestrationContext
from dagster._core.execution.plan.plan import ExecutionPlan
import os

from dagster import executor
import dagster._check as check

from dagster._core.execution.step_dependency_config import (
    StepDependencyConfig,
)
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import StepDelegatingExecutor
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


@executor(name="dynamic_executor")
def dynamic_executor(
    init_context: InitExecutorContext,
) -> Executor:
    """Dynamic executor that chooses an executor based on the `cfa_dagster/executor` tag"""
    config = init_context.executor_config
    log.debug(f"init_context: '{init_context}'")

    # this is the config from the Launchpad
    log.debug(f"config: '{config}'")

    return DynamicExecutor(
        init_context=init_context,
    )


EXECUTOR_CONFIG_KEY = "cfa_dagster/executor"


class DynamicExecutor(Executor):
    def __init__(
        self,
        init_context: InitExecutorContext,
    ):
        self._init_context = init_context
        default_executor_config = {"class": multiprocess_executor.__name__}
        self._executor = self._create_executor(default_executor_config)

    @property
    def retries(self):
        return self._executor.retries

    @property
    def step_dependency_config(self) -> StepDependencyConfig:
        return self._executor.step_dependency_config

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

        default_config = executor_class.config_schema.as_field().default_value
        log.debug(f"default_config: '{default_config}'")

        config = merge_dicts(
            executor_config.get("config", default_config),
            default_config
        )

        # default executors throw an error for env vars
        if executor_class_name not in (
            in_process_executor.__name__,
            multiprocess_executor.__name__
        ):
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

    def execute(
        self,
        plan_context: PlanOrchestrationContext,
        execution_plan: ExecutionPlan
    ):
        check.inst_param(plan_context, "plan_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)
        tags = plan_context.plan_data.dagster_run.tags
        executor_config = self._get_executor_config_from_tags(tags)
        self._executor = self._create_executor(executor_config)
        return self._executor.execute(plan_context, execution_plan)
