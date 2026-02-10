import logging
import os

import dagster._check as check
from dagster import (
    ExecutorDefinition,
    Selector,
    Shape,
    executor,
    in_process_executor,
    multiprocess_executor,
)
from dagster._core.execution.context.system import PlanOrchestrationContext
from dagster._core.execution.plan.plan import ExecutionPlan
from dagster._core.execution.step_dependency_config import (
    StepDependencyConfig,
)
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import StepDelegatingExecutor

from cfa_dagster import (
    azure_batch_executor,
    azure_container_app_job_executor,
    docker_executor,
)

from .utils import (
    ExecutionConfig,
    SelectorConfig,
    get_dynamic_executor_config_schema,
)

log = logging.getLogger(__name__)

EXECUTION_CONFIG_KEY = "cfa_dagster/execution"

VALID_EXECUTORS = [
    c.__name__
    for c in [
        in_process_executor,
        multiprocess_executor,
        docker_executor,
        azure_batch_executor,
        azure_container_app_job_executor,
    ]
]


def create_executor(
    init_context: InitExecutorContext,
    execution_config: ExecutionConfig,
) -> StepDelegatingExecutor:
    executor_class_name = execution_config.executor.class_name
    executor_config = execution_config.executor.config
    is_production = not os.getenv("DAGSTER_IS_DEV_CLI")
    match (is_production, executor_class_name):
        case (True, docker_executor.__name__):
            raise RuntimeError(
                f"You can't use {executor_class_name} in production!"
            )

    try:
        executor_class: ExecutorDefinition = globals()[executor_class_name]
    except KeyError:
        raise RuntimeError(
            f"Invalid executor class specified: '{executor_class_name}'. "
            "Must be one of: "
            f"{VALID_EXECUTORS}"
        )

    # default executors throw an error for env vars
    if executor_class_name not in (
        in_process_executor.__name__,
        multiprocess_executor.__name__,
    ):
        env_vars = executor_config.get("env_vars", [])
        # Need to check if env vars are present first or
        # each run will append them again
        if "DAGSTER_USER" not in env_vars:
            env_vars.append("DAGSTER_USER")
        if "DAGSTER_IS_DEV_CLI" not in env_vars and os.getenv(
            "DAGSTER_IS_DEV_CLI"
        ):
            env_vars.append("DAGSTER_IS_DEV_CLI")
        executor_config["env_vars"] = env_vars

    updated_context = init_context._replace(executor_config=executor_config)
    run_executor = executor_class.executor_creation_fn(updated_context)
    log.debug(f"run_executor: '{run_executor}'")
    return run_executor


class DynamicExecutor(Executor):
    def __init__(
        self,
        init_context: InitExecutorContext,
    ):
        self._init_context = init_context
        self._executor = create_executor(
            init_context,
            ExecutionConfig(
                executor=SelectorConfig(
                    class_name=multiprocess_executor.__name__, config={}
                )
            ),
        )

    @property
    def retries(self):
        return self._executor.retries

    @property
    def step_dependency_config(self) -> StepDependencyConfig:
        return self._executor.step_dependency_config

    def execute(
        self,
        plan_context: PlanOrchestrationContext,
        execution_plan: ExecutionPlan,
    ):
        check.inst_param(
            plan_context, "plan_context", PlanOrchestrationContext
        )
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)
        # TODO: use ExecutionConfig instead of local functions
        tags = plan_context.plan_data.dagster_run.tags
        log.debug(f"tags: '{tags}'")
        # 0. shouldn't see this code if RunConfig specifies execution
        # 1. get tag config
        # 2. get metadata config (shouldn't if using DynamicRunLauncher since
        #                         it will create tags before launching)
        execution_config = ExecutionConfig.from_run_tags(tags)
        if not execution_config.executor:
            log.debug("No executor configured in tags, checking repo metadata")
            job = plan_context.plan_data.job
            log.debug(f"job: '{job}'")

            repo_def = job.get_repository_definition()
            log.debug(f"repo_def: '{repo_def}'")

            metadata = repo_def.metadata
            log.debug(f"metadata: '{metadata}'")
            execution_config = ExecutionConfig.from_metadata(metadata)
        if not execution_config.executor:
            raise RuntimeError(
                "No executor found in run config, tags, or Definitions.metadata!"
            )
        self._executor = create_executor(self._init_context, execution_config)
        return self._executor.execute(plan_context, execution_plan)


def dynamic_executor():
    @executor(
        config_schema=get_dynamic_executor_config_schema(),
        name="dynamic_executor",
    )
    def dynamic_executor(
        init_context: InitExecutorContext,
    ) -> Executor:
        """Dynamic executor that chooses an executor based on the `cfa_dagster/executor` tag"""
        config = init_context.executor_config
        execution_config = ExecutionConfig.from_executor_config(config)
        return create_executor(init_context, execution_config)

    return dynamic_executor
