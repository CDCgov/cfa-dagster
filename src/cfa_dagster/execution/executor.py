import logging
import os
from typing import Optional

import dagster._check as check
from dagster import (
    ExecutorDefinition,
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

# ruff: noqa: F401
from dagster_docker import docker_executor

from cfa_dagster import (
    azure_batch_executor,
    azure_container_app_job_executor,
)

from .utils import (
    ExecutionConfig,
    SelectorConfig,
    get_dynamic_executor_config_schema,
)

log = logging.getLogger(__name__)


def create_executor(
    init_context: InitExecutorContext,
    execution_config: ExecutionConfig,
) -> StepDelegatingExecutor:
    executor_class_name = execution_config.executor.class_name
    executor_config = execution_config.executor.config

    try:
        executor_class: ExecutorDefinition = globals()[executor_class_name]
    except KeyError:
        raise RuntimeError(
            f"Invalid executor class specified: '{executor_class_name}'. "
        )

    # default executors throw an error for env vars
    if executor_class_name not in (
        in_process_executor.__name__,
        multiprocess_executor.__name__,
    ):
        env_vars = executor_config.get("env_vars", [])
        log.debug(f"env_vars before req: '{env_vars}'")
        # Need to check if env vars are present first or
        # each run will append them again
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
        log.debug(f"env_vars after req: '{env_vars}'")
        executor_config["env_vars"] = env_vars

    updated_context = init_context._replace(executor_config=executor_config)
    log.debug(
        f"creating '{executor_class_name}' with config: '{executor_config}'"
    )
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
        tags = plan_context.plan_data.dagster_run.tags
        log.debug(f"tags: '{tags}'")
        # 1. get tag config
        # 2. get run config
        execution_config = ExecutionConfig.from_run_tags(tags)
        log.debug(f"tag execution_config: '{execution_config}'")
        if not execution_config.executor:
            execution_config = ExecutionConfig.from_executor_config(
                self._init_context.executor_config
            )
            log.debug(f"run config execution_config: '{execution_config}'")
        if not execution_config.executor:
            raise RuntimeError(
                "No executor found in run config, tags, or Definitions.metadata!"
            )
        self._executor = create_executor(
            self._init_context, execution_config.validate()
        )
        plan_context.log.info(
            "[dynamic_executor] Launching run using "
            f"'{execution_config.executor.class_name}'"
        )
        return self._executor.execute(plan_context, execution_plan)


def dynamic_executor(
    default_config: Optional[ExecutionConfig] = None,
    alternate_configs: Optional[list[ExecutionConfig]] = None,
):
    default_config = default_config or ExecutionConfig.default()
    default_config = default_config.validate()
    schema = get_dynamic_executor_config_schema(
        default_config=default_config,
        alternate_configs=alternate_configs,
    )
    log.debug(f"schema: '{schema}'")

    @executor(
        config_schema=schema,
        name="dynamic_executor",
    )
    def dynamic_executor(
        init_context: InitExecutorContext,
    ) -> Executor:
        return DynamicExecutor(init_context)

    return dynamic_executor
