import logging
import os
import subprocess
from typing import Iterator, Optional, cast

import dagster._check as check
from dagster import (
    DagsterEvent,
    ExecutorDefinition,
    InitExecutorContext,
    in_process_executor,
    multiprocess_executor,
)
from dagster._core.executor.step_delegating import (
    CheckStepHealthResult,
    StepDelegatingExecutor,
    StepHandler,
    StepHandlerContext,
)

# used via globals()[executor_class_name]
# ruff: noqa: F401
from dagster_docker import docker_executor

from ..azure_batch import azure_batch_executor
from ..azure_container_app_job import azure_container_app_job_executor

# using relative import to avoid circular dependency
from .utils import (
    ExecutionConfig,
)

log = logging.getLogger(__name__)


class SynchronousStepHandler(StepHandler):
    """
    Executes steps one at a time by blocking until each subprocess completes.
    Unlike SubprocessStepHandler, this runs steps synchronously so the next
    step cannot start until this one finishes — useful for lightweight ops
    that should not incur container spin-up cost but also should
    not run concurrently with their dependents.
    """

    @property
    def name(self) -> str:
        return "SynchronousStepHandler"

    def launch_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[
            0
        ]
        step_context = step_handler_context.get_step_context(step_key)

        yield DagsterEvent.step_worker_starting(
            step_context,
            message=f"Running synchronous subprocess for {step_key}",
            metadata={},
        )

        result = subprocess.run(
            step_handler_context.execute_step_args.get_command_args()
        )

        if result.returncode != 0:
            raise Exception(
                f"Synchronous step {step_key} failed with return code {result.returncode}"
            )

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        # launch_step blocks until completion so by the time this is called
        # the step is always done
        return CheckStepHealthResult.healthy()

    def terminate_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        # Can't terminate a blocking subprocess after launch_step has returned
        return iter([])


class SubprocessStepHandler(StepHandler):
    """
    Executes each step in a subprocess.
    Supports the same config as multiprocess_executor.
    Note: tag_concurrency_limits is not supported — it is implemented at the
    StepDelegatingExecutor level and cannot be enforced per StepHandler.
    """

    def __init__(self):
        self._processes: dict[str, subprocess.Popen] = {}

    @property
    def name(self) -> str:
        return "SubprocessStepHandler"

    def launch_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[
            0
        ]
        step_context = step_handler_context.get_step_context(step_key)

        yield DagsterEvent.step_worker_starting(
            step_context,
            message=f"Launching subprocess for {step_key}",
            metadata={},
        )

        process = subprocess.Popen(
            step_handler_context.execute_step_args.get_command_args()
        )
        self._processes[step_key] = process

    def check_step_health(
        self, step_handler_context: StepHandlerContext
    ) -> CheckStepHealthResult:
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[
            0
        ]
        process = self._processes.get(step_key)
        if process is None or process.poll() is None:
            return CheckStepHealthResult.healthy()
        if process.returncode != 0:
            return CheckStepHealthResult.unhealthy(
                reason=f"Subprocess for {step_key} exited with code {process.returncode}"
            )
        return CheckStepHealthResult.healthy()

    def terminate_step(
        self, step_handler_context: StepHandlerContext
    ) -> Iterator[DagsterEvent]:
        step_key = step_handler_context.execute_step_args.step_keys_to_execute[
            0
        ]
        process = self._processes.pop(step_key, None)
        if process and process.poll() is None:
            process.terminate()
        return iter([])


def create_executor_step_handler(
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

    if executor_class_name == in_process_executor.__name__:
        return SynchronousStepHandler()
    if executor_class_name == multiprocess_executor.__name__:
        return SubprocessStepHandler()

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
    return run_executor._step_handler


class RoutingStepHandler(StepHandler):
    def __init__(
        self,
        init_context: InitExecutorContext,
    ):
        self._init_context = init_context
        self._handler_cache: dict[str, StepHandler] = {}

    @property
    def name(self) -> str:
        return "RoutingStepHandler"

    def _get_step_tags(self, step_handler_context: StepHandlerContext):
        step_key = self._get_step_key(step_handler_context)
        step_tags = step_handler_context.step_tags or {}
        return step_tags.get(step_key)

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

    def _get_handler(
        self, step_handler_context: StepHandlerContext
    ) -> StepHandler:
        # 1. Op-level executor tag
        step_tags = self._get_step_tags(step_handler_context)
        log.debug(f"step_tags: '{step_tags}'")
        execution_config: Optional[ExecutionConfig] = None
        step_tag_config = ExecutionConfig.from_run_tags(step_tags)
        if step_tag_config and step_tag_config.executor:
            execution_config = step_tag_config
            log.debug(f"from step tags: '{step_tag_config.executor}'")

        # 2. Run tags
        if not execution_config:
            run_tags = step_handler_context.dagster_run.tags or {}
            run_tag_config = ExecutionConfig.from_run_tags(run_tags)
            if run_tag_config and run_tag_config.executor:
                execution_config = execution_config or run_tag_config
                log.debug(f"from run tags: '{run_tag_config.executor}'")

        # 3. init_context/Launchpad executor config
        if not execution_config:
            executor_config = ExecutionConfig.from_executor_config(
                self._init_context.executor_config
            )
            if executor_config and executor_config.executor:
                execution_config = execution_config or executor_config
                log.debug(
                    f"from executor config: '{executor_config.executor}'"
                )

        handler_key = execution_config.executor.class_name
        if handler_key not in self._handler_cache:
            self._handler_cache[handler_key] = create_executor_step_handler(
                self._init_context,
                execution_config,
            )
        return self._handler_cache[handler_key]

    def launch_step(self, step_handler_context):
        return self._get_handler(step_handler_context).launch_step(
            step_handler_context
        )

    def check_step_health(self, step_handler_context):
        return self._get_handler(step_handler_context).check_step_health(
            step_handler_context
        )

    def terminate_step(self, step_handler_context):
        return self._get_handler(step_handler_context).terminate_step(
            step_handler_context
        )
