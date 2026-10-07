import os

import dagster._check as check
from dagster import executor
from dagster._annotations import beta
from dagster._core.execution.retries import RetryMode
from dagster._core.execution.step_dependency_config import StepDependencyConfig
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import StepDelegatingExecutor
from dagster._core.utils import parse_env_var
from dagster._utils.merger import merge_dicts
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.docker_executor import (
    DockerStepHandler,
)
from dagster_docker.docker_executor import (
    docker_executor as base_docker_executor,
)
from dagster_docker.utils import validate_docker_config

from cfa_dagster.profiling import (
    PROFILER_SOURCE_ENV,
    PROFILER_SOURCE_PSUTIL,
    PROFILING_CONFIG_SCHEMA,
    ProfilingConfig,
    get_profile_asset_observation_env,
    wrap_command_for_profiling,
)
from cfa_dagster.utils import require_dagster_user


class ProfiledDockerStepHandler(DockerStepHandler):
    def __init__(
        self,
        image: str | None,
        container_context: DockerContainerContext,
        profiling: ProfilingConfig,
    ):
        super().__init__(image, container_context)
        self._profiling = profiling

    def _create_step_container(
        self,
        client,
        container_context,
        step_image,
        step_handler_context,
    ):
        execute_step_args = step_handler_context.execute_step_args
        step_keys_to_execute = check.not_none(
            execute_step_args.step_keys_to_execute
        )
        assert len(step_keys_to_execute) == 1, (
            "Launching multiple steps is not currently supported"
        )
        step_key = step_keys_to_execute[0]

        container_kwargs = {**container_context.container_kwargs}
        container_kwargs.pop("stop_timeout", None)

        env_vars = dict(
            [parse_env_var(env_var) for env_var in container_context.env_vars]
        )
        env_vars["DAGSTER_RUN_JOB_NAME"] = (
            step_handler_context.dagster_run.job_name
        )
        env_vars["DAGSTER_RUN_ID"] = step_handler_context.dagster_run.run_id
        env_vars["DAGSTER_RUN_STEP_KEY"] = step_key
        env_vars[PROFILER_SOURCE_ENV] = PROFILER_SOURCE_PSUTIL
        asset_observation_env = get_profile_asset_observation_env(
            step_handler_context.get_step_context(step_key)
        )
        env_vars.update(asset_observation_env)

        command = wrap_command_for_profiling(
            execute_step_args.get_command_args(),
            self._profiling,
            track_step_status=bool(asset_observation_env),
        )

        return client.containers.create(
            step_image,
            name=self._get_container_name(step_handler_context),
            detach=True,
            network=container_context.networks[0]
            if len(container_context.networks)
            else None,
            command=command,
            environment=env_vars,
            **container_kwargs,
        )


@executor(
    name=base_docker_executor.name,
    config_schema=merge_dicts(
        base_docker_executor.config_schema.config_type.fields,
        PROFILING_CONFIG_SCHEMA,
    ),
    requirements=base_docker_executor._requirements_fn,
)
@beta
def docker_executor(init_context: InitExecutorContext) -> Executor:
    """Executor which launches steps as Docker containers.

    To use the `docker_executor`, set it as the `executor_def` when defining a job:

    .. literalinclude:: ../../../../../../python_modules/libraries/dagster-docker/dagster_docker_tests/test_example_executor.py
       :start-after: start_marker
       :end-before: end_marker
       :language: python

    Then you can configure the executor with run config as follows:

    .. code-block:: YAML

        execution:
          config:
            registry: ...
            network: ...
            networks: ...
            container_kwargs: ...

    If you're using the DockerRunLauncher, configuration set on the containers created by the run
    launcher will also be set on the containers that are created for each step.
    """
    config = dict(init_context.executor_config or {})
    profiling = ProfilingConfig.from_config(config)
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    require_dagster_user()
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
    config["env_vars"] = env_vars

    image = check.opt_str_elem(config, "image")
    registry = check.opt_dict_elem(config, "registry", key_type=str)
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

    return StepDelegatingExecutor(
        ProfiledDockerStepHandler(image, container_context, profiling),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
        step_dependency_config=StepDependencyConfig.from_config(
            config.get("step_dependency_config")
        ),
    )
