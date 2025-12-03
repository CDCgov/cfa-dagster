import dagster._check as check
from dagster import executor
from dagster._annotations import beta
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster_docker.docker_executor import (
    docker_executor as base_docker_executor,
)


@executor(
    name=base_docker_executor.name,
    config_schema=base_docker_executor.config_schema.__dict__,
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
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    env_vars.append("DAGSTER_USER")
    env_vars.append("DAGSTER_IS_DEV_CLI")
    config["env_vars"] = env_vars
    modified_context = init_context._replace(executor_config=config)
    return base_docker_executor.executor_creation_fn(modified_context)
