from typing import Optional, cast

import dagster._check as check
import os
from dagster import Field, IntSource, executor
from dagster._annotations import beta
from dagster._core.definitions.executor_definition import (
    multiple_process_executor_requirements,
)
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import StepDelegatingExecutor
from dagster._core.executor.step_delegating.step_handler.base import (
    StepHandlerContext,
)
from dagster._utils.merger import merge_dicts
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.docker_executor import (
    DockerStepHandler, 
)
from dagster_docker.utils import (
    DOCKER_CONFIG_SCHEMA,
    validate_docker_config,
)


@executor(
    name="docker",
    config_schema=merge_dicts(
        DOCKER_CONFIG_SCHEMA,
        {
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
        },
    ),
    requirements=multiple_process_executor_requirements(),
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
    config = init_context.executor_config
    image = check.opt_str_elem(config, "image")
    registry = check.opt_dict_elem(config, "registry", key_type=str)
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    network = check.opt_str_elem(config, "network")
    networks = check.opt_list_elem(config, "networks", of_type=str)
    container_kwargs = check.opt_dict_elem(config, "container_kwargs", key_type=str)
    retries = check.dict_elem(config, "retries", key_type=str)
    max_concurrent = check.opt_int_elem(config, "max_concurrent")
    tag_concurrency_limits = check.opt_list_elem(config, "tag_concurrency_limits")

    validate_docker_config(network, networks, container_kwargs)

    if network and not networks:
        networks = [network]

    env_vars = env_vars or []
    user = os.getenv("DAGSTER_USER")
    env_vars.append(f"DAGSTER_USER={user}")

    container_context = DockerContainerContext(
        registry=registry,
        env_vars=env_vars,
        networks=networks or [],
        container_kwargs=container_kwargs,
    )

    return StepDelegatingExecutor(
        CustomDockerStepHandler(image, container_context),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
    )


class CustomDockerStepHandler(DockerStepHandler):
    def __init__(
        self,
        image: Optional[str],
        container_context: DockerContainerContext,
    ):
        super().__init__(image, container_context)

        self._image = check.opt_str_param(image, "image")
        self._container_context = check.inst_param(
            container_context, "container_context", DockerContainerContext
        )

    def _get_image(self, step_handler_context: StepHandlerContext):
        # get image from step config
        step_key = self._get_step_key(step_handler_context)
        step_context = step_handler_context.get_step_context(step_key)
        image = (
            step_context.run_config
                        .get("ops", {})
                        .get(step_key, {})
                        .get("config", {})
                        .get("image")
        )

        if image:
            return image
        return super()._get_image(step_handler_context)
