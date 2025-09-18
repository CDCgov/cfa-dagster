from typing import TYPE_CHECKING, cast

import dagster._check as check
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
from dagster_docker.docker_executor import DockerStepHandler
from dagster_docker.utils import (
    DOCKER_CONFIG_SCHEMA,
    validate_docker_config,
)

if TYPE_CHECKING:
    from dagster._core.origin import JobPythonOrigin


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
    print(f"init_context: '{init_context}'\n")
    config = init_context.executor_config
    print(f"config: '{config}'\n")
    job = init_context.job
    print(f"job: '{job}'\n")
    asset_selection = job.asset_selection
    print(f"asset_selection: '{asset_selection}'\n")
    op_selection = job.op_selection
    print(f"op_selection: '{op_selection}'\n")
    job_def = job.get_definition()
    print(f"job_def: '{job_def}'\n")

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
        CustomDockerStepHandler(image, container_context),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
    )


class CustomDockerStepHandler(DockerStepHandler):
    def _get_image(self, step_handler_context: StepHandlerContext):
        from dagster_docker import DockerRunLauncher

        image = cast(
            "JobPythonOrigin", step_handler_context.dagster_run.job_code_origin
        ).repository_origin.container_image
        if not image:
            image = self._image

        run_launcher = step_handler_context.instance.run_launcher

        if not image and isinstance(run_launcher, DockerRunLauncher):
            image = run_launcher.image

        # get image from step config
        step_key = self._get_step_key(step_handler_context)
        step_context = step_handler_context.get_step_context(step_key)
        print(f"step_context: '{step_context}'\n")
        plan_data = step_context.plan_data
        print(f"plan_data: '{plan_data}'\n")
        execution_plan = step_context.execution_plan
        print(f"execution_plan: '{execution_plan}'\n")
        run_config = step_context.run_config
        print(f"run_config: '{run_config}'\n")

        step_config = run_config["ops"].get(step_key).get("config")
        print(f"step_config: '{step_config}'\n")
        step_image = step_config.get("image")
        print(f"step_image: '{step_image}'\n")

        image = step_image

        if not image:
            raise Exception(
                "No docker image specified by the executor config or repository"
            )

        return image
