import hashlib
import logging
import os
import re
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional, cast

import dagster._check as check
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
)
from azure.mgmt.msi import ManagedServiceIdentityClient
from azure.mgmt.subscription import SubscriptionClient
from dagster import Field, Float, Int, StringSource, executor
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
from dagster._utils.merger import merge_dicts
from dagster_docker import docker_executor as base_docker_executor
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    validate_docker_config,
    validate_docker_image,
)

from cfa_dagster.utils import require_dagster_user

from ..utils import get_run_timestamp

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dagster._core.origin import JobPythonOrigin

# Notes:
# We can ACPI standby pools for faster startup times but for the first iteration I will not
# Permissive() is a dagster config that allows open (not closed) schema definition
# One dagster step = One Container Group
# Dagster run ID + step key + retry number → ACPI container-group name
# _get_job_id(), _get_or_create_job(), and _get_task_id() will collapse into container-group naming function
# Turn ACPI restart policy to Never b/c dagster handles this already?


@executor(
    name="azure_container_instance_executor",
    config_schema=merge_dicts(
        base_docker_executor.config_schema.config_type.fields,
        {
            "cpu": Field(
                Float,
                is_required=False,
                default_value=1.0,
                description="Number of CPU cores requested for the ACPI container.",
            ),
            "memory": Field(
                Float,
                is_required=False,
                default_value=2.0,
                description="Memory requested for the ACPI container, in GB.",
            ),
            "max_concurrent": Field(
                Int,
                is_required=False,
                default_value=1,
                description="Maximum number of ACPI step containers running concurrently.s",
            ),
        },
    ),
    requirements=multiple_process_executor_requirements(),
)
def azure_container_instance_executor(
    init_context: InitExecutorContext,
) -> Executor:
    """Executor which launches steps as Azure Container Instance.

    To use the `azure_container_instance_executor`, set it as the `executor_def` when defining a job:

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
            image: ...
            env_vars: ...
            container_kwargs: ...

    """
    config = init_context.executor_config
    image = check.opt_str_elem(config, "image")
    registry = check.opt_dict_elem(config, "registry", key_type=str)
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    network = check.opt_str_elem(config, "network")
    networks = check.opt_list_elem(config, "networks", of_type=str)
    container_kwargs = check.opt_dict_elem(
        config, "container_kwargs", key_type=str
    )
    cpu = check.float_elem(config, "cpu")
    memory = check.float_elem(config, "memory")
    retries = check.dict_elem(config, "retries", key_type=str)
    max_concurrent = check.opt_int_elem(config, "max_concurrent")
    tag_concurrency_limits = check.opt_list_elem(
        config, "tag_concurrency_limits"
    )

    if max_concurrent is not None and max_concurrent > 5:
        raise ValueError("max_concurrent must be 5 or fewer")

    # propagate user & dev env vars
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
        AzureContainerInstanceStepHandler(
            image, container_context, cpu, memory
        ),
        retries=check.not_none(RetryMode.from_config(retries)),
        max_concurrent=max_concurrent,
        tag_concurrency_limits=tag_concurrency_limits,
    )


class AzureContainerInstanceStepHandler(StepHandler):
    def __init__(
        self,
        image: Optional[str],
        identity_name: Optional[str],
        container_context: DockerContainerContext,
        cpu: float,
        memory: float,
    ):
        super().__init__()

        credential = DefaultAzureCredential()

        self._subscription_id = (
            SubscriptionClient(credential)
            .subscriptions.list()
            .next()
            .subscription_id
        )

        self._location = "eastus"
        self._resource_group = "ext-edav-cfa-prd"

        self._azure_client = ContainerInstanceManagementClient(
            credential=credential,
            subscription_id=self._subscription_id,
        )

        self._identity = None
        if identity_name:
            self._identity = self._load_identity_by_name(
                credential,
                identity_name,
            )

        self._image = check.opt_str_param(image, "image")
        self._cpu = cpu
        self._memory = memory
        self._container_context = check.inst_param(
            container_context,
            "container_context",
            DockerContainerContext,
        )

    def _get_image(self, step_handler_context: StepHandlerContext):
        step_key = self._get_step_key(step_handler_context)
        step_context = step_handler_context.get_step_context(step_key)
        image = (
            step_context.run_config.get("ops", {})
            .get(step_key, {})
            .get("config", {})
            .get("image")
        )
        log.info("Resolved image: %s", image)
        if not image:
            image = self._image

        if not image:
            image = cast(
                "JobPythonOrigin",
                step_handler_context.dagster_run.job_code_origin,
            ).repository_origin.container_image

        if not image:
            raise Exception(
                "No docker image specified by the executor, run config, or code location"
            )

        return image

    def _load_identity_by_name(
        self,
        credential,
        identity_name: str,
    ):
        client = ManagedServiceIdentityClient(
            credential,
            self._subscription_id,
        )

        identities = list(
            client.user_assigned_identities.list_by_subscription()
        )

        matches = [
            identity
            for identity in identities
            if identity.name == identity_name
        ]

        if not matches:
            raise RuntimeError(
                f"Managed identity '{identity_name}' not found."
            )

        if len(matches) > 1:
            raise RuntimeError(
                f"Multiple managed identities named '{identity_name}' found."
            )

        return matches[0]

    def _get_docker_container_context(
        self, step_handler_context: StepHandlerContext
    ):
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
        return "AzureContainerInstanceStepHandler"

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

    def _get_container_group_id(
        self, step_handler_context: StepHandlerContext
    ):
        """
        Creates a unique container group id for Azure Container instance

        The container group id is a uuidv5 generated based on the DAGSTER_USER env
        variable, the code location name, and the hour the run was created.

        This ensures tasks are logically grouped into jobs without running into
        the max active job limit imposed by Batch. Since the job id is scoped to
        the run creation hour, jobs without active tasks can safely be cleaned up by a
        background process.
        """
        run = step_handler_context.dagster_run

        if run.remote_job_origin is not None:
            location_name = run.remote_job_origin.repository_origin.code_location_origin.location_name
        else:
            location_name = run.job_name
        dagster_user = require_dagster_user()

        run_creation_hour = None
        try:
            run_creation_hour = get_run_timestamp(run).strftime("%Y-%m-%dT%H")
        except (ValueError, KeyError):
            log.debug(
                "Failed to capture run_creation_hour from tags, falling back to run_record"
            )
        if not run_creation_hour:
            run_record = step_handler_context.instance.get_run_record_by_id(
                run.run_id
            )
            if not run_record:
                raise RuntimeError(f"No run record for run id: {run.run_id}")
            run_creation_hour = run_record.create_timestamp.strftime(
                "%Y-%m-%dT%H"
            )

        log.debug(f"dagster_user: '{dagster_user}'")
        log.debug(f"location_name: '{location_name}'")
        log.debug(f"run_creation_hour: '{run_creation_hour}'")
        base_id = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            ":".join(
                [
                    dagster_user,
                    location_name,
                    run_creation_hour,
                ]
            ),
        )

        return f"dagster-{base_id}"

    # build the object but don't hit Azure API yet -> return container object
    def _build_container_group(
        self,
        step_handler_context,
    ):
        container = Container(
            name=self._get_container_group_id(step_handler_context),
            image=self._get_image(step_handler_context),
            resources=ResourceRequirements(
                requests=ResourceRequests(
                    memory_in_gb=self._memory, cpu=self._cpu
                )
            ),
        )

        container_group_params = ContainerGroup(
            location=self._location,
            containers=[container],
            os_type=OperatingSystemTypes.LINUX,
        )

        return container_group_params

    def _clamp_with_hash(self, value: str, max_len: int) -> str:
        if len(value) <= max_len:
            return value

        # Reserve 6 chars for hash + "-"
        hash_len = 6
        keep_len = max_len - hash_len - 1
        digest = hashlib.sha1(value.encode()).hexdigest()[:hash_len]
        return f"{value[:keep_len]}-{digest}"

    def launch_step(
        self,
        step_handler_context: StepHandlerContext,
    ) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        container_group_name = self._get_container_group_id(
            step_handler_context
        )
        container_group = self._build_container_group(step_handler_context)

        self._azure_client.container_groups.begin_create_or_update(
            resource_group_name=self._resource_group,
            container_group_name=container_group_name,
            container_group=container_group,
        )

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=(
                f"Launching step {step_key!r} in Azure Container Instance "
                f"group {container_group_name!r}."
            ),
            metadata={
                "Azure Container Group": container_group_name,
                "Azure Resource Group": self._resource_group,
                "Azure Location": self._location,
            },
        )

    def check_step_health(
        self,
        step_handler_context: StepHandlerContext,
    ) -> CheckStepHealthResult:
        container_group_name = self._get_container_group_id(
            step_handler_context
        )

        try:
            container_group = self._azure_client.container_groups.get(
                resource_group_name=self._resource_group,
                container_group_name=container_group_name,
            )
        except ResourceNotFoundError:
            return CheckStepHealthResult.unhealthy(
                reason=(
                    f"Azure Container Instance group "
                    f"{container_group_name!r} was not found."
                )
            )
        except HttpResponseError as error:
            return CheckStepHealthResult.unhealthy(
                reason=(
                    f"Unable to inspect Azure Container Instance group "
                    f"{container_group_name!r}: {error}"
                )
            )

        if not container_group.containers:
            return CheckStepHealthResult.unhealthy(
                reason=(
                    f"Azure Container Instance group "
                    f"{container_group_name!r} contains no containers."
                )
            )

        container = container_group.containers[0]
        instance_view = container.instance_view

        if instance_view is None or instance_view.current_state is None:
            # Azure may not have populated runtime state immediately after creation.
            return CheckStepHealthResult.healthy()

        current_state = instance_view.current_state
        state = current_state.state
        exit_code = current_state.exit_code
        detail_status = current_state.detail_status

        if state in ("Waiting", "Running"):
            return CheckStepHealthResult.healthy()

        if state == "Terminated":
            if exit_code == 0:
                return CheckStepHealthResult.healthy()

            return CheckStepHealthResult.unhealthy(
                reason=(
                    f"Azure Container Instance group "
                    f"{container_group_name!r} terminated unsuccessfully. "
                    f"Exit code: {exit_code}. "
                    f"Detail: {detail_status or 'No detail supplied.'}"
                )
            )

        return CheckStepHealthResult.unhealthy(
            reason=(
                f"Azure Container Instance group "
                f"{container_group_name!r} has unexpected container state "
                f"{state!r}. Detail: {detail_status or 'No detail supplied.'}"
            )
        )

    def terminate_step(
        self,
        step_handler_context: StepHandlerContext,
    ) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)
        container_group_name = self._get_container_group_id(
            step_handler_context
        )

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=(
                f"Deleting Azure Container Instance group "
                f"{container_group_name!r} for step {step_key!r}."
            ),
            event_specific_data=EngineEventData(),
        )

        try:
            self._azure_client.container_groups.stop(
                resource_group_name=self._resource_group,
                container_group_name=container_group_name,
            )
        except ResourceNotFoundError:
            log.info(
                "Azure Container Instance group %r was already stopped.",
                container_group_name,
            )
        except HttpResponseError:
            log.exception(
                "Failed to stops Azure Container Instance group %r.",
                container_group_name,
            )
            raise
