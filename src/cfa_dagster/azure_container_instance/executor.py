import hashlib
import logging
import os
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional, cast

import dagster._check as check
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupIdentity,
    UserAssignedIdentities,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
)
from azure.mgmt.msi import ManagedServiceIdentityClient
from azure.mgmt.subscription import SubscriptionClient
from dagster import Field, Float, Int, String, executor
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
from dagster._utils.merger import merge_dicts
from dagster_docker import docker_executor as base_docker_executor
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    validate_docker_config,
)

from cfa_dagster.utils import require_dagster_user

from ..utils import get_run_timestamp

log = logging.getLogger(__name__)

azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.DEBUG)

if not azure_logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    )
    azure_logger.addHandler(handler)

if TYPE_CHECKING:
    from dagster._core.origin import JobPythonOrigin

# Notes:
# We can ACI standby pools for faster startup times but for the first iteration I will not
# One dagsters step = One Container Group
# Dagster run ID + step key + retry number → ACI container-group name
# Turn ACI restart policy to Never b/c dagster handles this already?


@executor(
    name="azure_container_instance_executor",
    config_schema=merge_dicts(
        base_docker_executor.config_schema.config_type.fields,
        {
            "cpu": Field(
                Float,
                is_required=False,
                default_value=1.0,
                description="Number of CPU cores requested for the ACI container.",
            ),
            "memory": Field(
                Float,
                is_required=False,
                default_value=2.0,
                description="Memory requested for the ACI container, in GB.",
            ),
            "max_concurrent": Field(
                Int,
                is_required=False,
                default_value=1,
                description="Maximum number of ACI step containers running concurrently.",
            ),
            "identity_name": Field(
                String,
                is_required=True,
                description=(
                    "Name of the user-assigned managed identity "
                    "to attach to the ACI container group."
                ),
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
            executor_def=azure_container_instance_executor,
            ..
        )

    Then you can configure the executor with run config as follows:

    .. code-block:: YAML

        execution:
          config:
            image: ...
            env_vars: ...

    """
    config = init_context.executor_config
    image = check.opt_str_elem(config, "image")
    registry = check.opt_dict_elem(config, "registry", key_type=str)
    env_vars = check.opt_list_elem(config, "env_vars", of_type=str)
    network = check.opt_str_elem(config, "network")
    networks = check.opt_list_elem(config, "networks", of_type=str)
    cpu = check.float_elem(config, "cpu")
    memory = check.float_elem(config, "memory")
    identity_name = check.opt_str_elem(config, "identity_name")
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

    if network and not networks:
        networks = [network]

    container_context = DockerContainerContext(
        registry=registry,
        env_vars=env_vars,
        networks=networks or [],
    )

    return StepDelegatingExecutor(
        AzureContainerInstanceStepHandler(
            image, identity_name, container_context, cpu, memory
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

        credential = DefaultAzureCredential(logging_enable=True)

        self._subscription_id = (
            SubscriptionClient(
                credential,
                logging_enable=True,
            )
            .subscriptions.list()
            .next()
            .subscription_id
        )

        self._azure_client = ContainerInstanceManagementClient(
            credential=credential,
            subscription_id=self._subscription_id,
            logging_enable=True,
        )
        self._identity = None
        self._container_group_identity = None
        self._image_registry_credentials = None

        self._location = "eastus"
        self._resource_group = "ext-edav-cfa-prd"

        if identity_name:
            self._identity = self._load_identity_by_name(credential, identity_name)

            identity_id = self._identity.id
            
            if not identity_id:
               raise RuntimeError(
                   f"Managed identity {self._identity.name!r} has no resource ID."
               )

            self._container_group_identity = ContainerGroupIdentity(
                type="UserAssigned",
                user_assigned_identities={
                    identity_id: UserAssignedIdentities()
                },
            )

            self._image_registry_credentials = [
                ImageRegistryCredential(
                    server="cfaprdbatchcr.azurecr.io",
                    identity=identity_id,
                )
            ]

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
            logging_enable=True,
        )      

        # TODO: MPW -> add query filter here if possible
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

        The container group id is a hash generated based on the DAGSTER_USER, 
        and step_key
        variable, the code location name, and the hour the run was created.

        This ensures tasks don't get the same full_id.
        """
        run = step_handler_context.dagster_run
        step_key = self._get_step_key(step_handler_context)
        dagster_user = require_dagster_user()

        readable_name = f"{dagster_user}-{step_key}".lower()
        readable_name = "".join(
            char if char.isalnum() else "-"
            for char in readable_name
        ).strip("-")

        # Create unique hash from unique run_id / step_key (take only first 10 of hash)
        unique_value = f"{run.run_id}:{step_key}"
        unique_hash = hashlib.sha1(unique_value.encode()).hexdigest()[:10]

        full_id = f"dagster-aci-{readable_name}-{unique_hash}"
        full_id = self._clamp_with_hash(full_id, max_len=63)

        log.debug("ACI container group ID: %r", full_id)

        return full_id

    # build the object but don't hit Azure API yet -> return container object
    def _build_container_group(
        self,
        step_handler_context,
    ):
        execute_step_args = step_handler_context.execute_step_args
        container = Container(
            name=self._get_container_group_id(step_handler_context),
            image=self._get_image(step_handler_context),
            resources=ResourceRequirements(
                requests=ResourceRequests(
                    memory_in_gb=self._memory, cpu=self._cpu
                )
            ),
            command = execute_step_args.get_command_args()
        )

        container_group_params = ContainerGroup(
            location=self._location,
            containers=[container],
            os_type=OperatingSystemTypes.LINUX,
            identity=self._container_group_identity,
            image_registry_credentials=self._image_registry_credentials,
            restart_policy="Never"
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
            logging_enable=True
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
