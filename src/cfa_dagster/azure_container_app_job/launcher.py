import logging
from collections.abc import Mapping
from typing import Any, Optional

import dagster._check as check
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.resource.subscriptions import SubscriptionClient
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from dagster._core.utils import parse_env_var
from dagster._grpc.types import ExecuteRunArgs, ResumeRunArgs
from dagster._serdes import ConfigurableClass
from dagster._serdes.config_class import ConfigurableClassData
from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import (
    validate_docker_config,
    validate_docker_image,
)
from typing_extensions import Self

from .utils import CAJ_CONFIG_SCHEMA, get_status_caj, start_caj, stop_caj

log = logging.getLogger(__name__)

DOCKER_CONTAINER_ID_TAG = "docker/container_id"
CAJ_EXECUTION_ID_KEY = "cfa_dagster/caj_execution_id"


class AzureContainerAppJobRunLauncher(RunLauncher, ConfigurableClass):
    """Launches runs in an Azure Container App Job."""

    def __init__(
        self,
        inst_data: Optional[ConfigurableClassData] = None,
        container_app_job_name="cfa-dagster",
        cpu: float = None,
        memory: float = None,
        image: str = None,
        registry: str = None,
        env_vars: list[str] = None,
        network: str = None,
        networks: list[str] = None,
        container_kwargs=None,
        **kwargs,
    ):
        self._inst_data = inst_data
        self.image = image
        self.container_app_job_name = container_app_job_name
        self.cpu = cpu
        self.memory = memory
        self.registry = registry
        self.env_vars = env_vars

        validate_docker_config(network, networks, container_kwargs)

        if network:
            self.networks = [network]
        elif networks:
            self.networks = networks
        else:
            self.networks = []

        self.container_kwargs = check.opt_dict_param(
            container_kwargs, "container_kwargs", key_type=str
        )

        self._resource_group = "ext-edav-cfa-prd"  # TODO: move to config?
        credential = DefaultAzureCredential()

        # Get first subscription for logged-in credential
        first_subscription_id = (
            SubscriptionClient(credential)
            .subscriptions.list()
            .next()
            .subscription_id
        )

        self._azure_caj_client = ContainerAppsAPIClient(
            credential=credential, subscription_id=first_subscription_id
        )

        super().__init__()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return CAJ_CONFIG_SCHEMA

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> Self:
        return cls(inst_data=inst_data, **config_value)

    def get_container_context(
        self, dagster_run: DagsterRun
    ) -> DockerContainerContext:
        return DockerContainerContext.create_for_run(dagster_run, self)

    def _get_image(self, job_code_origin):
        docker_image = job_code_origin.repository_origin.container_image

        if not docker_image:
            docker_image = self.image

        if not docker_image:
            raise Exception(
                "No docker image specified by the instance config or repository"
            )

        validate_docker_image(docker_image)
        return docker_image

    def _launch_container_with_command(self, run, docker_image, command):
        container_context = self.get_container_context(run)
        env_vars = dict(
            [parse_env_var(env_var) for env_var in container_context.env_vars]
        )
        env_vars["DAGSTER_RUN_JOB_NAME"] = run.job_name

        job_execution_id = start_caj(
            self._azure_caj_client,
            resource_group=self._resource_group,
            container_app_job_name=self.container_app_job_name,
            image=docker_image,
            env_vars=env_vars,
            command=command,
            cpu=self.cpu,
            memory=self.memory,
        )

        self._instance.report_engine_event(
            message=(
                "Launching run in a new container app job execution "
                f"{job_execution_id} with image {docker_image}"
            ),
            dagster_run=run,
            cls=self.__class__,
        )

        self._instance.add_run_tags(
            run.run_id,
            {
                CAJ_EXECUTION_ID_KEY: job_execution_id,
                DOCKER_IMAGE_TAG: docker_image,
            },  # pyright: ignore[reportArgumentType]
        )

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        job_code_origin = check.not_none(context.job_code_origin)
        docker_image = self._get_image(job_code_origin)

        command = ExecuteRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()

        self._launch_container_with_command(run, docker_image, command)

    @property
    def supports_resume_run(self):
        return True

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        job_code_origin = check.not_none(context.job_code_origin)
        docker_image = self._get_image(job_code_origin)

        command = ResumeRunArgs(
            job_origin=job_code_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
        ).get_command_args()

        self._launch_container_with_command(run, docker_image, command)

    def terminate(self, run_id):
        run = self._instance.get_run_by_id(run_id)

        if not run or run.is_finished:
            return False

        self._instance.report_run_canceling(run)

        job_execution_id = run.tags.get(CAJ_EXECUTION_ID_KEY)
        return stop_caj(
            self._azure_caj_client,
            self._resource_group,
            self.container_app_job_name,
            job_execution_id,
        )

    @property
    def supports_check_run_worker_health(self):
        return True

    def check_run_worker_health(self, run: DagsterRun):
        job_execution_id = run.tags.get(CAJ_EXECUTION_ID_KEY)
        if not job_execution_id:
            return CheckRunHealthResult(
                WorkerStatus.NOT_FOUND,
                msg=f"No tag found for {CAJ_EXECUTION_ID_KEY}!",
            )

        status = get_status_caj(
            self._azure_caj_client,
            resource_group=self._resource_group,
            container_app_job_name=self.container_app_job_name,
            job_execution_id=job_execution_id,
        )

        match status:
            case None:
                return CheckRunHealthResult(
                    WorkerStatus.NOT_FOUND,
                    msg=f"No container app job execution found for {job_execution_id}",
                )
            case "Running" | "Processing" | "Unknown":
                return CheckRunHealthResult(WorkerStatus.RUNNING)
            case _:
                return CheckRunHealthResult(
                    WorkerStatus.FAILED,
                    msg=f"Container app execution {job_execution_id} in status {status}!",
                )
        return CheckRunHealthResult(
            WorkerStatus.FAILED, msg="Unknown failure!"
        )
