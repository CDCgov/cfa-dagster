import os
import yaml
# from dagster._core.host_representation.origin import GrpcServerRepositoryLocationOrigin
# from dagster._api.snapshot_repository import sync_get_external_repositories_data_grpc
# from dagster._grpc.client import DagsterGrpcClient
import json
from collections.abc import Mapping
from typing import Any, Optional

import dagster._check as check
import docker
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster import (
    DefaultRunLauncher,
    JsonMetadataValue
)
from dagster_docker import DockerRunLauncher
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.storage.tags import DOCKER_IMAGE_TAG
from dagster._core.utils import parse_env_var
from dagster._grpc.types import ExecuteRunArgs, ResumeRunArgs
from dagster._serdes import ConfigurableClass
from dagster._serdes.config_class import ConfigurableClassData
from typing_extensions import Self

from dagster_docker.container_context import DockerContainerContext
from dagster_docker.utils import DOCKER_CONFIG_SCHEMA, validate_docker_config, validate_docker_image


class DynamicRunLauncher(RunLauncher, ConfigurableClass):
    """Launches a run using a runtime-configurable launcher"""

    def __init__(
        self,
        inst_data: Optional[ConfigurableClassData] = None
    ):
        self._inst_data = inst_data

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> Self:
        return cls(inst_data=inst_data, **config_value)

    def get_location_metadata(self, run: DagsterRun, workspace):
        """
            Gets metadata from the Definitions for a code location
        """
        code_location_names = workspace.code_location_names
        print(f"code_location_names: '{code_location_names}'")
        location_name = code_location_names[0]

        print(f"location_name: '{location_name}'")

        # THIS IS THE KEY LINE
        code_location = workspace.get_code_location(location_name)
        print(f"code_location: '{code_location}'")

        # Now you can get the repository
        repo_names = code_location.get_repository_names()
        print(f"repo_names: '{repo_names}'")
        repo_name = repo_names[0]
        print(f"repo_name: '{repo_name}'")
        repo = code_location.get_repository(repo_name)
        print(f"repo: '{repo}'")
        # display_metadata: '{'host': 'localhost', 'socket': '/tmp/tmpy8unnusp', 'python_file': 'dagster_defs.py', 'working_directory': '/app'}'
        metadata = repo.repository_snap.metadata
        print(f"metadata: '{metadata}'")
        return metadata

    def get_launcher(self, run: DagsterRun, workspace) -> tuple[str, RunLauncher]:
        metadata = self.get_location_metadata(run, workspace)
        cfa_dagster_metadata = metadata.get(
            "cfa-dagster",
            JsonMetadataValue({})
        ).value
        is_production = os.getenv("DAGSTER_IS_DEV_CLI", "false") == "false"
        run_launcher = cfa_dagster_metadata.get("runLauncher")
        run_launcher_config = cfa_dagster_metadata.get("config")
        # TODO: ensure only CAJ launcher can be used in production
        if not run_launcher:
            if is_production:
                return DockerRunLauncher()
            else:
                return DefaultRunLauncher()
        match run_launcher:
            case "docker":
                inst_data = ConfigurableClassData(
                    module_name="dagster_docker",
                    class_name="DockerRunLauncher",
                    config_yaml=yaml.dump(run_launcher_config)
                )
                return (
                    "docker",
                    DockerRunLauncher(
                        inst_data=inst_data,
                        **run_launcher_config
                    )
                )
            case "_":
                return ("default", DefaultRunLauncher())

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        name, launcher = self.get_launcher(run, context.workspace)

        self._instance.report_engine_event(
            message=f"Launching run using {name} launcher",
            dagster_run=run,
            cls=launcher.__class__,
        )

        self._instance.add_run_tags(
            run.run_id,
            {"RUN_LAUNCHER_TYPE": name, "RUN_LAUNCHER_INST_DATA": launcher.inst_data},  # pyright: ignore[reportArgumentType]
        )
        launcher.launch_run(context)

    @property
    def supports_resume_run(self):
        return True

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        name, launcher = self.get_launcher(run, context.workspace)
        launcher.resume_run(context)

    @property
    def supports_check_run_worker_health(self):
        return False

    # TODO: implement tagging system so we can determine the run launcher
    # from the tags
    def check_run_worker_health(self, run: DagsterRun):
        pass

    def terminate(self, run_id):
        pass
