import os
from collections.abc import Mapping
from typing import Any, Optional

import yaml
from dagster import DefaultRunLauncher, JsonMetadataValue
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    WorkerStatus,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._serdes import (
    ConfigurableClass,
    deserialize_value,
    serialize_value,
)
from dagster._serdes.config_class import ConfigurableClassData
from dagster_docker import DockerRunLauncher
from typing_extensions import Self

from cfa_dagster.azure_container_app_job.launcher import (
    AzureContainerAppJobRunLauncher,
)

LAUNCHER_CONFIG_KEY = "cfa_dagster/launcher"


class DynamicRunLauncher(RunLauncher, ConfigurableClass):
    """Launches a run using a runtime-configurable launcher"""

    def __init__(self, inst_data: Optional[ConfigurableClassData] = None):
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

    def get_location_metadata(self, workspace: BaseWorkspaceRequestContext):
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

    def patch_env_vars(self, env_vars: list[str]):
        # patch env vars
        clean_env_vars = []
        reserved_env_vars = ["DAGSTER_USER", "DAGSTER_IS_DEV_CLI"]
        for var in env_vars:
            for reserved_var in reserved_env_vars:
                if not var.startswith(f"{reserved_var}="):
                    clean_env_vars.append(var)

        user = os.getenv("DAGSTER_USER")
        dagster_is_dev_cli = os.getenv("DAGSTER_IS_DEV_CLI")
        clean_env_vars.append(f"DAGSTER_USER={user}")
        if dagster_is_dev_cli:
            clean_env_vars.append(f"DAGSTER_IS_DEV_CLI={dagster_is_dev_cli}")
        return clean_env_vars

    def create_launcher(
        self, workspace: BaseWorkspaceRequestContext
    ) -> RunLauncher:
        metadata = self.get_location_metadata(workspace)
        launcher_metadata = metadata.get(
            LAUNCHER_CONFIG_KEY, JsonMetadataValue({})
        ).value
        is_production = not os.getenv("DAGSTER_IS_DEV_CLI")
        launcher_type = launcher_metadata.get("type")
        launcher_config = launcher_metadata.get("config", {})

        launcher_config["env_vars"] = self.patch_env_vars(
            launcher_config.get("env_vars", [])
        )

        run_launcher = None
        match (is_production, launcher_type):
            case (False, None) | (False, "default"):
                inst_data = ConfigurableClassData(
                    module_name="dagster",
                    class_name="DefaultRunLauncher",
                    config_yaml=yaml.dump({}),
                )
                run_launcher = DefaultRunLauncher(inst_data)
            case (False, "docker"):
                inst_data = ConfigurableClassData(
                    module_name="dagster_docker",
                    class_name="DockerRunLauncher",
                    config_yaml=yaml.dump(launcher_config),
                )
                run_launcher = DockerRunLauncher(
                    inst_data=inst_data, **launcher_config
                )
            case (True, None) | (_, "azure_container_app_job_launcher"):
                inst_data = ConfigurableClassData(
                    module_name="cfa_dagster.azure_container_app_job.launcher",
                    class_name="AzureContainerAppJobRunLauncher",
                    config_yaml=yaml.dump(launcher_config),
                )
                run_launcher = AzureContainerAppJobRunLauncher(
                    inst_data=inst_data, **launcher_config
                )
            case _:
                raise RuntimeError(
                    "Only the azure_container_app_job_launcher is "
                    "supported in production!"
                )

        run_launcher.register_instance(self._instance)
        return run_launcher

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        launcher = self.create_launcher(context.workspace)

        self._instance.report_engine_event(
            message=f"Launching run using {launcher.__class__.__name__}",
            dagster_run=run,
            cls=self.__class__,
        )

        self._instance.add_run_tags(
            run.run_id,
            {LAUNCHER_CONFIG_KEY: f"{serialize_value(launcher.inst_data)}"},  # pyright: ignore[reportArgumentType]
        )
        launcher.launch_run(context)

    @property
    def supports_resume_run(self):
        return True

    def get_launcher(self, run: DagsterRun) -> RunLauncher:
        serialized_inst_data = run.tags.get(LAUNCHER_CONFIG_KEY)
        inst_data: ConfigurableClassData = deserialize_value(
            serialized_inst_data, ConfigurableClassData
        )
        run_launcher: RunLauncher = inst_data.rehydrate(RunLauncher)
        run_launcher.register_instance(self._instance)

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        run_launcher = self.get_launcher(run)
        run_launcher.resume_run(context)

    @property
    def supports_check_run_worker_health(self):
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        run_launcher = self.get_launcher(run)
        if not run_launcher.supports_check_run_worker_health:
            # Assume running if run worker doesn't support health check
            # This should only be for the DefaultRunLauncher
            return CheckRunHealthResult(WorkerStatus.RUNNING)
        return run_launcher.check_run_worker_health(run)

    def terminate(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        run_launcher = self.get_launcher(run)
        return run_launcher.terminate(run_id)
