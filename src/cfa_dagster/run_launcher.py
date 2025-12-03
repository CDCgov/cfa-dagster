import logging
import os
from collections.abc import Mapping
from typing import Any, Optional

import yaml
from dagster import DefaultRunLauncher, JsonMetadataValue
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
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

log = logging.getLogger(__name__)

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
        log.debug(f"code_location_names: '{code_location_names}'")
        location_name = code_location_names[0]

        log.debug(f"location_name: '{location_name}'")

        # THIS IS THE KEY LINE
        code_location = workspace.get_code_location(location_name)
        log.debug(f"code_location: '{code_location}'")

        # Now you can get the repository
        repo_names = code_location.get_repository_names()
        log.debug(f"repo_names: '{repo_names}'")
        repo_name = repo_names[0]
        log.debug(f"repo_name: '{repo_name}'")
        repo = code_location.get_repository(repo_name)
        log.debug(f"repo: '{repo}'")
        # display_metadata: '{'host': 'localhost', 'socket': '/tmp/tmpy8unnusp', 'python_file': 'dagster_defs.py', 'working_directory': '/app'}'
        metadata = repo.repository_snap.metadata
        log.debug(f"metadata: '{metadata}'")
        return metadata

    def create_launcher(
        self, workspace: BaseWorkspaceRequestContext
    ) -> RunLauncher:
        metadata = self.get_location_metadata(workspace)
        launcher_metadata = metadata.get(
            LAUNCHER_CONFIG_KEY, JsonMetadataValue({})
        ).value
        is_production = not os.getenv("DAGSTER_IS_DEV_CLI")
        launcher_class_name = launcher_metadata.get("class")

        match (is_production, launcher_class_name):
            case (False, None):
                launcher_class_name = DefaultRunLauncher.__name__
            case (True, None):
                launcher_class_name = AzureContainerAppJobRunLauncher.__name__
            case (True, DockerRunLauncher.__name__):
                raise RuntimeError(
                    f"You can't use {launcher_class_name} in production!"
                )

        launcher_class = globals()[launcher_class_name]
        launcher_module = launcher_class.__module__

        launcher_config = launcher_metadata.get("config", {})

        # default run launcher throws an error for env vars
        if launcher_class_name != DefaultRunLauncher.__name__:
            env_vars = launcher_config.get("env_vars", [])
            env_vars.append("DAGSTER_USER")
            env_vars.append("DAGSTER_IS_DEV_CLI")
            launcher_config["env_vars"] = env_vars

        inst_data = ConfigurableClassData(
            module_name=launcher_module,
            class_name=launcher_class_name,
            config_yaml=yaml.dump(launcher_config),
        )
        run_launcher = launcher_class(inst_data, **launcher_config)

        run_launcher.register_instance(self._instance)
        return run_launcher

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        log.debug(f"run.run_config: '{run.run_config}'")
        try:
            launcher = self.create_launcher(context.workspace)
        except KeyError:
            valid_launchers = [
                c.__name__
                for c in [
                    DefaultRunLauncher,
                    DockerRunLauncher,
                    AzureContainerAppJobRunLauncher,
                ]
            ]
            raise RuntimeError(
                "Invalid launcher class specified! "
                "Must be one of: "
                f"{valid_launchers}"
            )

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
        log.debug(f"serialized_inst_data: '{serialized_inst_data}'")
        inst_data: ConfigurableClassData = deserialize_value(
            serialized_inst_data, ConfigurableClassData
        )
        run_launcher: RunLauncher = inst_data.rehydrate(RunLauncher)
        log.debug(f"run_launcher: '{run_launcher}'")
        run_launcher.register_instance(self._instance)
        return run_launcher

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        run_launcher = self.get_launcher(run)
        run_launcher.resume_run(context)

    @property
    def supports_check_run_worker_health(self):
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        log.debug(
            f"Starting check_run_worker_health for '{self.__class__.__name__}'"
        )
        run_launcher = self.get_launcher(run)
        log.debug(
            f"Checking run worker health with launcher '{run_launcher.__class__.__name__}'"
        )
        if not run_launcher.supports_check_run_worker_health:
            log.debug(
                f"Skipping health check for launcher '{run_launcher.__class__.__name__}'"
            )
            # Assume running if run worker doesn't support health check
            # This should only be for the DefaultRunLauncher
            return CheckRunHealthResult(WorkerStatus.RUNNING)
        res = run_launcher.check_run_worker_health(run)
        log.debug(
            f"Returned health check status '{res.status}' with message "
            f"'{res.msg}' for launcher '{run_launcher.__class__.__name__}'"
        )
        return res

    def terminate(self, run_id):
        log.debug("Terminating run_id: " + run_id)
        run = self._instance.get_run_by_id(run_id)
        run_launcher = self.get_launcher(run)
        log.debug(
            f"Terminating run_id '{run_id}' with launcher '{run_launcher.__class__.__name__}'"
        )
        return run_launcher.terminate(run_id)
