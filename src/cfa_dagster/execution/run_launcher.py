import json
import logging
import os
from collections.abc import Mapping
from typing import Any, Optional, Union

import yaml
from dagster import (
    DefaultRunLauncher,
    JsonMetadataValue,
)
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    ResumeRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._serdes import ConfigurableClass
from dagster._serdes.config_class import ConfigurableClassData
from dagster_docker import DockerRunLauncher
from typing_extensions import Self

from cfa_dagster import (
    AzureContainerAppJobRunLauncher,
)

from .utils import ExecutionConfig, SelectorConfig

log = logging.getLogger(__name__)

LAUNCHER_CONFIG_KEY = "cfa_dagster/launcher"
EXECUTION_CONFIG_KEY = "cfa_dagster/execution"


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

    def _get_location_metadata(self, context: LaunchRunContext):
        """
        Gets metadata from the Definitions for a code location
        """
        workspace: BaseWorkspaceRequestContext = context.workspace
        location_name = context.dagster_run.remote_job_origin.location_name
        log.debug(f"location_name: '{location_name}'")
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

    def _get_config_from_launcher_tags(self, tags: dict):
        launcher_config_str = tags.get(LAUNCHER_CONFIG_KEY, "{}")
        try:
            return json.loads(launcher_config_str)
        except json.decoder.JSONDecodeError:
            raise RuntimeError(
                f"Invalid JSON for '{LAUNCHER_CONFIG_KEY}'. "
                f"Received: '{launcher_config_str}'"
            )

    def _get_config_from_launcher_metadata(self, metadata):
        launcher_config = metadata.get(
            LAUNCHER_CONFIG_KEY, JsonMetadataValue({})
        ).value
        return launcher_config

    def _get_config_from_execution_metadata(self, metadata):
        launcher_config = metadata.get(
            EXECUTION_CONFIG_KEY, JsonMetadataValue({})
        ).value
        return launcher_config.get("launcher")

    def _create_launcher(self, execution_config: ExecutionConfig):
        launcher_config = execution_config.launcher
        launcher_class_name = launcher_config.class_name
        try:
            launcher_class = globals()[launcher_class_name]
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
                f"Invalid launcher class specified: '{launcher_class_name}'. "
                "Must be one of: "
                f"{valid_launchers}"
            )
        launcher_module = launcher_class.__module__

        launcher_config = launcher_config.config or {}

        # default run launcher throws an error for env vars
        if launcher_class_name != DefaultRunLauncher.__name__:
            env_vars = launcher_config.get("env_vars", [])
            # Need to check if env vars are present first or
            # each run will append them again
            req_vars = [
                "DAGSTER_USER",
                "CFA_DAGSTER_ENV",
                "DAGSTER_IS_DEV_CLI",
            ]
            for env_var in req_vars:
                if os.getenv(env_var) and env_var not in req_vars:
                    env_vars.append(env_var)
            launcher_config["env_vars"] = env_vars

        inst_data = ConfigurableClassData(
            module_name=launcher_module,
            class_name=launcher_class_name,
            config_yaml=yaml.dump(launcher_config),
        )
        run_launcher = launcher_class(inst_data, **launcher_config)

        run_launcher.register_instance(self._instance)
        return run_launcher

    def _get_launcher_config(
        self,
        run: DagsterRun,
        context: Optional[Union[LaunchRunContext, ResumeRunContext]] = None,
    ) -> ExecutionConfig:
        """
        Resolve the ExecutionConfig using cascading checks **in ordr of precedence**:
            1. RunConfig(execution)
            2. Run tags cfa_dagster/execution
            3. Repo Definitions(metadata.cfa_dagster/execution)
            4. Legacy launcher tags cfa_dagster/launcher
            5. Legacy Repo Definitions(metadata.cfa_dagster/launcher)

        Any missing launcher or executor will be filled from ExecutionConfig.default().
        Once a launcher or executor is found, lower-priority sources are skipped for that part.
        """
        launcher: Optional[SelectorConfig] = None
        executor: Optional[SelectorConfig] = None

        # Run tags
        tag_config = ExecutionConfig.from_run_tags(run.tags)
        if tag_config:
            launcher = launcher or tag_config.launcher
            executor = executor or tag_config.executor
            log.debug(
                f"After tag config: launcher={launcher}, executor={executor}"
            )

        # Run config (only fill missing parts)
        if not launcher or not executor:
            run_config = ExecutionConfig.from_run_config(run.run_config)
            if run_config:
                launcher = run_config.launcher
                executor = run_config.executor
                log.debug(
                    f"After run config: launcher={launcher}, executor={executor}"
                )

        # Metadata from context
        if context and (not launcher or not executor):
            metadata = self._get_location_metadata(context)
            metadata_config = (
                ExecutionConfig.from_metadata(metadata) if metadata else None
            )
            if metadata_config:
                launcher = launcher or metadata_config.launcher
                executor = executor or metadata_config.executor
                log.debug(
                    f"After metadata config: launcher={launcher}, executor={executor}"
                )

        # Legacy launcher tags
        if not launcher:
            legacy_tags = self._get_config_from_launcher_tags(run.tags)
            if legacy_tags.get("class"):
                launcher = SelectorConfig(
                    class_name=legacy_tags["class"],
                    config=legacy_tags.get("config") or {},
                )
                log.debug(
                    f"After legacy tags: launcher={launcher}, executor={executor}"
                )

        # Legacy metadata
        if context and not launcher:
            legacy_metadata = self._get_config_from_launcher_metadata(metadata)
            if legacy_metadata.get("class"):
                launcher = SelectorConfig(
                    class_name=legacy_metadata["class"],
                    config=legacy_metadata.get("config") or {},
                )
                log.debug(
                    f"After legacy metadata: launcher={launcher}, executor={executor}"
                )

        # Fill in defaults for anything still missing
        defaults = ExecutionConfig.default()
        final_config = ExecutionConfig(
            launcher=launcher or defaults.launcher,
            executor=executor or defaults.executor,
        ).validate()
        log.debug(f"Final resolved execution config: {final_config}")
        return final_config

    # check run tags for launcher config
    # if not there, check repo metadata for launcher config
    # create_launcher from config
    # add launcher config as tags
    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        log.debug(f"run.job_code_origin: '{run.job_code_origin}'")
        log.debug(f"run.remote_job_origin: '{run.remote_job_origin}'")
        log.debug(f"run.run_config: '{run.run_config}'")

        execution_config = self._get_launcher_config(run, context)
        log.debug(f"execution_config: '{execution_config}'")

        launcher = self._create_launcher(execution_config)

        self._instance.add_run_tags(run.run_id, execution_config.to_run_tags())
        self._instance.report_engine_event(
            message=f"Launching run using {launcher.__class__.__name__}",
            dagster_run=run,
            cls=self.__class__,
        )

        launcher.launch_run(context)

    @property
    def supports_resume_run(self):
        return True

    def resume_run(self, context: ResumeRunContext) -> None:
        launcher_config = self._get_launcher_config(
            context.dagster_run, context
        )
        run_launcher = self._create_launcher(launcher_config)
        run_launcher.resume_run(context)

    @property
    def supports_check_run_worker_health(self):
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        log.debug(
            f"Starting check_run_worker_health for '{self.__class__.__name__}'"
        )
        launcher_config = self._get_launcher_config(run)
        run_launcher = self._create_launcher(launcher_config)
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
        launcher_config = self._get_config_from_launcher_tags(run.tags)
        run_launcher = self._create_launcher(launcher_config)
        log.debug(
            f"Terminating run_id '{run_id}' with launcher '{run_launcher.__class__.__name__}'"
        )
        return run_launcher.terminate(run_id)
