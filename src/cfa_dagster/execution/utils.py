import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from dagster import (
    DagsterInvalidConfigError,
    Field,
    MetadataValue,
    Selector,
    in_process_executor,
    multiprocess_executor,
)
from dagster._config import process_config

from cfa_dagster import (
    azure_batch_executor,
    azure_container_app_job_executor,
    docker_executor,
)

log = logging.getLogger(__name__)


def get_dynamic_executor_config_schema() -> dict:
    is_production = os.getenv("DAGSTER_IS_DEV_CLI") is None

    return {
        "launcher": Field(
            Selector(
                {
                    "DefaultRunLauncher": {},
                    "AzureContainerAppJobRunLauncher": (
                        azure_container_app_job_executor.config_schema.config_type.fields
                    ),
                    **(
                        {}
                        if is_production
                        else {
                            "DockerRunLauncher": (
                                docker_executor.config_schema.config_type.fields
                            )
                        }
                    ),
                }
            ),
            description=(
                "The run launcher determines the environment where "
                "the Dagster run occurs."
            ),
            default_value=(
                {"AzureContainerAppJobRunLauncher": {}}
                if is_production
                else {"DefaultRunLauncher": {}}
            ),
        ),
        "executor": Field(
            Selector(
                {
                    "in_process_executor": (
                        in_process_executor.config_schema.config_type.fields
                    ),
                    "multiprocess_executor": (
                        multiprocess_executor.config_schema.config_type.fields
                    ),
                    "azure_batch_executor": (
                        azure_batch_executor.config_schema.config_type.fields
                    ),
                    "azure_container_app_job_executor": (
                        azure_container_app_job_executor.config_schema.config_type.fields
                    ),
                    **(
                        {}
                        if is_production
                        else {
                            "docker_executor": (
                                docker_executor.config_schema.config_type.fields
                            )
                        }
                    ),
                }
            ),
            description=(
                "The executor determines how the steps in a run are "
                "parallelized within the run launcher environment"
            ),
            default_value={"multiprocess_executor": {}},
        ),
    }


@dataclass(frozen=True)
class SelectorConfig:
    class_name: str
    config: Dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        """
        True if either launcher or executor is set.
        False if both are None.
        """
        return bool(self.class_name and self.config is not None)

    @classmethod
    def from_run_config(
        cls, value: Optional[Mapping[str, Any]]
    ) -> Optional["SelectorConfig"]:
        if not value:
            return None
        if len(value) != 1:
            raise ValueError(
                f"Selector config must have exactly one key, got: {value}"
            )
        class_name, config = next(iter(value.items()))
        return cls(class_name=class_name, config=dict(config or {}))

    def to_run_config(self) -> Dict[str, Dict[str, Any]]:
        return {self.class_name: self.config}

    @classmethod
    def from_json(
        cls, value: Optional[Mapping[str, Any]]
    ) -> Optional["SelectorConfig"]:
        if not value:
            return None
        return cls.from_run_config(value)


@dataclass(frozen=True)
class ExecutionConfig:
    launcher: Optional[SelectorConfig] = None
    executor: Optional[SelectorConfig] = None

    TAG_KEY = "cfa_dagster/execution"

    def __post_init__(self):
        """
        Validate inputs and add defaults
        If only the launcher is specified, defaults will be applied to the
        supplied launcher class only. Same if only the executor is specified.
        """
        config_schema = get_dynamic_executor_config_schema()
        if self.launcher:
            result = process_config(
                config_schema["launcher"].config_type,
                self.launcher.to_run_config(),
            )
            if not result.success:
                raise DagsterInvalidConfigError(
                    "Invalid execution config",
                    result.errors,
                    self.launcher.to_run_config(),
                )
            object.__setattr__(
                self, "launcher", SelectorConfig.from_run_config(result.value)
            )

        if self.executor:
            result = process_config(
                config_schema["executor"].config_type,
                self.executor.to_run_config(),
            )
            if not result.success:
                raise DagsterInvalidConfigError(
                    "Invalid execution config",
                    result.errors,
                    self.executor.to_run_config(),
                )
            object.__setattr__(
                self, "executor", SelectorConfig.from_run_config(result.value)
            )

    def __bool__(self) -> bool:
        """
        True if either launcher or executor is set.
        False if both are None.
        """
        return bool(self.launcher or self.executor)

    @classmethod
    def default(cls) -> "ExecutionConfig":
        config_schema = get_dynamic_executor_config_schema()
        result = process_config(config_schema, {})
        return cls.from_executor_config(result.value)

    # -------------------------
    # From executor run config
    # -------------------------
    @classmethod
    def from_executor_config(
        cls, config: Mapping[str, Any]
    ) -> "ExecutionConfig":
        return cls(
            launcher=SelectorConfig.from_json(config.get("launcher")),
            executor=SelectorConfig.from_json(config.get("executor")),
        )

    # -------------------------
    # From run config
    # -------------------------
    @classmethod
    def from_run_config(cls, config: Mapping[str, Any]) -> "ExecutionConfig":
        executor_config = config.get("execution", {}).get("config", {})
        return cls.from_executor_config(executor_config)

    # -------------------------
    # From run tags
    # -------------------------
    @classmethod
    def from_run_tags(cls, tags: Mapping[str, str]) -> "ExecutionConfig":
        raw_json = tags.get(cls.TAG_KEY)
        if not raw_json:
            return cls()
        payload = json.loads(raw_json)
        # if payload.get("v") != cls.TAG_VERSION:
        #     raise ValueError(f"Unsupported execution tag version: {payload.get('v')}")
        return cls(
            launcher=SelectorConfig.from_json(payload.get("launcher")),
            executor=SelectorConfig.from_json(payload.get("executor")),
        )

    # -------------------------
    # From Dagster metadata
    # -------------------------
    @classmethod
    def from_metadata(
        cls, metadata: Optional[dict[str, MetadataValue]]
    ) -> "ExecutionConfig":
        if not metadata:
            return cls()
        data = metadata.get(cls.TAG_KEY)
        if not data:
            return cls()
        # Only unwrap the top-level value
        value = data.value
        if not isinstance(value, dict):
            return cls()
        return cls(
            launcher=SelectorConfig.from_json(value.get("launcher")),
            executor=SelectorConfig.from_json(value.get("executor")),
        )

    def to_run_config(self) -> Dict[str, str]:
        payload = {
            # "v": self.TAG_VERSION,
            "launcher": self.launcher.to_run_config()
            if self.launcher
            else None,
            "executor": self.executor.to_run_config()
            if self.executor
            else None,
        }
        log.debug(f"payload: '{payload}'")
        # remove None entries
        payload = {k: v for k, v in payload.items() if v is not None}
        return payload

    # -------------------------
    # To run tags
    # -------------------------
    def to_run_tags(self) -> Dict[str, str]:
        payload = self.to_run_config()
        return {self.TAG_KEY: json.dumps(payload, separators=(",", ":"))}
