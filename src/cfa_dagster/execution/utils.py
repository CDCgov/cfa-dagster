from typing import Dict, Any, Mapping, Optional
import os
import json
from dataclasses import dataclass
import logging
from dagster import (
    Field,
    Selector,
    in_process_executor,
    multiprocess_executor,
    DagsterInvalidConfigError,
    MetadataValue
)
from dagster._config import process_config
from dagster._utils.merger import merge_dicts
from cfa_dagster import (
    azure_batch_executor,
    azure_container_app_job_executor,
    docker_executor,
)

log = logging.getLogger(__name__)


DYNAMIC_EXECUTOR_CONFIG_SCHEMA = {
    # Dagster doesn't provide RunConfig for RunLaunchers
    # so we include it with executor config
    "launcher": Field(
        Selector({
            "DynamicRunLauncher": {},
            "DefaultRunLauncher": {},
            "AzureContainerAppJobRunLauncher": azure_container_app_job_executor.config_schema.config_type.fields,
            "DockerRunLauncher": docker_executor.config_schema.config_type.fields,
        })
        , description=(
            "The run launcher determines the environment where "
            "the Dagster run occurs."
        )
        , default_value={"DynamicRunLauncher": {}}
    ),
    "executor": Field(
        Selector({
            "in_process_executor": in_process_executor.config_schema.config_type.fields,
            "multiprocess_executor": multiprocess_executor.config_schema.config_type.fields,
            "docker_executor": docker_executor.config_schema.config_type.fields,
            "azure_batch_executor": azure_batch_executor.config_schema.config_type.fields,
            "azure_container_app_job_executor": azure_container_app_job_executor.config_schema.config_type.fields,
            "dynamic_executor": {},
        })
        , description=(
            "The executor determines how the steps in a run are "
                "parallelized within the run launcher environment"
        )
        , default_value={"dynamic_executor": {}}
    )
}


@dataclass(frozen=True)
class SelectorConfig:
    class_name: str
    config: Dict[str, Any]

    def __bool__(self) -> bool:
        """
        True if either launcher or executor is set.
        False if both are None.
        """
        return bool(self.class_name and self.config is not None)

    @classmethod
    def from_run_config(cls, value: Optional[Mapping[str, Any]]) -> Optional["SelectorConfig"]:
        if not value:
            return None
        if len(value) != 1:
            raise ValueError(f"Selector config must have exactly one key, got: {value}")
        class_name, config = next(iter(value.items()))
        return cls(class_name=class_name, config=dict(config or {}))

    def to_run_config(self) -> Dict[str, Dict[str, Any]]:
        return {self.class_name: self.config}

    @classmethod
    def from_json(cls, value: Optional[Mapping[str, Any]]) -> Optional["SelectorConfig"]:
        if not value:
            return None
        return cls.from_run_config(value)


@dataclass(frozen=True)
class ExecutionConfig:
    launcher: Optional[SelectorConfig] = None
    executor: Optional[SelectorConfig] = None

    TAG_KEY = "cfa_dagster/execution"
    # TAG_VERSION = 1

    def __bool__(self) -> bool:
        """
        True if either launcher or executor is set.
        False if both are None.
        """
        return bool(self.launcher or self.executor)

    @classmethod
    def default(cls) -> "ExecutionConfig":
        is_production = not os.getenv("DAGSTER_IS_DEV_CLI")
        if is_production:
            launcher_class = "AzureContainerAppRunLauncher"
        else:
            launcher_class = "DefaultRunLauncher"

        return cls(
            launcher=SelectorConfig(class_name=launcher_class, config={}),
            executor=SelectorConfig(class_name="multiprocess_executor", config={}),
        )

    @staticmethod
    def validate(raw_config: dict) -> dict:
        """
        Validate and normalize execution config using the schema.

        If only one of launcher/executor is provided, fill the other with defaults.
        """
        # Start with defaults from the schema
        default_config = {
            "launcher": DYNAMIC_EXECUTOR_CONFIG_SCHEMA["launcher"].default_value,
            "executor": DYNAMIC_EXECUTOR_CONFIG_SCHEMA["executor"].default_value,
        }

        # Merge user-provided values (None is ignored)
        merged_config = {
            k: v if v not in (None, {}) else default_config[k]
            for k, v in {**default_config, **raw_config}.items()
        }

        # If both are still None/empty, return empty
        if all(v in (None, {}) for v in merged_config.values()):
            return {}
        result = process_config(
            config_type=DYNAMIC_EXECUTOR_CONFIG_SCHEMA,
            config_dict=raw_config,
        )
        if not result.success:
            raise DagsterInvalidConfigError(
                "Invalid execution config",
                result.errors,
                raw_config,
            )
        log.debug(f"Validated execution config: {result.value}")
        # Merge defaults from schema
        return merge_dicts(raw_config, result.value)

    # -------------------------
    # From executor run config
    # -------------------------
    @classmethod
    def from_executor_config(cls, config: Mapping[str, Any]) -> "ExecutionConfig":
        return cls(
            launcher=SelectorConfig.from_json(config.get("launcher")),
            executor=SelectorConfig.from_json(config.get("executor")),
        )

    # -------------------------
    # From run config
    # -------------------------
    @classmethod
    def from_run_config(cls, config: Mapping[str, Any]) -> "ExecutionConfig":
        executor_config = (
            config
            .get("execution", {})
            .get("config", {})
        )
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
    def from_metadata(cls, metadata: Optional[dict[str, MetadataValue]]) -> "ExecutionConfig":
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

    # -------------------------
    # To run tags
    # -------------------------
    def to_run_tags(self) -> Dict[str, str]:
        payload = {
            # "v": self.TAG_VERSION,
            "launcher": self.launcher.to_run_config() if self.launcher else None,
            "executor": self.executor.to_run_config() if self.executor else None,
        }
        log.debug(f"payload: '{payload}'")
        # remove None entries
        payload = {k: v for k, v in payload.items() if v is not None}
        return {self.TAG_KEY: json.dumps(payload, separators=(",", ":"))}
