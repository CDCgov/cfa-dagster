import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from dagster import (
    DagsterInvalidConfigError,
    Field,
    Int,
    MetadataValue,
    Noneable,
    Selector,
    Shape,
    in_process_executor,
    multiprocess_executor,
)
from dagster._config import process_config
from dagster._utils.merger import merge_dicts
from dagster_docker import docker_executor
from dagster_docker.utils import DOCKER_CONFIG_SCHEMA

from ..azure_batch.executor import azure_batch_executor
from ..azure_container_app_job.executor import azure_container_app_job_executor
from ..utils import is_production

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SelectorConfig:
    class_name: str
    config: Dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        """
        True if class_name is truthy and config is not None
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
        self.validate(True)

    def validate(
        self, use_full_schema: Optional[bool] = False
    ) -> "ExecutionConfig":
        """
        Validates the execution config against an environment-aware schema
        with an option to use the full schema

        If only the launcher is specified, defaults will be applied to the
        supplied launcher class only.

        If only the executor is specified, defaults will be applied to the
        supplied executor class only.
        """
        if not self.launcher and not self.executor:
            return

        config_schema = get_dynamic_executor_config_schema(
            use_full_schema=use_full_schema
        )
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
        return self

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
        cls, config: Mapping[str, Any], should_skip_executor: bool = False
    ) -> "ExecutionConfig":
        if not config:
            return cls()
        return cls(
            launcher=SelectorConfig.from_json(config.get("launcher")),
            executor=None
            if should_skip_executor
            else SelectorConfig.from_json(config.get("executor")),
        )

    # -------------------------
    # From run config
    # -------------------------
    @classmethod
    def from_run_config(
        cls, config: Mapping[str, Any], should_skip_executor: bool = False
    ) -> "ExecutionConfig":
        if not config:
            return cls()
        executor_config = config.get("execution", {}).get("config", {})
        return cls.from_executor_config(executor_config, should_skip_executor)

    # -------------------------
    # From run tags
    # -------------------------
    @classmethod
    def from_run_tags(
        cls, tags: Mapping[str, str], should_skip_executor: bool = False
    ) -> "ExecutionConfig":
        if not tags:
            return cls()
        raw_json = tags.get(cls.TAG_KEY)
        if not raw_json:
            return cls()
        payload = json.loads(raw_json)
        # if payload.get("v") != cls.TAG_VERSION:
        #     raise ValueError(f"Unsupported execution tag version: {payload.get('v')}")
        return cls(
            launcher=SelectorConfig.from_json(payload.get("launcher")),
            executor=None
            if should_skip_executor
            else SelectorConfig.from_json(payload.get("executor")),
        )

    # -------------------------
    # From Dagster metadata
    # -------------------------
    @classmethod
    def from_metadata(
        cls,
        metadata: Optional[dict[str, MetadataValue]],
        should_skip_executor: bool = False,
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
            executor=None
            if should_skip_executor
            else SelectorConfig.from_json(value.get("executor")),
        )

    def to_dict(self) -> Dict[str, str]:
        payload = {
            # "v": self.TAG_VERSION,
            "launcher": self.launcher.to_run_config()
            if self.launcher
            else None,
            "executor": self.executor.to_run_config()
            if self.executor
            else None,
        }
        # remove None entries
        payload = {k: v for k, v in payload.items() if v is not None}
        return payload

    # -------------------------
    # To run tags
    # -------------------------
    def to_run_tags(self) -> Dict[str, str]:
        payload = self.to_dict()
        return {self.TAG_KEY: json.dumps(payload, separators=(",", ":"))}

    def to_metadata(self) -> Dict[str, str]:
        payload = self.to_dict()
        return {self.TAG_KEY: MetadataValue.json(payload)}

    def to_run_config(self) -> Dict[str, str]:
        return {"config": self.to_dict()}


def with_alternate_default(fields: dict, alternates: dict[str, dict]) -> dict:
    """
    Takes a Dagster config schema and returns a copy with alternate default values
    """
    result = {}
    for name, field_def in fields.items():
        if name in alternates and alternates[name] is not None:
            alternate_default = alternates[name]
            if isinstance(field_def, Field):
                config_type = field_def.config_type
            else:
                config_type = field_def

            if isinstance(config_type, dict):
                patched_fields = {}
                for fname, ffield in config_type.items():
                    if fname in alternate_default:
                        raw_type = (
                            ffield.config_type
                            if isinstance(ffield, Field)
                            else ffield
                        )
                        patched_fields[fname] = Field(
                            raw_type,
                            default_value=alternate_default[fname],
                            is_required=False,
                            description=ffield.description
                            if isinstance(ffield, Field)
                            else None,
                        )
                    else:
                        patched_fields[fname] = ffield
                result[name] = Field(
                    Shape(patched_fields),
                    default_value=alternate_default,
                    is_required=False,
                )
            else:
                result[name] = Field(
                    config_type,
                    default_value=alternate_default,
                    is_required=False,
                )
        else:
            result[name] = field_def
    return result


def get_dynamic_executor_config_schema(
    default_config: Optional[ExecutionConfig] = None,
    alternate_configs: Optional[list[ExecutionConfig]] = None,
    use_full_schema: Optional[bool] = False,
) -> dict:
    """
    Returns a config schema for the dynamic executor with parameters for
    defaults and an option to return the full schema without prod restrictions.

    alternate_configs: A list of ExecutionConfig objects whose launcher/executor
    configs will be used as default values for those selectors in the Launchpad.
    """
    use_full_schema = use_full_schema or not is_production()
    default_config = default_config or ExecutionConfig()
    default_launcher = default_config.launcher
    default_executor = default_config.executor

    # Build lookup of alternate defaults by class name
    alternate_launchers: dict[str, dict] = {}
    alternate_executors: dict[str, dict] = {}
    for alt in alternate_configs or []:
        if alt.launcher:
            alternate_launchers[alt.launcher.class_name] = alt.launcher.config
        if alt.executor:
            alternate_executors[alt.executor.class_name] = alt.executor.config

    launcher_fields = with_alternate_default(
        {
            "DefaultRunLauncher": {},
            "AzureContainerAppJobRunLauncher": (
                azure_container_app_job_executor.config_schema.config_type.fields
            ),
            **(
                {"DockerRunLauncher": DOCKER_CONFIG_SCHEMA}
                if use_full_schema
                else {}
            ),
        },
        alternate_launchers,
    )

    multiprocess_executor_schema = merge_dicts(
        multiprocess_executor.config_schema.config_type.fields,
        {
            "max_concurrent": Field(
                Noneable(Int),
                is_required=False,
                description=(
                    "Limit on the number of containers that will run concurrently within the scope "
                    "of a Dagster run. Note that this limit is per run, not global."
                ),
                default_value=5,
            ),
        },
    )

    docker_executor_schema = merge_dicts(
        docker_executor.config_schema.config_type.fields,
        {
            "max_concurrent": Field(
                Noneable(Int),
                is_required=False,
                description=(
                    "Limit on the number of containers that will run concurrently within the scope "
                    "of a Dagster run. Note that this limit is per run, not global."
                ),
                default_value=5,
            ),
        },
    )

    executor_fields = with_alternate_default(
        {
            "in_process_executor": in_process_executor.config_schema.config_type.fields,
            "multiprocess_executor": multiprocess_executor_schema,
            "azure_batch_executor": azure_batch_executor.config_schema.config_type.fields,
            "azure_container_app_job_executor": azure_container_app_job_executor.config_schema.config_type.fields,
            **(
                {"docker_executor": docker_executor_schema}
                if use_full_schema
                else {}
            ),
        },
        alternate_executors,
    )

    return {
        "launcher": Field(
            Selector(launcher_fields),
            description=(
                "The run launcher determines the environment where "
                "the Dagster run occurs."
            ),
            default_value=(
                default_launcher.to_run_config()
                if default_launcher
                else {"AzureContainerAppJobRunLauncher": {}}
                if is_production()
                else {"DefaultRunLauncher": {}}
            ),
        ),
        "executor": Field(
            Selector(executor_fields),
            description=(
                "The executor determines how the steps in a run are "
                "parallelized within the run launcher environment"
            ),
            default_value=(
                default_executor.to_run_config()
                if default_executor
                else {"multiprocess_executor": {}}
            ),
        ),
    }
