import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from dagster import (
    Array,
    DagsterInvalidConfigError,
    Field,
    IntSource,
    MetadataValue,
    Noneable,
    Permissive,
    Selector,
    Shape,
    in_process_executor,
    multiprocess_executor,
)
from dagster._config import ConfigTypeKind, process_config
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
        log.debug(f"payload: '{payload}'")
        # remove None entries
        payload = {k: v for k, v in payload.items() if v is not None}
        return payload

    # -------------------------
    # To run tags
    # -------------------------
    def to_run_tags(self) -> Dict[str, str]:
        payload = self.to_dict()
        return {self.TAG_KEY: json.dumps(payload, separators=(",", ":"))}

    def to_run_config(self) -> Dict[str, str]:
        return {"config": self.to_dict()}


def _patch_config_type(config_type, default_value):
    """
    Recursively patches a config type with concrete types where possible,
    using the default value to infer types for scalar/scalar_union fields.
    Since values are pre-validated, we can trust their structure.
    """
    kind = config_type.kind

    # Primitive scalars
    if kind == ConfigTypeKind.SCALAR:
        given_name = getattr(config_type, "given_name", None)
        return {
            "String": str,
            "Int": int,
            "Float": float,
            "Bool": bool,
        }.get(given_name, config_type)

    # StringSource, IntSource etc - infer from default value
    if kind == ConfigTypeKind.SCALAR_UNION:
        for t in (str, bool, int, float):
            if isinstance(default_value, t):
                return t
        return config_type

    # Noneable - recurse into the inner type if value is not None
    if kind == ConfigTypeKind.NONEABLE:
        if default_value is None:
            return config_type
        # Access the inner type directly
        inner = config_type.inner_type
        patched_inner = _patch_config_type(inner, default_value)
        return Noneable(patched_inner)

    # Strict or permissive shape - recurse into fields
    if kind in (ConfigTypeKind.STRICT_SHAPE, ConfigTypeKind.PERMISSIVE_SHAPE):
        if not isinstance(default_value, dict):
            return config_type
        patched_fields = {}
        existing_fields = getattr(config_type, "fields", {})
        for fname, ffield in existing_fields.items():
            if fname in default_value:
                raw_type = (
                    ffield.config_type if isinstance(ffield, Field) else ffield
                )
                patched_fields[fname] = Field(
                    _patch_config_type(raw_type, default_value[fname]),
                    default_value=default_value[fname],
                    is_required=False,
                    description=ffield.description
                    if isinstance(ffield, Field)
                    else None,
                )
            else:
                patched_fields[fname] = ffield
        if kind == ConfigTypeKind.PERMISSIVE_SHAPE:
            return Permissive(patched_fields)
        return Shape(patched_fields)

    # Array - recurse into the inner type using the first element as a hint
    if kind == ConfigTypeKind.ARRAY:
        if not isinstance(default_value, list) or not default_value:
            return config_type
        inner = config_type.inner_type
        # Use first element to infer inner type
        patched_inner = _patch_config_type(inner, default_value[0])
        return Array(patched_inner)

    # ANY, SELECTOR, etc - return as-is
    return config_type


def with_alternate_default(fields: dict, alternates: dict[str, dict]) -> dict:
    """
    Return a copy of a Dagster config field mapping with selected defaults overridden.

    This function walks a mapping of config ``fields`` (typically a config schema
    definition) and applies alternate default values specified in ``alternates``.
    For each top-level field name present in ``alternates``:

    - If the field's config type is a ``Shape`` (i.e., a dict of nested fields),
      only the matching inner fields are patched with:
          * a new config type adjusted via ``_patch_config_type``
          * ``default_value`` set to the provided alternate
          * ``is_required=False``

      The outer field itself is rebuilt as a non-required ``Field(Shape(...))``.
      No outer default is set, since the nested field defaults are sufficient.

    - If the field is a scalar (or non-dict config type), the field is rebuilt
      with:
          * its config type patched via ``_patch_config_type``
          * ``default_value`` set to the provided alternate
          * ``is_required=False``

    Fields not present in ``alternates`` are returned unchanged.

    The original ``fields`` mapping is not mutated; a new mapping is returned.

    Args:
        fields: A mapping of field names to Dagster ``Field`` objects or raw
            config types.
        alternates: A mapping of field names to alternate default values. For
            nested shapes, the value should itself be a dict mapping inner
            field names to their alternate defaults.

    Returns:
        A new dictionary of field definitions with alternate defaults applied
        where specified.
    """
    result = {}
    for name, field_def in fields.items():
        if name in alternates and alternates[name]:
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
                            _patch_config_type(
                                raw_type, alternate_default[fname]
                            ),
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
                    _patch_config_type(config_type, alternate_default),
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

    docker_executor_schema = merge_dicts(
        docker_executor.config_schema.config_type.fields,
        {
            "max_concurrent": Field(
                IntSource,
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
            "multiprocess_executor": multiprocess_executor.config_schema.config_type.fields,
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
