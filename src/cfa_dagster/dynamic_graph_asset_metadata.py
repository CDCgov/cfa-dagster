import re
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

import dagster as dg
from dagster._core.definitions.partitions.utils.multi import (
    MULTIPARTITION_KEY_DELIMITER,
)

SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS = (
    "should_input_manager_inherit_graph_dimensions"
)
DYNAMIC_GRAPH_IO_MANAGER_METADATA_KEY = "cfa_dagster_dynamic_graph"

# Choosing a lesser-used alpha char as a prefix to prevent Dagster/python keyword errors.
_SEGMENT_PREFIX = "x_"
# Triple underscore separates encoded values since single/double underscores can
# appear inside encoded characters.
_SEGMENT_SEPARATOR = "___"


@dataclass
class DynamicGraphIOManagerMetadata:
    asset_key_path: list[str] = field(default_factory=list)
    asset_partition_keys: list[Any] = field(default_factory=list)
    synthetic_partition_keys: list[str] = field(default_factory=list)
    skip_input: bool = False
    skip_output: bool = False

    @classmethod
    def from_metadata(
        cls,
        metadata: dict,
    ) -> Optional["DynamicGraphIOManagerMetadata"]:
        raw = metadata.get(DYNAMIC_GRAPH_IO_MANAGER_METADATA_KEY)
        if raw is None:
            return None
        if isinstance(raw, dg.MetadataValue):
            raw = raw.value
        if not isinstance(raw, dict):
            raise TypeError(
                f"Expected '{DYNAMIC_GRAPH_IO_MANAGER_METADATA_KEY}' metadata "
                f"to be a dict, got {type(raw)}"
            )
        return cls(
            asset_key_path=raw.get("asset_key_path", []),
            asset_partition_keys=raw.get("asset_partition_keys", []),
            synthetic_partition_keys=raw.get("synthetic_partition_keys", []),
            skip_input=raw.get("skip_input", False),
            skip_output=raw.get("skip_output", False),
        )

    def to_dict(self) -> dict:
        return {DYNAMIC_GRAPH_IO_MANAGER_METADATA_KEY: asdict(self)}


def _encode_segment(value: str) -> str:
    """Encode forbidden characters as _XX_ (hex), leave [a-zA-Z0-9] as-is."""
    encoded = re.sub(
        r"[^a-zA-Z0-9]|_",
        lambda m: f"_{ord(m.group()):02X}_",
        str(value),
    )
    return f"{_SEGMENT_PREFIX}{encoded}"


def _decode_segment(encoded: str) -> str:
    """Decode _XX_ sequences back to original characters."""
    encoded = encoded.removeprefix(_SEGMENT_PREFIX)

    return re.sub(
        r"_([0-9A-F]{2})_",
        lambda m: chr(int(m.group(1), 16)),
        encoded,
    )


def _encode_mapping_key(values: tuple) -> str:
    return _SEGMENT_SEPARATOR.join(_encode_segment(v) for v in values)


def _decode_mapping_key(mapping_key: str) -> list[str]:
    return [
        _decode_segment(segment)
        for segment in mapping_key.split(_SEGMENT_SEPARATOR)
    ]


def _get_mapping_key(context: dg.InputContext) -> Optional[str]:
    try:
        return context.step_context.step.get_mapping_key()
    except Exception:
        return None


def _get_asset_key_path(context: dg.InputContext) -> Optional[list[str]]:
    try:
        if context.has_asset_key:
            return list(context.asset_key.path)
    except Exception:
        return None
    return None


def _get_asset_partition_keys(context: dg.InputContext) -> list[Any]:
    try:
        if context.has_asset_partitions:
            return list(context.asset_partition_keys)
    except Exception:
        return []
    return []


def _normalize_partition_key(key: Any) -> str:
    return str(key).replace(MULTIPARTITION_KEY_DELIMITER, "/")


def expand_and_combine_partition_keys(
    real_keys: list[Any],
    synthetic_partition_keys: list[str],
) -> list[str]:
    expanded = [_normalize_partition_key(key) for key in real_keys]
    dim_suffix = "/".join(synthetic_partition_keys)

    if expanded and dim_suffix:
        return [f"{key}/{dim_suffix}" for key in expanded]
    if dim_suffix:
        return [dim_suffix]
    return expanded


def patch_context_with_dynamic_graph_metadata(
    context: dg.InputContext | dg.OutputContext,
    metadata: DynamicGraphIOManagerMetadata,
) -> None:
    if metadata.synthetic_partition_keys:
        real_keys = (
            context.asset_partition_keys
            if context.has_asset_partitions
            else metadata.asset_partition_keys or []
        )
        synthetic_keys = expand_and_combine_partition_keys(
            real_keys=list(real_keys),
            synthetic_partition_keys=metadata.synthetic_partition_keys,
        )
        has_partitions = bool(synthetic_keys)

        context.__class__.asset_partition_keys = property(
            lambda self: synthetic_keys
        )
        context.__class__.has_asset_partitions = property(
            lambda self: has_partitions
        )

    if metadata.asset_key_path:
        context.__class__.asset_key = property(
            lambda self: dg.AssetKey(metadata.asset_key_path)
        )
        context.__class__.has_asset_key = property(lambda self: True)


def get_inherited_graph_dimension_input_metadata(
    context: dg.InputContext,
) -> Optional[DynamicGraphIOManagerMetadata]:
    """
    Build dynamic graph IO metadata for a @dynamic_graph_asset input that opts into graph dimension inheritance.

    ``dg.In.metadata`` is static, so it cannot contain the current mapped graph
    dimension values. When the user sets
    ``should_input_manager_inherit_graph_dimensions=True``, derive those values
    from the mapped input context at load time instead.

    This metadata is more specific than static ``DynamicGraphIOManagerMetadata``
    on the input because it comes from the current mapped step and upstream
    asset context, so IO managers should let it take precedence when both are
    present.
    """
    if not context.definition_metadata.get(
        SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS,
        False,
    ):
        return None

    mapping_key = _get_mapping_key(context)
    if mapping_key is None:
        return None

    asset_key_path = _get_asset_key_path(context)
    if asset_key_path is None:
        return None

    return DynamicGraphIOManagerMetadata(
        asset_key_path=asset_key_path,
        asset_partition_keys=_get_asset_partition_keys(context),
        synthetic_partition_keys=_decode_mapping_key(mapping_key),
    )
