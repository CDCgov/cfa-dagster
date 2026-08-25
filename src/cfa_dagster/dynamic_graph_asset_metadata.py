import re
from dataclasses import dataclass
from typing import Any, Optional

import dagster as dg

SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS = (
    "should_input_manager_inherit_graph_dimensions"
)

# Choosing a lesser-used alpha char as a prefix to prevent Dagster/python keyword errors.
_SEGMENT_PREFIX = "x_"
# Triple underscore separates encoded values since single/double underscores can
# appear inside encoded characters.
_SEGMENT_SEPARATOR = "___"


@dataclass
class InheritedGraphDimensionInputMetadata:
    asset_key_path: list[str]
    asset_partition_keys: list[Any]
    synthetic_partition_keys: list[str]


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


def get_inherited_graph_dimension_input_metadata(
    context: dg.InputContext,
) -> Optional[InheritedGraphDimensionInputMetadata]:
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

    return InheritedGraphDimensionInputMetadata(
        asset_key_path=asset_key_path,
        asset_partition_keys=_get_asset_partition_keys(context),
        synthetic_partition_keys=_decode_mapping_key(mapping_key),
    )
