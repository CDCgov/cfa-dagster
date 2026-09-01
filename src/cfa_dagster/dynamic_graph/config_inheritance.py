import logging
from typing import Any

import dagster as dg

from .materialization_metadata import (
    _GRAPH_DIMENSIONS_METADATA_KEY,
)
from .types import _DimensionResourceInfo

log = logging.getLogger(__name__)


def _metadata_value(raw):
    if isinstance(raw, dg.MetadataValue):
        return raw.value
    return raw


def _get_upstream_asset_key(input_name: str, op_in: dg.In) -> dg.AssetKey:
    """Resolve the asset key represented by a dynamic graph input."""
    if isinstance(op_in.asset_key, dg.AssetKey):
        return op_in.asset_key
    return dg.AssetKey(input_name)


def _get_latest_materialization_metadata(
    context: dg.OpExecutionContext,
    asset_key: dg.AssetKey,
) -> dict[str, Any]:
    """Fetch latest upstream materialization metadata for this partition, if any."""
    filter_kwargs = {"asset_key": asset_key}
    if context.has_partition_key:
        filter_kwargs["asset_partitions"] = [context.partition_key]

    records_filter = dg.AssetRecordsFilter(**filter_kwargs)
    result = context.instance.fetch_materializations(records_filter, limit=1)
    if not result.records:
        return {}

    materialization = result.records[0].event_log_entry.asset_materialization
    if materialization is None:
        return {}
    return materialization.metadata or {}


def _get_graph_dimensions_metadata_parts(
    metadata: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[Any] | None]:
    """Read graph-dimension metadata from the consolidated materialization key."""
    graph_dimensions_metadata = _metadata_value(
        metadata.get(_GRAPH_DIMENSIONS_METADATA_KEY)
    )
    if isinstance(graph_dimensions_metadata, dict):
        signature = graph_dimensions_metadata.get("signature")
        materialized = graph_dimensions_metadata.get("materialized")
        if isinstance(signature, dict) and isinstance(materialized, list):
            return signature, materialized

    return None, None


def _get_inheritable_dimension_fields(
    context: dg.OpExecutionContext,
    dimension_resource_info: _DimensionResourceInfo,
) -> set[str]:
    """Return dimensions whose resource config allows upstream value inheritance."""
    dimension_resource = getattr(
        context.resources,
        dimension_resource_info.param_name,
    )
    inheritable_fields = set()

    for dimension_field in dimension_resource_info.dimension_fields:
        graph_dimension = getattr(dimension_resource, dimension_field)
        if getattr(graph_dimension, "inherit_values_from_upstream", True):
            inheritable_fields.add(dimension_field)

    return inheritable_fields


def _get_axes_from_inherited_values(
    axes: list[list[Any]],
    dimension_fields: list[str],
    inherited_values_by_field: dict[str, set[str]],
) -> list[list[Any]] | None:
    """Narrow axes to inherited values while preserving configured value order."""
    narrowed_axes = []
    did_narrow = False
    for dimension_field, axis in zip(dimension_fields, axes):
        inherited_values = inherited_values_by_field.get(dimension_field)
        if not inherited_values:
            narrowed_axes.append(axis)
            continue

        narrowed_axis = [
            value for value in axis if str(value) in inherited_values
        ]
        if narrowed_axis:
            narrowed_axes.append(narrowed_axis)
            did_narrow = True
            continue

        log.warning(
            "Inherited graph dimension values for '%s' did not match any "
            "configured values; using configured values instead.",
            dimension_field,
        )
        narrowed_axes.append(axis)

    return narrowed_axes if did_narrow else None


def get_inherited_graph_dimension_axes(
    context: dg.OpExecutionContext,
    configured_axes: list[list[Any]],
    dimension_resource_info: _DimensionResourceInfo,
    op_ins: dict[str, dg.In],
) -> list[list[Any]] | None:
    """Return inherited graph-dimension axes from matching upstream materializations."""
    inheritable_fields = _get_inheritable_dimension_fields(
        context,
        dimension_resource_info,
    )
    if not inheritable_fields:
        return None

    inherited_values_by_field: dict[str, set[str]] = {}

    for input_name, op_in in op_ins.items():
        metadata = _get_latest_materialization_metadata(
            context,
            _get_upstream_asset_key(input_name, op_in),
        )
        signature, graph_dimensions = _get_graph_dimensions_metadata_parts(
            metadata
        )

        if not isinstance(signature, dict) or not isinstance(
            graph_dimensions, list
        ):
            continue
        if (
            signature.get("dimension_resource_key")
            != dimension_resource_info.param_name
        ):
            continue

        upstream_fields = set(signature.get("dimension_fields") or [])
        matching_fields = (
            upstream_fields
            & set(dimension_resource_info.dimension_fields)
            & inheritable_fields
        )
        if not matching_fields:
            continue

        for graph_dimension in graph_dimensions:
            if not isinstance(graph_dimension, dict):
                continue
            for field in matching_fields:
                if field in graph_dimension:
                    inherited_values_by_field.setdefault(field, set()).add(
                        str(graph_dimension[field])
                    )

    if not inherited_values_by_field:
        return None

    return _get_axes_from_inherited_values(
        configured_axes,
        dimension_resource_info.dimension_fields,
        inherited_values_by_field,
    )
