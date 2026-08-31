import itertools
import math
from typing import Any, get_type_hints

import dagster as dg

from .types import (
    _DimensionResourceInfo,
    _ExclusionResourceInfo,
    _get_graph_dimension_value_type,
    _get_string_enum_values,
)

_GRAPH_DIMENSIONS_METADATA_KEY = "cfa_dagster/graph_dimensions"
_GRAPH_DIMENSIONS_MD_METADATA_KEY = "cfa_dagster/graph_dimensions_md"


def _graph_dimensions_markdown_table(
    dimension_fields: list[str],
    graph_dimensions: list[dict[str, str]],
    enum_dimension_coverage: list[dict[str, Any]] | None = None,
    cartesian_enum_domain_coverage: dict[str, Any] | None = None,
) -> str:
    """Render materialized graph dimensions and enum coverage for Dagster UI metadata."""
    if not graph_dimensions:
        markdown = "No graph dimensions materialized."
    else:
        header = "| " + " | ".join(dimension_fields) + " |"
        separator = "| " + " | ".join(["---"] * len(dimension_fields)) + " |"
        rows = [
            "| "
            + " | ".join(
                str(graph_dimension[field]) for field in dimension_fields
            )
            + " |"
            for graph_dimension in graph_dimensions
        ]
        markdown = "\n".join(
            ["### Materialized Graph Dimensions", "", header, separator, *rows]
        )

    if enum_dimension_coverage:
        coverage_rows = [
            "| {dimension} | {materialized_value_count} | {enum_domain_count} | {coverage_percent:.2f}% |".format(
                **coverage
            )
            for coverage in enum_dimension_coverage
        ]
        markdown = "\n".join(
            [
                markdown,
                "",
                "### Enum Dimension Coverage",
                "",
                "| Dimension | Materialized values | Enum domain | Coverage |",
                "| --- | ---: | ---: | ---: |",
                *coverage_rows,
            ]
        )

    if cartesian_enum_domain_coverage:
        markdown = "\n".join(
            [
                markdown,
                "",
                "### Graph Dimension Domain Coverage",
                "",
                "| Materialized combinations | Total enum domain | Coverage |",
                "| ---: | ---: | ---: |",
                "| {materialized_count} | {total_domain_count} | {coverage_percent:.2f}% |".format(
                    **cartesian_enum_domain_coverage
                ),
            ]
        )

    return markdown


def _get_graph_dimension_axes(
    context,
    dimension_resource_info: _DimensionResourceInfo,
    exclusion_resources_info: list[_ExclusionResourceInfo],
) -> list[list[Any]]:
    """Return dimension value axes after applying any configured exclusions."""
    dimension_resource = getattr(
        context.resources,
        dimension_resource_info.param_name,
    )

    axes = []

    for dimension_field in dimension_resource_info.dimension_fields:
        graph_dimension = getattr(
            dimension_resource,
            dimension_field,
        )

        values = graph_dimension.values

        if exclusion_resources_info:
            excluded_values: set = set()
            for excl_info in exclusion_resources_info:
                excl_resource = getattr(
                    context.resources, excl_info.param_name
                )
                exclusion_obj = getattr(
                    excl_resource,
                    dimension_field,
                    None,
                )
                if exclusion_obj is not None and exclusion_obj.values:
                    excluded_values.update(set(exclusion_obj.values))
            if excluded_values:
                values = [v for v in values if v not in excluded_values]

        axes.append(values)

    return axes


def _get_graph_dimension_combinations(
    dimension_fields: list[str],
    axes: list[list[Any]],
) -> list[dict[str, str]]:
    """Convert the cartesian product of dimension axes into metadata rows."""
    return [
        {field: str(value) for field, value in zip(dimension_fields, combo)}
        for combo in itertools.product(*axes)
    ]


def _get_materialized_graph_dimensions(
    context,
    dimension_resource_info: _DimensionResourceInfo,
    exclusion_resources_info: list[_ExclusionResourceInfo],
    should_return_all: bool,
) -> list[dict[str, str]]:
    """Return the graph dimension combinations represented by the asset output."""
    graph_dimensions = _get_graph_dimension_combinations(
        dimension_resource_info.dimension_fields,
        _get_graph_dimension_axes(
            context,
            dimension_resource_info,
            exclusion_resources_info,
        ),
    )

    return graph_dimensions if should_return_all else graph_dimensions[:1]


def _get_enum_dimension_coverage(
    graph_dimensions: list[dict[str, str]],
    dimension_resource_info: _DimensionResourceInfo,
) -> list[dict[str, Any]]:
    """Summarize materialized coverage for dimensions backed by string enums."""
    resource_hints = get_type_hints(dimension_resource_info.resource_cls)
    enum_dimension_coverage = []

    for dimension_field in dimension_resource_info.dimension_fields:
        value_type = _get_graph_dimension_value_type(
            resource_hints.get(dimension_field)
        )
        enum_values = _get_string_enum_values(value_type)
        if enum_values is None:
            continue

        materialized_values = {
            graph_dimension[dimension_field]
            for graph_dimension in graph_dimensions
        }
        enum_domain_count = len(enum_values)
        materialized_value_count = len(materialized_values)
        enum_dimension_coverage.append(
            {
                "dimension": dimension_field,
                "materialized_value_count": materialized_value_count,
                "enum_domain_count": enum_domain_count,
                "coverage_percent": (
                    materialized_value_count / enum_domain_count * 100
                )
                if enum_domain_count
                else 0,
            }
        )

    return enum_dimension_coverage


def _get_graph_dimension_enum_domains(
    dimension_resource_info: _DimensionResourceInfo,
) -> dict[str, list[str]] | None:
    """Return enum domains only when every graph dimension has a string enum type."""
    enum_domains = _get_graph_dimension_enum_domain_map(
        dimension_resource_info
    )
    if len(enum_domains) != len(dimension_resource_info.dimension_fields):
        return None
    return enum_domains


def _get_graph_dimension_enum_domain_map(
    dimension_resource_info: _DimensionResourceInfo,
) -> dict[str, list[str]]:
    """Return enum domains for graph dimensions that declare a string enum type."""
    resource_hints = get_type_hints(dimension_resource_info.resource_cls)
    enum_domains = {}

    for dimension_field in dimension_resource_info.dimension_fields:
        value_type = _get_graph_dimension_value_type(
            resource_hints.get(dimension_field)
        )
        enum_values = _get_string_enum_values(value_type)
        if enum_values is None:
            continue
        enum_domains[dimension_field] = enum_values

    return enum_domains


def _get_graph_dimension_signature(
    dimension_resource_info: _DimensionResourceInfo,
) -> dict[str, Any]:
    """Describe the graph-dimension resource contract without Python class identity."""
    return {
        "dimension_resource_key": dimension_resource_info.param_name,
        "dimension_fields": dimension_resource_info.dimension_fields,
        "enum_domains": _get_graph_dimension_enum_domain_map(
            dimension_resource_info
        ),
    }


def _get_cartesian_enum_domain_coverage(
    graph_dimensions: list[dict[str, str]],
    dimension_resource_info: _DimensionResourceInfo,
) -> dict[str, Any] | None:
    """Calculate materialized combinations over the full enum cartesian domain."""
    enum_domains = _get_graph_dimension_enum_domains(dimension_resource_info)
    if enum_domains is None:
        return None

    dimension_fields = dimension_resource_info.dimension_fields
    materialized_count = len(
        {
            tuple(graph_dimension[field] for field in dimension_fields)
            for graph_dimension in graph_dimensions
        }
    )
    total_domain_count = math.prod(
        len(enum_domains[field]) for field in dimension_fields
    )

    return {
        "materialized_count": materialized_count,
        "total_domain_count": total_domain_count,
        "coverage_percent": materialized_count / total_domain_count * 100
        if total_domain_count
        else 0,
    }


def _get_materialized_graph_dimensions_metadata(
    context,
    dimension_resource_info: _DimensionResourceInfo,
    exclusion_resources_info: list[_ExclusionResourceInfo],
    should_return_all: bool,
    graph_dimensions: list[dict[str, str]] | None = None,
) -> dict:
    """Build Dagster materialization metadata for dynamic graph dimensions."""
    if graph_dimensions is None:
        graph_dimensions = _get_materialized_graph_dimensions(
            context,
            dimension_resource_info,
            exclusion_resources_info,
            should_return_all,
        )
    enum_dimension_coverage = _get_enum_dimension_coverage(
        graph_dimensions,
        dimension_resource_info,
    )
    cartesian_enum_domain_coverage = _get_cartesian_enum_domain_coverage(
        graph_dimensions,
        dimension_resource_info,
    )

    signature = _get_graph_dimension_signature(dimension_resource_info)
    return {
        _GRAPH_DIMENSIONS_METADATA_KEY: dg.MetadataValue.json(
            {
                "materialized": graph_dimensions,
                "signature": signature,
                "coverage": {
                    "enum_dimensions": enum_dimension_coverage,
                    "cartesian_enum_domain": cartesian_enum_domain_coverage,
                },
            }
        ),
        _GRAPH_DIMENSIONS_MD_METADATA_KEY: dg.MetadataValue.md(
            _graph_dimensions_markdown_table(
                dimension_resource_info.dimension_fields,
                graph_dimensions,
                enum_dimension_coverage,
                cartesian_enum_domain_coverage,
            )
        ),
    }
