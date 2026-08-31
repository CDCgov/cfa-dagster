from enum import StrEnum
from pathlib import Path
from typing import Any

import dagster as dg
import pytest

from cfa_dagster.azure_adls2.pickle_io_manager import ADLS2PickleIOManager
from cfa_dagster.dynamic_graph import GraphDimension, dynamic_graph_asset
from cfa_dagster.dynamic_graph.metadata import (
    DYNAMIC_GRAPH_ASSET_METADATA_KEY,
    DYNAMIC_GRAPH_IO_MANAGER_METADATA_KEY,
    DynamicGraphIOManagerMetadata,
)

_GRAPH_DIMENSION_FIELDS_METADATA_KEY = "cfa_dagster/graph_dimension_fields"
_GRAPH_DIMENSIONS_METADATA_KEY = "cfa_dagster/graph_dimensions"
_GRAPH_DIMENSIONS_MATERIALIZED_METADATA_KEY = (
    "cfa_dagster/graph_dimensions_materialized"
)
_GRAPH_DIMENSIONS_MD_METADATA_KEY = "cfa_dagster/graph_dimensions_md"
_GRAPH_DIMENSIONS_MATERIALIZED_MD_METADATA_KEY = (
    "cfa_dagster/graph_dimensions_materialized_md"
)
_GRAPH_DIMENSION_SIGNATURE_METADATA_KEY = (
    "cfa_dagster/graph_dimension_signature"
)

_NO_RETURN_VALUES: list[str] = []
_FILE_OUTPUT_DIR: Path | None = None
_FIRST_MODE_CONSUMED: list[dict[str, str]] = []
_INHERITED_VALUES: list[dict[str, str]] = []


class NoReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class ObjectReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class FileReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class FirstModeDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class AlphaEnum(StrEnum):
    a = "a"
    b = "b"
    c = "c"


class NumberEnum(StrEnum):
    one = "one"
    two = "two"


class ReverseAlphaEnum(StrEnum):
    z = "z"
    y = "y"
    x = "x"


class EnumAllModeDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b"])


class EnumFirstModeDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b"])


class MultiEnumDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b"])
    number: GraphDimension[NumberEnum] = GraphDimension(["one", "two"])


class FirstModeMultiEnumDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b", "c"])
    reverse_letter: GraphDimension[ReverseAlphaEnum] = GraphDimension(
        ["x", "y"]
    )


class InheritanceDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b", "c"])


class OtherInheritanceDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b", "c"])


class OtherFieldInheritanceDims(dg.ConfigurableResource):
    number: GraphDimension[NumberEnum] = GraphDimension(["one", "two"])


class InheritanceOptOutDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(
        ["a", "b", "c"],
        inherit_values_from_upstream=False,
    )


class MixedInheritanceDims(dg.ConfigurableResource):
    letter: GraphDimension[AlphaEnum] = GraphDimension(["a", "b", "c"])
    number: GraphDimension[NumberEnum] = GraphDimension(
        ["one", "two"],
        inherit_values_from_upstream=False,
    )


class RecordingIOManager(dg.IOManager):
    def __init__(self):
        self.outputs: list[dict[str, Any]] = []
        self._step_outputs: dict[tuple[str, str], Any] = {}
        self._asset_outputs: dict[tuple[str, ...], Any] = {}

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        meta = DynamicGraphIOManagerMetadata.from_metadata(
            context.output_metadata or {}
        )
        self.outputs.append(
            {
                "step_key": context.step_key,
                "obj": obj,
                "metadata": meta,
            }
        )
        self._step_outputs[(context.step_key, context.name)] = obj
        if context.has_asset_key:
            self._asset_outputs[tuple(context.asset_key.path)] = obj

    def load_input(self, context: dg.InputContext) -> Any:
        if context.upstream_output:
            step_key = context.upstream_output.step_key
            output_name = context.upstream_output.name
            if (step_key, output_name) in self._step_outputs:
                return self._step_outputs[(step_key, output_name)]

        if context.has_asset_key:
            asset_key = tuple(context.asset_key.path)
            if asset_key in self._asset_outputs:
                return self._asset_outputs[asset_key]

        raise AssertionError(
            f"Unexpected input load for {context.name}: {context.definition_metadata}"
        )


@pytest.fixture(autouse=True)
def _use_in_memory_internal_config_io_manager(monkeypatch):
    class FakeInternalConfigIOManager:
        def __init__(self):
            self._step_outputs: dict[tuple[str, str], Any] = {}

        def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
            self._step_outputs[(context.step_key, context.name)] = obj

        def load_input(self, context: dg.InputContext) -> Any:
            if context.upstream_output:
                key = (
                    context.upstream_output.step_key,
                    context.upstream_output.name,
                )
                if key in self._step_outputs:
                    return self._step_outputs[key]

            raise AssertionError(
                f"Unexpected internal input load: {context.name}"
            )

    manager = FakeInternalConfigIOManager()
    monkeypatch.setattr(
        ADLS2PickleIOManager,
        "_internal_io_manager",
        property(lambda self: manager),
    )


def _asset_materialization_metadata(result, asset_key: dg.AssetKey):
    for event in result.get_asset_materialization_events():
        materialization = event.event_specific_data.materialization
        if materialization.asset_key == asset_key:
            return materialization.metadata
    raise AssertionError(f"No materialization found for {asset_key}")


def _graph_dimensions_metadata(metadata):
    return metadata[_GRAPH_DIMENSIONS_METADATA_KEY].value


def _assert_legacy_graph_dimension_keys_absent(metadata):
    assert _GRAPH_DIMENSION_FIELDS_METADATA_KEY not in metadata
    assert _GRAPH_DIMENSIONS_MATERIALIZED_METADATA_KEY not in metadata
    assert _GRAPH_DIMENSION_SIGNATURE_METADATA_KEY not in metadata
    assert _GRAPH_DIMENSIONS_MATERIALIZED_MD_METADATA_KEY not in metadata
    assert DYNAMIC_GRAPH_IO_MANAGER_METADATA_KEY not in metadata


@dynamic_graph_asset
def dynamic_asset_no_return(
    context: dg.OpExecutionContext,
    no_return_dims: NoReturnDims,
) -> None:
    _NO_RETURN_VALUES.append(no_return_dims.letter.current_value)


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="all")
def dynamic_asset_returns_objects(
    context: dg.OpExecutionContext,
    object_return_dims: ObjectReturnDims,
) -> dict[str, str]:
    return {"letter": object_return_dims.letter.current_value}


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="all")
def dynamic_asset_returns_files(
    context: dg.OpExecutionContext,
    file_return_dims: FileReturnDims,
) -> Path:
    assert _FILE_OUTPUT_DIR is not None
    path = _FILE_OUTPUT_DIR / f"{file_return_dims.letter.current_value}.txt"
    path.write_text(file_return_dims.letter.current_value)
    return path


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="first")
def dynamic_asset_returns_first_object(
    context: dg.OpExecutionContext,
    first_mode_dims: FirstModeDims,
) -> dict[str, str]:
    return {"letter": first_mode_dims.letter.current_value}


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="all")
def dynamic_asset_returns_enum_objects_all(
    context: dg.OpExecutionContext,
    enum_all_mode_dims: EnumAllModeDims,
) -> dict[str, str]:
    return {"letter": enum_all_mode_dims.letter.current_value}


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="first")
def dynamic_asset_returns_enum_objects_first(
    context: dg.OpExecutionContext,
    enum_first_mode_dims: EnumFirstModeDims,
) -> dict[str, str]:
    return {"letter": enum_first_mode_dims.letter.current_value}


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="all")
def dynamic_asset_returns_multi_enum_objects(
    context: dg.OpExecutionContext,
    multi_enum_dims: MultiEnumDims,
) -> dict[str, str]:
    return {
        "letter": multi_enum_dims.letter.current_value,
        "number": multi_enum_dims.number.current_value,
    }


@dynamic_graph_asset(io_manager_key="recording_io", output_mode="first")
def dynamic_asset_returns_multi_enum_objects_first(
    context: dg.OpExecutionContext,
    first_mode_multi_enum_dims: FirstModeMultiEnumDims,
) -> dict[str, str]:
    return {
        "letter": first_mode_multi_enum_dims.letter.current_value,
        "reverse_letter": first_mode_multi_enum_dims.reverse_letter.current_value,
    }


@dynamic_graph_asset
def inheritance_upstream(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    pass


@dynamic_graph_asset
def inheritance_upstream_b(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    pass


@dynamic_graph_asset
def same_resource_key_different_field_upstream(
    context: dg.OpExecutionContext,
    inheritance_dims: OtherFieldInheritanceDims,
) -> None:
    pass


@dynamic_graph_asset(
    ins={"inheritance_upstream": dg.In(dg.Nothing)},
)
def inheritance_downstream(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    _INHERITED_VALUES.append(
        {"letter": str(inheritance_dims.letter.current_value)}
    )


@dynamic_graph_asset(
    ins={
        "inheritance_upstream": dg.In(dg.Nothing),
        "inheritance_upstream_b": dg.In(dg.Nothing),
    },
)
def inheritance_multi_upstream_downstream(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    _INHERITED_VALUES.append(
        {"letter": str(inheritance_dims.letter.current_value)}
    )


@dynamic_graph_asset(
    ins={
        "inheritance_upstream": dg.In(dg.Nothing),
        "other_resource_key_upstream": dg.In(dg.Nothing),
    },
)
def matching_and_different_resource_key_downstream(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    _INHERITED_VALUES.append(
        {"letter": str(inheritance_dims.letter.current_value)}
    )


@dynamic_graph_asset(
    ins={
        "inheritance_upstream": dg.In(dg.Nothing),
        "same_resource_key_different_field_upstream": dg.In(dg.Nothing),
    },
)
def matching_and_different_field_downstream(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    _INHERITED_VALUES.append(
        {"letter": str(inheritance_dims.letter.current_value)}
    )


@dynamic_graph_asset
def other_resource_key_upstream(
    context: dg.OpExecutionContext,
    other_inheritance_dims: OtherInheritanceDims,
) -> None:
    pass


@dynamic_graph_asset(
    ins={"other_resource_key_upstream": dg.In(dg.Nothing)},
)
def different_resource_key_downstream(
    context: dg.OpExecutionContext,
    inheritance_dims: InheritanceDims,
) -> None:
    _INHERITED_VALUES.append(
        {"letter": str(inheritance_dims.letter.current_value)}
    )


@dynamic_graph_asset
def inheritance_opt_out_upstream(
    context: dg.OpExecutionContext,
    inheritance_opt_out_dims: InheritanceOptOutDims,
) -> None:
    pass


@dynamic_graph_asset(
    ins={"inheritance_opt_out_upstream": dg.In(dg.Nothing)},
)
def inheritance_opt_out_downstream(
    context: dg.OpExecutionContext,
    inheritance_opt_out_dims: InheritanceOptOutDims,
) -> None:
    _INHERITED_VALUES.append(
        {"letter": str(inheritance_opt_out_dims.letter.current_value)}
    )


@dynamic_graph_asset
def mixed_inheritance_upstream(
    context: dg.OpExecutionContext,
    mixed_inheritance_dims: MixedInheritanceDims,
) -> None:
    pass


@dynamic_graph_asset(
    ins={"mixed_inheritance_upstream": dg.In(dg.Nothing)},
)
def mixed_inheritance_downstream(
    context: dg.OpExecutionContext,
    mixed_inheritance_dims: MixedInheritanceDims,
) -> None:
    _INHERITED_VALUES.append(
        {
            "letter": str(mixed_inheritance_dims.letter.current_value),
            "number": str(mixed_inheritance_dims.number.current_value),
        }
    )


@dg.asset
def normal_asset_consumes_first_object(
    dynamic_asset_returns_first_object: dict[str, str],
) -> dict[str, str]:
    _FIRST_MODE_CONSUMED.append(dynamic_asset_returns_first_object)
    return dynamic_asset_returns_first_object


def test_dynamic_graph_asset_without_return_runs_all_dimensions():
    _NO_RETURN_VALUES.clear()

    result = dg.materialize(
        [dynamic_asset_no_return],
        resources={"no_return_dims": NoReturnDims()},
    )

    assert result.success
    assert sorted(_NO_RETURN_VALUES) == ["a", "b"]
    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_no_return.key,
    )
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["signature"]["dimension_fields"] == ["letter"]
    assert graph_dimensions["materialized"] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    assert _GRAPH_DIMENSIONS_MD_METADATA_KEY in metadata
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_metadata_records_output_mode():
    assert dynamic_asset_no_return.metadata_by_key[
        dynamic_asset_no_return.key
    ][DYNAMIC_GRAPH_ASSET_METADATA_KEY] == {"output_mode": "all"}
    assert dynamic_asset_returns_objects.metadata_by_key[
        dynamic_asset_returns_objects.key
    ][DYNAMIC_GRAPH_ASSET_METADATA_KEY] == {"output_mode": "all"}


def test_dynamic_graph_asset_materialization_records_dimension_signature():
    result = dg.materialize(
        [dynamic_asset_returns_enum_objects_all],
        resources={
            "enum_all_mode_dims": EnumAllModeDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                RecordingIOManager()
            ),
        },
    )

    assert result.success
    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_enum_objects_all.key,
    )
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["signature"] == {
        "dimension_resource_key": "enum_all_mode_dims",
        "dimension_fields": ["letter"],
        "enum_domains": {"letter": ["a", "b", "c"]},
    }
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_returning_objects_does_not_load_collected_outputs_in_all_mode():
    recording_io = RecordingIOManager()

    result = dg.materialize(
        [dynamic_asset_returns_objects],
        resources={
            "object_return_dims": ObjectReturnDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                recording_io
            ),
        },
    )

    assert result.success
    assert [output["obj"] for output in recording_io.outputs] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    assert [
        output["metadata"].synthetic_partition_keys
        for output in recording_io.outputs
    ] == [["a"], ["b"]]

    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_objects.key,
    )
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["signature"]["dimension_fields"] == ["letter"]
    assert graph_dimensions["materialized"] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    assert (
        "Enum Dimension Coverage"
        not in metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    )
    assert (
        "Graph Dimension Domain Coverage"
        not in metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    )
    assert "| a |" in metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_returning_files_does_not_load_collected_outputs_in_all_mode(
    tmp_path,
):
    global _FILE_OUTPUT_DIR
    _FILE_OUTPUT_DIR = tmp_path
    recording_io = RecordingIOManager()

    result = dg.materialize(
        [dynamic_asset_returns_files],
        resources={
            "file_return_dims": FileReturnDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                recording_io
            ),
        },
    )

    assert result.success
    assert [output["obj"].read_text() for output in recording_io.outputs] == [
        "a",
        "b",
    ]
    assert [
        output["metadata"].synthetic_partition_keys
        for output in recording_io.outputs
    ] == [["a"], ["b"]]


def test_dynamic_graph_asset_output_mode_first_can_feed_normal_asset():
    _FIRST_MODE_CONSUMED.clear()
    recording_io = RecordingIOManager()

    result = dg.materialize(
        [
            dynamic_asset_returns_first_object,
            normal_asset_consumes_first_object,
        ],
        resources={
            "first_mode_dims": FirstModeDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                recording_io
            ),
        },
    )

    assert result.success
    assert _FIRST_MODE_CONSUMED == [{"letter": "a"}]

    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_first_object.key,
    )
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["signature"]["dimension_fields"] == ["letter"]
    assert graph_dimensions["materialized"] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_inherits_upstream_dimension_values_by_default():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    upstream_result = dg.materialize(
        [inheritance_upstream],
        resources={
            "inheritance_dims": InheritanceDims(letter=GraphDimension(["a"])),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(inheritance_upstream.key),
            inheritance_downstream,
        ],
        resources={"inheritance_dims": InheritanceDims()},
        instance=instance,
    )

    assert upstream_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [{"letter": "a"}]
    metadata = _asset_materialization_metadata(
        downstream_result,
        inheritance_downstream.key,
    )
    assert _graph_dimensions_metadata(metadata)["materialized"] == [
        {"letter": "a"},
    ]
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_unions_values_from_multiple_matching_upstreams():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    upstream_a_result = dg.materialize(
        [inheritance_upstream],
        resources={
            "inheritance_dims": InheritanceDims(letter=GraphDimension(["a"])),
        },
        instance=instance,
    )
    upstream_b_result = dg.materialize(
        [inheritance_upstream_b],
        resources={
            "inheritance_dims": InheritanceDims(letter=GraphDimension(["b"])),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(inheritance_upstream.key),
            dg.AssetSpec(inheritance_upstream_b.key),
            inheritance_multi_upstream_downstream,
        ],
        resources={"inheritance_dims": InheritanceDims()},
        instance=instance,
    )

    assert upstream_a_result.success
    assert upstream_b_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [{"letter": "a"}, {"letter": "b"}]
    metadata = _asset_materialization_metadata(
        downstream_result,
        inheritance_multi_upstream_downstream.key,
    )
    assert _graph_dimensions_metadata(metadata)["materialized"] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_ignores_non_matching_resource_key_with_matching_upstream():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    matching_result = dg.materialize(
        [inheritance_upstream],
        resources={
            "inheritance_dims": InheritanceDims(letter=GraphDimension(["a"])),
        },
        instance=instance,
    )
    non_matching_result = dg.materialize(
        [other_resource_key_upstream],
        resources={
            "other_inheritance_dims": OtherInheritanceDims(
                letter=GraphDimension(["b"])
            ),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(inheritance_upstream.key),
            dg.AssetSpec(other_resource_key_upstream.key),
            matching_and_different_resource_key_downstream,
        ],
        resources={"inheritance_dims": InheritanceDims()},
        instance=instance,
    )

    assert matching_result.success
    assert non_matching_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [{"letter": "a"}]
    metadata = _asset_materialization_metadata(
        downstream_result,
        matching_and_different_resource_key_downstream.key,
    )
    assert _graph_dimensions_metadata(metadata)["materialized"] == [
        {"letter": "a"},
    ]
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_ignores_non_matching_fields_with_matching_upstream():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    matching_result = dg.materialize(
        [inheritance_upstream],
        resources={
            "inheritance_dims": InheritanceDims(letter=GraphDimension(["a"])),
        },
        instance=instance,
    )
    non_matching_result = dg.materialize(
        [same_resource_key_different_field_upstream],
        resources={
            "inheritance_dims": OtherFieldInheritanceDims(
                number=GraphDimension(["two"])
            ),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(inheritance_upstream.key),
            dg.AssetSpec(same_resource_key_different_field_upstream.key),
            matching_and_different_field_downstream,
        ],
        resources={"inheritance_dims": InheritanceDims()},
        instance=instance,
    )

    assert matching_result.success
    assert non_matching_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [{"letter": "a"}]
    metadata = _asset_materialization_metadata(
        downstream_result,
        matching_and_different_field_downstream.key,
    )
    assert _graph_dimensions_metadata(metadata)["materialized"] == [
        {"letter": "a"},
    ]
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_does_not_inherit_from_different_resource_key():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    upstream_result = dg.materialize(
        [other_resource_key_upstream],
        resources={
            "other_inheritance_dims": OtherInheritanceDims(
                letter=GraphDimension(["a"])
            ),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(other_resource_key_upstream.key),
            different_resource_key_downstream,
        ],
        resources={"inheritance_dims": InheritanceDims()},
        instance=instance,
    )

    assert upstream_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [
        {"letter": "a"},
        {"letter": "b"},
        {"letter": "c"},
    ]


def test_dynamic_graph_asset_can_opt_out_of_dimension_value_inheritance():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    upstream_result = dg.materialize(
        [inheritance_opt_out_upstream],
        resources={
            "inheritance_opt_out_dims": InheritanceOptOutDims(
                letter=GraphDimension(
                    ["a"],
                    inherit_values_from_upstream=False,
                )
            ),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(inheritance_opt_out_upstream.key),
            inheritance_opt_out_downstream,
        ],
        resources={"inheritance_opt_out_dims": InheritanceOptOutDims()},
        instance=instance,
    )

    assert upstream_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [
        {"letter": "a"},
        {"letter": "b"},
        {"letter": "c"},
    ]


def test_dynamic_graph_asset_inherits_only_enabled_dimensions():
    _INHERITED_VALUES.clear()
    instance = dg.DagsterInstance.ephemeral()

    upstream_result = dg.materialize(
        [mixed_inheritance_upstream],
        resources={
            "mixed_inheritance_dims": MixedInheritanceDims(
                letter=GraphDimension(["a"]),
                number=GraphDimension(
                    ["one"],
                    inherit_values_from_upstream=False,
                ),
            ),
        },
        instance=instance,
    )
    downstream_result = dg.materialize(
        [
            dg.AssetSpec(mixed_inheritance_upstream.key),
            mixed_inheritance_downstream,
        ],
        resources={"mixed_inheritance_dims": MixedInheritanceDims()},
        instance=instance,
    )

    assert upstream_result.success
    assert downstream_result.success
    assert _INHERITED_VALUES == [
        {"letter": "a", "number": "one"},
        {"letter": "a", "number": "two"},
    ]
    metadata = _asset_materialization_metadata(
        downstream_result,
        mixed_inheritance_downstream.key,
    )
    assert _graph_dimensions_metadata(metadata)["materialized"] == [
        {"letter": "a", "number": "one"},
        {"letter": "a", "number": "two"},
    ]
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_enum_coverage_markdown_all_mode():
    result = dg.materialize(
        [dynamic_asset_returns_enum_objects_all],
        resources={
            "enum_all_mode_dims": EnumAllModeDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                RecordingIOManager()
            ),
        },
    )

    assert result.success
    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_enum_objects_all.key,
    )
    markdown = metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["materialized"] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    assert graph_dimensions["coverage"] == {
        "enum_dimensions": [
            {
                "dimension": "letter",
                "materialized_value_count": 2,
                "enum_domain_count": 3,
                "coverage_percent": 2 / 3 * 100,
            }
        ],
        "cartesian_enum_domain": {
            "materialized_count": 2,
            "total_domain_count": 3,
            "coverage_percent": 2 / 3 * 100,
        },
    }
    assert "### Enum Dimension Coverage" in markdown
    assert "| letter | 2 | 3 | 66.67% |" in markdown
    assert "### Graph Dimension Domain Coverage" in markdown
    assert "| 2 | 3 | 66.67% |" in markdown
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_enum_coverage_markdown_first_mode():
    result = dg.materialize(
        [dynamic_asset_returns_enum_objects_first],
        resources={
            "enum_first_mode_dims": EnumFirstModeDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                RecordingIOManager()
            ),
        },
    )

    assert result.success
    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_enum_objects_first.key,
    )
    markdown = metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["materialized"] == [
        {"letter": "a"},
        {"letter": "b"},
    ]
    assert graph_dimensions["coverage"] == {
        "enum_dimensions": [
            {
                "dimension": "letter",
                "materialized_value_count": 2,
                "enum_domain_count": 3,
                "coverage_percent": 2 / 3 * 100,
            }
        ],
        "cartesian_enum_domain": {
            "materialized_count": 2,
            "total_domain_count": 3,
            "coverage_percent": 2 / 3 * 100,
        },
    }
    assert "### Enum Dimension Coverage" in markdown
    assert "| letter | 2 | 3 | 66.67% |" in markdown
    assert "### Graph Dimension Domain Coverage" in markdown
    assert "| 2 | 3 | 66.67% |" in markdown
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_first_mode_metadata_records_full_fanout():
    recording_io = RecordingIOManager()

    result = dg.materialize(
        [dynamic_asset_returns_multi_enum_objects_first],
        resources={
            "first_mode_multi_enum_dims": FirstModeMultiEnumDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                recording_io
            ),
        },
    )

    assert result.success
    assert [output["obj"] for output in recording_io.outputs] == [
        {"letter": "a", "reverse_letter": "x"},
        {"letter": "a", "reverse_letter": "x"},
    ]
    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_multi_enum_objects_first.key,
    )
    markdown = metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    graph_dimensions = _graph_dimensions_metadata(metadata)
    assert graph_dimensions["materialized"] == [
        {"letter": "a", "reverse_letter": "x"},
        {"letter": "a", "reverse_letter": "y"},
        {"letter": "b", "reverse_letter": "x"},
        {"letter": "b", "reverse_letter": "y"},
        {"letter": "c", "reverse_letter": "x"},
        {"letter": "c", "reverse_letter": "y"},
    ]
    assert graph_dimensions["coverage"] == {
        "enum_dimensions": [
            {
                "dimension": "letter",
                "materialized_value_count": 3,
                "enum_domain_count": 3,
                "coverage_percent": 100.0,
            },
            {
                "dimension": "reverse_letter",
                "materialized_value_count": 2,
                "enum_domain_count": 3,
                "coverage_percent": 2 / 3 * 100,
            },
        ],
        "cartesian_enum_domain": {
            "materialized_count": 6,
            "total_domain_count": 9,
            "coverage_percent": 6 / 9 * 100,
        },
    }
    assert "| letter | 3 | 3 | 100.00% |" in markdown
    assert "| reverse_letter | 2 | 3 | 66.67% |" in markdown
    assert "| 6 | 9 | 66.67% |" in markdown
    _assert_legacy_graph_dimension_keys_absent(metadata)


def test_dynamic_graph_asset_enum_domain_coverage_multiplies_dimensions():
    result = dg.materialize(
        [dynamic_asset_returns_multi_enum_objects],
        resources={
            "multi_enum_dims": MultiEnumDims(),
            "recording_io": dg.IOManagerDefinition.hardcoded_io_manager(
                RecordingIOManager()
            ),
        },
    )

    assert result.success
    metadata = _asset_materialization_metadata(
        result,
        dynamic_asset_returns_multi_enum_objects.key,
    )
    markdown = metadata[_GRAPH_DIMENSIONS_MD_METADATA_KEY].md_str
    assert _graph_dimensions_metadata(metadata)["materialized"] == [
        {"letter": "a", "number": "one"},
        {"letter": "a", "number": "two"},
        {"letter": "b", "number": "one"},
        {"letter": "b", "number": "two"},
    ]
    assert "| letter | 2 | 3 | 66.67% |" in markdown
    assert "| number | 2 | 2 | 100.00% |" in markdown
    assert "### Graph Dimension Domain Coverage" in markdown
    assert "| 4 | 6 | 66.67% |" in markdown
    _assert_legacy_graph_dimension_keys_absent(metadata)
