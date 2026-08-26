from pathlib import Path
from typing import Any

import dagster as dg

from cfa_dagster.dynamic_graph_asset import GraphDimension, dynamic_graph_asset
from cfa_dagster.dynamic_graph_asset_metadata import (
    DYNAMIC_GRAPH_ASSET_METADATA_KEY,
    DynamicGraphIOManagerMetadata,
)

_NO_RETURN_VALUES: list[str] = []
_FILE_OUTPUT_DIR: Path | None = None
_FIRST_MODE_CONSUMED: list[dict[str, str]] = []


class NoReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class ObjectReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class FileReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class FirstModeDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class RecordingIOManager(dg.IOManager):
    def __init__(self):
        self.outputs: list[dict[str, Any]] = []
        self.skipped_inputs: list[str] = []
        self.skipped_outputs: list[dict[str, Any]] = []
        self._step_outputs: dict[tuple[str, str], Any] = {}
        self._asset_outputs: dict[tuple[str, ...], Any] = {}

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        meta = DynamicGraphIOManagerMetadata.from_metadata(
            context.output_metadata or {}
        )
        if meta and meta.skip_output:
            self.skipped_outputs.append(
                {
                    "step_key": context.step_key,
                    "obj": obj,
                    "metadata": meta,
                }
            )
            return

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
        meta = DynamicGraphIOManagerMetadata.from_metadata(
            context.definition_metadata
        )
        if meta and meta.skip_input:
            self.skipped_inputs.append(context.name)
            return None

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


def test_dynamic_graph_asset_metadata_records_output_mode():
    assert dynamic_asset_no_return.metadata_by_key[dynamic_asset_no_return.key][
        DYNAMIC_GRAPH_ASSET_METADATA_KEY
    ] == {"output_mode": "all"}
    assert dynamic_asset_returns_objects.metadata_by_key[
        dynamic_asset_returns_objects.key
    ][DYNAMIC_GRAPH_ASSET_METADATA_KEY] == {"output_mode": "all"}


def test_dynamic_graph_asset_returning_objects_skips_output_transport_loads():
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
    assert recording_io.skipped_inputs == ["compute_result", "compute_result"]
    assert len(recording_io.skipped_outputs) == 1


def test_dynamic_graph_asset_returning_files_skips_output_transport_loads(
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
    assert recording_io.skipped_inputs == ["compute_result", "compute_result"]
    assert len(recording_io.skipped_outputs) == 1


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
    assert recording_io.skipped_inputs == []
    assert recording_io.skipped_outputs == []
