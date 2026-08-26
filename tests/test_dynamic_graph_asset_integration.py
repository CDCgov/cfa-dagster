from pathlib import Path
from typing import Any

import dagster as dg

from cfa_dagster.dynamic_graph_asset import GraphDimension, dynamic_graph_asset
from cfa_dagster.dynamic_graph_asset_metadata import (
    DynamicGraphIOManagerMetadata,
)

_NO_RETURN_VALUES: list[str] = []
_FILE_OUTPUT_DIR: Path | None = None


class NoReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class ObjectReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class FileReturnDims(dg.ConfigurableResource):
    letter: GraphDimension[str] = GraphDimension(["a", "b"])


class RecordingIOManager(dg.IOManager):
    def __init__(self):
        self.outputs: list[dict[str, Any]] = []
        self.skipped_inputs: list[str] = []
        self.skipped_outputs: list[dict[str, Any]] = []

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

    def load_input(self, context: dg.InputContext) -> Any:
        meta = DynamicGraphIOManagerMetadata.from_metadata(
            context.definition_metadata
        )
        if meta and meta.skip_input:
            self.skipped_inputs.append(context.name)
            return None

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


def test_dynamic_graph_asset_without_return_runs_all_dimensions():
    _NO_RETURN_VALUES.clear()

    result = dg.materialize(
        [dynamic_asset_no_return],
        resources={"no_return_dims": NoReturnDims()},
    )

    assert result.success
    assert sorted(_NO_RETURN_VALUES) == ["a", "b"]


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
