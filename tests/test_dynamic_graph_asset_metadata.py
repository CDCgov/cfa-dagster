import dagster as dg

from cfa_dagster.azure_adls2.filesystem_io_manager import (
    FilesystemADLS2IOManager,
)
from cfa_dagster.azure_adls2.filesystem_metadata import (
    ADLS2FilesystemIOManagerMetadata,
)
from cfa_dagster.dynamic_graph_asset_metadata import (
    SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS,
    _decode_mapping_key,
    _encode_mapping_key,
    get_inherited_graph_dimension_input_metadata,
)
from dagster._core.storage.upath_io_manager import UPathIOManager


class _FakeStep:
    def __init__(self, mapping_key):
        self._mapping_key = mapping_key

    def get_mapping_key(self):
        return self._mapping_key


class _FakeStepContext:
    def __init__(self, mapping_key):
        self.step = _FakeStep(mapping_key)


class _FakeInputContext:
    def __init__(
        self,
        *,
        definition_metadata,
        mapping_key,
        asset_key=dg.AssetKey(["upstream"]),
        asset_partition_keys=None,
    ):
        self.definition_metadata = definition_metadata
        self.step_context = _FakeStepContext(mapping_key)
        self.asset_key = asset_key
        self.has_asset_key = asset_key is not None
        self._asset_partition_keys = asset_partition_keys or []
        self.has_asset_partitions = bool(asset_partition_keys)

    @property
    def asset_partition_keys(self):
        return self._asset_partition_keys


def test_mapping_key_round_trips_special_characters():
    values = ("A_B", "flu/covid", "New York", "x-y")

    assert _decode_mapping_key(_encode_mapping_key(values)) == list(values)


def test_inherited_graph_dimension_metadata_requires_opt_in():
    context = _FakeInputContext(
        definition_metadata={},
        mapping_key=_encode_mapping_key(("A",)),
    )

    assert get_inherited_graph_dimension_input_metadata(context) is None


def test_inherited_graph_dimension_metadata_uses_input_context():
    context = _FakeInputContext(
        definition_metadata={
            SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS: True,
        },
        mapping_key=_encode_mapping_key(("A", "B")),
        asset_key=dg.AssetKey(["prefix", "upstream"]),
        asset_partition_keys=["2026-08-25"],
    )

    metadata = get_inherited_graph_dimension_input_metadata(context)

    assert metadata is not None
    assert metadata.asset_key_path == ["prefix", "upstream"]
    assert metadata.asset_partition_keys == ["2026-08-25"]
    assert metadata.synthetic_partition_keys == ["A", "B"]


def test_filesystem_metadata_defaults_to_empty_synthetic_keys():
    metadata = ADLS2FilesystemIOManagerMetadata.from_metadata(
        ADLS2FilesystemIOManagerMetadata(input_mode="reference").to_dict()
    )

    assert metadata is not None
    assert metadata.synthetic_partition_keys == []


def test_filesystem_load_input_merges_inherited_metadata(monkeypatch):
    captured = {}

    def fake_patch_context(context, meta):
        captured["meta"] = meta

    def fake_load_input(self, context):
        return "loaded"

    monkeypatch.setattr(
        FilesystemADLS2IOManager,
        "_patch_context",
        staticmethod(fake_patch_context),
    )
    monkeypatch.setattr(UPathIOManager, "load_input", fake_load_input)

    manager = object.__new__(FilesystemADLS2IOManager)
    context = _FakeInputContext(
        definition_metadata={
            SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS: True,
            **ADLS2FilesystemIOManagerMetadata(
                input_mode="reference",
                on_input_conflict="merge",
            ).to_dict(),
        },
        mapping_key=_encode_mapping_key(("A",)),
        asset_key=dg.AssetKey(["upstream"]),
        asset_partition_keys=["2026-08-25"],
    )

    assert manager.load_input(context) == "loaded"

    meta = captured["meta"]
    assert meta.input_mode == "reference"
    assert meta.on_input_conflict == "merge"
    assert meta.asset_key_path == ["upstream"]
    assert meta.asset_partition_keys == ["2026-08-25"]
    assert meta.synthetic_partition_keys == ["A"]
