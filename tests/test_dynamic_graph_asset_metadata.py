import dagster as dg
from dagster._core.storage.upath_io_manager import UPathIOManager

from cfa_dagster.azure_adls2.filesystem_io_manager import (
    FilesystemADLS2IOManager,
)
from cfa_dagster.azure_adls2.pickle_io_manager import ADLS2PickleIOManager
from cfa_dagster.dynamic_graph_asset_metadata import (
    SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS,
    DynamicGraphIOManagerMetadata,
    _decode_mapping_key,
    _encode_mapping_key,
    get_inherited_graph_dimension_input_metadata,
    patch_context_with_dynamic_graph_metadata,
)


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


class _FakeLog:
    def debug(self, message):
        pass


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


def test_dynamic_graph_io_manager_metadata_round_trips():
    metadata = DynamicGraphIOManagerMetadata(
        asset_key_path=["upstream"],
        asset_partition_keys=["2026-08-25"],
        synthetic_partition_keys=["A", "B"],
        skip_input=True,
        skip_output=True,
    )

    parsed = DynamicGraphIOManagerMetadata.from_metadata(metadata.to_dict())

    assert parsed == metadata


def test_patch_context_with_dynamic_graph_metadata_combines_partitions():
    class FakeContext:
        has_asset_partitions = False
        has_asset_key = False

    context = FakeContext()

    patch_context_with_dynamic_graph_metadata(
        context,
        DynamicGraphIOManagerMetadata(
            asset_key_path=["upstream"],
            asset_partition_keys=["2026-08-25"],
            synthetic_partition_keys=["A", "B"],
        ),
    )

    assert context.asset_key == dg.AssetKey(["upstream"])
    assert context.has_asset_key is True
    assert context.asset_partition_keys == ["2026-08-25/A/B"]
    assert context.has_asset_partitions is True


def test_filesystem_load_input_patches_inherited_graph_dimensions(monkeypatch):
    def fake_load_input(self, context):
        assert context.asset_key == dg.AssetKey(["upstream"])
        assert context.asset_partition_keys == ["2026-08-25/A"]
        return "loaded"

    monkeypatch.setattr(UPathIOManager, "load_input", fake_load_input)

    class FakeInputContext(_FakeInputContext):
        pass

    manager = object.__new__(FilesystemADLS2IOManager)
    context = FakeInputContext(
        definition_metadata={
            SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS: True,
        },
        mapping_key=_encode_mapping_key(("A",)),
        asset_key=dg.AssetKey(["upstream"]),
        asset_partition_keys=["2026-08-25"],
    )

    assert manager.load_input(context) == "loaded"


def test_filesystem_load_input_honors_generic_skip(monkeypatch):
    def fake_load_input(self, context):
        raise AssertionError("skip_input should return before internal load")

    monkeypatch.setattr(UPathIOManager, "load_input", fake_load_input)

    class FakeInputContext(_FakeInputContext):
        pass

    manager = object.__new__(FilesystemADLS2IOManager)
    context = FakeInputContext(
        definition_metadata=DynamicGraphIOManagerMetadata(
            skip_input=True
        ).to_dict(),
        mapping_key=_encode_mapping_key(("A",)),
    )

    assert manager.load_input(context) is None


def test_filesystem_handle_output_honors_generic_skip(monkeypatch):
    def fake_handle_output(self, context, obj):
        raise AssertionError(
            "skip_output should return before internal output"
        )

    class FakeOutputContext:
        output_metadata = DynamicGraphIOManagerMetadata(
            skip_output=True
        ).to_dict()
        log = _FakeLog()

    monkeypatch.setattr(UPathIOManager, "handle_output", fake_handle_output)

    manager = object.__new__(FilesystemADLS2IOManager)
    assert manager.handle_output(FakeOutputContext(), object()) is None


def test_pickle_handle_output_patches_context_before_delegating(monkeypatch):
    captured = {}

    class FakeInternalIOManager:
        def handle_output(self, context, obj):
            captured["asset_key"] = context.asset_key
            captured["asset_partition_keys"] = context.asset_partition_keys
            captured["obj"] = obj

    class FakeOutputContext:
        has_asset_key = False
        has_asset_partitions = False

        def __init__(self):
            self.output_metadata = DynamicGraphIOManagerMetadata(
                asset_key_path=["pickle_upstream"],
                asset_partition_keys=["2026-08-25"],
                synthetic_partition_keys=["A"],
            ).to_dict()

    monkeypatch.setattr(
        ADLS2PickleIOManager,
        "_internal_io_manager",
        property(lambda self: FakeInternalIOManager()),
    )

    manager = ADLS2PickleIOManager(overrides={})
    manager.handle_output(FakeOutputContext(), {"value": 1})

    assert captured["asset_key"] == dg.AssetKey(["pickle_upstream"])
    assert captured["asset_partition_keys"] == ["2026-08-25/A"]
    assert captured["obj"] == {"value": 1}


def test_pickle_load_input_patches_context_before_delegating(monkeypatch):
    captured = {}

    class FakeInternalIOManager:
        def load_input(self, context):
            captured["asset_key"] = context.asset_key
            captured["asset_partition_keys"] = context.asset_partition_keys
            return {"loaded": True}

    class FakeInputContext(_FakeInputContext):
        upstream_output = None
        log = _FakeLog()

    monkeypatch.setattr(
        ADLS2PickleIOManager,
        "_internal_io_manager",
        property(lambda self: FakeInternalIOManager()),
    )

    context = FakeInputContext(
        definition_metadata={
            SHOULD_INPUT_MANAGER_INHERIT_GRAPH_DIMENSIONS: True,
        },
        mapping_key=_encode_mapping_key(("A",)),
        asset_key=dg.AssetKey(["pickle_upstream"]),
        asset_partition_keys=["2026-08-25"],
    )
    manager = ADLS2PickleIOManager(overrides={})

    assert manager.load_input(context) == {"loaded": True}
    assert captured["asset_key"] == dg.AssetKey(["pickle_upstream"])
    assert captured["asset_partition_keys"] == ["2026-08-25/A"]


def test_pickle_load_input_honors_generic_skip(monkeypatch):
    class FakeInternalIOManager:
        def load_input(self, context):
            raise AssertionError(
                "skip_input should return before internal load"
            )

    class FakeInputContext(_FakeInputContext):
        upstream_output = None
        log = _FakeLog()

    monkeypatch.setattr(
        ADLS2PickleIOManager,
        "_internal_io_manager",
        property(lambda self: FakeInternalIOManager()),
    )

    context = FakeInputContext(
        definition_metadata=DynamicGraphIOManagerMetadata(
            skip_input=True
        ).to_dict(),
        mapping_key=_encode_mapping_key(("A",)),
    )
    manager = ADLS2PickleIOManager(overrides={})

    assert manager.load_input(context) is None


def test_pickle_handle_output_honors_generic_skip(monkeypatch):
    class FakeInternalIOManager:
        def handle_output(self, context, obj):
            raise AssertionError(
                "skip_output should return before internal output"
            )

    class FakeOutputContext:
        output_metadata = DynamicGraphIOManagerMetadata(
            skip_output=True
        ).to_dict()
        log = _FakeLog()

    monkeypatch.setattr(
        ADLS2PickleIOManager,
        "_internal_io_manager",
        property(lambda self: FakeInternalIOManager()),
    )

    manager = ADLS2PickleIOManager(overrides={})
    assert manager.handle_output(FakeOutputContext(), object()) is None


def test_pickle_load_input_overrides_short_circuit(monkeypatch):
    class FakeInternalIOManager:
        def load_input(self, context):
            raise AssertionError("override should return before internal load")

    class FakeUpstreamOutput:
        has_asset_key = True
        asset_key = dg.AssetKey(["pickle_upstream"])

    class FakeInputContext:
        upstream_output = FakeUpstreamOutput()
        log = _FakeLog()

    monkeypatch.setattr(
        ADLS2PickleIOManager,
        "_internal_io_manager",
        property(lambda self: FakeInternalIOManager()),
    )

    manager = ADLS2PickleIOManager(
        overrides={"pickle_upstream": {"override": True}}
    )

    assert manager.load_input(FakeInputContext()) == {"override": True}
