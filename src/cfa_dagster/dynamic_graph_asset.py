import ast
import inspect
import itertools
import logging
import re
import sys
import textwrap
from collections.abc import Sequence
from types import GeneratorType
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    TypedDict,
    Union,
    get_origin,
    get_type_hints,
)
from typing import Sequence as TypeSequence

import dagster as dg
from dagster._core.definitions.events import (
    CoercibleToAssetKey,
    CoercibleToAssetKeyPrefix,
)
from dagster._core.definitions.metadata import RawMetadataMapping
from typing_extensions import Unpack

from .execution.utils import ExecutionConfig, SelectorConfig

log = logging.getLogger(__name__)


def _is_register_output_call(node) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "register_output"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "context"
    )


def _has_return_value(fn) -> bool:
    """Check if a function contains a return statement with a value."""
    source = inspect.getsource(fn)
    # dedent in case it's a method or indented function
    source = textwrap.dedent(source)
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Return) and node.value is not None:
            return True
    return False


def _has_register_output_fn(fn):
    """
    Checks if context.register_output was called
    If it is called, but not as the first line of the function, throws a ValueError
    """
    source = textwrap.dedent(inspect.getsource(fn))
    tree = ast.parse(source)

    fn_def = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == fn.__name__
    )

    register_output_lines = [
        node.lineno
        for node in ast.walk(fn_def)
        if _is_register_output_call(node)
    ]

    if not register_output_lines:
        return False  # optional, not present — fine

    first = fn_def.body[0]
    if not (
        isinstance(first, ast.Expr) and _is_register_output_call(first.value)
    ):
        raise ValueError(
            f"@dynamic_graph_asset '{fn.__name__}': context.register_output(...) was "
            f"found at line(s) {register_output_lines} but must be the first statement "
            f"of the function if used."
        )
    return True


# using this causes the code location to crash when running multiple assets at once
multiprocess_config = ExecutionConfig(
    executor=SelectorConfig(class_name="multiprocess_executor")
)
in_process_config = ExecutionConfig(
    executor=SelectorConfig(class_name="in_process_executor")
)


# -- Mapping key encoding --
def _encode_segment(value: str) -> str:
    """Encode forbidden characters as _XX_ (hex), leave [a-zA-Z0-9] as-is."""
    return re.sub(
        r"[^a-zA-Z0-9]", lambda m: f"_{ord(m.group()):02X}_", str(value)
    )


def _decode_segment(encoded: str) -> str:
    """Decode _XX_ sequences back to original characters."""
    return re.sub(
        r"_([0-9A-F]{2})_", lambda m: chr(int(m.group(1), 16)), encoded
    )


def _encode_mapping_key(values: tuple) -> str:
    return "__".join(_encode_segment(v) for v in values)


def _decode_mapping_key(mapping_key: str) -> list:
    return [_decode_segment(segment) for segment in mapping_key.split("__")]


def _in_to_asset_in(name: str, op_in: dg.In) -> dg.AssetIn:
    """
    Translate a dg.In (op-level) to a dg.AssetIn (asset-level).

    Direct mappings:
        In.metadata            → AssetIn.metadata
        In.input_manager_key   → AssetIn.input_manager_key
        In.dagster_type        → AssetIn.dagster_type
        In.asset_key           → AssetIn.key          (if static AssetKey)

    No equivalent:
        In.description         → dropped (no AssetIn equivalent)
        In.default_value       → dropped (asset deps are always required)
        In.asset_partitions    → dropped (use AssetIn.partition_mapping instead)
        In.asset_key callable  → dropped (AssetIn.key is static only)
    """
    # Resolve asset key: In.asset_key can be a callable, AssetIn.key must be static
    asset_key = None
    if isinstance(op_in.asset_key, dg.AssetKey):
        asset_key = op_in.asset_key
    elif callable(op_in.asset_key):
        log.warning(
            f"In(asset_key=<callable>) for input '{name}' cannot be translated to "
            f"AssetIn.key (which must be static). The asset key will be inferred from "
            f"the parameter name instead."
        )

    return dg.AssetIn(
        key=asset_key,
        metadata=op_in.metadata,
        input_manager_key=op_in.input_manager_key,
        dagster_type=op_in.dagster_type,
        # key_prefix and partition_mapping have no In equivalent — left as defaults
    )


class DynamicGraphAssetMetadata:
    """
    Class to represent dynamic graph asset metadata for Dagster.
    Replaces the old `has_partitions` boolean with a list of `asset_partition_keys`.
    """

    def __init__(
        self,
        asset_key: List[str],
        graph_dimensions: Optional[List[str]] = [],
        should_return_parent: Optional[bool] = False,
        asset_partition_keys: Optional[List[str]] = [],
    ):
        self.asset_key = asset_key
        self.asset_partition_keys = asset_partition_keys or []
        self.graph_dimensions = graph_dimensions or []
        self.should_return_parent = should_return_parent

    @staticmethod
    def _extract_value(obj: Any) -> Any:
        """
        Recursively extract .value from dg.MetadataValue objects.
        """
        if isinstance(obj, dg.MetadataValue):
            return obj.value
        elif isinstance(obj, dict):
            return {
                k: DynamicGraphAssetMetadata._extract_value(v)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [DynamicGraphAssetMetadata._extract_value(v) for v in obj]
        else:
            return obj

    @classmethod
    def from_metadata(
        cls, metadata: Dict[str, Any]
    ) -> Optional["DynamicGraphAssetMetadata"]:
        """
        Construct a DynamicGraphAssetMetadata object from Dagster metadata,
        handling dg.MetadataValue wrappers.
        """
        dynamic_meta = metadata.get("dynamic_graph_asset")
        if dynamic_meta is None:
            return None

        dynamic_meta = cls._extract_value(dynamic_meta)

        asset_key = dynamic_meta.get("asset_key", [])
        asset_partition_keys = dynamic_meta.get("asset_partition_keys", [])
        graph_dimensions = dynamic_meta.get("graph_dimensions", [])
        should_return_parent = dynamic_meta.get("should_return_parent", False)

        if not isinstance(asset_key, list) or not isinstance(
            graph_dimensions, list
        ):
            raise TypeError(
                "Expected 'asset_key' and 'graph_dimensions' to be lists"
            )
        if not isinstance(asset_partition_keys, list):
            raise TypeError("Expected 'asset_partition_keys' to be a list")

        return cls(
            asset_key=asset_key,
            graph_dimensions=graph_dimensions,
            asset_partition_keys=asset_partition_keys,
            should_return_parent=should_return_parent,
        )

    def to_metadata(self) -> Dict[str, Any]:
        """
        Convert the object back into Dagster metadata format (plain Python values).
        """
        return {
            "dynamic_graph_asset": {
                "asset_key": self.asset_key,
                "asset_partition_keys": self.asset_partition_keys,
                "graph_dimensions": self.graph_dimensions,
                "should_return_parent": self.should_return_parent,
            }
        }

    def __repr__(self):
        return (
            f"DynamicGraphAssetMetadata(asset_key={self.asset_key}, "
            f"asset_partition_keys={self.asset_partition_keys}, "
            f"should_return_parent={self.should_return_parent}, "
            f"graph_dimensions={self.graph_dimensions})"
        )


class _CaptureOutput(Exception):
    def __init__(self, output: dg.Output):
        self._output = output


class DynamicGraphAssetExecutionContext(dg.OpExecutionContext):
    """
    OpExecutionContext subclass that exposes the current graph dimension as a dictionary.

        @dynamic_graph_asset(graph_dimensions=["state", "disease"], ...)
        def my_asset(context: DynamicGraphAssetExecutionContext, config: MyConfig):
            context.graph_dimension["state"]    # "AK"
            context.graph_dimension["disease"]  # "COVID-19"

    This subclass also exposes a register_output() function to provide an Output for the asset
    in the absence of the traditional `yield dg.Output or dg.AssetMaterialization`
    """

    _mapping_key: dict | None = None
    _mapping_key_names: list[str] = []
    _should_run_output_fn = False

    @property
    def graph_dimension(self) -> dict[str, str]:
        if self._mapping_key is None:
            values = _decode_mapping_key(self.get_mapping_key())
            self._mapping_key = dict(zip(self._mapping_key_names, values))
        return self._mapping_key

    def register_output(self, output_fn: Callable[[], dg.Output]) -> dg.Output:
        log.debug(f"output_fn: '{output_fn}'")
        log.debug(f"should_run_output_fn: '{self._should_run_output_fn}'")
        output = None
        if self._should_run_output_fn:
            if output_fn:
                output = output_fn()
                log.debug(f"output from fn: '{output}'")
            else:
                output = dg.Output(dg.Nothing)
                log.debug("Didn't run output fn")
            # raise an exception to exit before running the user code
            raise _CaptureOutput(output)

    @staticmethod
    def inject(
        context: dg.OpExecutionContext,
        mapping_key_names: list[str],
        should_run_output_fn: bool,
    ) -> "DynamicGraphAssetExecutionContext":
        """
        Patch an existing OpExecutionContext instance into a DynamicGraphAssetExecutionContext
        by reassigning its __class__ and injecting the mapping key names.
        This works because DynamicGraphAssetExecutionContext adds no new __init__ parameters.
        """
        context.__class__ = DynamicGraphAssetExecutionContext
        # casting is not enough: AttributeError: 'OpExecutionContext' object has no attribute 'register_output'
        # context = cast(DynamicGraphAssetExecutionContext, context)
        context._mapping_key_names = mapping_key_names
        context._mapping_key = None
        context._should_run_output_fn = should_run_output_fn
        return context  # type: ignore[return-value]


def add_metadata_to_output(
    result,
    asset_key: dg.AssetKey,
    should_return_parent: Optional[bool] = False,
    graph_dimensions: Optional[list[str]] = [],
    asset_partition_keys: Optional[list[str]] = [],
) -> dg.Output:
    # Extract user metadata if the user returned an Output
    if isinstance(result, dg.Output):
        user_value = result.value
        user_metadata = result.metadata or {}
    else:
        user_value = result
        user_metadata = {}

    # Merge our graph_dimensions metadata
    merged_metadata = {
        **dict(user_metadata),
        **DynamicGraphAssetMetadata(
            asset_key=asset_key.path,
            graph_dimensions=graph_dimensions,
            asset_partition_keys=asset_partition_keys,
            should_return_parent=should_return_parent,
        ).to_metadata(),
    }
    log.debug(f"merged_metadata: '{merged_metadata}'")

    # Yield a new Output object with merged metadata
    return dg.Output(value=user_value, metadata=merged_metadata)


class GraphAssetKwargs(TypedDict, total=False):
    name: Optional[str] = (None,)
    description: Optional[str] = (None,)
    # ins: Optional[Mapping[str, dg.AssetIn]] = None,
    config: Optional[Union[dg.ConfigMapping, Mapping[str, Any]]] = (None,)
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = (None,)
    group_name: Optional[str] = (None,)
    partitions_def: Optional[dg.PartitionsDefinition] = (None,)
    hooks: Optional[AbstractSet[dg.HookDefinition]] = (None,)
    metadata: Optional[RawMetadataMapping] = (None,)
    tags: Optional[Mapping[str, str]] = (None,)
    owners: Optional[TypeSequence[str]] = (None,)
    automation_condition: Optional[dg.AutomationCondition] = (None,)
    backfill_policy: Optional[dg.BackfillPolicy] = (None,)
    resource_defs: Optional[Mapping[str, dg.ResourceDefinition]] = (None,)
    check_specs: Optional[TypeSequence[dg.AssetCheckSpec]] = (None,)
    code_version: Optional[str] = (None,)
    key: Optional[CoercibleToAssetKey] = (None,)
    kinds: Optional[AbstractSet[str]] = (None,)


# TODO: remove register_output
# -- Decorator --
def dynamic_graph_asset(
    graph_dimensions: List[str],
    ins: dict[str, dg.In] = None,
    io_manager_key: Optional[str] = None,
    **graph_asset_kwargs: Unpack[GraphAssetKwargs],
):
    """
    Decorator that wires a function into a dynamic graph asset.
    See https://docs.dagster.io/guides/build/ops/dynamic-graphs#using-dynamic-outputs
    and https://docs.dagster.io/guides/build/assets/graph-backed-assets

    The decorated function becomes the compute op, called once per combination
    of graph_dimensions field values. Config fields listed in `graph_dimensions` are unpacked to
    single scalar values inside the function body.

    graph_dimensions values are encoded into the internal op mapping key using _XX_ hex escaping
    so original values (including spaces, hyphens, etc.) are recovered exactly in
    the compute op, while safe characters remain human-readable in the Dagster UI.

    Return type annotations are required since they determine the behavior of the output.
    To return a single Output, use a simple type like str, int, bool.
    To return an Output from each graph dimension, use a 'list[some_type]'.
    This is especially powerful with the ADLS2FilesystemIOManager since you can upload a file or directory for each graph dimension and return the parent directory as the final output.
    Downstream assets using the ADLS2FilesystemIOManager will download the structured directory automatically.

    Args:
        graph_dimensions: List of config field names to fan out over. Must be iterable
                fields on the Config class (e.g. List[str]). The cartesian
                product of all fields becomes the set of dynamic mapping keys.

        name (Optional[str]): The name of the asset.  If not provided, defaults to the name of the
            decorated function. The asset's name must be a valid name in Dagster (ie only contains
            letters, numbers, and underscores) and may not contain Python reserved keywords.
        description (Optional[str]):
            A human-readable description of the asset.
            about the input.
        ins (Optional[Dict[str, In]]):
            Information about the inputs to the op. Information provided here will be combined
            with what can be inferred from the function signature.
        config (Optional[Union[ConfigMapping], Mapping[str, Any]):
            Describes how the graph underlying the asset is configured at runtime.

            If a :py:class:`ConfigMapping` object is provided, then the graph takes on the config
            schema of this object. The mapping will be applied at runtime to generate the config for
            the graph's constituent nodes.

            If a dictionary is provided, then it will be used as the default run config for the
            graph. This means it must conform to the config schema of the underlying nodes. Note
            that the values provided will be viewable and editable in the Dagster UI, so be careful
            with secrets.

            If no value is provided, then the config schema for the graph is the default (derived
            from the underlying nodes).
        key_prefix (Optional[Union[str, Sequence[str]]]): If provided, the asset's key is the
            concatenation of the key_prefix and the asset's name, which defaults to the name of
            the decorated function. Each item in key_prefix must be a valid name in Dagster (ie only
            contains letters, numbers, and underscores) and may not contain Python reserved keywords.
        group_name (Optional[str]): A string name used to organize multiple assets into groups. If
            not provided, the name "default" is used.
        partitions_def (Optional[PartitionsDefinition]): Defines the set of partition keys that
            compose the asset.
        hooks (Optional[AbstractSet[HookDefinition]]): A set of hooks to attach to the asset.
            These hooks will be executed when the asset is materialized.
        metadata (Optional[RawMetadataMapping]): Dictionary of metadata to be associated with
            the asset.
        io_manager_key (Optional[str]): The resource key of the IOManager used
            for storing the output of the op as an asset, and for loading it in downstream ops
            (default: "io_manager").
        tags (Optional[Mapping[str, str]]): Tags for filtering and organizing. These tags are not
            attached to runs of the asset.
        owners (Optional[Sequence[str]]): A list of strings representing owners of the asset. Each
            string can be a user's email address, or a team name prefixed with `team:`,
            e.g. `team:finops`.
        kinds (Optional[Set[str]]): A list of strings representing the kinds of the asset. These
            will be made visible in the Dagster UI.
        automation_condition (Optional[AutomationCondition]): The AutomationCondition to use
            for this asset.
        backfill_policy (Optional[BackfillPolicy]): The BackfillPolicy to use for this asset.
        code_version (Optional[str]): Version of the code that generates this asset. In
            general, versions should be set only for code that deterministically produces the same
            output when given the same inputs.
        key (Optional[CoeercibleToAssetKey]): The key for this asset. If provided, cannot specify key_prefix or name.


    Example:
        Single value for all graph dimensions:

        class MyAssetConfig(dg.Config):
            disease: List[str]
            state: List[str]
            container: str

        @dynamic_graph_asset(
            graph_dimensions=["disease", "state"],
            partitions_def=daily_partitions,
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, config: MyAssetConfig, upstream_asset) -> str:
            my_code_pipeline(context.graph_dimension["disease"], context.graph_dimension["state"], upstream_asset)
            return dg.Output(
                value=f"staging/{context.partition_key}",
                metadata={"container": config.base_output_prefix},
            )
        Returns: staging/2026-03-10

        Aggregated value from each graph dimension:

        class MyAssetConfig(dg.Config):
            disease: List[str]
            state: List[str]
            container: str

        @dynamic_graph_asset(
            graph_dimensions=["disease", "state"],
            partitions_def=daily_partitions,
            io_manager_key="ADLS2PickleIOManager",
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, config: MyAssetConfig, upstream_asset) -> list[str]:
            result = my_code_pipeline(context.graph_dimension["disease"], context.graph_dimension["state"], upstream_asset)
            return dg.Output(
                value=result,
                metadata={"container": config.base_output_prefix},
            )
        Returns: [result_1, result_2, ... result_n]

        File from each graph dimension:

        class MyAssetConfig(dg.Config):
            disease: List[str]
            state: List[str]
            container: str

        @dynamic_graph_asset(
            graph_dimensions=["disease", "state"],
            partitions_def=daily_partitions,
            io_manager_key="ADLS2FilesystemIOManager",
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, config: MyAssetConfig, upstream_asset) -> list[str]:
            output_path = my_code_pipeline(context.graph_dimension["disease"], context.graph_dimension["state"], upstream_asset)
            return dg.Output(
                value=output_path,
                metadata={"container": config.base_output_prefix},
            )
        Returns: abfss://cfadagster/dagster-files/username/my_dynamic_asset/2026-03-20
        Downstream dependencies will download the files directly with the following structure:
        my_dynamic_asset/
        └── 2026-04-13
            ├── COVID
            │   ├── AZ
            │   │   └── COVID_AZ.txt
            │   └── NY
            │       └── COVID_NY.txt
            └── FLU
                ├── AZ
                │   └── FLU_AZ.txt
                └── NY
                    └── FLU_NY.txt

    """

    def decorator(fn):
        asset_name = fn.__name__

        hints = get_type_hints(fn)
        return_type = hints.get("return")
        log.debug(f"return_type: '{return_type}'")
        origin = get_origin(return_type) or return_type

        is_annotated_sequence = (
            isinstance(origin, type)
            and issubclass(origin, Sequence)
            and not issubclass(origin, (str, bytes))
        )
        log.debug(f"is_annotated_sequence: '{is_annotated_sequence}'")

        sig = inspect.signature(fn)

        # -- Validate register_output is first --
        did_register_output = _has_register_output_fn(fn)
        log.debug(f"did_register_output: '{did_register_output}'")
        does_return_value = inspect.isgeneratorfunction(
            fn
        ) or _has_return_value(fn)
        log.debug(f"does_return_value: '{does_return_value}'")

        if does_return_value and return_type is None:
            raise ValueError(
                f"@dynamic_graph_asset '{asset_name}': decorated function must "
                "have a return type annotation when returning a value. Use list[some_type] "
                "to return the value from each graph_dimension."
            )

        if did_register_output and does_return_value:
            raise ValueError(
                f"@dynamic_graph_asset '{asset_name}': decorated function can not "
                f"use both register_output() and return a value. "
            )

        # -- Locate the Config parameter --
        hints = get_type_hints(fn, globalns=vars(sys.modules[fn.__module__]))
        config_cls = next(
            (
                hint
                for hint in hints.values()
                if inspect.isclass(hint) and issubclass(hint, dg.Config)
            ),
            None,
        )
        if config_cls is None:
            raise ValueError(
                f"@dynamic_graph_asset '{asset_name}': decorated function must have a "
                f"parameter annotated with a dg.Config subclass"
            )

        # -- Validate graph_dimensions fields --
        for field in graph_dimensions:
            if field not in config_cls.model_fields:
                raise ValueError(
                    f"@dynamic_graph_asset '{asset_name}': graph_dimensions field '{field}' "
                    f"does not exist on {config_cls.__name__}. "
                    f"Available fields: {list(config_cls.model_fields.keys())}"
                )
            field_annotation = config_cls.model_fields[field].annotation
            origin = get_origin(field_annotation)
            if origin is None or origin not in (list, tuple, set, frozenset):
                raise ValueError(
                    f"@dynamic_graph_asset '{asset_name}': graph_dimensions field '{field}' "
                    f"on {config_cls.__name__} must be an iterable type (e.g. list[str]), "
                    f"got '{field_annotation}'"
                )

        # Determine the base key: explicit key or function name
        base_key = graph_asset_kwargs.get("key") or fn.__name__
        final_asset_key = base_key

        # Apply key prefix if provided
        key_prefix = graph_asset_kwargs.get("key_prefix")
        if key_prefix:
            # Support list or single string
            if isinstance(key_prefix, str):
                final_asset_key = dg.AssetKey([key_prefix, base_key])
            elif isinstance(key_prefix, list):
                final_asset_key = dg.AssetKey(key_prefix + [base_key])
            else:
                raise ValueError("key_prefix must be str or list of str")
        else:
            final_asset_key = dg.AssetKey(base_key)
        log.debug(f"final_asset_key: '{final_asset_key}'")

        # check for partitions
        has_partitions = graph_asset_kwargs.get("partitions_def") is not None
        log.debug(f"has_partitions: '{has_partitions}'")

        # Infer upstream op ins from all parameters that aren't context or config
        inferred_ins = {
            name: dg.In()
            for name in sig.parameters
            if name not in ["config", "context"]
        }

        # User overrides at op level
        op_ins = {**inferred_ins, **(ins or {})}

        # Asset-level ins for @graph_asset
        asset_ins: dict[str, dg.AssetIn] = {
            name: _in_to_asset_in(name, op_in)
            for name, op_in in op_ins.items()
        }

        log.debug(f"inferred_ins: '{inferred_ins}'")
        log.debug(f"op_ins: '{op_ins}'")
        log.debug(f"asset_ins: '{asset_ins}'")

        # -- config op to create DynamicOutputs for fanout --
        @dg.op(
            name=f"{asset_name}__config",
            ins={
                "_": dg.In(dg.Nothing),
                **{name: dg.In(dg.Nothing) for name in op_ins},
            },
            out=dg.DynamicOut(dg.Nothing),
            tags=in_process_config.to_run_tags(),
        )
        def gen_config(context, **kwargs):
            log.debug(f"kwargs: '{kwargs}'")
            config = config_cls(**context.op_config)
            axes = [getattr(config, field) for field in graph_dimensions]
            for combo in itertools.product(*axes):
                mapping_key = _encode_mapping_key(combo)
                yield dg.DynamicOutput(value=None, mapping_key=mapping_key)

        # -- compute op to run decorated function body --
        @dg.op(
            name=f"{asset_name}__compute",
            ins={"_": dg.In(dg.Nothing), **op_ins},
            **(
                {"out": dg.Out(dg.Nothing)}
                if did_register_output or not does_return_value
                else {
                    "out": dg.Out(
                        is_required=False,
                        **(
                            {"io_manager_key": io_manager_key}
                            if io_manager_key
                            else {}
                        ),
                    )
                }
            ),
            config_schema=config_cls.to_config_schema(),
        )
        def compute(context, **kwargs):
            upstream_kwargs = {k: v for k, v in kwargs.items() if k != "_"}
            dynamic_context = DynamicGraphAssetExecutionContext.inject(
                context,
                graph_dimensions,
                False,
            )
            config = context.op_config
            log.debug(f"config: '{config}'")
            graph_dimension = dynamic_context.graph_dimension
            log.debug(f"graph_dimension: '{graph_dimension}'")

            is_first_dimension = all(
                graph_dimension.get(key) == values[0]
                for key, values in config.items()
                if key in graph_dimension
            )
            log.debug(f"is_first_dimension: '{is_first_dimension}'")

            result = fn(dynamic_context, context.op_config, **upstream_kwargs)
            # handle yielded Output
            if isinstance(result, GeneratorType):
                result = next(result)
            log.debug(f"compute result: '{result}'")
            if is_annotated_sequence or is_first_dimension:
                yield add_metadata_to_output(
                    result=result,
                    asset_key=final_asset_key,
                    graph_dimensions=list(
                        dynamic_context.graph_dimension.values()
                    ),
                    asset_partition_keys=dynamic_context.partition_keys,
                )

        # -- output op to return results --
        @dg.op(
            name=f"{asset_name}__output",
            ins={
                "compute_result": (
                    dg.In(dg.Nothing)
                    if did_register_output or not does_return_value
                    else dg.In(
                        **(
                            {"input_manager_key": io_manager_key}
                            if io_manager_key
                            else {}
                        ),
                        metadata=DynamicGraphAssetMetadata(
                            asset_key=final_asset_key.path,
                        ).to_metadata(),
                    )
                ),
                **op_ins,
            },
            config_schema=config_cls.to_config_schema(),
            **(
                {
                    "out": dg.Out(
                        **(
                            {"io_manager_key": io_manager_key}
                            if io_manager_key
                            else {}
                        ),
                    )
                }
                if did_register_output or does_return_value
                else {"out": dg.Out(dg.Nothing)}
            ),
            tags=in_process_config.to_run_tags(),
        )
        def output_op(context, **kwargs):
            config = config_cls(**context.op_config)
            log.debug(f"kwargs.keys(): '{kwargs.keys()}'")

            compute_result = kwargs.get("compute_result")
            log.debug(f"compute_result: '{compute_result}'")
            upstream_kwargs = {
                k: v
                for k, v in kwargs.items()
                if k != "_" and k != f"{asset_name}__compute"
            }
            dynamic_context = DynamicGraphAssetExecutionContext.inject(
                context,
                graph_dimensions,
                True,
            )
            if does_return_value:
                result = compute_result
                # handle yielded Output
                if isinstance(result, GeneratorType):
                    result = next(result)
                return add_metadata_to_output(
                    result=result
                    if is_annotated_sequence
                    else compute_result[0],
                    asset_key=final_asset_key,
                    should_return_parent=True,
                    graph_dimensions=graph_dimensions,
                    asset_partition_keys=dynamic_context.partition_keys,
                )
            if not did_register_output:
                return
            try:
                fn(dynamic_context, config, **upstream_kwargs)
            except _CaptureOutput as e:
                return add_metadata_to_output(
                    result=e._output,
                    asset_key=final_asset_key,
                    should_return_parent=True,
                    graph_dimensions=graph_dimensions,
                    asset_partition_keys=dynamic_context.partition_keys,
                )

        # -- config mapping --
        @dg.config_mapping(config_schema=config_cls.to_config_schema())
        def _config_mapping(config):
            ops = {
                f"{asset_name}__{op}": {"config": config}
                for op in ("config", "compute", "output")
            }
            return ops

        # -- graph asset --
        @dg.graph_asset(
            name=asset_name,
            config=_config_mapping,
            ins={k: v for k, v in asset_ins.items()},
            **graph_asset_kwargs,
        )
        def _asset(**ins_kwargs):
            keys = gen_config(**ins_kwargs)
            res = keys.map(lambda nothing: compute(_=nothing, **ins_kwargs))
            return output_op(compute_result=res.collect(), **ins_kwargs)

        return _asset

    return decorator
