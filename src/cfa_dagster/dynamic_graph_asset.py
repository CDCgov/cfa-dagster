import ast
import inspect
import itertools
import logging
import re
import sys
import textwrap
import warnings
from dataclasses import dataclass
from types import GeneratorType
from typing import (
    AbstractSet,
    Any,
    Callable,
    Generic,
    Literal,
    Mapping,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
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
from dagster._utils.warnings import BetaWarning
from pydantic import PrivateAttr
from typing_extensions import Unpack

from .azure_adls2.filesystem_metadata import ADLS2FilesystemIOManagerMetadata
from .azure_adls2.pickle_io_manager import ADLS2PickleIOManager
from .execution.utils import ExecutionConfig, SelectorConfig

log = logging.getLogger(__name__)

INTERNAL_CONFIG_IO_MANAGER = ADLS2PickleIOManager().get_resource_definition()


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


@dataclass
class DimensionResourceInfo:
    param_name: str
    resource_cls: type
    dimension_fields: list[str]


T = TypeVar("T", default=str)


class GraphDimension(dg.Config, Generic[T]):
    values: list[T]
    _current_value: Optional[T] = PrivateAttr(default=None)

    def __init__(self, values: list[str], current_value: Optional[str] = None):
        super().__init__(values=values, _current_value=current_value)

    def set_current_value(self, value: T) -> None:
        self._current_value = value

    @property
    def current_value(self) -> Optional[T]:
        return self._current_value


def _get_resource_params(hints) -> dict[str, type]:
    resource_params: dict[str, type] = {
        name: hint
        for name, hint in hints.items()
        if name != "return"
        and inspect.isclass(hint)
        and issubclass(hint, dg.ConfigurableResource)
    }
    log.debug(f"resource_params: '{resource_params}'")
    return resource_params


def _get_dimension_resource_info(
    asset_name: str, resource_params: dict[str, type]
) -> DimensionResourceInfo:
    dimension_info: DimensionResourceInfo | None = None

    for param_name, resource_cls in resource_params.items():
        resource_hints = get_type_hints(resource_cls)

        dimension_fields = [
            field_name
            for field_name, annotation in resource_hints.items()
            if _is_dimension_annotation(annotation)
        ]

        if dimension_fields:
            if dimension_info is not None:
                raise ValueError(
                    f"@dynamic_graph_asset '{asset_name}': "
                    "Multiple ConfigurableResources contain GraphDimension fields. "
                    f"Found: '{dimension_info.param_name}' and '{param_name}'. "
                    "Only one resource may contain GraphDimension fields."
                )
            dimension_info = DimensionResourceInfo(
                param_name=param_name,
                resource_cls=resource_cls,
                dimension_fields=dimension_fields,
            )

    if dimension_info is None:
        raise ValueError(
            f"@dynamic_graph_asset '{asset_name}': "
            "No ConfigurableResource parameter contains GraphDimension fields. "
            "At least one resource parameter must include fields typed as GraphDimension."
        )

    log.debug(f"dimension_resource_info: {dimension_info}")
    return dimension_info


def _get_config_cls(hints):
    config_cls = next(
        (
            hint
            for hint in hints.values()
            if inspect.isclass(hint)
            and issubclass(hint, dg.Config)
            and not issubclass(hint, dg.ConfigurableResource)
        ),
        None,
    )
    return config_cls


def _get_asset_key(
    asset_name: str, graph_asset_kwargs: GraphAssetKwargs
) -> dg.AssetKey:
    base_key = graph_asset_kwargs.get("key") or asset_name
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
            raise ValueError(
                f"@dynamic_graph_asset '{asset_name}': "
                "key_prefix must be str or list of str"
            )
    else:
        final_asset_key = dg.AssetKey(base_key)
    log.debug(f"final_asset_key: '{final_asset_key}'")
    return final_asset_key


def _is_dimension_annotation(annotation) -> bool:
    if get_origin(annotation) is GraphDimension:
        return True

    metadata = getattr(
        annotation,
        "__pydantic_generic_metadata__",
        None,
    )

    return metadata is not None and metadata.get("origin") is GraphDimension


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


# using this causes the code location to crash when running multiple assets at once
multiprocess_config = ExecutionConfig(
    executor=SelectorConfig(class_name="multiprocess_executor")
)
in_process_config = ExecutionConfig(
    executor=SelectorConfig(class_name="in_process_executor")
)

#  Choosing a lesser-used alpha char as a prefix to prevent Dagster/python keyword errors
# DagsterInvalidDefinitionError: "in" is not a valid name in Dagster. It conflicts with a Dagster or python reserved keyword.
SEGMENT_PREFIX = "x_"
# Using triple underscore as a separator since a single underscore could be an
# encoded character and a double underscore could be two consecutive encoded character
SEGMENT_SEPARATOR = "___"


# -- Mapping key encoding --
def _encode_segment(value: str) -> str:
    """Encode forbidden characters as _XX_ (hex), leave [a-zA-Z0-9] as-is."""
    encoded = re.sub(
        r"[^a-zA-Z0-9]|_",
        lambda m: f"_{ord(m.group()):02X}_",
        str(value),
    )
    return f"{SEGMENT_PREFIX}{encoded}"


def _decode_segment(encoded: str) -> str:
    """Decode _XX_ sequences back to original characters."""
    encoded = encoded.removeprefix(SEGMENT_PREFIX)

    return re.sub(
        r"_([0-9A-F]{2})_",
        lambda m: chr(int(m.group(1), 16)),
        encoded,
    )


def _encode_mapping_key(values: tuple) -> str:
    return SEGMENT_SEPARATOR.join(_encode_segment(v) for v in values)


def _decode_mapping_key(mapping_key: str) -> list:
    return [
        _decode_segment(segment)
        for segment in mapping_key.split(SEGMENT_SEPARATOR)
    ]


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


def unpack_output(output) -> tuple[Any, dict]:
    if isinstance(output, dg.Output):
        user_value = output.value
        user_metadata = output.metadata or {}
    else:
        user_value = output
        user_metadata = {}
    return user_value, user_metadata


def _infer_ins(
    sig: inspect.Signature,
    resource_params_keys: set[str],
    ins: dict[str, dg.In] | None,
) -> tuple[dict[str, dg.In], dict[str, dg.AssetIn]]:
    inferred_ins = {
        name: dg.In()
        for name in sig.parameters
        if name not in ["config", "context", *resource_params_keys]
    }
    op_ins = {**inferred_ins, **(ins or {})}
    asset_ins = {
        name: _in_to_asset_in(name, op_in) for name, op_in in op_ins.items()
    }
    return op_ins, asset_ins


def _apply_graph_dimensions(
    context: dg.OpExecutionContext,
    dimension_resource_info: DimensionResourceInfo,
    graph_dimensions: dict[str, str],
) -> Any:
    dimension_resource = getattr(
        context.resources,
        dimension_resource_info.param_name,
    )
    for field_name, value in graph_dimensions.items():
        graph_dim_obj = cast(
            GraphDimension,
            getattr(dimension_resource, field_name),
        )
        graph_dim_obj.set_current_value(value)
    return dimension_resource


# -- Decorator --
def dynamic_graph_asset(
    fn: Callable[..., Any] | None = None,
    *,
    ins: dict[str, dg.In] | None = None,
    io_manager_key: str | None = None,
    retry_policy: dg.RetryPolicy | None = None,
    output_mode: Literal["first", "all"] = "first",
    **graph_asset_kwargs: Unpack[GraphAssetKwargs],
) -> dg.AssetsDefinition | Callable[[Callable[..., Any]], dg.AssetsDefinition]:
    """
    Decorator that wires a function into a dynamic graph asset to run steps in parallel based on a provided ConfigurableResource.
    See https://docs.dagster.io/guides/build/ops/dynamic-graphs#using-dynamic-outputs
    and https://docs.dagster.io/guides/build/assets/graph-backed-assets

    The decorated function becomes the compute op, called once per combination
    of graph_dimensions field values. ConfigurableResource fields types as `GraphDimension`s  are unpacked to
    single scalar values inside the function body.

    graph_dimensions values are encoded into the internal op mapping key using _XX_ hex escaping
    so original values (including spaces, hyphens, etc.) are recovered exactly in
    the compute op, while safe characters remain human-readable in the Dagster UI.

    Use ``output_mode`` to control whether the output is emitted from a single dimension or collected from all dimensions.
    This is especially powerful with the ADLS2FilesystemIOManager since you can upload a file or directory for each graph dimension and return the parent directory as the final output.
    Downstream assets using the ADLS2FilesystemIOManager will download the structured directory automatically.

    Args:
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
        retry_policy (Optional[RetryPolicy]): The retry policy for this asset.
        output_mode (Literal["first", "all"]): Controls whether the output is emitted from a
            single dimension (``"first"``, the default) or collected across all dimensions
            (``"all"``). Use ``"all"`` when each dimension should independently produce an
            output, e.g. with the ADLS2FilesystemIOManager where each dimension uploads
            files. Defaults to ``"first"``.
        key (Optional[CoeercibleToAssetKey]): The key for this asset. If provided, cannot specify key_prefix or name.


    Example:
        Define ConfigurableResource with GraphDimensions:

        class MyConfig(dg.ConfigurableResource):
            disease: Dimension[str] = Dimension(["covid", "flu", "rsv"])
            state: Dimension[str] = Dimension(["CA", "TX", "NY"])
            container: str

        defs = dg.Definitions(
            ...
            resources = {
                "my_config": MyConfig()
            }
        )

        Single value for all graph dimensions:

        @dynamic_graph_asset(
            partitions_def=daily_partitions,
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
            output_mode="first",
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, my_config: MyConfig, upstream_asset):
            my_code_pipeline(my_config.disease.current_value, my_config.state.current_value, upstream_asset)
            return dg.Output(
                value=f"staging/{context.partition_key}",
                metadata={"container": config.base_output_prefix},
            )
        Returns: staging/2026-03-10

        Aggregated value from each graph dimension:

        @dynamic_graph_asset(
            partitions_def=daily_partitions,
            io_manager_key="ADLS2PickleIOManager",
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
            output_mode="all",
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, my_config: MyConfig, upstream_asset):
            result = my_code_pipeline(my_config.disease.current_value, my_config.state.current_value, upstream_asset)
            return dg.Output(
                value=result,
                metadata={"container": config.base_output_prefix},
            )
        Returns: [result_1, result_2, ... result_n]

        File from each graph dimension:

        @dynamic_graph_asset(
            partitions_def=daily_partitions,
            io_manager_key="ADLS2FilesystemIOManager",
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
            output_mode="all",
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, my_config: MyConfig, upstream_asset):
            output_path = my_code_pipeline(my_config.disease.current_value, my_config.state.current_value, upstream_asset)
            return dg.Output(
                value=output_path,
                metadata={"container": config.base_output_prefix},
            )
        Returns: abfss://cfadagster/dagster-files/username/my_dynamic_asset/2026-04-13
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

        hints = get_type_hints(fn, globalns=vars(sys.modules[fn.__module__]))
        sig = inspect.signature(fn)

        does_return_value = inspect.isgeneratorfunction(
            fn
        ) or _has_return_value(fn)
        log.debug(f"does_return_value: '{does_return_value}'")

        should_return_all = output_mode == "all"

        # -- Locate the Config parameter --
        config_cls = _get_config_cls(hints)

        # capture kwargs from decorated function to be later used in compute op
        decorated_fn_kwargs = {
            name
            for name, param in sig.parameters.items()
            if param.kind
            in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        }
        log.debug(f"decorated_fn_kwargs: {decorated_fn_kwargs}")

        final_asset_key = _get_asset_key(asset_name, graph_asset_kwargs)

        # -- Locate Resource parameters --
        resource_params = _get_resource_params(hints)

        # Build the resource_params_keys set so @graph_asset and inner @op
        # declarations both declare the resources they need
        resource_params_keys: set[str] = set(resource_params.keys())
        log.debug(f"resource_params_keys: '{resource_params_keys}'")

        # -- Locate resources containing GraphDimension fields --
        dimension_resource_info = _get_dimension_resource_info(
            asset_name, resource_params
        )

        op_ins, asset_ins = _infer_ins(sig, resource_params_keys, ins)
        log.debug(f"op_ins: '{op_ins}'")
        log.debug(f"asset_ins: '{asset_ins}'")

        # Internal IO manager used ONLY for transporting shared config
        # between isolated mapped compute steps.
        INTERNAL_CONFIG_IO_MANAGER_KEY = (
            "internal_dynamic_graph_asset_io_manager"
        )

        required_resource_keys = (
            list(resource_params_keys)
            + list(graph_asset_kwargs.get("resource_defs", {}).keys())
            + [INTERNAL_CONFIG_IO_MANAGER_KEY]
        )

        # -- config op to create DynamicOutputs for fanout --
        @dg.op(
            name=f"{asset_name}__config",
            ins={
                "_": dg.In(dg.Nothing),
                **{name: dg.In(dg.Nothing) for name in op_ins},
            },
            out={
                # Cheap fanout signal
                "dga_internal_fanout": dg.DynamicOut(dg.Nothing),
                **(
                    {
                        # Persisted ONCE and loaded by every mapped compute op
                        "dga_internal_shared_config": dg.Out(
                            io_manager_key=INTERNAL_CONFIG_IO_MANAGER_KEY,
                        ),
                    }
                    if config_cls is not None
                    else {}
                ),
            },
            required_resource_keys=required_resource_keys,
            tags=in_process_config.to_run_tags(),
        )
        def gen_config(context, **kwargs):
            log.debug(f"kwargs: '{kwargs}'")
            if config_cls is not None:
                config = config_cls(**context.op_config)

                # Persist config once for all isolated compute workers
                yield dg.Output(
                    value=config.model_dump(),
                    output_name="dga_internal_shared_config",
                )

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

                axes.append(graph_dimension.values)

            for combo in itertools.product(*axes):
                mapping_key = _encode_mapping_key(combo)

                yield dg.DynamicOutput(
                    value=None,
                    mapping_key=mapping_key,
                    output_name="dga_internal_fanout",
                )

        # -- compute op to run decorated function body --
        @dg.op(
            name=f"{asset_name}__compute",
            ins={
                "_": dg.In(dg.Nothing),
                **(
                    {
                        # Shared config loaded from internal IO manager
                        "dga_internal_shared_config": dg.In(
                            input_manager_key=INTERNAL_CONFIG_IO_MANAGER_KEY,
                        ),
                    }
                    if config_cls is not None
                    else {}
                ),
                **op_ins,
            },
            **(
                {
                    "out": dg.Out(
                        is_required=False,
                        **(
                            {"io_manager_key": io_manager_key}
                            if io_manager_key
                            else {}
                        ),
                    )
                }
                if does_return_value
                else {"out": dg.Out(dg.Nothing)}
            ),
            # set resources based on decorated fn signature, explicit decorator param, and internal io manager
            required_resource_keys=required_resource_keys,
            retry_policy=retry_policy,
            config_schema=config_cls.to_config_schema()
            if config_cls
            else None,
        )
        def compute(
            context: dg.OpExecutionContext,
            **kwargs,
        ):
            upstream_kwargs = {
                k: v for k, v in kwargs.items() if k in decorated_fn_kwargs
            }
            log.debug(f"upstream_kwargs: {upstream_kwargs}")
            # Pull each declared resource off context and pass it through to fn
            resource_kwargs = {
                name: getattr(context.resources, name)
                for name in resource_params
            }
            log.debug(f"resource_kwargs: {resource_kwargs}")

            # decode graph_dimensions from mapping_key
            mapping_key = cast(str, context.get_mapping_key())
            decoded = _decode_mapping_key(mapping_key)
            graph_dimensions = dict(
                zip(dimension_resource_info.dimension_fields, decoded)
            )
            log.debug(f"graph_dimensions: '{graph_dimensions}'")

            dimension_resource = _apply_graph_dimensions(
                context, dimension_resource_info, graph_dimensions
            )

            is_first_dimension = all(
                graph_dimensions[field]
                == getattr(dimension_resource, field).values[0]
                for field in dimension_resource_info.dimension_fields
            )

            log.debug(f"is_first_dimension: '{is_first_dimension}'")

            if config_cls is not None:
                dga_internal_shared_config = kwargs.get(
                    "dga_internal_shared_config"
                )
                config = config_cls(**dga_internal_shared_config)
                log.debug(f"config: '{config}'")
                result = fn(
                    context,
                    config,
                    **upstream_kwargs,
                    **resource_kwargs,
                )
            else:
                result = fn(
                    context,
                    **upstream_kwargs,
                    **resource_kwargs,
                )

            if isinstance(result, GeneratorType):
                result = next(result)
            log.debug(f"compute result: '{result}'")
            if should_return_all or is_first_dimension:
                result, metadata = unpack_output(result)

                # Merge our graph_dimensions metadata
                merged_metadata = {
                    **dict(metadata),
                    **ADLS2FilesystemIOManagerMetadata(
                        asset_key_path=final_asset_key.path,
                        asset_partition_keys=context.partition_keys
                        if context.has_partition_key
                        else [],
                        synthetic_partition_keys=list(
                            graph_dimensions.values()
                        ),
                    ).to_dict(),
                }
                log.debug(f"merged_metadata: '{merged_metadata}'")

                yield dg.Output(value=result, metadata=merged_metadata)

        # -- output op to return results --
        @dg.op(
            name=f"{asset_name}__output",
            ins={
                "compute_result": (
                    dg.In(
                        **(
                            {"input_manager_key": io_manager_key}
                            if io_manager_key
                            else {}
                        ),
                        metadata=(
                            ADLS2FilesystemIOManagerMetadata(
                                skip_input=True
                            ).to_dict()
                        ),
                    )
                    if does_return_value
                    else dg.In(dg.Nothing)
                ),
            },
            config_schema=config_cls.to_config_schema()
            if config_cls
            else None,
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
                if does_return_value
                else {"out": dg.Out(dg.Nothing)}
            ),
            tags=in_process_config.to_run_tags(),
        )
        def output_op(context, **kwargs):
            compute_result = kwargs.get("compute_result")
            if does_return_value:
                result = compute_result
                # handle yielded Output
                if isinstance(result, GeneratorType):
                    result = next(result)

                # handle sequences
                result = (result if should_return_all else result[0],)

                result, metadata = unpack_output(result)

                # Merge our graph_dimensions metadata
                merged_metadata = {
                    **dict(metadata),
                    **ADLS2FilesystemIOManagerMetadata(
                        skip_output=True
                    ).to_dict(),
                }
                log.debug(f"merged_metadata: '{merged_metadata}'")

                # Yield a new Output object with merged metadata
                yield dg.Output(value=result, metadata=merged_metadata)

        # -- config mapping --
        if config_cls is not None:

            @dg.config_mapping(config_schema=config_cls.to_config_schema())
            def _config_mapping(config):
                # initialize the config class to ensure any default_factories run
                # once and pass the same values to all compute ops
                resolved = config_cls(**config)
                resolved_dict = resolved.model_dump()
                ops = {
                    f"{asset_name}__{op}": {"config": resolved_dict}
                    for op in ("config", "compute", "output")
                }
                return ops

        # Merge internal IO manager resource with any user resources
        existing_resource_defs = graph_asset_kwargs.get("resource_defs") or {}

        merged_resource_defs = {
            INTERNAL_CONFIG_IO_MANAGER_KEY: INTERNAL_CONFIG_IO_MANAGER,
            **existing_resource_defs,
        }

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=".*resource_defs.*",
                category=BetaWarning,
            )

            # -- graph asset --
            @dg.graph_asset(
                name=asset_name,
                **({} if config_cls is None else {"config": _config_mapping}),
                ins={k: v for k, v in asset_ins.items()},
                resource_defs=merged_resource_defs,
                **{
                    k: v
                    for k, v in graph_asset_kwargs.items()
                    if k != "resource_defs"
                },
            )
            def _asset(**ins_kwargs):
                gen_result = gen_config(**ins_kwargs)

                if config_cls is not None:
                    dga_internal_fanout, dga_internal_shared_config = (
                        gen_result
                    )
                else:
                    dga_internal_fanout = gen_result
                    dga_internal_shared_config = None

                # Map fanout while passing shared config to every isolated compute worker
                res = dga_internal_fanout.map(
                    lambda nothing,
                    _shared=dga_internal_shared_config: compute(
                        _=nothing,
                        **(
                            {"dga_internal_shared_config": _shared}
                            if _shared is not None
                            else {}
                        ),
                        **ins_kwargs,
                    )
                )
                return output_op(compute_result=res.collect())

            return _asset

    if fn is not None:
        return decorator(fn)
    return decorator
