import ast
import inspect
import itertools
import logging
import re
import sys
import textwrap
from typing import (
    Callable,
    List,
    get_origin,
    get_type_hints,
)

import dagster as dg

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


def _validate_register_output_first(fn):
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
        return  # optional, not present — fine

    first = fn_def.body[0]
    if not (
        isinstance(first, ast.Expr) and _is_register_output_call(first.value)
    ):
        raise ValueError(
            f"@dynamic_graph_asset '{fn.__name__}': context.register_output(...) was "
            f"found at line(s) {register_output_lines} but must be the first statement "
            f"of the function if used."
        )


multiprocess_config = ExecutionConfig(
    executor=SelectorConfig(class_name="multiprocess_executor")
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


class _CaptureOutput(Exception):
    def __init__(self, output: dg.Output):
        self._output = output


class DynamicGraphAssetExecutionContext(dg.OpExecutionContext):
    """
    OpExecutionContext subclass that exposes the current fanout mapping key
    as a typed object with named attributes for each fanout axis.

    Use as a type hint on the context parameter of a @dynamic_graph_asset
    decorated function to:

        @dynamic_graph_asset(mapping_keys=["state", "disease"], ...)
        def my_asset(context: DynamicGraphAssetExecutionContext, config: MyConfig):
            context.mapping_key["states"]    # "AK"
            context.mapping_key["diseases"]  # "COVID-19"
    """

    _mapping_key: dict | None = None
    _mapping_key_names: list[str] = []
    _should_run_output_fn = False

    @property
    def mapping_key(self) -> dict[str, str]:
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


# -- Decorator --
def dynamic_graph_asset(
    mapping_keys: List[str],
    **graph_asset_kwargs,
):
    """
    Decorator that wires a function into a dynamic graph asset.
    See https://docs.dagster.io/guides/build/ops/dynamic-graphs#using-dynamic-outputs
    and https://docs.dagster.io/guides/build/assets/graph-backed-assets

    The decorated function becomes the compute op, called once per combination
    of mapping_keys field values. Config fields listed in `mapping_keys` are unpacked to
    single scalar values inside the function body.

    mapping_keys values are encoded into the mapping key using _XX_ hex escaping so
    original values (including spaces, hyphens, etc.) are recovered exactly in
    the compute op, while safe characters remain human-readable in the Dagster UI.

    Output can be returned by calling context.register_output(Callable[Any, dg.Output]) in
    the first line of the decorated function.

    Args:
        mapping_keys: List of config field names to fan out over. Must be iterable
                fields on the Config class (e.g. List[str]). The cartesian
                product of all fields becomes the set of dynamic mapping keys.
        **graph_asset_kwargs: Passed through to @dg.graph_asset, e.g.
                              partitions_def, ins, metadata, etc.

    Example:
        class MyAssetConfig(dg.Config):
            disease: List[str]
            state: List[str]
            container: str

        @dynamic_graph_asset(
            mapping_keys=["disease", "state"],
            partitions_def=daily_partitions,
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
        )
        def my_dynamic_asset(context: dg.OpExecutionContext, config: MyAssetConfig, upstream_asset):
            context.register_output(lambda: dg.Output(
                value=f"staging/{context.partition_key}",
                metadata={"container": config.base_output_prefix},
            ))
            my_code_pipeline(context.mapping_key["disease"], context.mapping_key["state"], upstream_asset)
    """

    def decorator(fn):
        asset_name = fn.__name__
        sig = inspect.signature(fn)

        # -- Validate register_output is first --
        _validate_register_output_first(fn)

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

        # -- Validate mapping_keys fields --
        for field in mapping_keys:
            if field not in config_cls.model_fields:
                raise ValueError(
                    f"@dynamic_graph_asset '{asset_name}': mapping_keys field '{field}' "
                    f"does not exist on {config_cls.__name__}. "
                    f"Available fields: {list(config_cls.model_fields.keys())}"
                )
            field_annotation = config_cls.model_fields[field].annotation
            origin = get_origin(field_annotation)
            if origin is None or origin not in (list, tuple, set, frozenset):
                raise ValueError(
                    f"@dynamic_graph_asset '{asset_name}': mapping_keys field '{field}' "
                    f"on {config_cls.__name__} must be an iterable type (e.g. list[str]), "
                    f"got '{field_annotation}'"
                )
        # Infer upstream asset ins from all parameters that aren't context or config
        inferred_ins = {
            name: dg.AssetIn(name)
            for name in sig.parameters
            if name not in ["config", "context"]
        }

        log.debug(f"inferred_ins: '{inferred_ins}'")

        # -- Pull ins out of graph_asset_kwargs --
        asset_ins = {**inferred_ins, **graph_asset_kwargs.pop("ins", {})}

        # -- gen_config op --
        @dg.op(
            name=f"{asset_name}__gen_config",
            out=dg.DynamicOut(dg.Nothing),
            tags=multiprocess_config.to_run_tags(),
        )
        def gen_config(context):
            config = config_cls(**context.op_config)
            axes = [getattr(config, field) for field in mapping_keys]
            for combo in itertools.product(*axes):
                mapping_key = _encode_mapping_key(combo)
                yield dg.DynamicOutput(value=None, mapping_key=mapping_key)

        # -- compute op --
        @dg.op(
            name=f"{asset_name}__compute",
            ins={"_": dg.In(dg.Nothing), **{k: dg.In() for k in asset_ins}},
            out=dg.Out(dg.Nothing),
            config_schema=config_cls.to_config_schema(),
        )
        def compute(context, **kwargs):
            original_values = _decode_mapping_key(context.get_mapping_key())
            overrides = {k: [v] for k, v in zip(mapping_keys, original_values)}
            # override mapping_keys with dimension values for this iteration
            config = config_cls(**{**context.op_config, **overrides})
            upstream_kwargs = {k: v for k, v in kwargs.items() if k != "_"}
            dynamic_context = DynamicGraphAssetExecutionContext.inject(
                context,
                mapping_keys,
                False,
            )
            fn(dynamic_context, config, **upstream_kwargs)

        # -- output op --
        @dg.op(
            name=f"{asset_name}__output",
            ins={"_": dg.In(dg.Nothing), **{k: dg.In() for k in asset_ins}},
            config_schema=config_cls.to_config_schema(),
            tags=multiprocess_config.to_run_tags(),
        )
        def output_op(context, **kwargs):
            config = config_cls(**context.op_config)
            upstream_kwargs = {k: v for k, v in kwargs.items() if k != "_"}
            dynamic_context = DynamicGraphAssetExecutionContext.inject(
                context,
                mapping_keys,
                True,
            )
            try:
                fn(dynamic_context, config, **upstream_kwargs)
            except _CaptureOutput as e:
                output = e._output
                return output

        # -- config mapping --
        @dg.config_mapping(config_schema=config_cls.to_config_schema())
        def _config_mapping(config):
            ops = {
                f"{asset_name}__{op}": {"config": config}
                for op in ("gen_config", "compute", "output")
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
            keys = gen_config()
            res = keys.map(lambda nothing: compute(_=nothing, **ins_kwargs))
            return output_op(_=res.collect(), **ins_kwargs)

        return _asset

    return decorator
