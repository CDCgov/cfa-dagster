import dagster as dg
import inspect
import itertools
import re
from typing import (
    Callable,
    get_origin,
    List,
    get_type_hints,
)
import sys
import logging
from .execution.utils import ExecutionConfig, SelectorConfig


log = logging.getLogger(__name__)

multiprocess_config = ExecutionConfig(
    executor=SelectorConfig(class_name="multiprocess_executor")
)


# -- Mapping key encoding --
def _encode_segment(value: str) -> str:
    """Encode forbidden characters as _XX_ (hex), leave [a-zA-Z0-9] as-is."""
    return re.sub(r'[^a-zA-Z0-9]', lambda m: f"_{ord(m.group()):02X}_", str(value))


def _decode_segment(encoded: str) -> str:
    """Decode _XX_ sequences back to original characters."""
    return re.sub(r'_([0-9A-F]{2})_', lambda m: chr(int(m.group(1), 16)), encoded)


def _encode_mapping_key(values: tuple) -> str:
    return "__".join(_encode_segment(v) for v in values)


def _decode_mapping_key(mapping_key: str) -> list:
    return [_decode_segment(segment) for segment in mapping_key.split("__")]


# -- Decorator --
def dynamic_graph_asset(
    mapping_keys: List[str],
    output: Callable[[dg.OpExecutionContext, dg.Config], dg.Output],
    **graph_asset_kwargs,
):
    """
    Decorator that wires a function into a dynamic graph asset.
    See https://docs.dagster.io/guides/build/ops/dynamic-graphs#using-dynamic-outputs

    The decorated function becomes the compute op, called once per combination
    of mapping_keys field values. Config fields listed in `mapping_keys` are unpacked to
    single scalar values inside the function body.

    mapping_keys values are encoded into the mapping key using _XX_ hex escaping so
    original values (including spaces, hyphens, etc.) are recovered exactly in
    the compute op, while safe characters remain human-readable in the Dagster UI.

    Args:
        mapping_keys: List of config field names to fan out over. Must be iterable
                fields on the Config class (e.g. List[str]). The cartesian
                product of all fields becomes the set of dynamic mapping keys.
        output: Callable that receives the config and returns a dg.Output.
                Called once after all branches complete.
        **graph_asset_kwargs: Passed through to @dg.graph_asset, e.g.
                              partitions_def, ins, metadata, etc.

    Example:
        class MyAssetConfig(dg.Config):
            disease: List[str]
            state: List[str]
            container: str

        @dynamic_graph_asset(
            mapping_keys=["disease", "state"],
            output=lambda config: dg.Output(
                value=f"staging/{config.container}",
                metadata={"container": config.container},
            ),
            partitions_def=daily_partitions,
            ins={"upstream": dg.AssetIn("some_upstream_asset")},
        )
        def cfa_county_rt(context: dg.OpExecutionContext, config: MyAssetConfig, upstream):
            # run my_code_pipeline for each combination of disease and state in MyAssetConfig
            my_code_pipeline(config.disease.pop(), config.state.pop(), upstream)
    """
    def decorator(fn):
        asset_name = fn.__name__
        sig = inspect.signature(fn)

        # -- Locate the Config parameter --
        hints = get_type_hints(fn, globalns=vars(sys.modules[fn.__module__]))
        config_cls = next(
            (
                hint for hint in hints.values()
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
        skip = {
            name for name, hint in hints.items()
            if hint is dg.OpExecutionContext
            or (inspect.isclass(hint) and issubclass(hint, dg.Config))
        }
        inferred_ins = {
            name: dg.AssetIn(name)
            for name in sig.parameters
            if name not in skip
            and name != "context"  # catch unannotated context too
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
            ins={
                "_": dg.In(dg.Nothing),
                **{k: dg.In() for k in asset_ins}
            },
            out=dg.Out(dg.Nothing),
            config_schema=config_cls.to_config_schema(),
        )
        def compute(context, **kwargs):
            log.debug(f"kwargs keys: {list(kwargs.keys())}")
            original_values = _decode_mapping_key(context.get_mapping_key())
            overrides = {k: [v] for k, v in zip(mapping_keys, original_values)}
            # override mapping_keys with dimension values for this iteration
            config = config_cls(**{**context.op_config, **overrides})
            upstream_kwargs = {k: v for k, v in kwargs.items() if k != "_"}
            fn(context, config, **upstream_kwargs)

        # -- output op --
        @dg.op(
            name=f"{asset_name}__output",
            ins={"_": dg.In(dg.Nothing)},
            tags=multiprocess_config.to_run_tags(),
        )
        def output_op(context):
            config = config_cls(**context.op_config)
            return output(context, config)

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
            return output_op(res.collect())

        return _asset

    return decorator
