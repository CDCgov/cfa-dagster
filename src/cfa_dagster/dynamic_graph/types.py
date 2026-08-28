import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Generic, Optional, TypeVar, get_origin

import dagster as dg
from pydantic import PrivateAttr


@dataclass
class _DimensionResourceInfo:
    param_name: str
    resource_cls: type
    dimension_fields: list[str]


T = TypeVar("T", default=str)


class GraphDimension(dg.Config, Generic[T]):
    values: list[T]
    inherit_values_from_upstream: bool = True
    _current_value: Optional[T] = PrivateAttr(default=None)

    def __init__(
        self,
        values: list[T],
        current_value: Optional[T] = None,
        inherit_values_from_upstream: bool = True,
    ):
        super().__init__(
            values=values,
            inherit_values_from_upstream=inherit_values_from_upstream,
            _current_value=current_value,
        )

    def set_current_value(self, value: T) -> None:
        self._current_value = value

    @property
    def current_value(self) -> Optional[T]:
        return self._current_value


class GraphDimensionExclusion(dg.Config, Generic[T]):
    values: list[T] = []

    def __init__(self, values: list[str]):
        super().__init__(values=values)


@dataclass
class _ExclusionResourceInfo:
    param_name: str
    resource_cls: type
    exclusion_fields: list[str]


def _is_dimension_annotation(annotation) -> bool:
    if get_origin(annotation) is GraphDimension:
        return True

    metadata = getattr(
        annotation,
        "__pydantic_generic_metadata__",
        None,
    )

    return metadata is not None and metadata.get("origin") is GraphDimension


def _is_exclusion_annotation(annotation) -> bool:
    if get_origin(annotation) is GraphDimensionExclusion:
        return True

    metadata = getattr(
        annotation,
        "__pydantic_generic_metadata__",
        None,
    )

    return (
        metadata is not None
        and metadata.get("origin") is GraphDimensionExclusion
    )


def _get_graph_dimension_value_type(annotation) -> Optional[type]:
    metadata = getattr(
        annotation,
        "__pydantic_generic_metadata__",
        None,
    )
    if metadata is None or metadata.get("origin") is not GraphDimension:
        return None

    args = metadata.get("args") or ()
    return args[0] if args else None


def _get_string_enum_values(value_type) -> Optional[list[str]]:
    if (
        inspect.isclass(value_type)
        and issubclass(value_type, Enum)
        and issubclass(value_type, str)
    ):
        return [str(member.value) for member in value_type]
    return None
