"""Storage for structs read as named tuples by ClickHouse's Parquet reader."""

import pyarrow as pa

from openknowledge_contracts.tables.fields import ArrowField, required

LABEL = pa.dictionary(pa.int32(), pa.string())
"""An enum-like string, stored as ``LowCardinality``."""

_ID_PREFIX = b"openalex_id"


def openalex_id(name: str, prefix: str, *, nullable: bool = True) -> ArrowField:
    """An OpenAlex URL such as ``https://openalex.org/I27837315``, stored as its number.

    The load parses it and refuses one that is not ``prefix`` followed by digits.
    """
    return pa.field(name, pa.uint64(), nullable=nullable, metadata={_ID_PREFIX: prefix.encode()})


def openalex_ids(name: str, prefix: str) -> ArrowField:
    """A list of OpenAlex URLs of one entity, stored as numbers; a null item is refused."""
    return required(name, pa.list_(openalex_id("item", prefix, nullable=False)))


def id_prefix(field: ArrowField) -> str | None:
    """The entity letter of an OpenAlex ID field, or ``None`` for any other field."""
    prefix = (field.metadata or {}).get(_ID_PREFIX)
    return None if prefix is None else prefix.decode()


def stored_fields(fields: list[ArrowField]) -> list[ArrowField]:
    """A top-level struct has a tuple and a flag indicating that it has data.

    A null struct reads as null scalars and empty collections, including inside other
    tuples. The Parquet reader exposes the same values for a struct with all empty members.
    """
    result: list[ArrowField] = []
    for field in fields:
        if isinstance(field.type, pa.StructType) and field.nullable:
            result.extend(
                (required(field.name, field.type), required(f"has_{field.name}", pa.bool_()))
            )
        else:
            result.append(field)
    return result
