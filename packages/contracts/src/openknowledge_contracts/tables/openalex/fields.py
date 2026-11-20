"""Storage for structs read as named tuples by ClickHouse's Parquet reader."""

import pyarrow as pa

from openknowledge_contracts.tables.fields import ArrowField, required


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
