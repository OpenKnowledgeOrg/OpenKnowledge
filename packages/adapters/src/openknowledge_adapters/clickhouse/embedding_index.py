"""The Parquet index of one converted embeddings shard, in the columns ClickHouse loads."""

from __future__ import annotations

from collections.abc import Sequence
from typing import BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

from openknowledge_core.embeddings import IndexRow

SCHEMA = pa.schema(
    [
        pa.field("corpusid", pa.uint64(), nullable=False),
        pa.field("row", pa.uint32(), nullable=False),
        pa.field("is_deleted", pa.uint8(), nullable=False),
    ]
)


def write_index(rows: Sequence[IndexRow], sink: BinaryIO) -> None:
    table = pa.Table.from_pylist(
        [{"corpusid": r.corpusid, "row": r.row, "is_deleted": int(r.is_deleted)} for r in rows],
        schema=SCHEMA,
    )
    with pq.ParquetWriter(sink, SCHEMA) as writer:
        writer.write_table(table)


def read_index(source: BinaryIO) -> list[IndexRow]:
    # pyarrow-stubs leave the ``columns`` parameter's list unparametrised.
    table = pq.ParquetFile(source).read()  # pyright: ignore[reportUnknownMemberType]
    if table.schema != SCHEMA:
        raise ValueError(f"embedding index has schema {table.schema}, expected {SCHEMA}")
    return [
        IndexRow(found["corpusid"], found["row"], bool(found["is_deleted"]))
        for found in table.to_pylist()
    ]
