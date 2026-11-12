"""One raw SPECTER2 shard to Float32 little-endian vectors and a corpusid index."""

from __future__ import annotations

import json
import math
import sys
from array import array
from collections.abc import Iterable
from typing import BinaryIO, cast

from openknowledge_core.embeddings.models import VECTOR_DIM, IndexRow, ShardPart


def convert_shard(lines: Iterable[bytes], part: ShardPart, blob: BinaryIO) -> list[IndexRow]:
    """Write each record's vector to ``blob`` in order; a repeated corpusid keeps its last row."""
    deleted = part == "deletes"
    rows: dict[int, int] = {}
    written = 0
    for number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        corpusid, record = _record(line, number)
        if deleted:
            rows[corpusid] = 0
            continue
        blob.write(_vector(corpusid, record.get("vector")).tobytes())
        rows[corpusid] = written
        written += 1
    return [IndexRow(corpusid, rows[corpusid], deleted) for corpusid in sorted(rows)]


def _record(line: bytes, number: int) -> tuple[int, dict[str, object]]:
    try:
        value: object = json.loads(line)
    except ValueError as error:
        raise ValueError(f"line {number}: not a JSON object") from error
    record = cast("dict[str, object]", value) if isinstance(value, dict) else {}
    corpusid = record.get("corpusid")
    if type(corpusid) is not int or not 0 <= corpusid < 2**64:
        raise ValueError(f"line {number}: expected a corpusid between 0 and 2^64 - 1")
    return corpusid, record


def _vector(corpusid: int, text: object) -> array[float]:
    try:
        vector = array("f", json.loads(cast(str, text)))
    except (TypeError, ValueError) as error:
        raise ValueError(f"corpusid {corpusid}: vector is not JSON text of numbers") from error
    if len(vector) != VECTOR_DIM or not all(map(math.isfinite, vector)):
        raise ValueError(f"corpusid {corpusid}: expected {VECTOR_DIM} finite Float32 values")
    if sys.byteorder == "big":
        vector.byteswap()
    return vector
