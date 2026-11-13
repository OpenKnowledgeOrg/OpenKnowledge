import asyncio
import struct
from collections.abc import Sequence
from datetime import date

import pytest

from openknowledge_core.embeddings import (
    MAX_CONCURRENT_READS,
    ROW_BYTES,
    VECTOR_DIM,
    EmbeddingVectors,
    TooManyIds,
    lookup_vectors,
)
from openknowledge_core.ingestion.errors import IncompleteTransfer

RELEASE = date(2026, 10, 6)
BLOB = "embeddings/specter_v2/2026-10-06/base/part-0.f32"


class Index:
    def __init__(self, located: dict[int, tuple[str, int]]) -> None:
        self.located = located
        self.asked: list[Sequence[int]] = []

    async def locate(self, corpusids: Sequence[int]) -> tuple[date, dict[int, tuple[str, int]]]:
        self.asked.append(corpusids)
        return RELEASE, {c: self.located[c] for c in corpusids if c in self.located}


def row_of(value: float) -> bytes:
    return struct.pack(f"<{VECTOR_DIM}f", *[value] * VECTOR_DIM)


async def blob_rows(bucket: str, name: str, start: int, length: int) -> bytes:
    assert (bucket, name, length) == ("curated", BLOB, ROW_BYTES)
    return row_of(start // ROW_BYTES + 0.5)


async def test_lookup_returns_found_and_missing_in_request_order() -> None:
    index = Index({1: (BLOB, 4), 2: (BLOB, 7)})

    found = await lookup_vectors([3, 2, 2, 99, 1], index.locate, blob_rows, "curated")

    assert index.asked == [[3, 2, 99, 1]]
    assert found == EmbeddingVectors(
        release=RELEASE,
        vectors={2: (7.5,) * VECTOR_DIM, 1: (4.5,) * VECTOR_DIM},
        missing=(3, 99),
    )
    assert list(found.vectors) == [2, 1]


async def test_lookup_refuses_more_than_20000_ids() -> None:
    index = Index({})

    with pytest.raises(TooManyIds, match="20001"):
        await lookup_vectors(range(20_001), index.locate, blob_rows, "curated")
    assert index.asked == []


async def test_empty_lookup_queries_nothing() -> None:
    index = Index({})

    found = await lookup_vectors([], index.locate, blob_rows, "curated")

    assert found == EmbeddingVectors(release=None, vectors={}, missing=())
    assert index.asked == []


async def test_reads_are_bounded_to_64_at_once() -> None:
    index = Index({c: (BLOB, c) for c in range(500)})
    active = peak = 0

    async def slow(bucket: str, name: str, start: int, length: int) -> bytes:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.001)
        active -= 1
        return row_of(0.0)

    found = await lookup_vectors(range(500), index.locate, slow, "curated")

    assert len(found.vectors) == 500
    assert peak == MAX_CONCURRENT_READS == 64


async def test_short_read_is_an_error() -> None:
    index = Index({5: (BLOB, 3)})

    async def short(bucket: str, name: str, start: int, length: int) -> bytes:
        return b"\0" * (ROW_BYTES - 4)

    with pytest.raises(IncompleteTransfer, match=f"gs://curated/{BLOB} row 3"):
        await lookup_vectors([5], index.locate, short, "curated")
