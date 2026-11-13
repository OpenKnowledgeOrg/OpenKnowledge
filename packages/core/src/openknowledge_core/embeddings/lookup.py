"""Each paper's SPECTER2 vector, read from its row of a flat Float32 file in the curated bucket."""

from __future__ import annotations

import asyncio
import struct
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import date

from openknowledge_core.embeddings.errors import TooManyIds
from openknowledge_core.embeddings.models import ROW_BYTES, VECTOR_DIM
from openknowledge_core.ingestion.errors import IncompleteTransfer

MAX_LOOKUP_IDS = 20_000
MAX_CONCURRENT_READS = 64

type Locate = Callable[[list[int]], Awaitable[tuple[date, dict[int, tuple[str, int]]]]]
"""The release the index holds, and the object and row of each live paper among these."""
type ReadRange = Callable[[str, str, int, int], Awaitable[bytes]]
"""``(bucket, name, start, length)``: at most ``length`` bytes of the object from ``start``."""


@dataclass(frozen=True)
class EmbeddingVectors:
    release: date | None
    """The release the index held, or None when no IDs were asked and none was consulted."""
    vectors: dict[int, tuple[float, ...]]
    missing: tuple[int, ...]
    """IDs never loaded or deleted, in the order first asked."""


async def lookup_vectors(
    corpusids: Iterable[int], locate: Locate, read_range: ReadRange, bucket: str
) -> EmbeddingVectors:
    wanted = list(dict.fromkeys(corpusids))
    if len(wanted) > MAX_LOOKUP_IDS:
        raise TooManyIds(
            f"{len(wanted)} distinct corpus IDs asked; one lookup takes at most {MAX_LOOKUP_IDS}"
        )
    if not wanted:
        return EmbeddingVectors(release=None, vectors={}, missing=())
    release, located = await locate(wanted)
    reads = asyncio.Semaphore(MAX_CONCURRENT_READS)

    async def read(name: str, row: int) -> tuple[float, ...]:
        async with reads:
            data = await read_range(bucket, name, row * ROW_BYTES, ROW_BYTES)
        if len(data) != ROW_BYTES:
            raise IncompleteTransfer(
                f"gs://{bucket}/{name} row {row} gave {len(data)} of {ROW_BYTES} bytes; "
                "reconvert that shard and load its index again"
            )
        return struct.unpack(f"<{VECTOR_DIM}f", data)

    try:
        async with asyncio.TaskGroup() as group:
            tasks = {
                corpusid: group.create_task(read(*located[corpusid]))
                for corpusid in wanted
                if corpusid in located
            }
    except BaseExceptionGroup as failed:
        raise failed.exceptions[0] from None
    return EmbeddingVectors(
        release=release,
        vectors={corpusid: task.result() for corpusid, task in tasks.items()},
        missing=tuple(corpusid for corpusid in wanted if corpusid not in located),
    )
