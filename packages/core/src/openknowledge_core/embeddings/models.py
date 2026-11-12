"""Values the embeddings feature passes around."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Literal
from uuid import UUID, uuid4

VECTOR_DIM = 768
ROW_BYTES = VECTOR_DIM * 4
DATASET = "embeddings-specter_v2"
CONVERT_FAMILY = "embeddings-convert"
"""The Batch label of conversion jobs, which keeps archive downloads from adopting them."""

type ShardPart = Literal["base", "updates", "deletes"]


@dataclass(frozen=True)
class IndexRow:
    """Where a paper's vector sits in its shard's blob; a deleted paper's ``row`` means nothing."""

    corpusid: int
    row: int
    is_deleted: bool


@dataclass(frozen=True)
class ConvertJob:
    """Convert these shards of one archived part, each in its own Batch task."""

    release: str
    raw_bucket: str
    prefix: str
    curated_bucket: str
    part: ShardPart
    shards: tuple[int, ...]
    from_release: str | None = None
    operation_id: UUID = field(default_factory=uuid4)


def output_prefix(release: str, part: ShardPart) -> str:
    return f"embeddings/specter_v2/{release}/{part}"


def output_names(release: str, part: ShardPart, source_object: str) -> tuple[str, str]:
    """The blob and index object names converted from the raw object ``source_object``."""
    stem = f"{output_prefix(release, part)}/{PurePosixPath(source_object).stem}"
    return f"{stem}.f32", f"{stem}.index.parquet"
