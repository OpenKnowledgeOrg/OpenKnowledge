"""Values the embeddings feature passes around."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

VECTOR_DIM = 768
ROW_BYTES = VECTOR_DIM * 4

type ShardPart = Literal["base", "updates", "deletes"]


@dataclass(frozen=True)
class IndexRow:
    """Where a paper's vector sits in its shard's blob; a deleted paper's ``row`` means nothing."""

    corpusid: int
    row: int
    is_deleted: bool
