"""Commands of the embeddings feature."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, NonNegativeInt, model_validator

from openknowledge_contracts.datasets import DiffPart, ReleaseId
from openknowledge_contracts.manifests import BucketName, ObjectPath, ObjectPrefix
from openknowledge_core.embeddings.models import ShardPart
from openknowledge_core.ingestion.models import check_diff


class ConvertEmbeddings(BaseModel):
    """Convert an archived SPECTER2 base, or one part of an archived diff."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    release: ReleaseId
    raw_bucket: BucketName
    curated_bucket: BucketName
    prefix: ObjectPrefix = "semantic-scholar"
    from_release: ReleaseId | None = None
    part: DiffPart | None = None

    @model_validator(mode="after")
    def _diff(self) -> Self:
        check_diff(self.from_release, self.part, self.release)
        return self

    @property
    def shard_part(self) -> ShardPart:
        return self.part or "base"


class ConvertEmbeddingShard(ConvertEmbeddings):
    shard: NonNegativeInt
    """Index into the part's archive manifest."""


class ConvertResult(BaseModel):
    release: ReleaseId
    part: ShardPart
    shards: int
    submitted: tuple[int, ...]
    job: str | None


class ConvertShardResult(BaseModel):
    source: ObjectPath
    blob: ObjectPath | None
    index: ObjectPath
    papers: int
