"""Ports the embeddings feature needs."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Protocol

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.embeddings.models import ConvertJob
from openknowledge_core.ingestion.models import JobOutcome, StoredObject, TableState
from openknowledge_core.ingestion.ports import ArtifactStore


class VectorRanges(Protocol):
    async def read_range(self, bucket: str, name: str, start: int, length: int) -> bytes:
        """At most ``length`` bytes of the object from ``start``, fewer where it ends first."""
        ...


class VectorLocations(Protocol):
    async def locate(self, corpusids: Sequence[int]) -> tuple[date, dict[int, tuple[str, int]]]:
        """The release the index holds, and the blob and row of each live paper among these."""
        ...


class EmbeddingStore(ArtifactStore, VectorRanges, Protocol):
    async def replace_file(
        self, path: ObjectPath, file: Path, *, content_type: str
    ) -> StoredObject:
        """Upload a local file, replacing any object at ``path``."""
        ...


class LocationIndex(VectorLocations, Protocol):
    async def table(self) -> TableState: ...

    async def load(self, release: ReleaseId, blob: str, parquet: bytes) -> int:
        """Insert one shard's index, its papers located in ``blob``, and return the rows written."""
        ...

    async def papers(self, release: ReleaseId) -> int:
        """The distinct papers that ``release`` located, which merges do not change."""
        ...

    async def releases(self) -> set[ReleaseId]:
        """Every release that rows of the table came from."""
        ...


class ConvertJobs(Protocol):
    async def submit_convert(self, job: ConvertJob) -> str:
        """Start one task per shard and return the job's name."""
        ...

    async def running(self, family: str) -> dict[str, bool]:
        """Return this family's active jobs, each with whether it is still queued."""
        ...

    async def delete(self, name: str) -> None: ...

    async def wait(self, name: str) -> JobOutcome: ...
