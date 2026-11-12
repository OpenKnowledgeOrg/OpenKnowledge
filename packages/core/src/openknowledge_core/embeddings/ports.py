"""Ports the embeddings feature needs."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.embeddings.models import ConvertJob
from openknowledge_core.ingestion.models import JobOutcome, StoredObject, TableState
from openknowledge_core.ingestion.ports import ArtifactStore


class EmbeddingStore(ArtifactStore, Protocol):
    async def replace_file(
        self, path: ObjectPath, file: Path, *, content_type: str
    ) -> StoredObject:
        """Upload a local file, replacing any object at ``path``."""
        ...


class LocationIndex(Protocol):
    async def table(self) -> TableState: ...

    async def load(self, release: ReleaseId, blob: str, parquet: bytes) -> int:
        """Insert one shard's index, its papers located in ``blob``, and return the rows written."""
        ...

    async def papers(self, release: ReleaseId) -> int:
        """The distinct papers that ``release`` located, which merges do not change."""
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
