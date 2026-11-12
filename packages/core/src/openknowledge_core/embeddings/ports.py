"""Ports the embeddings feature needs."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.embeddings.models import ConvertJob
from openknowledge_core.ingestion.models import JobOutcome, StoredObject
from openknowledge_core.ingestion.ports import ArtifactStore


class EmbeddingStore(ArtifactStore, Protocol):
    async def replace_file(
        self, path: ObjectPath, file: Path, *, content_type: str
    ) -> StoredObject:
        """Upload a local file, replacing any object at ``path``."""
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
