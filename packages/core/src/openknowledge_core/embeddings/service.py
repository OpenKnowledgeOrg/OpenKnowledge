"""The services of the embeddings feature."""

from __future__ import annotations

import asyncio
import gzip
from collections.abc import Callable, Sequence
from contextlib import aclosing
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO

from openknowledge_contracts.datasets import DiffPart
from openknowledge_contracts.manifests import ManifestEntry, ObjectPath, Producer
from openknowledge_core.embeddings.commands import (
    ConvertEmbeddings,
    ConvertEmbeddingShard,
    ConvertResult,
    ConvertShardResult,
    LoadEmbeddingIndex,
    LoadIndexResult,
)
from openknowledge_core.embeddings.convert import convert_shard
from openknowledge_core.embeddings.errors import ConversionFailed, IndexIncomplete, ShardInvalid
from openknowledge_core.embeddings.models import (
    CONVERT_FAMILY,
    DATASET,
    ConvertJob,
    IndexRow,
    ShardPart,
    output_names,
    output_prefix,
)
from openknowledge_core.embeddings.ports import ConvertJobs, EmbeddingStore, LocationIndex
from openknowledge_core.ingestion.archive_progress import ArchiveReader
from openknowledge_core.ingestion.commands import DownloadDataset, DownloadDiff
from openknowledge_core.ingestion.errors import (
    BaseRefused,
    DiffRefused,
    ObjectChanged,
    ShardNotListed,
)
from openknowledge_core.ingestion.integrity import StreamCheck
from openknowledge_core.ingestion.models import (
    AppliedRelease,
    JobOutcome,
    StoredObject,
    held_release,
)
from openknowledge_core.ingestion.ports import AppliedReleases, ArtifactStore

type IndexWriter = Callable[[Sequence[IndexRow], BinaryIO], None]

BLOB_TYPE = "application/octet-stream"
INDEX_TYPE = "application/vnd.apache.parquet"


class EmbeddingConversion:
    def __init__(self, store: EmbeddingStore, write_index: IndexWriter) -> None:
        self._store = store
        self._write_index = write_index
        self._archive = ArchiveReader(store)

    async def convert(self, command: ConvertEmbeddings, jobs: ConvertJobs) -> ConvertResult:
        """Run one Batch task for each shard that lacks its blob or its index."""
        for name, queued in (await jobs.running(CONVERT_FAMILY)).items():
            if queued:
                await jobs.delete(name)
            await jobs.wait(name)
        converted = await _converted(self._store, self._archive, command)
        missing = [shard for shard, (_, index) in enumerate(converted) if index is None]
        name = None
        if missing:
            job = ConvertJob(
                release=command.release,
                raw_bucket=command.raw_bucket,
                prefix=command.prefix,
                curated_bucket=command.curated_bucket,
                part=command.shard_part,
                shards=tuple(missing),
                from_release=command.from_release,
            )
            name = await jobs.submit_convert(job)
            if await jobs.wait(name) is JobOutcome.FAILED:
                raise ConversionFailed(
                    f"{name} failed; rerun okp embeddings convert to convert the shards "
                    "still missing"
                )
        return ConvertResult(
            release=command.release,
            part=command.shard_part,
            shards=len(converted),
            submitted=tuple(missing),
            job=name,
        )

    async def convert_shard(self, command: ConvertEmbeddingShard) -> ConvertShardResult:
        """Write the blob first and the index last, replacing what an earlier attempt left."""
        part = command.shard_part
        entries = await _entries(self._archive, command)
        if command.shard >= len(entries):
            raise ShardNotListed(
                f"the {part} of {DATASET} {command.release} has {len(entries)} shards, "
                f"so there is no shard {command.shard}"
            )
        entry = entries[command.shard]
        blob, index = (
            ObjectPath(bucket=command.curated_bucket, name=name)
            for name in output_names(command.release, part, entry.object_ref.path.name)
        )
        with TemporaryDirectory() as scratch:
            raw, vectors, rows = (Path(scratch) / name for name in ("raw", "f32", "parquet"))
            await self._stage(entry, raw)
            papers = await asyncio.to_thread(self._convert, entry, part, raw, vectors, rows)
            if part != "deletes":
                await self._store.replace_file(blob, vectors, content_type=BLOB_TYPE)
            await self._store.replace_file(index, rows, content_type=INDEX_TYPE)
        return ConvertShardResult(
            source=entry.object_ref.path,
            blob=None if part == "deletes" else blob,
            index=index,
            papers=papers,
        )

    async def _stage(self, entry: ManifestEntry, raw: Path) -> None:
        check = StreamCheck()
        source = await self._store.open_read(entry.object_ref)
        with raw.open("wb") as sink:
            async with aclosing(source.chunks) as chunks:
                async for chunk in chunks:
                    check.update(chunk)
                    sink.write(chunk)
        report = check.finish(entry.size)
        if (report.sha256, report.md5, report.records) != (entry.sha256, entry.md5, entry.records):
            raise ObjectChanged(f"{entry.object_ref.path}: read bytes disagree with its manifest")

    def _convert(
        self, entry: ManifestEntry, part: ShardPart, raw: Path, vectors: Path, rows: Path
    ) -> int:
        try:
            with gzip.open(raw, "rb") as lines, vectors.open("wb") as blob:
                index = convert_shard(lines, part, blob)
        except ValueError as exc:
            raise ShardInvalid(f"{entry.object_ref.path}: {exc}") from None
        with rows.open("wb") as sink:
            self._write_index(index, sink)
        return len(index)


class EmbeddingIndexLoad:
    def __init__(
        self,
        *,
        store: ArtifactStore,
        index: LocationIndex,
        applied: AppliedReleases,
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> None:
        self._store = store
        self._archive = ArchiveReader(store)
        self._index = index
        self._applied = applied
        self._producer = producer
        self._clock = clock

    async def load(self, command: LoadEmbeddingIndex) -> LoadIndexResult:
        """Load every shard's index once all are converted, then record the release as applied.

        A table takes a base only while empty or holding that release, and a diff only from the
        release it holds, or again once it holds the diff's. A rerun writes the same rows again.
        """
        table = await self._index.table()
        held = held_release(table, await self._applied.latest(table.name))
        if command.from_release is None and held not in (None, command.release):
            raise BaseRefused(
                f"{table.name} holds {held}, so it cannot take the base {command.release}; "
                "a different base needs an empty or new table"
            )
        if command.from_release is not None and held not in (command.from_release, command.release):
            raise DiffRefused(
                f"{table.name} holds {held or 'no recorded release'}, so it cannot take the "
                f"diff from {command.from_release} to {command.release}"
            )
        diff = command.from_release is not None
        parts: tuple[DiffPart | None, ...] = ("updates", "deletes") if diff else (None,)
        indexes: list[tuple[str, StoredObject]] = []
        for part in parts:
            shards = ConvertEmbeddings(**command.model_dump(), part=part)
            converted = await _converted(self._store, self._archive, shards)
            missing = sum(index is None for _, index in converted)
            if missing:
                raise IndexIncomplete(
                    f"{missing} of {len(converted)} {shards.shard_part} shards of {DATASET} "
                    f"{command.release} are not converted; run okp embeddings convert first"
                )
            indexes += [
                ("" if part == "deletes" else blob, index)
                for blob, index in converted
                if index is not None
            ]
        rows = 0
        for blob, index in indexes:
            rows += await self._index.load(command.release, blob, await self._read(index))
        await self._applied.record(
            AppliedRelease(
                target=table.name,
                target_uuid=table.uuid,
                release=command.release,
                from_release=command.from_release,
                software_version=self._producer.software_version,
                image_digest=self._producer.image_digest,
                applied_at=self._clock(),
            )
        )
        repeated = rows - await self._index.papers(command.release)
        return LoadIndexResult(release=command.release, rows=rows, repeated=repeated)

    async def _read(self, index: StoredObject) -> bytes:
        source = await self._store.open_read(index.ref)
        async with aclosing(source.chunks) as chunks:
            return b"".join([chunk async for chunk in chunks])


async def _entries(archive: ArchiveReader, command: ConvertEmbeddings) -> tuple[ManifestEntry, ...]:
    base = DownloadDataset(
        release=command.release,
        dataset=DATASET,
        bucket=command.raw_bucket,
        prefix=command.prefix,
    )
    target = (
        base
        if command.from_release is None
        else DownloadDiff(**base.model_dump(), from_release=command.from_release)
    )
    manifest = (await archive.completed_manifests(target)).get(command.shard_part)
    return () if manifest is None else tuple(manifest.entries)


async def _converted(
    store: ArtifactStore, archive: ArchiveReader, command: ConvertEmbeddings
) -> list[tuple[str, StoredObject | None]]:
    """Each archived shard's blob name and its index, which is None until both are written."""
    part = command.shard_part
    prefix = ObjectPath.under(command.curated_bucket, output_prefix(command.release, part))
    present = {stored.ref.path.name: stored for stored in await store.list_objects(prefix)}
    converted: list[tuple[str, StoredObject | None]] = []
    for entry in await _entries(archive, command):
        blob, index = output_names(command.release, part, entry.object_ref.path.name)
        written = part == "deletes" or blob in present
        converted.append((blob, present.get(index) if written else None))
    return converted
