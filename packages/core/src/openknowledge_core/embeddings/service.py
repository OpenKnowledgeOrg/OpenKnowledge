"""The services of the embeddings feature."""

from __future__ import annotations

import asyncio
import gzip
from collections.abc import Callable, Sequence
from contextlib import aclosing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO

from openknowledge_contracts.manifests import ManifestEntry, ObjectPath
from openknowledge_core.embeddings.commands import (
    ConvertEmbeddings,
    ConvertEmbeddingShard,
    ConvertResult,
    ConvertShardResult,
)
from openknowledge_core.embeddings.convert import convert_shard
from openknowledge_core.embeddings.errors import ConversionFailed, ShardInvalid
from openknowledge_core.embeddings.models import (
    CONVERT_FAMILY,
    DATASET,
    ConvertJob,
    IndexRow,
    ShardPart,
    output_names,
    output_prefix,
)
from openknowledge_core.embeddings.ports import ConvertJobs, EmbeddingStore
from openknowledge_core.ingestion.archive_progress import ArchiveReader
from openknowledge_core.ingestion.commands import DownloadDataset, DownloadDiff
from openknowledge_core.ingestion.errors import ObjectChanged, ShardNotListed
from openknowledge_core.ingestion.integrity import StreamCheck
from openknowledge_core.ingestion.models import JobOutcome

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
        part = command.shard_part
        entries = await self._entries(command)
        prefix = ObjectPath.under(command.curated_bucket, output_prefix(command.release, part))
        present = {stored.ref.path.name for stored in await self._store.list_objects(prefix)}
        missing: list[int] = []
        for shard, entry in enumerate(entries):
            blob, index = output_names(command.release, part, entry.object_ref.path.name)
            if index not in present or (part != "deletes" and blob not in present):
                missing.append(shard)
        name = None
        if missing:
            job = ConvertJob(
                release=command.release,
                raw_bucket=command.raw_bucket,
                prefix=command.prefix,
                curated_bucket=command.curated_bucket,
                part=part,
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
            part=part,
            shards=len(entries),
            submitted=tuple(missing),
            job=name,
        )

    async def convert_shard(self, command: ConvertEmbeddingShard) -> ConvertShardResult:
        """Write the blob first and the index last, replacing what an earlier attempt left."""
        part = command.shard_part
        entries = await self._entries(command)
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

    async def _entries(self, command: ConvertEmbeddings) -> tuple[ManifestEntry, ...]:
        base = DownloadDataset(
            release=command.release,
            dataset=DATASET,
            bucket=command.raw_bucket,
            prefix=command.prefix,
        )
        archive = (
            base
            if command.from_release is None
            else DownloadDiff(**base.model_dump(), from_release=command.from_release)
        )
        manifest = (await self._archive.completed_manifests(archive)).get(command.shard_part)
        return () if manifest is None else tuple(manifest.entries)

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
