"""The OpenAlex snapshot on S3: one manifest names the files of the one release it serves."""

from __future__ import annotations

import re
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, NonNegativeInt, SecretStr, ValidationError

from openknowledge_contracts.datasets import (
    DatasetName,
    DatasetSummary,
    ReleaseId,
    ReleaseMetadata,
    ShardRef,
)
from openknowledge_contracts.sources import SOURCES
from openknowledge_core.ingestion.errors import (
    TRANSIENT_STATUSES,
    SourceRejected,
    SourceUnavailable,
)
from openknowledge_core.ingestion.models import (
    DatasetListing,
    DiffChain,
    ReleaseDocument,
    ShardLink,
)

MANIFEST_PATH = "data/parquet/manifest.json"
DELETED_PATH = "/data/parquet/works/deleted_ids.csv.gz"
DELETED_DATASET = "works-deleted"
WORKS = "works"
AGAIN = "rerun later, and report it to OpenAlex if it repeats"
_ENTITIES = frozenset(SOURCES["openalex"].datasets) - {DELETED_DATASET}


class _Meta(BaseModel):
    model_config = ConfigDict(extra="allow", hide_input_in_errors=True)

    content_length: NonNegativeInt
    record_count: NonNegativeInt


class _File(BaseModel):
    model_config = ConfigDict(extra="allow", hide_input_in_errors=True)

    url: str
    meta: _Meta


class _Entity(BaseModel):
    model_config = ConfigDict(extra="allow", hide_input_in_errors=True)

    entity: str
    files: list[_File]


class _Manifest(BaseModel):
    model_config = ConfigDict(extra="allow", hide_input_in_errors=True)

    date: ReleaseId
    entities: list[_Entity]


class OpenAlexCatalog:
    """Release metadata and dataset listings, read from the manifest each time they are asked."""

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self._http = http
        self._base = base_url.rstrip("/")

    async def list_releases(self) -> tuple[ReleaseId, ...]:
        manifest, _ = await self._manifest()
        return (manifest.date,)

    async def resolve_release(self, selector: ReleaseId | Literal["latest"]) -> ReleaseMetadata:
        manifest, _ = await self._manifest(None if selector == "latest" else selector)
        names = [*(entity.entity for entity in manifest.entities), DELETED_DATASET]
        return ReleaseMetadata(
            release_id=manifest.date,
            README="",
            datasets=tuple(_summary(name) for name in names),
        )

    async def release_documents(self, release_id: ReleaseId) -> tuple[ReleaseDocument, ...]:
        _, raw = await self._manifest(release_id)
        return (ReleaseDocument(name="manifest.json", data=raw),)

    async def list_dataset(self, release_id: ReleaseId, dataset: DatasetName) -> DatasetListing:
        manifest, _ = await self._manifest(release_id)
        if dataset == DELETED_DATASET:
            shard = ShardRef(source_path=DELETED_PATH, file_name="deleted_ids.csv.gz")
            link = ShardLink(shard=shard, url=SecretStr(self._base + DELETED_PATH))
            return DatasetListing(summary=_summary(dataset), links=(link,))
        entity = next((e for e in manifest.entities if e.entity == dataset), None)
        if entity is None:
            raise SourceRejected(f"release {release_id} of OpenAlex has no dataset {dataset!r}")
        pattern = re.compile(
            rf"s3://openalex/(data/parquet/{re.escape(dataset)}"
            r"/updated_date=\d{4}-\d{2}-\d{2}/[A-Za-z0-9][A-Za-z0-9._-]*\.parquet)"
        )
        links: list[ShardLink] = []
        for file in entity.files:
            found = pattern.fullmatch(file.url)
            if found is None:
                raise SourceRejected(
                    f"a file of {dataset!r} in the OpenAlex manifest is outside the "
                    f"data/parquet/{dataset}/updated_date=.../ layout; {AGAIN}"
                )
            key = found.group(1)
            links.append(
                ShardLink(
                    shard=ShardRef(source_path="/" + key, file_name=key.rsplit("/", 1)[-1]),
                    url=SecretStr(f"{self._base}/{key}"),
                    size=file.meta.content_length,
                    records=file.meta.record_count,
                )
            )
        return DatasetListing(summary=_summary(dataset), links=tuple(links))

    async def list_diffs(
        self, start: ReleaseId, end: ReleaseId | Literal["latest"], dataset: DatasetName
    ) -> DiffChain:
        raise SourceRejected("OpenAlex publishes no diffs; each release loads whole")

    async def _manifest(self, release_id: ReleaseId | None = None) -> tuple[_Manifest, bytes]:
        """The manifest and its bytes as served; a release no longer served is refused."""
        try:
            response = await self._http.get(MANIFEST_PATH)
        except httpx.TransportError as exc:
            raise SourceUnavailable(
                f"OpenAlex did not answer for its manifest ({type(exc).__name__}); rerun later"
            ) from None
        except httpx.HTTPError as exc:
            raise SourceRejected(
                f"the OpenAlex manifest could not be read ({type(exc).__name__}); rerun later"
            ) from None
        if not response.is_success:
            status = response.status_code
            error = SourceUnavailable if status in TRANSIENT_STATUSES else SourceRejected
            raise error(f"OpenAlex answered HTTP {status} for its manifest; rerun later")
        try:
            manifest = _Manifest.model_validate_json(response.content)
        except ValidationError:
            raise SourceRejected(
                f"the OpenAlex manifest has missing or malformed fields; {AGAIN}"
            ) from None
        _check_entities(manifest)
        if release_id is not None and release_id != manifest.date:
            raise SourceRejected(
                f"OpenAlex serves release {manifest.date} now, not {release_id}; it keeps only "
                "its latest release"
            )
        return manifest, response.content


def _summary(name: str) -> DatasetSummary:
    return DatasetSummary(name=name, description=f"OpenAlex {name}", README="")


def _check_entities(manifest: _Manifest) -> None:
    names = [entity.entity for entity in manifest.entities]
    for name in names:
        if names.count(name) > 1:
            raise SourceRejected(f"the OpenAlex manifest lists {name!r} twice; {AGAIN}")
    if WORKS not in names:
        raise SourceRejected(f"the OpenAlex manifest lists no works entity; {AGAIN}")
    for name in names:
        if name not in _ENTITIES:
            raise SourceRejected(
                f"the OpenAlex manifest lists a new entity {name!r}; add it to the source registry"
            )
