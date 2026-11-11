import json
from copy import deepcopy

import httpx
import pytest

from openknowledge_adapters.openalex import OpenAlexConfig, connect
from openknowledge_core.ingestion.errors import SourceRejected

CONFIG = OpenAlexConfig(
    timeout_seconds=5, download_connect_timeout_seconds=5, download_read_timeout_seconds=5
)
WORKS = "s3://openalex/data/parquet/works/updated_date=2026-10-14/part_000{}.parquet"


def _file(url: str, size: int, rows: int) -> dict[str, object]:
    return {"url": url, "meta": {"content_length": size, "record_count": rows}}


MANIFEST: dict[str, object] = {
    "date": "2026-10-14",
    "format": "parquet",
    "meta": {"record_count": 245_115, "content_length": 819_986_432},
    "entities": [
        {
            "entity": "works",
            "record_count": 245_115,
            "content_length": 819_986_435,
            "files": [
                _file(WORKS.format(0), 409_993_216, 245_112),
                _file(WORKS.format(1), 409_993_216, 2),
                _file(WORKS.format(2), 3, 1),
            ],
        },
        {
            "entity": "topics",
            "record_count": 1,
            "content_length": 9,
            "files": [
                _file(
                    "s3://openalex/data/parquet/topics/updated_date=2026-10-05/part_0000.parquet",
                    9,
                    1,
                )
            ],
        },
    ],
}


def serve(manifest: dict[str, object]) -> httpx.MockTransport:
    body = json.dumps(manifest, indent=2).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/data/parquet/manifest.json"
        return httpx.Response(200, content=body)

    return httpx.MockTransport(handler)


async def test_the_manifest_lists_works_files_with_declared_size_and_rows() -> None:
    async with connect(CONFIG, transport=serve(MANIFEST)) as oa:
        assert (await oa.catalog.resolve_release("latest")).release_id == "2026-10-14"
        listing = await oa.catalog.list_dataset("2026-10-14", "works")
    first = listing.links[0]
    assert [link.shard.file_name for link in listing.links] == [
        "part_0000.parquet",
        "part_0001.parquet",
        "part_0002.parquet",
    ]
    assert first.shard.source_path == (
        "/data/parquet/works/updated_date=2026-10-14/part_0000.parquet"
    )
    assert first.url.get_secret_value() == (
        "https://openalex.s3.amazonaws.com/data/parquet/works/updated_date=2026-10-14/part_0000.parquet"
    )
    assert (first.size, first.records) == (409_993_216, 245_112)


async def test_the_other_datasets_and_the_documents_come_from_the_same_manifest() -> None:
    async with connect(CONFIG, transport=serve(MANIFEST)) as oa:
        release = await oa.catalog.resolve_release("2026-10-14")
        topics = await oa.catalog.list_dataset("2026-10-14", "topics")
        deleted = await oa.catalog.list_dataset("2026-10-14", "works-deleted")
        (document,) = await oa.catalog.release_documents("2026-10-14")
        with pytest.raises(SourceRejected, match="no diffs"):
            await oa.catalog.list_diffs("2026-10-14", "latest", "works")
    assert [d.name for d in release.datasets] == ["works", "topics", "works-deleted"]
    assert topics.links[0].shard.source_path.startswith("/data/parquet/topics/updated_date=")
    (link,) = deleted.links
    assert (link.shard.source_path, link.size, link.records) == (
        "/data/parquet/works/deleted_ids.csv.gz",
        None,
        None,
    )
    assert (document.name, document.data) == (
        "manifest.json",
        json.dumps(MANIFEST, indent=2).encode(),
    )


async def test_a_release_the_bucket_no_longer_serves_is_refused_with_both_dates() -> None:
    async with connect(CONFIG, transport=serve(MANIFEST | {"date": "2027-01-13"})) as oa:
        with pytest.raises(SourceRejected, match="2027-01-13.*2026-10-14|2026-10-14.*2027-01-13"):
            await oa.catalog.list_dataset("2026-10-14", "works")


async def test_a_file_outside_the_works_layout_is_refused() -> None:
    manifest = deepcopy(MANIFEST)
    manifest["entities"][0]["files"][1]["url"] = (  # type: ignore[index]
        "s3://openalex/data/jsonl/works/updated_date=2026-10-14/part_0001.parquet"
    )
    async with connect(CONFIG, transport=serve(manifest)) as oa:
        with pytest.raises(SourceRejected, match="layout"):
            await oa.catalog.list_dataset("2026-10-14", "works")


@pytest.mark.parametrize("entity", ["new-thing", "works"])
async def test_a_new_or_repeated_entity_is_refused_by_name(entity: str) -> None:
    manifest = deepcopy(MANIFEST)
    manifest["entities"].append({"entity": entity, "files": []})  # type: ignore[attr-defined]
    async with connect(CONFIG, transport=serve(manifest)) as oa:
        with pytest.raises(SourceRejected, match=entity):
            await oa.catalog.resolve_release("latest")
