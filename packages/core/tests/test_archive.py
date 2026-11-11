"""release.json, dataset.json and manifest.json: written once, read back by every rerun."""

import hashlib
import json

import pytest

from openknowledge_contracts.manifests import DatasetManifest, ObjectPath
from openknowledge_contracts.sources import SOURCES, ArchivedDataset, Source
from openknowledge_core.ingestion import DownloadDataset, DownloadDatasetService, PinRelease
from openknowledge_core.ingestion import service as service_module
from openknowledge_core.ingestion.archive_progress import ArchiveProgress, completion_path
from openknowledge_core.ingestion.errors import (
    ArchiveConflict,
    ReceiptInvalid,
    RowCountMismatch,
    SourceRejected,
    StoredCopyMismatch,
)
from openknowledge_core.ingestion.models import PinnedDataset, ReleaseDocument, release_record_path
from openknowledge_core.ingestion.service import SERVED_NOW
from tests.fakes.ports import (
    COMMAND,
    PAPERS,
    PRODUCER,
    RELEASE,
    FakeSource,
    FakeStore,
    clock,
    gz,
    parquet,
)

RELEASE_JSON = "gs://raw/s2/2026-10-06/release.json"
DATASET_JSON = "gs://raw/s2/2026-10-06/papers/dataset.json"


def add_papers(source: FakeSource) -> None:
    for name, data in PAPERS.items():
        source.add("papers", name, data)


async def test_release_and_inventory_land_before_any_shard(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    assert sorted(store.objects) == [DATASET_JSON, RELEASE_JSON]


async def test_archived_documents_keep_readmes_and_unknown_fields(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    release = json.loads(store.objects[RELEASE_JSON].data)["metadata"]
    assert release["README"] == source.readme
    assert release["license_notes"] == "ODC-BY for most datasets"
    assert release["datasets"][0]["README"] == "About papers"
    inventory = json.loads(store.objects[DATASET_JSON].data)
    assert inventory["dataset"]["citation"] == "Kinney et al. 2023"
    assert [shard["file_name"] for shard in inventory["shards"]] == ["part-0.gz", "part-1.gz"]
    for stored in store.objects.values():
        assert b"Signature" not in stored.data and b"files.test" not in stored.data


async def test_a_rerun_reads_the_archive_back_and_keeps_its_order(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    first = await service.pin_dataset(COMMAND)
    source.reorder = True
    rerun = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
        source=SOURCES["semantic-scholar"],
    )
    assert (await rerun.pin_dataset(COMMAND)).shards == first.shards


async def test_an_inventory_unlike_the_listing_conflicts(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    add_papers(source)
    await service.pin_dataset(COMMAND)
    source.add("papers", "part-2.gz", gz(b"x\n"))
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetInventory with a different set of shards than .* {SERVED_NOW}",
    ):
        await service.pin_dataset(COMMAND)


async def test_an_inventory_of_another_release_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    path = (await service.pin_dataset(COMMAND)).inventory_path
    stored = json.loads(store.data(path))
    stored["release_id"] = "2026-09-29"
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetInventory with a different release id than .* {SERVED_NOW}",
    ):
        await service.pin_dataset(COMMAND)


async def test_an_inventory_of_another_dataset_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    path = (await service.pin_dataset(COMMAND)).inventory_path
    stored = json.loads(store.data(path))
    stored["dataset"]["name"] = "authors"
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetInventory with a different dataset than .* {SERVED_NOW}",
    ):
        await service.pin_dataset(COMMAND)


async def test_a_release_record_of_another_release_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    await service.pin_release(PinRelease(release=RELEASE, bucket="raw", prefix="s2"))
    path = release_record_path("raw", "s2", RELEASE)
    stored = json.loads(store.data(path))
    stored["metadata"]["release_id"] = "2026-09-29"
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict, match=f"a ReleaseRecord with a different release id than .* {SERVED_NOW}"
    ):
        await service.pin_dataset(COMMAND)


async def test_a_corrupt_document_conflicts_without_echoing_it(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    store.put(release_record_path("raw", "s2", RELEASE), b"topsecret garbage")
    with pytest.raises(
        ArchiveConflict,
        match=r"not a valid ReleaseRecord \(the document could not be parsed as JSON",
    ) as info:
        await service.pin_dataset(COMMAND)
    assert "topsecret" not in str(info.value)


async def test_a_signing_parameter_upstream_is_refused_and_nothing_written(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    source.readme = "Get it at https://x.test/a?X-Amz-Signature=topsecret"
    with pytest.raises(SourceRejected, match=r"\$\.metadata\.README") as info:
        await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert "topsecret" not in str(info.value)
    assert store.objects == {}


async def test_ordinary_readme_links_archive(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    source.readme = "Docs at https://example.org/datasets?page=2&lang=en"
    await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert RELEASE_JSON in store.objects


async def test_the_manifest_waits_for_every_receipt_and_lists_exact_objects(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    await service.archive_shard(pinned, pinned.shards[0])
    with pytest.raises(ReceiptInvalid, match="is missing"):
        await service.write_manifest(pinned)
    await service.archive_shard(pinned, pinned.shards[1])
    path = await service.write_manifest(pinned)
    manifest = DatasetManifest.from_json(store.data(path))
    assert [entry.object_ref.path for entry in manifest.entries] == [
        pinned.object_path(shard) for shard in pinned.shards
    ]
    assert [entry.object_ref.generation for entry in manifest.entries] == [
        store.generation(pinned.object_path(shard)) for shard in pinned.shards
    ]
    assert [entry.sha256 for entry in manifest.entries] == [
        hashlib.sha256(PAPERS[shard.file_name]).hexdigest() for shard in pinned.shards
    ]
    assert await service.write_manifest(pinned) == path


async def test_a_manifest_that_disagrees_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    path = await service.write_manifest(pinned)
    stored = json.loads(store.data(path))
    stored["entries"][0]["object_ref"]["generation"] += 1
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match="a DatasetManifest with a different entry list than .* the receipts beside it",
    ):
        await service.write_manifest(pinned)


@pytest.mark.parametrize(
    ("field", "value", "named"),
    [("release_id", "2026-09-29", "release id"), ("dataset", "authors", "dataset")],
)
async def test_a_manifest_of_another_release_or_dataset_conflicts(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    field: str,
    value: str,
    named: str,
) -> None:
    add_papers(source)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    path = await service.write_manifest(pinned)
    stored = json.loads(store.data(path))
    stored[field] = value
    store.put(path, json.dumps(stored).encode())
    with pytest.raises(
        ArchiveConflict,
        match=f"a DatasetManifest with a different {named} than .* the receipts beside it",
    ):
        await service.write_manifest(pinned)


async def test_a_document_too_large_to_read_back_is_never_written(
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    add_papers(source)
    monkeypatch.setattr(service_module, "DOCUMENT_LIMIT", 100)
    with pytest.raises(SourceRejected, match="more than a rerun reads back"):
        await service.pin_release(PinRelease(bucket="raw", prefix="s2"))
    assert store.objects == {}


async def test_a_parquet_footer_unlike_the_listed_rows_leaves_no_object_or_receipt(
    source: FakeSource, store: FakeStore
) -> None:
    service, pinned = await pin_works(source, store)
    shard = pinned.shards[0]
    with pytest.raises(RowCountMismatch):
        await service.archive_shard(pinned, shard)
    assert str(pinned.object_path(shard)) not in store.objects
    assert str(pinned.receipt_path(shard)) not in store.objects


async def test_a_stored_parquet_copy_unlike_the_listed_size_is_not_receipted(
    source: FakeSource, store: FakeStore
) -> None:
    service, pinned = await pin_works(source, store, size=999_999)
    shard = pinned.shards[0]
    store.put(pinned.object_path(shard), parquet(10))
    with pytest.raises(StoredCopyMismatch, match="lists 999999"):
        await service.archive_shard(pinned, shard)
    assert str(pinned.receipt_path(shard)) not in store.objects


async def test_partition_folders_keep_same_named_files_apart(
    source: FakeSource, store: FakeStore
) -> None:
    openalex = Source(
        name="openalex",
        prefix="openalex",
        datasets={"works": ArchivedDataset("parquet", folders=1)},
        contracts={},
    )
    service = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
        source=openalex,
    )
    for day in ("2026-10-14", "2026-10-13"):
        source.add("works", f"updated_date={day}/part_0000.parquet", parquet(10))
    pinned = await service.pin_dataset(
        DownloadDataset(release=RELEASE, dataset="works", bucket="raw", prefix="openalex")
    )
    assert [str(pinned.object_path(shard)) for shard in pinned.shards] == [
        f"gs://raw/openalex/{RELEASE}/works/updated_date=2026-10-14/part_0000.parquet",
        f"gs://raw/openalex/{RELEASE}/works/updated_date=2026-10-13/part_0000.parquet",
    ]


async def test_partitioned_archives_complete(source: FakeSource, store: FakeStore) -> None:
    service = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
        source=SOURCES["openalex"],
    )
    for day in ("2026-10-14", "2026-10-13"):
        source.add("works", f"updated_date={day}/part_0000.parquet", parquet(10))
    command = DownloadDataset(release=RELEASE, dataset="works", bucket="raw", prefix="openalex")
    pinned = await service.pin_dataset(command)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    await ArchiveProgress(store, service, PRODUCER, clock).finalize(command)
    assert str(completion_path(command)) in store.objects


async def test_flat_shards_with_one_file_name_are_still_refused(
    service: DownloadDatasetService, source: FakeSource
) -> None:
    source.add("papers", "a/part-0.gz", gz(b"{}\n"))
    source.add("papers", "b/part-0.gz", gz(b"{}\n"))
    with pytest.raises(SourceRejected, match="same path or file name"):
        await service.pin_dataset(COMMAND)


async def pin_works(
    source: FakeSource, store: FakeStore, size: int | None = None
) -> tuple[DownloadDatasetService, PinnedDataset]:
    openalex = Source(
        name="openalex",
        prefix="openalex",
        datasets={"works": ArchivedDataset("parquet")},
        contracts={},
    )
    service = DownloadDatasetService(
        catalog=source,
        downloads=source,
        store=store,
        producer=PRODUCER,
        clock=clock,
        source=openalex,
    )
    source.add("works", "part-0.parquet", parquet(10))
    source.records["part-0.parquet"] = 11
    if size is not None:
        source.sizes["part-0.parquet"] = size
    pinned = await service.pin_dataset(
        DownloadDataset(release=RELEASE, dataset="works", bucket="raw", prefix="openalex")
    )
    return service, pinned


async def test_release_documents_land_verbatim_and_a_changed_copy_conflicts(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> None:
    add_papers(source)
    command = PinRelease(release="latest", bucket="raw", prefix="s2")
    first = b'{"date": "2026-10-06"}\n'
    source.documents = (ReleaseDocument(name="manifest.json", data=first),)
    await service.pin_release(command)
    path = ObjectPath.under("raw", "s2", RELEASE, "manifest.json")
    assert store.data(path) == first
    await service.pin_release(command)
    changed = b'{"date": "2026-10-06" }\n'
    source.documents = (ReleaseDocument(name="manifest.json", data=changed),)
    with pytest.raises(ArchiveConflict, match="manifest.json"):
        await service.pin_release(command)
