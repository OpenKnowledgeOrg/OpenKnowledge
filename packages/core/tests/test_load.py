from uuid import UUID

import pytest

from openknowledge_contracts.manifests import (
    DatasetManifest,
    ManifestEntry,
    ObjectPath,
    ObjectRef,
)
from openknowledge_contracts.tables.contract import UpdateMode
from openknowledge_core.ingestion import (
    DownloadDatasetService,
    LoadDataset,
    LoadDatasetResult,
    LoadDatasetService,
    LoadDiff,
    LoadDiffResult,
)
from openknowledge_core.ingestion.errors import (
    BaseRefused,
    ConflictingKeys,
    DiffRefused,
    IngestionError,
    ObjectChanged,
    RowCountMismatch,
    TableUnavailable,
)
from openknowledge_core.ingestion.models import Duplicates, TableState
from tests.fakes.ports import (
    COMMAND,
    DIFF,
    NEXT,
    PAPERS,
    PRODUCER,
    RELEASE,
    FakeApplied,
    FakeSource,
    FakeStore,
    FakeTables,
    clock,
    gz,
    parquet,
)

LOAD = LoadDataset(**COMMAND.model_dump())
LOAD_DIFF = LoadDiff(**DIFF.model_dump())
PART_0 = ObjectPath.under(COMMAND.bucket, COMMAND.prefix, COMMAND.release, "papers", "part-0.gz")
CONFLICT = Duplicates(identical_rows=0, conflicting_keys=1, examples=("(7)",))


@pytest.fixture
async def tables(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> FakeTables:
    for name, data in PAPERS.items():
        source.add("papers", name, data)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    await service.write_manifest(pinned)
    return FakeTables(store)


@pytest.fixture
async def diff(
    request: pytest.FixtureRequest, service: DownloadDatasetService, source: FakeSource
) -> None:
    updates = {"update-0.gz": gz(b'{"id": 1}\n{"id": 3}\n')}
    deletes = {"delete-0.gz": gz(b'{"id": 2}\n')} if getattr(request, "param", True) else {}
    source.add_diff("papers", RELEASE, NEXT, updates, deletes)
    pinned = await service.pin_diff(DIFF)
    for part in (pinned.updates, pinned.deletes):
        if part.shards:
            for shard in part.shards:
                await service.archive_shard(part, shard)
            await service.write_manifest(part)


def loader(store: FakeStore, tables: FakeTables, applied: FakeApplied) -> LoadDatasetService:
    return LoadDatasetService(
        store=store, tables=tables, applied=applied, producer=PRODUCER, clock=clock
    )


async def load(
    store: FakeStore,
    tables: FakeTables,
    applied: FakeApplied | None = None,
    command: LoadDataset = LOAD,
) -> LoadDatasetResult:
    service = loader(store, tables, applied or FakeApplied())
    prepared = await service.prepare(command)
    outcomes = [await service.load_object(prepared, entry) for entry in prepared.manifest.entries]
    return await service.finish(prepared, outcomes)


async def load_diff(store: FakeStore, tables: FakeTables, applied: FakeApplied) -> LoadDiffResult:
    service = loader(store, tables, applied)
    prepared = await service.prepare_diff(LOAD_DIFF)
    outcomes = [
        await service.load_object(part, entry)
        for part in prepared.parts
        for entry in part.manifest.entries
    ]
    return await service.finish_diff(prepared, outcomes)


async def test_a_rerun_skips_every_loaded_object(store: FakeStore, tables: FakeTables) -> None:
    first = await load(store, tables)
    again = await load(store, tables)
    assert (first.loaded, first.already_loaded, first.live_rows) == (2, 0, 51)
    assert (again.loaded, again.already_loaded) == (0, 2)
    assert len(tables.loads) == 2
    assert tables.scanned == []


@pytest.mark.parametrize("change", ["recreated", "truncated", "new SQL"])
async def test_a_recreated_or_emptied_table_or_new_sql_loads_again(
    store: FakeStore, tables: FakeTables, change: str
) -> None:
    await load(store, tables)
    if change == "recreated":
        tables.uuid = UUID(int=2)
    elif change == "new SQL":
        # Receipts from an older load SQL count for nothing; every object loads again.
        tables.load_sql = "2" * 64
    else:
        tables.rows.clear()
    assert (await load(store, tables)).loaded == 2


async def test_an_object_with_another_generation_is_not_loaded(
    store: FakeStore, tables: FakeTables
) -> None:
    store.put(PART_0, PAPERS["part-0.gz"])
    with pytest.raises(ObjectChanged):
        await load(store, tables)
    assert tables.loads == []


async def test_missing_rows_fail_the_load(store: FakeStore, tables: FakeTables) -> None:
    tables.short_by = 1
    with pytest.raises(RowCountMismatch, match="wrote 0 rows"):
        await load(store, tables)
    assert tables.records == {}


async def test_keys_repeated_identically_are_counted(store: FakeStore, tables: FakeTables) -> None:
    tables.collapsed = 1
    tables.found = Duplicates(identical_rows=1, conflicting_keys=0, examples=())
    result = await load(store, tables)
    assert (result.live_rows, result.identical_duplicates) == (50, 1)
    assert tables.scanned == ["part-0.gz", "part-1.gz"]


async def test_context_enrichments_are_counted_separately_and_explain_missing_rows(
    store: FakeStore, tables: FakeTables
) -> None:
    tables.collapsed = 1
    tables.found = Duplicates(identical_rows=0, enriched_rows=1, conflicting_keys=0, examples=())
    result = await load(store, tables)
    assert (result.live_rows, result.identical_duplicates, result.enriched_duplicates) == (50, 0, 1)
    tables.collapsed = 2
    with pytest.raises(RowCountMismatch, match="1 context enrichments"):
        await load(store, tables)


@pytest.mark.usefixtures("diff")
async def test_diff_context_enrichments_are_reported_without_changing_delete_counts(
    store: FakeStore, tables: FakeTables
) -> None:
    applied = FakeApplied()
    await load(store, tables, applied)
    tables.found = Duplicates(identical_rows=0, enriched_rows=1, conflicting_keys=0, examples=())
    result = await load_diff(store, tables, applied)
    assert (result.identical_duplicates, result.enriched_duplicates, result.deleted) == (0, 1, 1)


@pytest.mark.parametrize(
    ("found", "error", "message"),
    [
        (
            Duplicates(identical_rows=0, conflicting_keys=1, examples=("(7)",)),
            ConflictingKeys,
            r"\(1, such as \(7\)\)",
        ),
        (
            Duplicates(identical_rows=0, conflicting_keys=0, examples=()),
            RowCountMismatch,
            "0 of them repeat",
        ),
    ],
    ids=["conflicting", "unexplained"],
)
async def test_other_repeated_keys_fail_the_load(
    store: FakeStore,
    tables: FakeTables,
    found: Duplicates,
    error: type[IngestionError],
    message: str,
) -> None:
    tables.collapsed = 1
    tables.found = found
    with pytest.raises(error, match=message):
        await load(store, tables)


async def test_conflicting_finalization_retry_preserves_checkpoints_without_applying(
    store: FakeStore, tables: FakeTables
) -> None:
    applied = FakeApplied()
    tables.collapsed = 1
    tables.found = CONFLICT
    for _ in range(2):
        with pytest.raises(ConflictingKeys, match="replaying the same objects cannot resolve"):
            await load(store, tables, applied)
        assert tables.loads == ["part-0.gz", "part-1.gz"]
        assert len(tables.records) == 2
        assert (tables.marked, applied.records) == ([], [])


@pytest.mark.parametrize(
    ("updates", "removed", "refused"),
    [("diffs", 0, NEXT), ("snapshot", 3, "2026-09-29")],
    ids=["diffs-another-base", "snapshot-older-base"],
)
async def test_only_a_base_load_that_passes_flags_and_records_what_the_table_holds(
    store: FakeStore, tables: FakeTables, updates: UpdateMode, removed: int, refused: str
) -> None:
    applied = FakeApplied()
    tables.updates = updates
    tables.flagged = 3
    tables.broken = 2
    tables.collapsed = 1
    tables.found = CONFLICT
    with pytest.raises(ConflictingKeys):
        await load(store, tables, applied)
    assert (tables.marked, applied.records) == ([], [])
    tables.collapsed = 0
    result = await load(store, tables, applied)
    assert (result.removed, tables.marked) == (removed, [RELEASE] if removed else [])
    assert result.broken_indexes == 2
    assert [
        (found.target_uuid, found.release, found.from_release) for found in applied.records
    ] == [(tables.uuid, RELEASE, None)]
    before = (tables.loads[:], tables.marked[:], applied.records[:])
    with pytest.raises(BaseRefused, match=f"holds {RELEASE}, so it cannot take the base {refused}"):
        await loader(store, tables, applied).prepare(LOAD.model_copy(update={"release": refused}))
    assert (tables.loads, tables.marked, applied.records) == before


@pytest.fixture
async def staged(
    service: DownloadDatasetService, source: FakeSource, tables: FakeTables
) -> FakeTables:
    """A snapshot table loaded through a staging copy, with the next release archived too."""
    source.releases.append(NEXT)
    pinned = await service.pin_dataset(COMMAND.model_copy(update={"release": NEXT}))
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    await service.write_manifest(pinned)
    tables.staged, tables.updates, tables.flagged = True, "snapshot", 3
    return tables


LOAD_NEXT = LOAD.model_copy(update={"release": NEXT})


def held(applied: FakeApplied) -> list[tuple[UUID, str]]:
    return [(found.target_uuid, found.release) for found in applied.records]


async def test_a_staged_table_takes_each_release_through_a_copy_it_swaps_in(
    store: FakeStore, staged: FakeTables
) -> None:
    applied = FakeApplied()
    service = loader(store, staged, applied)
    assert not await service.holds(LOAD)
    first = await load(store, staged, applied)
    assert (first.loaded, staged.copies, staged.swaps, staged.uuid) == (2, 1, 1, UUID(int=101))
    assert await service.holds(LOAD)
    # Holding the release under current receipts, the table is only checked again.
    again = await load(store, staged, applied)
    assert (again.loaded, again.already_loaded, staged.copies, staged.swaps) == (0, 2, 1, 2)
    result = await load(store, staged, applied, LOAD_NEXT)
    assert (result.loaded, result.live_rows, result.removed, staged.marked) == (2, 51, 0, [])
    assert (staged.copies, staged.uuid) == (2, UUID(int=102))
    assert held(applied) == [(UUID(int=101), RELEASE)] * 2 + [(UUID(int=102), NEXT)]
    # A receipt of another generation sends the rerun into a fresh copy, though every object
    # still has a receipt, and leaves the live table as it is until the swap.
    key = next(key for key, found in staged.records.items() if found.source_release == NEXT)
    staged.records[key] = staged.records[key].model_copy(update={"generation": 99})
    assert not await service.holds(LOAD_NEXT)
    live = dict(staged.rows)
    prepared = await service.prepare(LOAD_NEXT)
    outcomes = [await service.load_object(prepared, entry) for entry in prepared.manifest.entries]
    assert (prepared.table.into, staged.rows) == (f"{prepared.table.name}_staging", live)
    rerun = await service.finish(prepared, outcomes)
    assert (rerun.loaded, staged.copies, staged.uuid) == (2, 3, UUID(int=103))
    assert await service.holds(LOAD_NEXT)
    # An object without a receipt leaves the table behind the release.
    del staged.records[key]
    assert not await service.holds(LOAD_NEXT)


async def test_only_a_tidy_check_of_a_held_release_drops_the_copy_left_over(
    store: FakeStore, staged: FakeTables
) -> None:
    applied = FakeApplied()
    service = loader(store, staged, applied)
    await load(store, staged, applied)
    # A crash between swap and drop left the release before in the copy.
    staged.staging_uuid, staged.staging_release = UUID(int=900), "2026-09-29"
    assert await service.holds(LOAD) and staged.staging_release == "2026-09-29"
    assert not await service.holds(LOAD_NEXT, tidy=True) and staged.staging_release is not None
    assert await service.holds(LOAD, tidy=True) and staged.staging_release is None
    # A copy of a newer release is a load to resume.
    staged.staging_uuid, staged.staging_release = UUID(int=901), NEXT
    assert await service.holds(LOAD, tidy=True) and staged.staging_release == NEXT


async def test_a_failed_check_or_a_crash_before_the_swap_leaves_the_live_table(
    store: FakeStore, staged: FakeTables, monkeypatch: pytest.MonkeyPatch
) -> None:
    applied = FakeApplied()
    await load(store, staged, applied)
    staged.collapsed = 1
    with pytest.raises(RowCountMismatch):
        await load(store, staged, applied, LOAD_NEXT)
    assert (staged.uuid, staged.swaps, held(applied)) == (
        UUID(int=101),
        1,
        [(UUID(int=101), RELEASE)],
    )
    staged.collapsed = 0

    async def crash(table: TableState) -> None:
        raise TableUnavailable("the connection dropped")

    monkeypatch.setattr(staged, "swap", crash)
    with pytest.raises(TableUnavailable):
        await load(store, staged, applied, LOAD_NEXT)
    assert (staged.uuid, held(applied)[-1]) == (UUID(int=101), (UUID(int=102), NEXT))
    monkeypatch.undo()
    # The latest marker names the copy, but the live table still refuses an older release.
    with pytest.raises(BaseRefused, match=f"holds {RELEASE}"):
        await load(store, staged, applied, LOAD.model_copy(update={"release": "2026-09-29"}))
    # The copy is reused.
    service = loader(store, staged, applied)
    prepared = await service.prepare(LOAD_NEXT)
    outcomes = [await service.load_object(prepared, entry) for entry in prepared.manifest.entries]
    await service.finish(prepared, outcomes)
    retried = await service.finish(prepared, outcomes)
    assert (retried.already_loaded, retried.live_rows) == (2, 51)
    assert (staged.copies, staged.uuid, staged.loads.count("part-0.gz")) == (2, UUID(int=102), 2)


@pytest.mark.parametrize(
    ("diff", "loaded"),
    [
        (True, [("update-0.gz", "upsert"), ("delete-0.gz", "delete")]),
        (False, [("update-0.gz", "upsert")]),
    ],
    ids=["both-parts", "updates-only"],
    indirect=["diff"],
)
async def test_a_diff_loads_its_updates_then_its_deletes_once(
    store: FakeStore, tables: FakeTables, diff: None, loaded: list[tuple[str, str]]
) -> None:
    applied = FakeApplied()
    await load(store, tables, applied)
    tables.found = Duplicates(identical_rows=1, conflicting_keys=0, examples=())
    first = await load_diff(store, tables, applied)
    again = await load_diff(store, tables, applied)
    assert list(zip(tables.loads, tables.operations, strict=True))[2:] == loaded
    operations = [operation for _, operation in loaded]
    assert [record.operation for record in tables.records.values()][2:] == operations
    assert (first.loaded, first.upserted, first.deleted, first.identical_duplicates) == (
        len(loaded),
        2,
        operations.count("delete"),
        1,
    )
    assert len(first.manifests) == len(loaded)
    assert (again.loaded, again.already_loaded) == (0, len(loaded))
    assert tables.scanned == ["update-0.gz"]
    assert [(found.release, found.from_release) for found in applied.records] == [
        (RELEASE, None),
        (NEXT, RELEASE),
        (NEXT, RELEASE),
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("another-release", "holds 2026-10-20, so"),
        ("no-record", "holds no recorded release"),
        ("emptied", "holds no recorded release"),
        ("recreated", "holds no recorded release"),
        ("snapshot", "never a diff"),
    ],
)
@pytest.mark.usefixtures("diff")
async def test_a_diff_needs_a_table_of_diffs_that_holds_its_start(
    store: FakeStore, tables: FakeTables, change: str, message: str
) -> None:
    applied = FakeApplied()
    await load(store, tables, applied)
    if change == "another-release":
        applied.records.append(applied.records[0].model_copy(update={"release": "2026-10-20"}))
    elif change == "no-record":
        applied.records.clear()
    elif change == "emptied":
        tables.rows.clear()
    elif change == "recreated":
        tables.uuid = UUID(int=2)
    else:
        tables.updates = "snapshot"
    with pytest.raises(DiffRefused, match=message):
        await load_diff(store, tables, applied)
    assert tables.loads == ["part-0.gz", "part-1.gz"]


@pytest.mark.usefixtures("diff")
async def test_conflicting_keys_in_the_update_files_fail_the_diff(
    store: FakeStore, tables: FakeTables
) -> None:
    applied = FakeApplied()
    await load(store, tables, applied)
    tables.found = CONFLICT
    with pytest.raises(ConflictingKeys, match=r"updates/manifest\.json with other values"):
        await load_diff(store, tables, applied)
    assert tables.loads == ["part-0.gz", "part-1.gz"]
    assert [found.release for found in applied.records] == [RELEASE]


async def test_a_contract_reads_the_manifest_of_its_archived_dataset(store: FakeStore) -> None:
    object_path = ObjectPath.under("raw", "openalex", RELEASE, "works", "part-0.parquet")
    data = parquet(1)
    entry = ManifestEntry(
        object_ref=ObjectRef(path=object_path, generation=store.put(object_path, data)),
        size=len(data),
        sha256="0" * 64,
        md5="AAAAAAAAAAAAAAAAAAAAAA==",
        crc32c="AAAAAA==",
        records=1,
    )
    manifest = DatasetManifest(
        schema_version=1,
        release_id=RELEASE,
        dataset="works",
        entries=(entry,),
        producer=PRODUCER,
        completed_at=clock(),
    )
    path = ObjectPath.under("raw", "openalex", RELEASE, "works", "manifest.json")
    store.put(path, manifest.to_json())
    tables = FakeTables(store, archive={"works-abstracts": "works"})
    service = loader(store, tables, FakeApplied())
    prepared = await service.prepare(
        LoadDataset(release=RELEASE, dataset="works-abstracts", bucket="raw", prefix="openalex")
    )
    assert prepared.manifest_path == path
    assert prepared.manifest == manifest
    outcomes = [await service.load_object(prepared, e) for e in prepared.manifest.entries]
    result = await service.finish(prepared, outcomes)
    assert result.dataset == "works-abstracts"
    assert tables.keys == {"works-abstracts"}
