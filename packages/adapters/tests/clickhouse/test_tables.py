import json
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from openknowledge_adapters import clickhouse
from openknowledge_adapters.clickhouse.load_sql import sql_sha256
from openknowledge_adapters.clickhouse.parquet_sql import parquet_upsert_query
from openknowledge_adapters.clickhouse.tables import staging_table
from openknowledge_contracts.manifests import ObjectPath, ObjectRef
from openknowledge_contracts.tables.clickhouse import version_expression
from openknowledge_contracts.tables.openalex import CONTRACTS as OPENALEX_CONTRACTS
from openknowledge_contracts.tables.s2 import CITATIONS
from openknowledge_core.ingestion import LoadDataset, LoadDatasetService
from openknowledge_core.ingestion.errors import (
    ContractViolation,
    CredentialRejected,
    IngestionError,
    TableRejected,
    TableUnavailable,
)
from openknowledge_core.ingestion.models import LoadObject, TableState
from tests.clickhouse_local import MIGRATIONS, needs_clickhouse, run, section, up
from tests.fakes.ports import PRODUCER, FakeApplied, FakeStore, clock

CONFIG = clickhouse.ClickHouseConfig.model_validate(
    {
        "url": "http://clickhouse:8123",
        "user": "openknowledge",
        "password": "hunter2",
        "raw_bucket": "raw",
        "timeout_seconds": 5,
    }
)
VIOLATION = (
    "Code: 395. DB::Exception: papers: corpusid breaks the contract: while executing "
    "'FUNCTION throwIf(...)'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 26.8.22.13)"
)


@pytest.mark.parametrize(
    ("status", "code", "body", "error", "message"),
    [
        (500, "395", VIOLATION, ContractViolation, "papers: corpusid breaks the contract; "),
        (403, "516", "Code: 516. DB::Exception: wrong password", CredentialRejected, "ClickHouse"),
        (503, "", "", TableUnavailable, "ClickHouse query"),
        (404, "60", "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)", TableRejected, ""),
        (0, "", "", TableUnavailable, "ClickHouse query"),
    ],
)
async def test_errors_map_by_exception_code(
    status: int, code: str, body: str, error: type[IngestionError], message: str
) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert "hunter2" not in str(request.url)
        assert request.headers["X-ClickHouse-Key"] == "hunter2"
        if not status:
            raise httpx.ConnectError("refused")
        return httpx.Response(status, headers={"X-ClickHouse-Exception-Code": code}, text=body)

    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as ch:
        with pytest.raises(error) as raised:
            await ch.tables.table("papers")
    assert str(raised.value).startswith(message)
    assert "hunter2" not in str(raised.value)


@pytest.mark.parametrize("current", [False, True])
async def test_citations_require_enrichment_versions_before_checkpoint_reuse(current: bool) -> None:
    queries: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        query = request.content.decode()
        queries.append(query)
        if "system.tables" in query:
            row = {"uuid": "00000000-0000-0000-0000-000000000001", "total_rows": 100}
        else:
            assert "system.columns" in query
            row = {
                "default_expression": version_expression(CITATIONS)
                if current
                else "(toUInt64(source_release) * 2) + is_deleted"
            }
        return httpx.Response(200, text=json.dumps(row))

    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as ch:
        if current:
            assert (await ch.tables.table("citations")).has_rows
        else:
            service = LoadDatasetService(
                store=FakeStore(),
                tables=ch.tables,
                applied=FakeApplied(),
                producer=PRODUCER,
                clock=clock,
            )
            command = LoadDataset(release="2026-10-13", dataset="citations", bucket="raw")
            with pytest.raises(TableRejected, match="populated legacy table must be preserved"):
                await service.prepare(command)
    assert len(queries) == 2  # No checkpoint lookup, archive scan, or INSERT on legacy versions.


@pytest.mark.parametrize("found", [False, True])
async def test_applied_took_and_rebased_match_target_uuid_and_releases(found: bool) -> None:
    target_uuid = UUID("00000000-0000-0000-0000-000000000001")

    def handle(request: httpx.Request) -> httpx.Response:
        query, params = request.content.decode(), request.url.params
        assert (params["param_target"], params["param_uuid"]) == ("papers", str(target_uuid))
        if "from_release" in query:
            # A base load records no from_release; a diff records the release it starts from.
            assert query == (
                "SELECT 1 FROM applied WHERE target = {target:String} "
                "AND target_uuid = {uuid:UUID} AND from_release IS NULL "
                "AND release > {after:Date} AND release <= {through:Date} "
                "LIMIT 1 FORMAT JSONEachRow"
            )
            assert (params["param_after"], params["param_through"]) == ("2026-10-06", "2026-10-13")
        else:
            assert query == (
                "SELECT 1 FROM applied WHERE target = {target:String} "
                "AND target_uuid = {uuid:UUID} AND release = {release:String} LIMIT 1 "
                "FORMAT JSONEachRow"
            )
            assert params["param_release"] == "2026-10-06"
        return httpx.Response(200, text='{"1":1}\n' if found else "")

    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as ch:
        assert await ch.applied.took("papers", target_uuid, "2026-10-06") is found
        assert await ch.applied.rebased("papers", target_uuid, "2026-10-06", "2026-10-13") is found


@pytest.mark.parametrize("dataset", ["works-abstracts", "openalex-authors"])
async def test_openalex_loads_from_the_contracts_archived_parquet(dataset: str) -> None:
    contract = OPENALEX_CONTRACTS[dataset]
    queries: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        queries.append(request.content.decode())
        if "system.tables" in queries[-1]:
            row = {"uuid": "00000000-0000-0000-0000-000000000001", "total_rows": 0}
            return httpx.Response(200, text=json.dumps(row))
        if queries[-1].startswith("SELECT count()"):
            return httpx.Response(200, text='{"rows":2}')
        return httpx.Response(200, headers={"X-ClickHouse-Summary": '{"written_rows": "2"}'})

    path = ObjectPath(
        bucket="raw",
        name=f"openalex/2026-10-14/{contract.archived}/updated_date=2026-10-14/part_0000.parquet",
    )
    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as ch:
        table = await ch.tables.table(dataset)
        written = await ch.tables.load(
            LoadObject(
                dataset=dataset,
                into=table.into,
                release="2026-10-14",
                object_ref=ObjectRef(path=path, generation=1),
            )
        )
        assert await ch.tables.live_rows(table, "2026-10-14") == 2
    assert (table.archive, table.name, written) == (contract.archived, contract.table, 2)
    assert table.staged and table.into == table.name
    assert table.load_sql_sha256 == sql_sha256(parquet_upsert_query(contract))
    assert queries[-2] == parquet_upsert_query(contract)
    settings = "\nSETTINGS max_threads = 1, max_memory_usage = 6000000000"
    assert ("max_memory_usage = 6000000000" in queries[-2]) == (dataset == "openalex-authors")
    # A wide table's parts close at 256 MB of data, not at a row count.
    byte_blocks = "min_insert_block_size_rows = 0, min_insert_block_size_bytes = 268435456"
    assert (byte_blocks in queries[-2]) == (dataset == "openalex-authors")
    assert "max_insert_block_size_rows" not in queries[-2]
    suffix = settings if dataset == "openalex-authors" else ""
    assert queries[-1] == (
        "SELECT count() AS rows FROM {table:Identifier} FINAL "
        "WHERE source_release = {release:Date} AND NOT removed"
        f"{suffix} FORMAT JSONEachRow"
    )


def local_clickhouse(workdir: Path) -> httpx.MockTransport:
    """ClickHouse's HTTP interface, answered by ``clickhouse local`` on ``workdir``."""

    def handle(request: httpx.Request) -> httpx.Response:
        params = {
            key.removeprefix("param_"): value
            for key, value in request.url.params.items()
            if key.startswith("param_")
        }
        result = run(request.content.decode(), workdir, **params)
        assert result.returncode == 0, result.stderr
        return httpx.Response(200, text=result.stdout)

    return httpx.MockTransport(handle)


@needs_clickhouse
async def test_stale_checks_receipts_in_the_configured_archive_for_sql_and_uuid(
    tmp_path: Path,
) -> None:
    release = "2026-10-14"
    table = TableState(
        name="openalex_works",
        into="openalex_works",
        staged=True,
        dataset="works",
        archive="works",
        uuid=UUID(int=1),
        has_rows=True,
        load_sql_sha256="1" * 64,
        delete_sql_sha256="2" * 64,
        updates="snapshot",
    )
    receipt = (
        "INSERT INTO loads (target, target_uuid, source_release, source_object, load_sql_sha256) "
        "VALUES ('openalex_works', {uuid:UUID}, {release:Date}, {path:String}, {hash:String});"
    )
    migration = section(MIGRATIONS / "00008_create_loads.sql", "Up")
    result = run(
        migration + receipt,
        tmp_path,
        uuid=str(table.uuid),
        release=release,
        path="gs://raw/old/openalex/2026-10-14/works/part.parquet",
        hash="0" * 64,
    )
    assert result.returncode == 0, result.stderr
    async with clickhouse.connect(CONFIG, transport=local_clickhouse(tmp_path)) as ch:
        assert await ch.tables.stale(table, release, prefix="old/openalex")
        assert not await ch.tables.stale(table, release, prefix="openalex")
        # Loaded into a staging copy under current SQL, but not swapped in.
        result = run(
            receipt,
            tmp_path,
            uuid=str(UUID(int=2)),
            release=release,
            path="gs://raw/openalex/2026-10-14/works/part.parquet",
            hash=table.load_sql_sha256,
        )
        assert result.returncode == 0, result.stderr
        assert await ch.tables.stale(table, release, prefix="openalex")
        assert await ch.tables.current(table, release, prefix="openalex") == 0
        result = run(
            receipt,
            tmp_path,
            uuid=str(table.uuid),
            release=release,
            path="gs://raw/openalex/2026-10-14/works/part2.parquet",
            hash=table.load_sql_sha256,
        )
        assert result.returncode == 0, result.stderr
        assert await ch.tables.current(table, release, prefix="openalex") == 1


@needs_clickhouse
async def test_a_staging_copy_is_reused_only_for_its_release_and_the_live_schema(
    tmp_path: Path,
) -> None:
    def sql(statement: str) -> list[str]:
        result = run(statement, tmp_path)
        assert result.returncode == 0, result.stderr
        return result.stdout.split()

    def insert(table: str, *works: tuple[int, str]) -> None:
        rows = ", ".join(f"({work}, '{release}')" for work, release in works)
        sql(f"INSERT INTO {table} (work_id, source_release) VALUES {rows}")

    sql(up())
    old, new = "2026-10-14", "2027-01-13"
    insert("openalex_works", (1, old))
    async with clickhouse.connect(CONFIG, transport=local_clickhouse(tmp_path)) as ch:
        live = await ch.tables.table("works")
        staged = await ch.tables.stage(live, new)
        assert (staged.name, staged.into, staged.has_rows) == (
            live.name,
            staging_table(live.name),
            False,
        )
        insert(staged.into, (2, new))
        staged = await ch.tables.stage(live, new)
        assert staged.has_rows
        assert (await ch.tables.live_rows(staged, new), await ch.tables.live_rows(live, new)) == (
            1,
            0,
        )
        # Rows of another release, or a migration of the live table, start an empty copy.
        insert(staged.into, (3, old))
        assert not (await ch.tables.stage(live, new)).has_rows
        insert(staged.into, (2, new))
        sql("ALTER TABLE openalex_works ADD COLUMN extra UInt8")
        assert not (await ch.tables.stage(live, new)).has_rows
        assert "extra" in sql(f"SELECT name FROM system.columns WHERE table = '{staged.into}'")
        insert(staged.into, (2, new), (4, new))
        await ch.tables.swap(staged)
        await ch.tables.swap(live)  # a retry only drops what is left
        # A copy of a newer release is a load to resume; one of the held release is left over.
        sql(f"CREATE TABLE {staged.into} AS openalex_works")
        insert(staged.into, (5, "2027-04-14"))
        await ch.tables.drop_leftover(live, new)
        assert sql(f"SELECT count() FROM {staged.into}") == ["1"]
        sql(f"TRUNCATE TABLE {staged.into}")
        insert(staged.into, (5, old))
        await ch.tables.drop_leftover(live, new)
    assert sql("SELECT work_id, source_release FROM openalex_works ORDER BY work_id") == [
        "2",
        new,
        "4",
        new,
    ]
    assert sql("SELECT name FROM system.tables WHERE name LIKE '%staging'") == []


async def test_a_retried_parquet_duplicates_check_keeps_its_query_id() -> None:
    ids: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        ids.append(request.url.params["query_id"])
        row: dict[str, object] = {"identical_rows": 1, "conflicting_keys": 0, "examples": []}
        return httpx.Response(200, text=json.dumps(row))

    paths = [
        ObjectPath(bucket="raw", name=f"openalex/2026-10-14/works/part_{i}.parquet") for i in (0, 1)
    ]
    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as ch:
        for _ in range(2):
            await ch.tables.duplicates("works", paths)
    assert ids[0] == ids[1]
