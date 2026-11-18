import json
from uuid import UUID

import httpx
import pytest

from openknowledge_adapters import clickhouse
from openknowledge_adapters.clickhouse.load_sql import sql_sha256
from openknowledge_adapters.clickhouse.parquet_sql import parquet_upsert_query
from openknowledge_contracts.manifests import ObjectPath, ObjectRef
from openknowledge_contracts.tables.clickhouse import version_expression
from openknowledge_contracts.tables.openalex import WORKS_ABSTRACTS
from openknowledge_contracts.tables.s2 import CITATIONS
from openknowledge_core.ingestion import LoadDataset, LoadDatasetService
from openknowledge_core.ingestion.errors import (
    ContractViolation,
    CredentialRejected,
    IngestionError,
    TableRejected,
    TableUnavailable,
)
from openknowledge_core.ingestion.models import LoadObject
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
            await ch.tables.live_rows("papers", "2026-10-06")
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


async def test_abstracts_load_from_the_archived_works_parquet() -> None:
    queries: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        queries.append(request.content.decode())
        if "system.tables" in queries[-1]:
            row = {"uuid": "00000000-0000-0000-0000-000000000001", "total_rows": 0}
            return httpx.Response(200, text=json.dumps(row))
        return httpx.Response(200, headers={"X-ClickHouse-Summary": '{"written_rows": "2"}'})

    path = ObjectPath(
        bucket="raw", name="openalex/2026-10-14/works/updated_date=2026-10-14/part_0000.parquet"
    )
    async with clickhouse.connect(CONFIG, transport=httpx.MockTransport(handle)) as ch:
        table = await ch.tables.table("works-abstracts")
        written = await ch.tables.load(
            LoadObject(
                dataset="works-abstracts",
                release="2026-10-14",
                object_ref=ObjectRef(path=path, generation=1),
            )
        )
    assert (table.archive, table.name, written) == ("works", "openalex_abstracts", 2)
    assert table.load_sql_sha256 == sql_sha256(parquet_upsert_query(WORKS_ABSTRACTS))
    assert queries[-1] == parquet_upsert_query(WORKS_ABSTRACTS)


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
