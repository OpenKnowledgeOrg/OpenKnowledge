"""The ClickHouse tables adapter against ``clickhouse local``: receipts and the staging copy."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import httpx

from openknowledge_adapters import clickhouse
from openknowledge_adapters.clickhouse.tables import staging_table
from openknowledge_core.ingestion.models import TableState
from tests.clickhouse_local import MIGRATIONS, needs_clickhouse, run, section, up

CONFIG = clickhouse.ClickHouseConfig.model_validate(
    {
        "url": "http://clickhouse:8123",
        "user": "openknowledge",
        "password": "hunter2",
        "raw_bucket": "raw",
        "timeout_seconds": 5,
    }
)


def local_clickhouse(workdir: Path) -> httpx.MockTransport:
    """ClickHouse's HTTP interface, answered by ``clickhouse local`` on ``workdir``."""

    def handle(request: httpx.Request) -> httpx.Response:
        params = {
            key.removeprefix("param_"): value
            for key, value in request.url.params.items()
            if key.startswith("param_")
        }
        # The client asks for ISO timestamps, which a receipt reads back with their zone.
        iso = "SET date_time_output_format = 'iso';\n"
        result = run(iso + request.content.decode(), workdir, **params)
        assert result.returncode == 0, result.stderr
        return httpx.Response(200, text=result.stdout)

    return httpx.MockTransport(handle)


@needs_clickhouse
async def test_receipts_are_read_from_the_configured_archive_only(
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
        "INSERT INTO loads (target, target_uuid, source_release, source_object, generation, "
        "sha256, load_sql_sha256) VALUES ('openalex_works', {uuid:UUID}, {release:Date}, "
        "{path:String}, 1, repeat('a', 64), {hash:String});"
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
        old = await ch.tables.receipts(table, release, prefix="old/openalex")
        assert {path: found.load_sql_sha256 for path, found in old.items()} == {
            "gs://raw/old/openalex/2026-10-14/works/part.parquet": "0" * 64
        }
        assert await ch.tables.receipts(table, release, prefix="openalex") == {}
        # A receipt of a staging copy is read too; the loader judges its UUID.
        path = "gs://raw/openalex/2026-10-14/works/part.parquet"
        result = run(
            receipt,
            tmp_path,
            uuid=str(UUID(int=2)),
            release=release,
            path=path,
            hash=table.load_sql_sha256,
        )
        assert result.returncode == 0, result.stderr
        found = await ch.tables.receipts(table, release, prefix="openalex")
        assert [(key, record.target_uuid) for key, record in found.items()] == [(path, UUID(int=2))]


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
