"""Which Float32 file in the curated bucket, and which row of it, holds each paper's vector."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.load_sql import sql_sha256
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_core.embeddings.errors import NoIndexLoaded
from openknowledge_core.ingestion.errors import TableRejected
from openknowledge_core.ingestion.models import TableState, held_release

TABLE = "paper_embedding_locations"
# Conversion's Parquet has no blob or release column, so the insert adds both as constants.
INSERT = (
    "INSERT INTO paper_embedding_locations (corpusid, object, row, source_release, is_deleted) "
    "SELECT corpusid, {blob:String}, row, {release:Date}, is_deleted "
    "FROM input('corpusid UInt64, row UInt32, is_deleted UInt8') FORMAT Parquet"
)
# Grouped in key order, so a release never sits in memory at once.
PAPERS = (
    "SELECT count() AS papers FROM (SELECT corpusid FROM paper_embedding_locations "
    "WHERE source_release = {release:Date} GROUP BY corpusid) "
    "SETTINGS optimize_aggregation_in_order = 1"
)
# The IDs go into the URL, which httpx caps at 64 KiB; a UInt64 takes up to 21 bytes there.
LOCATE_CHUNK = 2_000
LOCATE = (
    "SELECT corpusid, object, row FROM paper_embedding_locations FINAL "
    "WHERE corpusid IN {ids:Array(UInt64)} AND is_deleted = 0"
)


class EmbeddingLocations:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client
        self._applied = ClickHouseAppliedReleases(client)

    async def table(self) -> TableState:
        rows = await self._client.rows(
            "SELECT uuid, total_rows FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=TABLE,
        )
        if not rows:
            raise TableRejected(f"table {TABLE} does not exist; run the migrations")
        digest = sql_sha256(INSERT)
        return TableState(
            name=TABLE,
            uuid=rows[0]["uuid"],
            has_rows=bool(rows[0]["total_rows"]),
            load_sql_sha256=digest,
            delete_sql_sha256=digest,
            updates="diffs",
        )

    async def load(self, release: ReleaseId, blob: str, parquet: bytes) -> int:
        return await self._client.written(INSERT, data=parquet, blob=blob, release=release)

    async def papers(self, release: ReleaseId) -> int:
        rows = await self._client.rows(PAPERS, release=release)
        return int(rows[0]["papers"])

    async def locate(self, corpusids: Sequence[int]) -> tuple[date, dict[int, tuple[str, int]]]:
        """The release the table holds, and the blob and row of each live paper among these."""
        held = held_release(await self.table(), await self._applied.latest(TABLE))
        if held is None:
            raise NoIndexLoaded(
                f"no embedding index loaded into {TABLE}; run okp embeddings load-index"
            )
        located: dict[int, tuple[str, int]] = {}
        for start in range(0, len(corpusids), LOCATE_CHUNK):
            chunk = corpusids[start : start + LOCATE_CHUNK]
            ids = "[" + ",".join(str(int(corpusid)) for corpusid in chunk) + "]"
            for row in await self._client.rows(LOCATE, ids=ids):
                located[row["corpusid"]] = (row["object"], row["row"])
        return date.fromisoformat(held), located
