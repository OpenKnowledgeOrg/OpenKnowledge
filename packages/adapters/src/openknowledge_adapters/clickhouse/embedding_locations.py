"""Which Float32 file in the curated bucket, and which row of it, holds each paper's vector."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.load_sql import sql_sha256
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_core.embeddings.errors import IndexAhead, NoIndexLoaded
from openknowledge_core.embeddings.models import DATASET
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
RELEASES = "SELECT DISTINCT source_release AS release FROM paper_embedding_locations"
# The IDs go into the URL, which httpx caps at 64 KiB; a UInt64 takes up to 21 bytes there.
LOCATE_CHUNK = 2_000
# Every row, not FINAL, so a delete past the recorded release shows too.
LOCATE = (
    "SELECT corpusid, object, row, source_release, is_deleted FROM paper_embedding_locations "
    "WHERE corpusid IN {ids:Array(UInt64)}"
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
            dataset=DATASET,
            archive=DATASET,
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

    async def releases(self) -> set[ReleaseId]:
        return {row["release"] for row in await self._client.rows(RELEASES)}

    async def locate(self, corpusids: Sequence[int]) -> tuple[date, dict[int, tuple[str, int]]]:
        """The release the table holds, and the blob and row of each live paper among these."""
        held = held_release(await self.table(), await self._applied.latest(TABLE))
        if held is None:
            raise NoIndexLoaded(
                f"no embedding index loaded into {TABLE}; run okp embeddings load-index"
            )
        latest: dict[int, dict[str, Any]] = {}
        ahead: set[str] = set()
        for start in range(0, len(corpusids), LOCATE_CHUNK):
            chunk = corpusids[start : start + LOCATE_CHUNK]
            ids = "[" + ",".join(str(int(corpusid)) for corpusid in chunk) + "]"
            for row in await self._client.rows(LOCATE, ids=ids):
                if row["source_release"] > held:
                    ahead.add(row["source_release"])
                kept = latest.get(row["corpusid"])
                if kept is None or _version(row) > _version(kept):
                    latest[row["corpusid"]] = row
        if ahead:
            raise IndexAhead(
                f"{TABLE} has rows of {', '.join(sorted(ahead))} past the recorded {held} for "
                "these papers; a diff is loading or did not finish, so rerun load-index first"
            )
        located = {
            corpusid: (row["object"], row["row"])
            for corpusid, row in latest.items()
            if not row["is_deleted"]
        }
        return date.fromisoformat(held), located


def _version(row: dict[str, Any]) -> tuple[str, int]:
    """The table's version order: a later release wins, and a delete wins within one."""
    return row["source_release"], row["is_deleted"]
