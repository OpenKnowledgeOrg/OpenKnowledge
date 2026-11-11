import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, nullcontext
from datetime import datetime
from time import monotonic
from uuid import UUID

from pydantic import TypeAdapter

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.crosswalk import (
    BEST_COLUMNS,
    XWALK_COLUMNS,
    best_select,
    expansion_sql,
    xwalk_select,
)
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.manifests import Producer
from openknowledge_core.ingestion.errors import TableAccessDenied, TableRejected
from openknowledge_core.ingestion.models import AppliedRelease, TableState, held_release
from openknowledge_core.ingestion.read_tables import (
    GROUP_SOURCES,
    GROUPS,
    QUARTERLY,
    READ_TABLES,
    REBUILT_TOGETHER,
    SOURCE_OF,
    BuildReadTablesResult,
    GroupPlan,
    GroupState,
    ReadTableResult,
    ReadTablesPath,
    group_release,
    plan_group,
    quarterly_marker,
)

PAPERS, OUT, IN, XWALK, BEST = READ_TABLES
TABLES = READ_TABLES
QUERY_ID = "citation-edges"  # Keep the recovery key stable across the rename.
DELETE_QUERY_ID = "read-tables-delete"
COLUMNS = {
    PAPERS: "corpusid, title, year, venue, citationcount, influentialcitationcount, referencecount",
    OUT: "citing, cited, isinfluential, intents, citationid",
    IN: "citing, cited, isinfluential, intents, citationid",
    XWALK: XWALK_COLUMNS,
    BEST: BEST_COLUMNS,
}
EDGES_IN = f"SELECT {COLUMNS[IN]} FROM {OUT}"
SOURCES = {
    PAPERS: f"SELECT {COLUMNS[PAPERS]} FROM papers FINAL WHERE is_deleted = 0",
    OUT: "SELECT citingcorpusid, assumeNotNull(citedcorpusid), isinfluential, "
    "toUInt8(bitOr(bitOr("
    "if(has(arrayFlatten(intents), 'background'), 1, 0), "
    "if(has(arrayFlatten(intents), 'methodology'), 2, 0)), "
    "if(has(arrayFlatten(intents), 'result'), 4, 0))), citationid "
    "FROM citations FINAL WHERE citedcorpusid IS NOT NULL",
    # One scan of citations: the incoming table is the outgoing one, sorted the other way.
    IN: f"{EDGES_IN}_new",
    XWALK: xwalk_select(None),
    # The abstracts follow the crosswalk built just before them.
    BEST: best_select(f"{XWALK}_new", None),
}
MEMORY = (
    "SETTINGS max_memory_usage = 8000000000, max_execution_time = 14400, max_insert_threads = 1"
)
SETTINGS = f"{MEMORY}, "
EDGE_BLOCKS = "min_insert_block_size_rows = 50000000, min_insert_block_size_bytes = 0"
SPILL = (
    "join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 64, "
    "max_bytes_before_external_sort = 4000000000, "
    "max_bytes_before_external_group_by = 4000000000"
)
BLOCKS = {
    # Paper rows carry titles: 50M of them would not fit the memory limit, so cap blocks by bytes.
    PAPERS: "min_insert_block_size_rows = 0, min_insert_block_size_bytes = 1073741824",
    OUT: EDGE_BLOCKS,
    IN: EDGE_BLOCKS,
    XWALK: f"{EDGE_BLOCKS}, {SPILL}",
}
BLOCKS[BEST] = f"{BLOCKS[PAPERS]}, {SPILL}"
BYTES_PER_CITATION = 28
"""(4 + 4 + 5.31 + 5.31) bytes for both new copies with IDs, with 50% merge room."""
BYTES_PER_PAPER = 100
"""Provisional free disk per papers row, pending a measured build."""
BYTES_PER_XWALK = 40
"""Provisional free disk per papers row, pending a measured build."""
BYTES_PER_ABSTRACT = 1000
"""Provisional free disk per abstracts and openalex_abstracts row, pending a measured build."""
BYTES_PER_IN_ONLY = 14
"""(4 + 5.31) bytes for one new incoming copy with IDs, with 50% merge room."""
MIN_KEPT = 0.9
"""A rebuild below this share of any live table is refused."""
DELTA = {
    "papers": "read_delta_papers",
    "citations": "read_delta_citations",
    "openalex": "read_delta_openalex",
}
"""Scratch tables holding the keys a group's sources took in since its release."""
KEY = {"papers": "corpusid", "citations": "citationid", "openalex": "corpusid"}
BYTES_PER_KEY = {
    "papers": BYTES_PER_PAPER,
    "citations": BYTES_PER_CITATION,
    "openalex": BYTES_PER_XWALK,
}
SHARE_OF = {"openalex": PAPERS}
"""The table a group's touched keys are measured against, one row per key, when its own tables
are not: ``paper_xwalk`` is empty until OpenAlex is loaded."""
S2_SOURCES = ("papers", "citations")
OPENALEX = {"openalex_works": "works", "openalex_abstracts": "works-abstracts"}
"""The OpenAlex tables by the dataset that loads them."""
KEPT_DELTA: frozenset[str] = frozenset({"openalex"})
"""Groups whose delta survives a failed run: it holds keys the deletes made unrecoverable,
the other papers that shared a work with a delta key."""
BYTES_PER_SCRATCH_KEY = 16
"""An uncompressed UInt64 plus equal merge room; independent of the source row size."""
DELTA_HEADROOM = 5 * 2**30
"""Free disk an in-place update needs on top of its rows: lightweight deletes write
``_row_exists`` masks and copy whole Compact parts, which stay below ``min_bytes_for_wide_part``."""
_RELEASE = TypeAdapter(ReleaseId)
log = logging.getLogger(__name__)


def delta_sql(group: str, start: ReleaseId, end: ReleaseId, max_keys: int) -> list[str]:
    """Collect the keys ``group``'s sources wrote in ``(start, end]``, deletions included.

    It stops one key past ``max_keys``, so a delta over the cap never has to fit in memory. A
    kept delta adds to the keys of a failed run of the same window, then adds the papers whose
    choice of work shares a work with a delta key's old or new choice.
    """
    start, end = _RELEASE.validate_python(start), _RELEASE.validate_python(end)
    delta = DELTA[group]
    window = f"source_release > '{start}' AND source_release <= '{end}'"
    if group not in KEPT_DELTA:
        return [
            f"DROP TABLE IF EXISTS {delta}",
            f"CREATE TABLE {delta} (key UInt64) ENGINE = MergeTree ORDER BY key",
            f"INSERT INTO {delta} SELECT DISTINCT {KEY[group]} FROM {group} "
            f"WHERE {window} "
            # One thread: each would hold its own set of up to max_keys keys.
            f"LIMIT {max_keys + 1} {MEMORY}, max_threads = 1",
        ]
    sources = " UNION ALL ".join(
        f"SELECT {KEY[group]} AS key FROM {source} WHERE {window}"
        for source in GROUP_SOURCES[group]
    )
    return [
        f"CREATE TABLE IF NOT EXISTS {delta} (key UInt64) ENGINE = MergeTree ORDER BY key",
        f"INSERT INTO {delta} SELECT DISTINCT key FROM ({sources}) "
        f"WHERE key NOT IN (SELECT key FROM {delta}) "
        f"LIMIT {max_keys + 1} {MEMORY}, max_threads = 1",
        f"INSERT INTO {delta} {expansion_sql(delta)} {MEMORY}, {SPILL}",
    ]


def apply_sql(group: str) -> list[str]:
    """Replace the rows of the delta keys in ``group``'s read tables with their current rows.

    Deleting every delta key first makes a rerun after a partial run reach the same state.
    """
    keys = f"SELECT key FROM {DELTA[group]}"
    touched = f"{KEY[group]} IN ({keys})"
    tables = GROUPS[group]
    # Waited on: until a delete finishes, the old rows still show next to the new ones.
    deletes = [
        f"DELETE FROM {table} WHERE {touched} SETTINGS lightweight_deletes_sync = 2"
        for table in tables
    ]
    # The incoming edges and the abstracts follow tables this run updates first.
    special = {
        IN: f"{EDGES_IN} WHERE {touched}",
        XWALK: xwalk_select(keys),
        BEST: best_select(XWALK, keys),
    }
    selects = {table: special.get(table, f"{SOURCES[table]} AND {touched}") for table in tables}
    return deletes + [
        f"INSERT INTO {table} ({COLUMNS[table]}) {selects[table]} {SETTINGS}{BLOCKS[table]}"
        for table in tables
    ]


def in_only_sql() -> list[str]:
    """Rebuild only the incoming edges into ``_new``, from the live outgoing ones."""
    return [
        f"CREATE TABLE {IN}_new AS {IN}",
        f"INSERT INTO {IN}_new ({COLUMNS[IN]}) {EDGES_IN} {SETTINGS}{BLOCKS[IN]}",
    ]


def _no_stage(name: str) -> AbstractAsyncContextManager[None]:
    return nullcontext()


class ClickHouseReadTables:
    def __init__(
        self,
        client: ClickHouseClient,
        tables: ClickHouseTables,
        applied: ClickHouseAppliedReleases,
        stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
        max_delta_share: float = 0.1,
        max_delta_keys: int = 100_000_000,
    ) -> None:
        self._client = client
        self._tables = tables
        self._applied = applied
        self._stage: Callable[[str], AbstractAsyncContextManager[None]] = stage or _no_stage
        self._max_delta_share = max_delta_share
        """Touched keys over this share of a read table rebuild in full. Each key is at most one
        row there, so the default never drops more than a full rebuild would keep."""
        self._max_delta_keys = max_delta_keys
        """Touched keys over this count rebuild in full: an update holds them all in memory."""

    async def is_current(self, releases: Mapping[str, ReleaseId]) -> bool:
        """Whether a build would change nothing; a group it would skip is not checked."""
        live = await self._live()
        openalex = await self._openalex()
        for group in await self._groups(openalex):
            for table in GROUPS[group]:
                applied = await self._applied.latest(table)
                if (
                    applied is None
                    or applied.release != releases.get(SOURCE_OF[table])
                    or live.get(table) != applied.target_uuid
                ):
                    return False
            if group in QUARTERLY and not await self._quarterly_current(
                group, live, held_release(*openalex[QUARTERLY[group]])
            ):
                return False
        return True

    async def reclaim(self) -> None:
        # A cancelled run's statement keeps running on the server. Replacing its query ID stops
        # it, unlike KILL QUERY, without a grant.
        for query_id in (QUERY_ID, DELETE_QUERY_ID):
            await self._client.post(
                "SELECT 1 SETTINGS replace_running_query = 1", query_id=query_id
            )
        # Reclaim abandoned copies, including an old live table after a partial swap or a lost
        # marker answer. Stop the old insert first; never drop the published names.
        for table in TABLES:
            await self._client.post(f"DROP TABLE IF EXISTS {table}_new SYNC")
        for group, table in DELTA.items():
            if group not in KEPT_DELTA:
                await self._client.post(f"DROP TABLE IF EXISTS {table}")

    async def build(
        self, producer: Producer, clock: Callable[[], datetime], *, full: bool = False
    ) -> BuildReadTablesResult:
        releases: dict[str, ReleaseId] = {}
        uuids: dict[str, UUID] = {}
        for name in S2_SOURCES:
            source = await self._tables.table(name)
            release = held_release(source, await self._applied.latest(name))
            if release is None:
                raise TableRejected(
                    f"{name} holds no completed release; "
                    "finish its load before building read tables"
                )
            releases[name], uuids[name] = release, source.uuid
        openalex = await self._openalex()
        held = {table: held_release(*found) for table, found in openalex.items()}
        if (release := held["openalex_works"]) is not None:
            releases["openalex_works"] = release
        groups = await self._groups(openalex)
        plans: dict[str, GroupPlan] = {}
        if not full:
            async with self._stage("read tables: recovery and plan"):
                await self.reclaim()
                live = await self._live()
                for name in groups:
                    state = await self._state(name, releases, uuids, live, held)
                    plans[name] = plan_group(state, full=False)
        result = BuildReadTablesResult(source_releases=releases, tables={})
        for unit in REBUILT_TOGETHER:
            built = await self._unit(
                tuple(name for name in unit if name in groups), plans, releases, producer, clock
            )
            result = result.model_copy(
                update={
                    "tables": result.tables | built.tables,
                    "paths": result.paths | built.paths,
                    "touched": result.touched | built.touched,
                }
            )
        return result

    async def _unit(
        self,
        groups: tuple[str, ...],
        plans: Mapping[str, GroupPlan],
        releases: Mapping[str, ReleaseId],
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> BuildReadTablesResult:
        """Bring groups that are rebuilt together current; ``plans`` is empty for a full build."""
        if not groups:
            return BuildReadTablesResult(source_releases=dict(releases), tables={})
        if any(name not in plans or plans[name].path is ReadTablesPath.FULL for name in groups):
            return await self._full(producer, clock, releases, groups=groups)
        results: dict[str, ReadTableResult] = {}
        touched: dict[str, int] = {}
        for name in groups:
            plan = plans[name]
            if plan.path is ReadTablesPath.IN_ONLY or plan.in_only_first:
                results[IN] = await self._in_only(plan.start or plan.end, producer, clock)
            if plan.path is ReadTablesPath.INCREMENTAL and plan.start is not None:
                touched[name], tables = await self._incremental(
                    name, plan.start, plan.end, producer, clock
                )
                if tables is None:
                    return await self._full(producer, clock, releases, touched, groups=groups)
                results |= tables
        return BuildReadTablesResult(
            source_releases=dict(releases),
            tables=results,
            paths={name: plans[name].path for name in groups},
            touched=touched,
        )

    async def _groups(
        self, openalex: Mapping[str, tuple[TableState, AppliedRelease | None]]
    ) -> list[str]:
        """The groups a build brings current: a quarterly group waits for a settled load."""
        return [name for name in GROUPS if name not in QUARTERLY or await self._settled(openalex)]

    async def _openalex(self) -> dict[str, tuple[TableState, AppliedRelease | None]]:
        found: dict[str, tuple[TableState, AppliedRelease | None]] = {}
        for table, dataset in OPENALEX.items():
            found[table] = (await self._tables.table(dataset), await self._applied.latest(table))
        return found

    async def _settled(
        self, openalex: Mapping[str, tuple[TableState, AppliedRelease | None]]
    ) -> bool:
        """Whether both OpenAlex tables show just their one applied release.

        A failed or running load leaves newer rows that FINAL already shows, so the group keeps
        its last build until the load completes.
        """
        held = {table: held_release(*found) for table, found in openalex.items()}
        if len(set(held.values())) > 1:
            log.warning("the OpenAlex tables hold different releases %s; skipping openalex", held)
            return False
        for table, (state, _) in openalex.items():
            release = held[table]
            if release is None:
                pending = state.has_rows
            else:
                rows = await self._client.rows(
                    f"SELECT count() AS rows FROM {table} WHERE source_release > {{release:Date}}",
                    release=release,
                )
                pending = int(rows[0]["rows"]) > 0
            if pending:
                log.warning(
                    "%s holds rows past its applied release %s, from a load that has not "
                    "finished; skipping openalex",
                    table,
                    release,
                )
                return False
        return True

    async def _state(
        self,
        group: str,
        releases: Mapping[str, ReleaseId],
        uuids: Mapping[str, UUID],
        live: Mapping[str, UUID],
        held: Mapping[str, ReleaseId | None],
    ) -> GroupState:
        tables = GROUPS[group]
        end = group_release(group, releases)
        markers = [await self._applied.latest(table) for table in tables]
        # A marker counts only for the live table it names, and the group's tables must agree.
        found = {
            None if applied is None or applied.target_uuid != live.get(table) else applied.release
            for table, applied in zip(tables, markers, strict=True)
        }
        marker = found.pop() if len(found) == 1 else None
        took = rebased = False
        if marker is not None:
            took = all(
                [
                    await self._applied.took(source, uuids[source], marker)
                    for source in GROUP_SOURCES[group]
                ]
            )
        if took and marker is not None:
            rebased = any(
                [
                    await self._applied.rebased(source, uuids[source], marker, end)
                    for source in GROUP_SOURCES[group]
                ]
            )
        edges = took and OUT in tables
        if edges:
            await self._require_ids()
        quarterly = group not in QUARTERLY or await self._quarterly_current(
            group, live, held[QUARTERLY[group]]
        )
        return GroupState(
            marker=marker,
            source=end,
            source_took_marker=took,
            source_rebased=rebased,
            ids_missing_out=edges and await self._missing_ids(OUT),
            ids_missing_in=edges and await self._missing_ids(IN),
            quarterly_current=quarterly,
        )

    async def _quarterly_current(
        self, group: str, live: Mapping[str, UUID], held: ReleaseId | None
    ) -> bool:
        """Whether the live tables were built from the quarterly source's held release.

        Without a held release, a table is current unless its live copy was built from one.
        """
        for table in GROUPS[group]:
            applied = await self._applied.latest(quarterly_marker(table, QUARTERLY[group]))
            built = applied is not None and applied.target_uuid == live.get(table)
            if held is None and built:
                return False
            if held is not None and (not built or applied is None or applied.release != held):
                return False
        return True

    async def _missing_ids(self, table: str) -> bool:
        """Whether rows from before ``citationid`` remain, which an update cannot key on."""
        started = monotonic()
        found = await self._client.rows(f"SELECT 1 FROM {table} WHERE citationid = 0 LIMIT 1")
        log.info("zero-ID probe on %s took %.1f s", table, monotonic() - started)
        return bool(found)

    async def _in_only(
        self, release: ReleaseId, producer: Producer, clock: Callable[[], datetime]
    ) -> ReadTableResult:
        started = monotonic()
        async with self._stage("read tables: in-only"):
            outgoing = (await self._counts(OUT))[OUT]
            await self._require_free(outgoing * BYTES_PER_IN_ONLY, f"rebuilding {IN}")
            create, insert = in_only_sql()
            await self._run(create)
            rows = await self._run(insert)
            if (await self._counts(f"{IN}_new"))[f"{IN}_new"] != outgoing:
                raise TableRejected(f"citation edge counts differ; {IN} was not swapped")
            await self._client.post(f"EXCHANGE TABLES {IN}_new AND {IN}")
            await self._applied.record(await self._marker(IN, release, None, producer, clock))
            await self._client.post(f"DROP TABLE {IN}_new SYNC")
        return ReadTableResult(rows=rows, seconds=monotonic() - started)

    async def _incremental(
        self,
        source: str,
        start: ReleaseId,
        end: ReleaseId,
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> tuple[int, dict[str, ReadTableResult] | None]:
        """Take ``source``'s diffs into its read tables in place; no tables to rebuild them all."""
        started = monotonic()
        tables = GROUPS[source]
        async with self._stage(f"read tables: {source} delta"):
            # Reserve for delta_sql's full cap plus its over-cap sentinel before writing keys.
            await self._require_free(
                (self._max_delta_keys + 1) * BYTES_PER_SCRATCH_KEY + DELTA_HEADROOM,
                f"collecting the {source} delta keys",
            )
            for statement in delta_sql(source, start, end, self._max_delta_keys):
                await self._run(statement)
            touched = (await self._counts(DELTA[source]))[DELTA[source]]
            share = SHARE_OF.get(source)
            total = min((await self._counts(*((share,) if share else tables))).values())
            if touched > self._max_delta_keys or touched > self._max_delta_share * total:
                log.info(
                    "%s touched %d keys for %d read table rows, over the %d key cap or the "
                    "%.0f%% share; rebuilding in full",
                    source,
                    touched,
                    total,
                    self._max_delta_keys,
                    self._max_delta_share * 100,
                )
                return touched, None
            await self._require_free(
                touched * BYTES_PER_KEY[source] + DELTA_HEADROOM,
                f"updating the {source} read tables",
            )
        async with self._stage(f"read tables: {source} apply"):
            written: dict[str, int] = {}
            for statement in apply_sql(source):
                rows = await self._run(statement)
                if statement.startswith("INSERT"):
                    written[statement.split()[2]] = rows
            if OUT in tables:
                counts = await self._counts(OUT, IN)
                if counts[OUT] != counts[IN]:
                    log.warning(
                        "citation edge counts differ after the update (%d out, %d in); "
                        "rebuilding in full",
                        counts[OUT],
                        counts[IN],
                    )
                    return touched, None
            await self._applied.record_all(
                [await self._marker(table, end, start, producer, clock) for table in tables]
            )
            await self._client.post(f"DROP TABLE IF EXISTS {DELTA[source]}")
        seconds = monotonic() - started
        return touched, {
            table: ReadTableResult(rows=written[table], seconds=seconds) for table in tables
        }

    async def _full(
        self,
        producer: Producer,
        clock: Callable[[], datetime],
        releases: Mapping[str, ReleaseId],
        touched: Mapping[str, int] | None = None,
        *,
        groups: tuple[str, ...],
    ) -> BuildReadTablesResult:
        tables = tuple(table for group in groups for table in GROUPS[group])
        async with self._stage("read tables: preflight and recovery"):
            await self.reclaim()
            live = await self._preflight(groups)
        results: dict[str, ReadTableResult] = {}
        for table in tables:
            started = monotonic()
            async with self._stage(f"read tables: build {table}"):
                await self._client.post(f"CREATE TABLE {table}_new AS {table}")
                rows = await self._client.written(
                    f"INSERT INTO {table}_new ({COLUMNS[table]}) {SOURCES[table]} "
                    f"{SETTINGS}{BLOCKS[table]}",
                    query_id=QUERY_ID,
                )
                results[table] = ReadTableResult(rows=rows, seconds=monotonic() - started)
        async with self._stage("read tables: validate counts"):
            if "citations" in groups and results[IN].rows != results[OUT].rows:
                raise TableRejected("citation edge counts differ; no read tables were swapped")
            for table in tables:
                if results[table].rows < live[table] * MIN_KEPT:
                    raise TableRejected(
                        f"the rebuild holds {results[table].rows} rows where {table} has "
                        f"{live[table]}; no read tables were swapped. If the drop is expected, "
                        "truncate the affected live table and rerun"
                    )
        # EXCHANGE is not atomic across tables, even in one statement. Undo a partial swap so a
        # failure leaves the old generation live, and record nothing until all are live.
        # An uncommitted marker insert leaves the new generation live but not current, so the next
        # sync rebuilds it; one whose answer was lost may have committed, and then it is current
        # and the next sync only reclaims the old tables.
        # Swapping back here could strand the edge tables a generation apart.
        async with self._stage("read tables: publish"):
            swapped: list[str] = []
            try:
                for table in tables:
                    await self._client.post(f"EXCHANGE TABLES {table}_new AND {table}")
                    swapped.append(table)
            except BaseException:
                for table in reversed(swapped):
                    await self._client.post(f"EXCHANGE TABLES {table}_new AND {table}")
                raise
            markers = [
                await self._marker(table, group_release(group, releases), None, producer, clock)
                for group in groups
                for table in GROUPS[group]
            ]
            # Which quarterly release built the group; none while that source holds no release.
            markers += [
                await self._marker(
                    table,
                    releases[QUARTERLY[group]],
                    None,
                    producer,
                    clock,
                    target=quarterly_marker(table, QUARTERLY[group]),
                )
                for group in groups
                if QUARTERLY.get(group) in releases
                for table in GROUPS[group]
            ]
            await self._applied.record_all(markers)
            for table in tables:
                await self._client.post(f"DROP TABLE {table}_new SYNC")
            for group in KEPT_DELTA.intersection(groups):
                await self._client.post(f"DROP TABLE IF EXISTS {DELTA[group]}")
        return BuildReadTablesResult(
            source_releases=dict(releases),
            tables=results,
            paths=dict.fromkeys(groups, ReadTablesPath.FULL),
            touched=dict(touched or {}),
        )

    async def _run(self, statement: str) -> int:
        """Run under the kind's fixed query ID, which a rerun replaces; return rows inserted."""
        started = monotonic()
        rows = 0
        if statement.startswith("INSERT"):
            rows = await self._client.written(statement, query_id=QUERY_ID)
        else:
            query_id = DELETE_QUERY_ID if statement.startswith("DELETE") else None
            await self._client.post(statement, query_id=query_id)
        log.info("%s took %.1f s", " ".join(statement.split()[:3]), monotonic() - started)
        return rows

    async def _marker(
        self,
        table: str,
        release: ReleaseId,
        from_release: ReleaseId | None,
        producer: Producer,
        clock: Callable[[], datetime],
        target: str | None = None,
    ) -> AppliedRelease:
        return AppliedRelease(
            target=target or table,
            target_uuid=await self._uuid(table),
            release=release,
            from_release=from_release,
            software_version=producer.software_version,
            image_digest=producer.image_digest,
            applied_at=clock(),
        )

    async def _live(self) -> dict[str, UUID]:
        rows = await self._client.rows(
            "SELECT name, uuid FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ({', '.join(f"'{table}'" for table in TABLES)})"
        )
        return {row["name"]: UUID(row["uuid"]) for row in rows}

    async def _uuid(self, table: str) -> UUID:
        rows = await self._client.rows(
            "SELECT uuid FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=table,
        )
        return UUID(rows[0]["uuid"])

    async def _counts(self, *tables: str) -> dict[str, int]:
        """Rows the tables hold; ``total_rows`` also counts rows deleted but not yet merged."""
        counts: dict[str, int] = {}
        for table in tables:
            rows = await self._client.rows(f"SELECT count() AS rows FROM {table}")
            counts[table] = int(rows[0]["rows"])
        return counts

    async def _preflight(self, groups: tuple[str, ...]) -> dict[str, int]:
        """Check the read tables of ``groups`` and capacity; return their live row counts."""
        tables = tuple(table for group in groups for table in GROUPS[group])
        names = ", ".join(f"'{name}'" for name in ("papers", "citations", *tables))
        rows = await self._client.rows(
            "SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ({names})"
        )
        found = {row["name"]: int(row["total_rows"] or 0) for row in rows}
        if not set(tables) <= found.keys():
            raise TableRejected("the read tables do not exist; run the migrations")
        if "citations" in groups:
            await self._require_ids()
        per_row = {
            "papers": ("papers", BYTES_PER_PAPER),
            "citations": ("citations", BYTES_PER_CITATION),
            "openalex": ("papers", BYTES_PER_XWALK),
        }
        await self._require_free(
            sum(found[per_row[group][0]] * per_row[group][1] for group in groups),
            "building read tables",
        )
        return await self._counts(*tables)

    async def _require_ids(self) -> None:
        columns = await self._client.rows(
            "SELECT table FROM system.columns WHERE database = currentDatabase() "
            f"AND table IN ('{OUT}', '{IN}') AND name = 'citationid'"
        )
        have_ids = {row["table"] for row in columns}
        for table, migration in ((OUT, "00018"), (IN, "00020")):
            if table not in have_ids:
                raise TableRejected(
                    f"{table}.citationid is missing; run migration {migration} before rebuilding"
                )

    async def _require_free(self, needed: int, action: str) -> None:
        try:
            disk = await self._client.rows(
                "SELECT free_space FROM system.disks WHERE name = 'default'"
            )
        except TableAccessDenied:
            raise TableAccessDenied(
                "the read table rebuild checks free disk first; grant SELECT ON system.disks"
            ) from None
        free = int(disk[0]["free_space"])
        if free < needed:
            raise TableRejected(
                f"{action} needs about {needed // 2**30} GiB free, but the disk "
                f"has {free // 2**30} GiB; nothing was built"
            )
