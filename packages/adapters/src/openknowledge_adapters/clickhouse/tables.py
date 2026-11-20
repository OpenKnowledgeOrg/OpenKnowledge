"""The loader's side of ClickHouse, over its HTTP interface."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.load_sql import (
    delete_query,
    duplicates_parameters,
    duplicates_query,
    identifier,
    literal,
    query_parameters,
    removed_query,
    sql_sha256,
    upsert_query,
)
from openknowledge_adapters.clickhouse.parquet_sql import (
    broken_indexes_query,
    parquet_duplicates_parameters,
    parquet_duplicates_query,
    parquet_upsert_query,
    wide_read_settings,
)
from openknowledge_contracts.datasets import DatasetName, ReleaseId
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_contracts.sources import contract_for
from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.clickhouse import version_expression
from openknowledge_contracts.tables.contract import REMOVED
from openknowledge_core.ingestion.errors import TableRejected
from openknowledge_core.ingestion.models import (
    AppliedRelease,
    Duplicates,
    LoadObject,
    LoadRecord,
    TableState,
)

RECORD_COLUMNS = ", ".join(LoadRecord.model_fields)
APPLIED_COLUMNS = ", ".join(AppliedRelease.model_fields)
# ClickHouse local runs in an Overlay database, which exchanges tables too.
EXCHANGING_ENGINES = frozenset({"Atomic", "Replicated", "Shared", "Overlay"})


def staging_table(table: str) -> str:
    return f"{table}_staging"


def stage_statements(table: str) -> tuple[str, str]:
    """Replace the staging copy with an empty one of the live table's current schema."""
    staging = identifier(staging_table(table))
    return (
        f"DROP TABLE IF EXISTS {staging} SYNC",
        f"CREATE TABLE {staging} AS {identifier(table)}",
    )


def swap_statements(table: str) -> tuple[str, str]:
    """Swap the staging copy in under the live name, then drop the table it replaced."""
    staging = identifier(staging_table(table))
    return (
        f"EXCHANGE TABLES {identifier(table)} AND {staging}",
        f"DROP TABLE IF EXISTS {staging} SYNC",
    )


class ClickHouseTables:
    def __init__(self, client: ClickHouseClient, *, raw_bucket: str) -> None:
        self._client = client
        self._raw_bucket = raw_bucket

    async def table(self, dataset: DatasetName) -> TableState:
        contract = _contract(dataset)
        rows = await self._client.rows(
            "SELECT uuid, total_rows FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=contract.table,
        )
        if not rows:
            raise TableRejected(f"table {contract.table} does not exist; run the migrations")
        if contract.enriched is not None:
            version = await self._client.rows(
                "SELECT default_expression FROM system.columns "
                "WHERE database = currentDatabase() AND table = {table:String} "
                "AND name = 'version'",
                table=contract.table,
            )
            if not version or version[0]["default_expression"] != version_expression(contract):
                raise TableRejected(
                    f"{contract.table} needs the version migration that ranks "
                    f"{contract.enriched}; a populated legacy table must be preserved and "
                    "swapped by hand, as migration 00014 describes"
                )
        return TableState(
            name=contract.table,
            into=contract.table,
            staged=_parquet(contract) and contract.updates == "snapshot",
            dataset=contract.name,
            archive=contract.archived,
            uuid=rows[0]["uuid"],
            has_rows=bool(rows[0]["total_rows"]),
            load_sql_sha256=sql_sha256(_upsert(contract)),
            # A Parquet table is a snapshot and never takes a delete, so it hashes its upsert.
            delete_sql_sha256=sql_sha256(
                _upsert(contract) if _parquet(contract) else delete_query(contract)
            ),
            updates=contract.updates,
        )

    async def loaded(self, dataset: DatasetName, source_object: ObjectPath) -> LoadRecord | None:
        rows = await self._client.rows(
            f"SELECT {RECORD_COLUMNS} FROM loads FINAL "
            "WHERE target = {target:String} AND source_object = {source_object:String}",
            target=_contract(dataset).table,
            source_object=str(source_object),
        )
        return LoadRecord.model_validate(rows[0]) if rows else None

    async def receipts(
        self, table: TableState, release: ReleaseId, *, prefix: str
    ) -> dict[str, LoadRecord]:
        archive = ObjectPath.under(self._raw_bucket, prefix.strip("/"), release, table.archive)
        rows = await self._client.rows(
            f"SELECT {RECORD_COLUMNS} FROM loads FINAL WHERE target = {{target:String}} "
            "AND source_release = {release:Date} AND startsWith(source_object, {archive:String})",
            target=table.name,
            release=release,
            archive=f"{archive}/",
        )
        return {row["source_object"]: LoadRecord.model_validate(row) for row in rows}

    async def stage(self, table: TableState, release: ReleaseId) -> TableState:
        """Reuse a staging copy only with the live schema and no rows of another release."""
        database = await self._client.rows(
            "SELECT engine FROM system.databases WHERE name = currentDatabase()"
        )
        if database[0]["engine"] not in EXCHANGING_ENGINES:
            raise TableRejected(
                f"{table.name} reloads through a copy it exchanges, which a database of engine "
                f"{database[0]['engine']} cannot do; load it into an Atomic database"
            )
        staging = staging_table(table.name)
        found = await self._found(table.name, staging)
        copy = found.get(staging)
        if (
            copy is None
            or copy["create_table_query"].replace(staging, table.name, 1)
            != found[table.name]["create_table_query"]
            or await self._client.rows(
                "SELECT 1 FROM {staging:Identifier} WHERE source_release != {release:Date} LIMIT 1",
                staging=staging,
                release=release,
            )
        ):
            for statement in stage_statements(table.name):
                await self._client.post(statement)
            copy = (await self._found(staging))[staging]
        return table.model_copy(
            update={
                "into": staging,
                "uuid": UUID(copy["uuid"]),
                "has_rows": bool(int(copy["total_rows"] or 0)),
            }
        )

    async def swap(self, table: TableState) -> None:
        exchange, drop = swap_statements(table.name)
        if table.into != table.name:
            await self._client.post(exchange)
        await self._client.post(drop)

    async def drop_leftover(self, table: TableState, held: ReleaseId) -> None:
        staging = staging_table(table.name)
        if not table.staged or staging not in await self._found(staging):
            return
        newer = await self._client.rows(
            "SELECT 1 FROM {staging:Identifier} WHERE source_release > {held:Date} LIMIT 1",
            staging=staging,
            held=held,
        )
        if not newer:
            await self._client.post(f"DROP TABLE IF EXISTS {identifier(staging)} SYNC")

    async def load(self, request: LoadObject) -> int:
        contract = _contract(request.dataset)
        path = self._readable(request.object_ref.path)
        if request.operation == "delete":
            if _parquet(contract):
                raise TableRejected(f"{contract.table} is a snapshot and takes no deletes")
            query = delete_query(contract)
        elif _parquet(contract):
            query = parquet_upsert_query(contract, into=request.into)
        else:
            query = upsert_query(contract)
        parameters = query_parameters(
            source_file=path.name, release=request.release, source_object=str(path)
        )
        return await self._client.written(query, **parameters)

    async def record(self, record: LoadRecord) -> None:
        await self._client.post(
            "INSERT INTO loads FORMAT JSONEachRow", data=record.model_dump_json()
        )

    async def live_rows(self, table: TableState, release: ReleaseId) -> int:
        contract = _contract(table.dataset)
        query = (
            "SELECT count() AS rows FROM {table:Identifier} FINAL "
            "WHERE source_release = {release:Date}"
        )
        if contract.updates == "snapshot":
            query += f" AND NOT {REMOVED}"
        rows = await self._client.rows(
            query + wide_read_settings(contract), table=table.into, release=release
        )
        return int(rows[0]["rows"])

    async def broken_indexes(self, table: TableState, release: ReleaseId) -> int:
        query = broken_indexes_query(_contract(table.dataset))
        if query is None:
            return 0
        rows = await self._client.rows(query, table=table.into, release=release)
        return int(rows[0]["rows"])

    async def duplicates(self, dataset: DatasetName, objects: Sequence[ObjectPath]) -> Duplicates:
        names = [self._readable(path).name for path in objects]
        contract = _contract(dataset)
        query_id = None
        if _parquet(contract):
            query, parameters = (
                parquet_duplicates_query(contract),
                parquet_duplicates_parameters(names),
            )
            # The same for a retry, which then replaces a query its client stopped waiting for.
            glob = parameters["source_files"].encode()
            query_id = f"duplicates-{contract.table}-{hashlib.sha256(glob).hexdigest()[:32]}"
        else:
            query, parameters = duplicates_query(contract, len(names)), duplicates_parameters(names)
        rows = await self._client.rows(query, query_id=query_id, **parameters)
        return Duplicates.model_validate(rows[0])

    async def mark_removed(self, dataset: DatasetName, release: ReleaseId) -> int:
        return await self._client.written(removed_query(_contract(dataset)), source_release=release)

    async def _found(self, *names: str) -> dict[str, dict[str, Any]]:
        rows = await self._client.rows(
            "SELECT name, uuid, total_rows, create_table_query FROM system.tables "
            "WHERE database = currentDatabase() AND has({names:Array(String)}, name)",
            names="[" + ", ".join(literal(name) for name in names) + "]",
        )
        return {row["name"]: row for row in rows}

    def _readable(self, path: ObjectPath) -> ObjectPath:
        if path.bucket != self._raw_bucket:
            raise TableRejected(f"ClickHouse reads only {self._raw_bucket}, not {path}")
        return path


class ClickHouseAppliedReleases:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def record(self, applied: AppliedRelease) -> None:
        await self.record_all([applied])

    async def record_all(self, applied: Sequence[AppliedRelease]) -> None:
        """Write the markers in one insert, which ClickHouse applies whole or not at all."""
        await self._client.post(
            "INSERT INTO applied FORMAT JSONEachRow",
            data="\n".join(item.model_dump_json() for item in applied),
        )

    async def took(self, target: str, target_uuid: UUID, release: ReleaseId) -> bool:
        rows = await self._client.rows(
            "SELECT 1 FROM applied WHERE target = {target:String} "
            "AND target_uuid = {uuid:UUID} AND release = {release:String} LIMIT 1",
            target=target,
            uuid=str(target_uuid),
            release=release,
        )
        return bool(rows)

    async def rebased(
        self, target: str, target_uuid: UUID, after: ReleaseId, through: ReleaseId
    ) -> bool:
        """Whether the table took a base release in ``(after, through]``; a diff names its start."""
        rows = await self._client.rows(
            "SELECT 1 FROM applied WHERE target = {target:String} AND target_uuid = {uuid:UUID} "
            "AND from_release IS NULL AND release > {after:Date} AND release <= {through:Date} "
            "LIMIT 1",
            target=target,
            uuid=str(target_uuid),
            after=after,
            through=through,
        )
        return bool(rows)

    async def latest(self, target: str, target_uuid: UUID | None = None) -> AppliedRelease | None:
        rows = await self._client.rows(
            f"SELECT {APPLIED_COLUMNS} FROM applied WHERE target = {{target:String}} "
            + ("" if target_uuid is None else "AND target_uuid = {uuid:UUID} ")
            + "ORDER BY applied_at DESC LIMIT 1",
            target=target,
            **({} if target_uuid is None else {"uuid": str(target_uuid)}),
        )
        return AppliedRelease.model_validate(rows[0]) if rows else None


def _contract(dataset: DatasetName) -> DatasetContract:
    try:
        return contract_for(dataset)[1]
    except KeyError:
        raise TableRejected(f"no table contract for dataset {dataset}") from None


def _parquet(contract: DatasetContract) -> bool:
    source, _ = contract_for(contract.name)
    return source.datasets[contract.archived].format == "parquet"


def _upsert(contract: DatasetContract) -> str:
    return parquet_upsert_query(contract) if _parquet(contract) else upsert_query(contract)
