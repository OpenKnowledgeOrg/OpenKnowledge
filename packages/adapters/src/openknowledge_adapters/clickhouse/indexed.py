"""Managed search pass receipts and the identity of their live source tables."""

import json

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_core.search.progress import Indexed, SearchState


class ClickHouseIndexed:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def state(self) -> SearchState:
        rows = await self._client.rows(
            "SELECT a.target, a.release, a.target_uuid FROM "
            "(SELECT target, release, target_uuid FROM applied "
            "WHERE target IN ('papers', 'abstracts', 'tldrs', 'paper_abstract_best', "
            "'openalex_works') "
            "ORDER BY applied_at DESC LIMIT 1 BY target) a "
            "INNER JOIN system.tables t ON t.name = a.target AND t.uuid = a.target_uuid "
            "WHERE t.database = currentDatabase()"
        )
        # The live read table's release counts, not its UUID: rebuilding it from the same
        # releases changes no document, so it must not force a new index.
        built = await self._client.rows(
            "SELECT a.release AS release FROM applied a INNER JOIN system.tables t "
            "ON t.uuid = a.target_uuid AND t.name = 'paper_abstract_best' "
            "WHERE a.target = 'paper_abstract_best@openalex_works' "
            "AND t.database = currentDatabase() ORDER BY a.applied_at DESC LIMIT 1"
        )
        return SearchState.model_validate(
            {
                "releases": {r["target"]: r["release"] for r in rows},
                "sources": {
                    r["target"]: r["target_uuid"]
                    for r in rows
                    if r["target"] != "paper_abstract_best"
                },
                "built_from": {"openalex_works": built[0]["release"]} if built else {},
            }
        )

    async def receipts(self) -> list[Indexed]:
        rows = await self._client.rows(
            "SELECT plan, kind, part, recorded_at FROM indexed ORDER BY recorded_at"
        )
        return [Indexed.model_validate({**r, "plan": json.loads(r["plan"])}) for r in rows]

    async def record(self, receipt: Indexed) -> None:
        data = receipt.model_dump(mode="json")
        data["plan"] = receipt.plan.model_dump_json()
        await self._client.post("INSERT INTO indexed FORMAT JSONEachRow", data=json.dumps(data))
