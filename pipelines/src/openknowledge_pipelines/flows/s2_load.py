"""The s2-load flows, one task per object so that a retry repeats only that object."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import CoroutineType
from typing import Any

from prefect import Flow, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from openknowledge_contracts.manifests import ManifestEntry
from openknowledge_core.ingestion import (
    LoadDataset,
    LoadDatasetResult,
    LoadDatasetService,
    LoadDiff,
    LoadDiffResult,
    LoadOutcome,
    PreparedDiff,
    PreparedLoad,
)
from openknowledge_pipelines.flows.s2_download import retry_transient

type S2LoadFlow = Flow[[LoadDataset], CoroutineType[Any, Any, LoadDatasetResult]]
type S2LoadDiffFlow = Flow[[LoadDiff], CoroutineType[Any, Any, LoadDiffResult]]


@dataclass(frozen=True)
class S2LoadFlows:
    dataset: S2LoadFlow
    diff: S2LoadDiffFlow


def s2_load_flows(
    service: LoadDatasetService,
    *,
    task_retries: int,
    retry_delay_seconds: float,
    parallelism: int = 1,
) -> S2LoadFlows:
    @task(
        name="prepare-load",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def prepare(command: LoadDataset) -> PreparedLoad:
        return await service.prepare(command)

    @task(
        name="prepare-diff",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def prepare_diff(command: LoadDiff) -> PreparedDiff:
        return await service.prepare_diff(command)

    @task(
        name="load-object",
        task_run_name="object {entry.object_ref.path.name}",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def load_object(prepared: PreparedLoad, entry: ManifestEntry) -> LoadOutcome:
        outcome = await service.load_object(prepared, entry)
        get_run_logger().info("%s was %s", entry.object_ref.path, outcome)
        return outcome

    @task(
        name="finish-load",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def finish(prepared: PreparedLoad, outcomes: list[LoadOutcome]) -> LoadDatasetResult:
        return await service.finish(prepared, outcomes)

    @task(
        name="finish-diff",
        retries=task_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=retry_transient,
        cache_policy=NO_CACHE,
        persist_result=False,
    )
    async def finish_diff(prepared: PreparedDiff, outcomes: list[LoadOutcome]) -> LoadDiffResult:
        return await service.finish_diff(prepared, outcomes)

    async def load_objects(prepared: PreparedLoad) -> list[LoadOutcome]:
        """Up to ``parallelism`` objects at a time.

        After a failure no object starts, and the inserts in flight finish before it is raised.
        """
        slots = asyncio.Semaphore(parallelism)
        failed = False

        async def one(entry: ManifestEntry) -> LoadOutcome | None:
            nonlocal failed
            async with slots:
                if failed:
                    return None
                try:
                    return await load_object(prepared, entry)
                except BaseException:
                    failed = True
                    raise

        results = await asyncio.gather(
            *(one(entry) for entry in prepared.manifest.entries), return_exceptions=True
        )
        if error := next((r for r in results if isinstance(r, BaseException)), None):
            raise error
        return [result for result in results if isinstance(result, LoadOutcome)]

    @flow(name="s2-load", persist_result=False)
    async def s2_load(command: LoadDataset) -> LoadDatasetResult:
        prepared = await prepare(command)
        return await finish(prepared, await load_objects(prepared))

    @flow(name="s2-load-diff", persist_result=False)
    async def s2_load_diff(command: LoadDiff) -> LoadDiffResult:
        prepared = await prepare_diff(command)
        outcomes = [
            await load_object(part, entry)
            for part in prepared.parts
            for entry in part.manifest.entries
        ]
        return await finish_diff(prepared, outcomes)

    return S2LoadFlows(dataset=s2_load, diff=s2_load_diff)
