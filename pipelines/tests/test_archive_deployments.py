"""Managed entry points and the real Prefect shared-budget behavior (loopback test server)."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Literal

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.objects import ConcurrencyLeaseHolder
from prefect.concurrency.asyncio import ConcurrencySlotAcquisitionError

from openknowledge_contracts.sources import SOURCES
from openknowledge_core.ingestion import (
    ArchiveRelease,
    BatchDownloadResult,
    DownloadDataset,
    DownloadDatasetService,
    ShardJob,
    ShardJobs,
)
from openknowledge_core.ingestion.errors import (
    DatasetNotInRelease,
    IngestionError,
    RowCountMismatch,
)
from openknowledge_pipelines import admission, bootstrap, deployments
from openknowledge_pipelines.settings import BatchSettings, LoadSettings, OpenAlexBatchSettings
from tests.fakes.ports import COMMAND, DIFF, FakeJobs, FakeSource, FakeStore


@pytest.mark.parametrize("dataset", ["s2orc_v2", "embeddings-specter_v2", "s2orc", "unknown"])
async def test_core_mismatch_precedes_settings_and_clients(
    monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    monkeypatch.delenv("S2_API_KEY", raising=False)
    with pytest.raises(ValueError, match="belongs to|Unsupported archive dataset"):
        await deployments.s2_batch_download.fn(COMMAND.model_copy(update={"dataset": dataset}))
    with pytest.raises(ValueError, match="belongs to|Unsupported archive dataset"):
        await deployments.s2_batch_download_diff.fn(DIFF.model_copy(update={"dataset": dataset}))


async def test_archive_mismatch_precedes_client_construction() -> None:
    for base in (deployments.s2_batch_download_fulltext, deployments.s2_batch_download_embeddings):
        with pytest.raises(ValueError, match="belongs to core"):
            await base.fn(COMMAND)
    for diff in (
        deployments.s2_batch_download_fulltext_diff,
        deployments.s2_batch_download_embeddings_diff,
    ):
        with pytest.raises(ValueError, match="belongs to core"):
            await diff.fn(DIFF)


@pytest.mark.parametrize("dataset", ["papers", "s2orc_v2", "embeddings-specter_v2"])
@pytest.mark.parametrize("diff", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_download_lease_refusal_or_cancellation_precedes_admission(
    monkeypatch: pytest.MonkeyPatch, dataset: str, diff: bool, cancel: bool
) -> None:
    monkeypatch.setattr(deployments, "BatchSettings", BatchSettings.model_construct)

    @asynccontextmanager
    async def refused() -> AsyncGenerator[None]:
        if cancel:
            raise asyncio.CancelledError()
        raise ConcurrencySlotAcquisitionError("busy semantic-scholar")
        yield  # pragma: no cover

    @asynccontextmanager
    async def forbidden() -> AsyncGenerator[None]:
        pytest.fail("a refused or cancelled lease must not take admission")
        yield  # pragma: no cover

    monkeypatch.setattr(deployments, "one_s2_user", refused)
    monkeypatch.setattr(admission, "execution", forbidden)
    with pytest.raises(asyncio.CancelledError if cancel else ConcurrencySlotAcquisitionError):
        if diff:
            route = {
                "papers": deployments.s2_batch_download_diff,
                "s2orc_v2": deployments.s2_batch_download_fulltext_diff,
                "embeddings-specter_v2": deployments.s2_batch_download_embeddings_diff,
            }[dataset]
            await route.fn(DIFF.model_copy(update={"dataset": dataset}))
        else:
            base_route = {
                "papers": deployments.s2_batch_download,
                "s2orc_v2": deployments.s2_batch_download_fulltext,
                "embeddings-specter_v2": deployments.s2_batch_download_embeddings,
            }[dataset]
            await base_route.fn(COMMAND.model_copy(update={"dataset": dataset}))


@pytest.mark.usefixtures("prefect_server")
async def test_managed_routes_share_budget_and_release_after_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Clients must never consume settings when the real shared budget is busy.
    monkeypatch.setattr(deployments, "BatchSettings", BatchSettings.model_construct)

    @asynccontextmanager
    async def forbidden(
        settings: BatchSettings,
    ) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs]]:
        pytest.fail("busy controllers must not open clients")
        yield  # pragma: no cover

    monkeypatch.setattr(bootstrap, "open_batch_download", forbidden)
    deployments.ensure_limits()
    before = admission.status().active_slots
    async with deployments.one_s2_user():
        with pytest.raises(ConcurrencySlotAcquisitionError, match="semantic-scholar"):
            await deployments.s2_batch_download_fulltext.fn(
                COMMAND.model_copy(update={"dataset": "s2orc_v2"})
            )
        with pytest.raises(ConcurrencySlotAcquisitionError, match="semantic-scholar"):
            await deployments.s2_batch_download_embeddings_diff.fn(
                DIFF.model_copy(update={"dataset": "embeddings-specter_v2"})
            )
    assert admission.status().active_slots == before
    with pytest.raises(RuntimeError, match="job wait failed"):
        async with deployments.one_s2_user():
            raise RuntimeError("job wait failed")
    async with deployments.one_s2_user():
        pass


@pytest.mark.usefixtures("prefect_server")
async def test_missing_shared_limit_refuses_to_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deployments, "S2_LIMIT", "missing-archive-budget-test")
    with pytest.raises(ConcurrencySlotAcquisitionError, match="must be created"):
        async with deployments.one_s2_user():
            pytest.fail("a missing budget must not permit work")


@pytest.mark.usefixtures("prefect_server")
async def test_slow_successful_acquisition_has_no_one_second_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    acquire = PrefectClient.increment_concurrency_slots_with_lease

    async def delayed(
        self: PrefectClient,
        names: list[str],
        slots: int,
        mode: Literal["concurrency", "rate_limit"],
        lease_duration: float,
        holder: ConcurrencyLeaseHolder | None = None,
    ) -> httpx.Response:
        await asyncio.sleep(1.1)
        return await acquire(self, names, slots, mode, lease_duration, holder)

    deployments.ensure_limits()
    monkeypatch.setattr(PrefectClient, "increment_concurrency_slots_with_lease", delayed)
    async with deployments.one_s2_user():
        pass


async def test_managed_manual_diff_retries_only_anchored_completions(
    monkeypatch: pytest.MonkeyPatch,
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
) -> None:
    from openknowledge_contracts.sources import SOURCES
    from openknowledge_core.ingestion.archive_progress import (
        ArchiveCompletion,
        ArchiveProgress,
        completion_path,
    )
    from openknowledge_core.ingestion.coordinator import ArchiveBaseline, SyncCommand
    from openknowledge_core.ingestion.errors import IngestionError
    from tests.fakes.ports import NEXT, PAPERS, PRODUCER, clock

    progress = ArchiveProgress(store, service, PRODUCER, clock, SOURCES["semantic-scholar"])
    base = COMMAND.model_copy(update={"dataset": "s2orc_v2"})
    diff = DIFF.model_copy(update={"dataset": base.dataset})
    source.add(base.dataset, "part.gz", PAPERS["part-0.gz"])
    pinned = await service.pin_dataset(base)
    await service.archive_shard(pinned, pinned.shards[0])
    anchor = await progress.finalize(base)
    config = SyncCommand(
        bucket=base.bucket,
        prefix=base.prefix,
        archives={base.dataset: ArchiveBaseline(base=base, completion=anchor)},
    )

    @asynccontextmanager
    async def lease() -> AsyncGenerator[None]:
        yield

    @asynccontextmanager
    async def opened(settings: BatchSettings) -> AsyncGenerator[ArchiveProgress]:
        yield progress

    @asynccontextmanager
    async def forbidden(settings: BatchSettings) -> AsyncGenerator[None]:
        pytest.fail("completed manual retries must not open Batch or submit jobs")
        yield

    monkeypatch.setattr(deployments, "one_s2_user", lease)
    monkeypatch.setattr(admission, "execution", lease)
    monkeypatch.setattr(
        deployments, "BatchSettings", lambda: BatchSettings.model_construct(sync=config)
    )
    monkeypatch.setattr(bootstrap, "open_archive_progress", opened)
    monkeypatch.setattr(bootstrap, "open_batch_download", forbidden)
    predecessor = anchor
    completion_manifest = None
    for command in (diff, diff.model_copy(update={"from_release": NEXT, "release": "2026-10-20"})):
        source.add_diff(
            base.dataset,
            command.from_release,
            command.release,
            {"update.gz": PAPERS["part-0.gz"]},
            {},
        )
        pinned_diff = await service.pin_diff(command)
        for shard in pinned_diff.updates.shards:
            await service.archive_shard(pinned_diff.updates, shard)
        if command == diff:
            completion_manifest = pinned_diff.updates.manifest_path
        predecessor = await progress.finalize(command, predecessor)
        result = await deployments.s2_batch_download_fulltext_diff.fn(diff)
        assert (
            result.from_release,
            result.release_id,
            result.updates,
            result.deletes,
            result.jobs,
        ) == (
            diff.from_release,
            diff.release,
            1,
            0,
            (),
        )
        assert result.manifests == (completion_manifest,)

    baseline = config.archives[base.dataset]
    for field, value in (("dataset", "papers"), ("bucket", "other"), ("prefix", "other")):
        config.archives[base.dataset] = baseline.model_copy(
            update={"base": base.model_copy(update={field: value})}
        )
        with pytest.raises(IngestionError, match="another dataset/location"):
            await deployments.s2_batch_download_fulltext_diff.fn(diff)
    config.archives[base.dataset] = baseline

    # An existing completion detached from this anchor cannot make a retry successful.
    record = await progress.read(predecessor, ArchiveCompletion)
    store.put(completion_path(command), record.model_copy(update={"predecessor": anchor}).to_json())
    with pytest.raises(IngestionError, match="predecessor"):
        await deployments.s2_batch_download_fulltext_diff.fn(diff)


async def test_closed_admission_refuses_edge_build_before_clickhouse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    @asynccontextmanager
    async def capacity(*, wait: bool = True) -> AsyncGenerator[None]:
        assert not wait
        events.append("capacity")
        try:
            yield
        finally:
            events.append("released")

    @asynccontextmanager
    async def closed() -> AsyncGenerator[None]:
        assert events == ["capacity"]
        raise admission.AdmissionError("admission is closed")
        yield  # pragma: no cover

    def forbidden(*args: object) -> None:
        pytest.fail("closed admission must not open ClickHouse")

    monkeypatch.setattr(deployments, "LoadSettings", LoadSettings.model_construct)
    monkeypatch.setattr(admission, "execution", closed)
    monkeypatch.setattr(deployments, "one_clickhouse_loader", capacity)
    monkeypatch.setattr(bootstrap, "open_read_tables", forbidden)
    with pytest.raises(admission.AdmissionError, match="closed"):
        await deployments.s2_build_read_tables.fn()
    assert events == ["capacity", "released"]


@pytest.mark.usefixtures("prefect_server")
async def test_a_second_openalex_archive_is_refused_while_one_runs(
    monkeypatch: pytest.MonkeyPatch, oa_wired: DownloadDatasetService
) -> None:
    started, gate = asyncio.Event(), asyncio.Event()

    class BlockedJobs(FakeJobs):
        async def submit(self, job: ShardJob) -> str:
            started.set()
            await gate.wait()
            return await super().submit(job)

    jobs = BlockedJobs(oa_wired, failing=set())

    @asynccontextmanager
    async def open_openalex_download(
        settings: OpenAlexBatchSettings,
    ) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs]]:
        yield oa_wired, jobs

    monkeypatch.setattr(deployments, "OpenAlexBatchSettings", OpenAlexBatchSettings.model_construct)
    monkeypatch.setattr(bootstrap, "open_openalex_download", open_openalex_download)
    deployments.ensure_limits()
    first = asyncio.create_task(
        deployments.openalex_batch_download.fn(
            ArchiveRelease(release="2026-10-06", bucket="raw", dataset="works")
        )
    )
    try:
        async with asyncio.timeout(60):
            await started.wait()
            with pytest.raises(ConcurrencySlotAcquisitionError):
                await deployments.openalex_batch_download.fn(
                    ArchiveRelease(release="2026-10-06", bucket="raw", dataset="works-deleted")
                )
    finally:
        gate.set()
    assert (await first).datasets[0].dataset == "works"
    assert jobs.deleted == []


@pytest.mark.usefixtures("prefect_server")
async def test_an_openalex_archive_goes_on_past_a_failed_or_unlisted_dataset(
    monkeypatch: pytest.MonkeyPatch, oa_wired: DownloadDatasetService
) -> None:
    tried: list[str] = []

    async def dataset(command: DownloadDataset) -> BatchDownloadResult:
        tried.append(command.dataset)
        if command.dataset == "concepts":
            raise DatasetNotInRelease("release 2026-10-06 of OpenAlex has no dataset 'concepts'")
        if command.dataset == "authors":
            raise RowCountMismatch("part_0001.parquet holds 3 rows, the manifest 4")
        return BatchDownloadResult.model_construct(dataset=command.dataset)

    @asynccontextmanager
    async def open_openalex_download(
        settings: OpenAlexBatchSettings,
    ) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs]]:
        yield oa_wired, FakeJobs(oa_wired, failing=set())

    def flows(*args: object) -> SimpleNamespace:
        return SimpleNamespace(dataset=SimpleNamespace(fn=dataset))

    monkeypatch.setattr(deployments, "OpenAlexBatchSettings", OpenAlexBatchSettings.model_construct)
    monkeypatch.setattr(bootstrap, "open_openalex_download", open_openalex_download)
    monkeypatch.setattr(bootstrap, "batch_download_flows", flows)
    deployments.ensure_limits()
    with pytest.raises(IngestionError, match=r"1 datasets failed.*authors \(RowCountMismatch"):
        await deployments.openalex_batch_download(
            ArchiveRelease(release="2026-10-06", bucket="raw")
        )
    assert tried[:4] == ["works", "sources", "works-deleted", "authors"]
    assert sorted(tried) == sorted(SOURCES["openalex"].datasets)
