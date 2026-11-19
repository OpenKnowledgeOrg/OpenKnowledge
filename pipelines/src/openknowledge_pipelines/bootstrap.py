"""Wires adapters to the core ports for one run, as the only module that imports them."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from importlib.metadata import version
from typing import TYPE_CHECKING

from openknowledge_adapters import batch, clickhouse, gcs, openalex, opensearch, semantic_scholar
from openknowledge_contracts.datasets import DatasetName, ReleaseId
from openknowledge_contracts.manifests import TRANSFORM_VERSION, Producer
from openknowledge_contracts.sources import SOURCES
from openknowledge_core.ingestion import (
    DownloadDatasetService,
    LoadDatasetService,
    ShardJobs,
    WatchdogService,
)
from openknowledge_core.ingestion.archive_progress import ArchiveProgress
from openknowledge_core.ingestion.consumers import ArchiveConsumer
from openknowledge_core.ingestion.coordinator import CORE_ORDER, LARGE_ORDER
from openknowledge_core.ingestion.errors import IngestionError
from openknowledge_core.ingestion.families import ArchiveFamily
from openknowledge_core.ingestion.measurement import LoadObservation
from openknowledge_core.ingestion.models import held_release
from openknowledge_core.ingestion.ports import LoadTelemetry, ReadTables
from openknowledge_core.search import IndexPapers, SearchIndexService
from openknowledge_core.search.commands import IndexPapersResult
from openknowledge_pipelines.settings import (
    BatchJobSettings,
    BatchSettings,
    CommonSettings,
    LoadSettings,
    OpenAlexBatchSettings,
    OpenAlexSettings,
    PipelineSettings,
    SearchSettings,
    SyncSettings,
    WatchdogSettings,
)

if TYPE_CHECKING:
    from openknowledge_pipelines.flows.s2_batch_download import S2BatchDownloadFlows
    from openknowledge_pipelines.flows.s2_download import S2DownloadFlow
    from openknowledge_pipelines.flows.s2_load import S2LoadFlows
    from openknowledge_pipelines.flows.s2_sync import SyncServices
    from openknowledge_pipelines.flows.search_index import SearchIndexResult
    from openknowledge_pipelines.flows.watchdog import WatchdogFlow


@asynccontextmanager
async def open_service(settings: PipelineSettings) -> AsyncGenerator[DownloadDatasetService, None]:
    s2_config = semantic_scholar.SemanticScholarConfig(
        api_key=settings.s2_api_key,
        api_url=settings.s2_api_url,
        request_interval_seconds=settings.s2_request_interval_seconds,
        attempts=settings.s2_attempts,
        backoff_seconds=settings.s2_backoff_seconds,
        timeout_seconds=settings.s2_timeout_seconds,
        download_connect_timeout_seconds=settings.download_connect_timeout_seconds,
        download_read_timeout_seconds=settings.download_read_timeout_seconds,
    )
    async with semantic_scholar.connect(s2_config) as s2, gcs.connect(_gcs(settings)) as store:
        yield DownloadDatasetService(
            catalog=s2.catalog,
            downloads=s2.downloads,
            store=store,
            producer=producer(settings),
            clock=lambda: datetime.now(UTC),
            source=SOURCES["semantic-scholar"],
        )


@asynccontextmanager
async def open_openalex_service(
    settings: OpenAlexSettings,
) -> AsyncGenerator[DownloadDatasetService, None]:
    config = openalex.OpenAlexConfig(
        base_url=settings.openalex_base_url,
        timeout_seconds=settings.openalex_timeout_seconds,
        download_connect_timeout_seconds=settings.download_connect_timeout_seconds,
        download_read_timeout_seconds=settings.download_read_timeout_seconds,
    )
    async with openalex.connect(config) as oa, gcs.connect(_gcs(settings)) as store:
        yield DownloadDatasetService(
            catalog=oa.catalog,
            downloads=oa.downloads,
            store=store,
            producer=producer(settings),
            clock=lambda: datetime.now(UTC),
            source=SOURCES["openalex"],
        )


@asynccontextmanager
async def open_load_service(settings: LoadSettings) -> AsyncGenerator[LoadDatasetService, None]:
    async with (
        gcs.connect(_gcs(settings)) as store,
        clickhouse.connect(_clickhouse(settings, settings.clickhouse_timeout_seconds)) as ch,
    ):
        yield LoadDatasetService(
            store=store,
            tables=ch.tables,
            applied=ch.applied,
            producer=producer(settings),
            clock=lambda: datetime.now(UTC),
        )


@asynccontextmanager
async def open_watchdog(settings: WatchdogSettings) -> AsyncGenerator[WatchdogService, None]:
    config = _clickhouse(settings, settings.watchdog_timeout_seconds)
    async with clickhouse.connect(config) as ch:
        yield WatchdogService(applied=ch.applied, clock=lambda: datetime.now(UTC))


def _gcs(settings: CommonSettings) -> gcs.GcsConfig:
    return gcs.GcsConfig(
        request_timeout_seconds=settings.gcs_request_timeout_seconds,
        min_transfer_rate=settings.gcs_min_transfer_bytes_per_second,
        api_root=settings.gcs_api_root,
    )


def _clickhouse(
    settings: LoadSettings, timeout_seconds: float | None
) -> clickhouse.ClickHouseConfig:
    return clickhouse.ClickHouseConfig(
        url=settings.clickhouse_url,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        raw_bucket=settings.clickhouse_raw_bucket,
        timeout_seconds=timeout_seconds,
    )


def _opensearch(settings: SearchSettings) -> opensearch.OpenSearchConfig:
    return opensearch.OpenSearchConfig(
        url=settings.opensearch_url,
        user=settings.opensearch_user,
        password=settings.opensearch_password,
        timeout_seconds=settings.opensearch_timeout_seconds,
    )


def producer(settings: CommonSettings) -> Producer:
    return Producer(
        transform="verbatim",
        transform_version=TRANSFORM_VERSION,
        software_version=version("openknowledge-pipelines"),
        image_digest=settings.image_digest,
    )


def download_flow(service: DownloadDatasetService, settings: PipelineSettings) -> S2DownloadFlow:
    # Imported here so that `okp s2 pin` and `okp s2 download-shard` never load Prefect.
    from openknowledge_pipelines.flows.s2_download import s2_download_flow

    return s2_download_flow(
        service,
        task_retries=settings.task_retries,
        retry_delay_seconds=settings.task_retry_delay_seconds,
    )


def load_flows(
    service: LoadDatasetService, settings: LoadSettings, *, parallelism: int = 1
) -> S2LoadFlows:
    from openknowledge_pipelines.flows.s2_load import s2_load_flows

    return s2_load_flows(
        service,
        task_retries=settings.task_retries,
        retry_delay_seconds=settings.task_retry_delay_seconds,
        parallelism=parallelism,
    )


def watchdog_flow(service: WatchdogService, settings: WatchdogSettings) -> WatchdogFlow:
    from openknowledge_pipelines.flows import watchdog

    return watchdog.watchdog_flow(
        service,
        max_age=timedelta(days=settings.watchdog_max_age_days),
        task_retries=settings.task_retries,
        retry_delay_seconds=settings.task_retry_delay_seconds,
    )


@asynccontextmanager
async def open_batch_download(
    settings: BatchSettings,
) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs], None]:
    config = _batch_config(
        settings,
        parallelism=settings.batch_parallelism,
        secrets={"S2_API_KEY": settings.batch_s2_key_secret},
    )
    async with open_service(settings) as service, batch.connect(config) as jobs:
        yield service, jobs


@asynccontextmanager
async def open_openalex_download(
    settings: OpenAlexBatchSettings,
) -> AsyncGenerator[tuple[DownloadDatasetService, ShardJobs], None]:
    config = _batch_config(settings, parallelism=settings.batch_openalex_parallelism, secrets={})
    async with open_openalex_service(settings) as service, batch.connect(config) as jobs:
        yield service, jobs


def _batch_config(
    settings: BatchJobSettings, *, parallelism: int, secrets: dict[str, str]
) -> batch.BatchConfig:
    return batch.BatchConfig(
        project=settings.batch_project,
        region=settings.batch_region,
        service_account=settings.batch_service_account,
        network=settings.batch_network,
        subnetwork=settings.batch_subnetwork,
        image=f"{settings.batch_image_repository}@{settings.image_digest}",
        machine_type=settings.batch_machine_type,
        parallelism=parallelism,
        provisioning_model=settings.batch_provisioning_model,
        max_run_seconds=settings.batch_max_run_seconds,
        poll_seconds=settings.batch_poll_seconds,
        wait_timeout_seconds=settings.batch_wait_timeout_seconds,
        environment={"OKP_IMAGE_DIGEST": settings.image_digest},
        secrets=secrets,
    )


def batch_download_flows(
    service: DownloadDatasetService,
    jobs: ShardJobs,
    settings: BatchJobSettings,
    family: ArchiveFamily = ArchiveFamily.CORE,
) -> S2BatchDownloadFlows:
    from openknowledge_pipelines.flows.s2_batch_download import s2_batch_download_flows

    return s2_batch_download_flows(
        service,
        jobs,
        family=family,
        rounds=settings.task_retries + 1,
        task_retries=settings.task_retries,
        retry_delay_seconds=settings.task_retry_delay_seconds,
    )


@asynccontextmanager
async def open_archive_progress(
    settings: PipelineSettings,
) -> AsyncGenerator[ArchiveProgress, None]:
    async with open_service(settings) as service, gcs.connect(_gcs(settings)) as store:
        yield ArchiveProgress(
            store,
            service,
            producer(settings),
            lambda: datetime.now(UTC),
            SOURCES["semantic-scholar"],
        )


@asynccontextmanager
async def open_sync(
    settings: SyncSettings,
    s2_lease: Callable[[], AbstractAsyncContextManager[None]],
    load_lease: Callable[[], AbstractAsyncContextManager[None]],
    *,
    read_table_stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
) -> AsyncGenerator[SyncServices, None]:
    from openknowledge_core.ingestion.families import archive_family
    from openknowledge_pipelines.flows.s2_sync import SyncServices

    async with (
        open_batch_download(settings) as (service, jobs),
        gcs.connect(_gcs(settings)) as store,
        clickhouse.connect(_clickhouse(settings, settings.clickhouse_timeout_seconds)) as ch,
        open_read_tables(settings, stage=read_table_stage) as edges,
    ):
        progress = ArchiveProgress(
            store,
            service,
            producer(settings),
            lambda: datetime.now(UTC),
            SOURCES["semantic-scholar"],
        )
        loader = LoadDatasetService(
            store=store,
            tables=ch.tables,
            applied=ch.applied,
            producer=producer(settings),
            clock=lambda: datetime.now(UTC),
        )
        yield SyncServices(
            jobs=jobs,
            catalog=service.catalog,
            tables=ch.tables,
            applied=ch.applied,
            progress=progress,
            downloads={
                dataset: batch_download_flows(
                    service, jobs, settings, archive_family(dataset, "semantic-scholar")
                )
                for dataset in (*CORE_ORDER, *LARGE_ORDER)
            },
            loads=load_flows(loader, settings),
            s2_lease=s2_lease,
            load_lease=load_lease,
            read_tables=edges,
            build_read_tables=lambda: edges.build(producer(settings), lambda: datetime.now(UTC)),
        )


@asynccontextmanager
async def open_archive_consumer(settings: CommonSettings) -> AsyncGenerator[ArchiveConsumer]:
    async with gcs.connect(_gcs(settings)) as store:
        yield ArchiveConsumer(store, SOURCES["semantic-scholar"])


@asynccontextmanager
async def open_measured_load(
    settings: LoadSettings,
) -> AsyncGenerator[tuple[LoadDatasetService, LoadObservation, LoadTelemetry]]:
    observation = LoadObservation()
    async with (
        gcs.connect(_gcs(settings)) as store,
        clickhouse.connect(_clickhouse(settings, settings.clickhouse_timeout_seconds)) as ch,
    ):
        yield (
            LoadDatasetService(
                store=store,
                tables=ch.tables,
                applied=ch.applied,
                producer=producer(settings),
                clock=lambda: datetime.now(UTC),
                observer=observation,
            ),
            observation,
            ch.telemetry,
        )


@asynccontextmanager
async def open_read_tables(
    settings: LoadSettings,
    *,
    stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
) -> AsyncGenerator[ReadTables, None]:
    from openknowledge_pipelines.flows.sync_observability import SyncObservation

    observer = SyncObservation()
    # A full rebuild can outlast the per-object load timeout. Keep the connect timeout.
    async with clickhouse.connect(
        _clickhouse(settings, None),
        read_table_stage=stage or observer.stage,
        read_tables_max_delta_share=settings.read_tables_max_delta_share,
        read_tables_max_delta_keys=settings.read_tables_max_delta_keys,
    ) as ch:
        yield ch.read_tables


@asynccontextmanager
async def open_held_releases(
    settings: LoadSettings,
) -> AsyncGenerator[Callable[[DatasetName], Awaitable[ReleaseId | None]], None]:
    """The release each dataset's table holds: applied to its live table, which has rows."""
    async with clickhouse.connect(_clickhouse(settings, settings.clickhouse_timeout_seconds)) as ch:

        async def held(dataset: DatasetName) -> ReleaseId | None:
            table = await ch.tables.table(dataset)
            return held_release(table, await ch.applied.latest(table.name))

        yield held


async def _index_search_part(settings: SearchSettings, command: IndexPapers) -> IndexPapersResult:
    async with (
        clickhouse.connect(_clickhouse(settings, settings.clickhouse_timeout_seconds)) as ch,
        opensearch.connect(_opensearch(settings)) as sink,
    ):
        service = SearchIndexService(source=ch.search, sink=sink, shards=settings.index_shards)
        return await service.index(command)


def _search_part_worker(settings: SearchSettings, command: IndexPapers) -> IndexPapersResult:
    # Never send vendor exceptions, response bodies, or settings over the result pipe.
    try:
        return asyncio.run(_index_search_part(settings, command))
    except IngestionError as exc:
        raise type(exc)(str(exc)) from None
    except Exception:
        raise RuntimeError(
            "Search part failed; inspect source and sink health before resuming"
        ) from None


async def run_search_index(settings: SearchSettings) -> SearchIndexResult:
    from openknowledge_pipelines import admission
    from openknowledge_pipelines.flows.process_parts import PartProcesses
    from openknowledge_pipelines.flows.search_index import run_index

    async with (
        clickhouse.connect(_clickhouse(settings, settings.clickhouse_timeout_seconds)) as ch,
        opensearch.connect(_opensearch(settings)) as sink,
    ):
        service = SearchIndexService(source=ch.search, sink=sink, shards=settings.index_shards)
        workers = PartProcesses(partial(_search_part_worker, settings), settings.index_parallelism)
        try:
            return await run_index(
                service,
                ch.search,
                sink,
                ch.indexed,
                execution=lambda: admission.execution(release_on_exception=True),
                base_parts=settings.index_base_parts,
                diff_parts=settings.index_diff_parts,
                parallelism=settings.index_parallelism,
                index=workers.index,
            )
        finally:
            await workers.close()
