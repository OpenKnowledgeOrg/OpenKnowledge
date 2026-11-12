"""okp embeddings: one Batch task per unconverted shard, and the task that converts it."""

import asyncio
import json
import struct
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path

import pytest
from click.testing import CliRunner

from openknowledge_adapters.batch import BatchConfig, convert_spec
from openknowledge_adapters.clickhouse.embedding_index import read_index
from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.embeddings import ConvertJob, IndexRow, output_names
from openknowledge_core.ingestion import DownloadDataset, DownloadDatasetService
from openknowledge_core.ingestion.archive_progress import ArchiveProgress
from openknowledge_core.ingestion.errors import StoreUnavailable
from openknowledge_core.ingestion.models import StoredObject
from openknowledge_pipelines import cli, deployments
from tests.fakes.ports import PRODUCER, FakeJobs, FakeSource, FakeStore, clock, gz

RELEASE = "2026-10-06"
CURATED = "curated"
BATCH_ENV = {
    "S2_API_KEY": "k",
    "OKP_IMAGE_DIGEST": "sha256:" + "a" * 64,
    "OKP_BATCH_PROJECT": "p",
    "OKP_BATCH_REGION": "europe-west4",
    "OKP_BATCH_SERVICE_ACCOUNT": "download@p.iam.gserviceaccount.com",
    "OKP_BATCH_NETWORK": "n",
    "OKP_BATCH_SUBNETWORK": "s",
    "OKP_BATCH_IMAGE_REPOSITORY": "repo/pipelines",
    "OKP_BATCH_S2_KEY_SECRET": "projects/p/secrets/s2-api-key/versions/latest",
}


def record(corpusid: int, value: float) -> bytes:
    vector = json.dumps([value] * 768)
    return json.dumps({"corpusid": corpusid, "model": "specter_v2", "vector": vector}).encode()


SHARDS = {
    "part-0.gz": gz(record(1, 0.5) + b"\n" + record(2, 1.5) + b"\n"),
    "part-1.gz": gz(record(3, 2.5) + b"\n"),
}


def vectors(*values: float) -> bytes:
    return b"".join(struct.pack("<768f", *[value] * 768) for value in values)


def outputs(shard: str) -> tuple[ObjectPath, ObjectPath]:
    blob, index = output_names(RELEASE, "base", f"s2/{RELEASE}/embeddings-specter_v2/{shard}")
    return ObjectPath(bucket=CURATED, name=blob), ObjectPath(bucket=CURATED, name=index)


@pytest.fixture
def archived(
    monkeypatch: pytest.MonkeyPatch,
    service: DownloadDatasetService,
    source: FakeSource,
    store: FakeStore,
) -> FakeStore:
    command = DownloadDataset(
        release=RELEASE, dataset="embeddings-specter_v2", bucket="raw", prefix="s2"
    )
    for name, data in SHARDS.items():
        source.add(command.dataset, name, data)

    async def archive() -> None:
        pinned = await service.pin_dataset(command)
        for shard in pinned.shards:
            await service.archive_shard(pinned, shard)
        await ArchiveProgress(store, service, PRODUCER, clock).finalize(command)

    asyncio.run(archive())

    @asynccontextmanager
    async def open_store(config: object) -> AsyncGenerator[FakeStore]:
        yield store

    monkeypatch.setattr("openknowledge_adapters.gcs.connect", open_store)
    return store


def batch_task(monkeypatch: pytest.MonkeyPatch, shard: int) -> tuple[int, str]:
    """Run the command and environment the Batch adapter gives the task for ``shard``."""
    config = BatchConfig(
        project="p",
        region="europe-west4",
        service_account="sa",
        network="n",
        subnetwork="s",
        image="repo/pipelines@sha256:" + "a" * 64,
        machine_type="e2-standard-2",
        parallelism=1,
        max_run_seconds=60,
        poll_seconds=1,
        environment={},
        secrets={},
    )
    job = ConvertJob(
        release=RELEASE,
        raw_bucket="raw",
        prefix="s2",
        curated_bucket=CURATED,
        part="base",
        shards=(shard,),
    )
    [group] = convert_spec(config, job)["taskGroups"]
    environment = {
        **group["taskSpec"]["environment"]["variables"],
        **group["taskEnvironments"][0]["variables"],
        "BATCH_TASK_INDEX": "0",
        "BATCH_TASK_COUNT": "1",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    [runnable] = group["taskSpec"]["runnables"]
    result = CliRunner().invoke(cli.main, runnable["container"]["commands"])
    return result.exit_code, result.output


@pytest.mark.usefixtures("prefect_server")
def test_convert_submits_one_task_per_missing_shard(
    monkeypatch: pytest.MonkeyPatch, archived: FakeStore, service: DownloadDatasetService
) -> None:
    deployments.ensure_limits()
    for path in outputs("part-0.gz"):
        archived.put(path, b"converted")
    archived.put(outputs("part-1.gz")[0], b"lone blob")
    jobs = FakeJobs(service, set())

    @asynccontextmanager
    async def open_jobs(config: BatchConfig) -> AsyncGenerator[FakeJobs]:
        assert config.secrets == {}, "a conversion task needs no S2 key"
        yield jobs

    monkeypatch.setattr("openknowledge_adapters.batch.connect", open_jobs)
    for name, value in BATCH_ENV.items():
        monkeypatch.setenv(name, value)
    result = CliRunner().invoke(
        cli.main,
        ["embeddings", "convert", "--release", RELEASE, "--raw-bucket", "raw", "--prefix", "s2"]
        + ["--curated-bucket", CURATED],
    )
    assert result.exit_code == 0, result.output
    [job] = jobs.converts
    assert (job.release, job.part, job.shards) == (RELEASE, "base", (1,))
    assert json.loads(result.output)["submitted"] == [1]


def test_shard_rerun_rewrites_a_lone_blob(
    monkeypatch: pytest.MonkeyPatch, archived: FakeStore
) -> None:
    blob, index = outputs("part-0.gz")
    archived.put(blob, b"partial output of an earlier attempt")
    code, output = batch_task(monkeypatch, 0)
    assert code == 0, output
    assert archived.data(blob) == vectors(0.5, 1.5)
    assert read_index(BytesIO(archived.data(index))) == [
        IndexRow(1, 0, False),
        IndexRow(2, 1, False),
    ]


def test_convert_shard_writes_index_last(
    monkeypatch: pytest.MonkeyPatch, archived: FakeStore
) -> None:
    blob, index = outputs("part-1.gz")
    replace = archived.replace_file

    async def refuse_index(path: ObjectPath, file: Path, *, content_type: str) -> StoredObject:
        if path == index:
            raise StoreUnavailable(f"GCS answered HTTP 503 when asked to upload {path}")
        return await replace(path, file, content_type=content_type)

    monkeypatch.setattr(archived, "replace_file", refuse_index)
    code, output = batch_task(monkeypatch, 1)
    assert code == 1
    assert "HTTP 503" in output
    assert archived.data(blob) == vectors(2.5)
    assert str(index) not in archived.objects
