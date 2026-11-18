"""Cloud Batch jobs over its REST API, one task per shard: a download or a conversion."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast
from uuid import UUID

import aiohttp
from gcloud.aio.auth import Token

from openknowledge_adapters.batch.config import BatchConfig
from openknowledge_contracts.sources import SourceName
from openknowledge_core.embeddings import CONVERT_FAMILY, ConvertJob
from openknowledge_core.ingestion.errors import (
    TRANSIENT_STATUSES,
    BatchRejected,
    BatchTimedOut,
    BatchUnavailable,
    SharedCapacityError,
)
from openknowledge_core.ingestion.families import ARCHIVE_DATASETS, ArchiveFamily, archive_family
from openknowledge_core.ingestion.models import JobOutcome, ShardJob

ENDED = {
    "SUCCEEDED": JobOutcome.SUCCEEDED,
    "FAILED": JobOutcome.FAILED,
    "CANCELLED": JobOutcome.FAILED,
}
# No VM yet, so no task of the job has used the S2 key.
QUEUED = {"QUEUED", "SCHEDULED"}
CLI_GROUPS: Mapping[SourceName, str] = {"semantic-scholar": "s2", "openalex": "openalex"}
JOB_PREFIXES: Mapping[SourceName, str] = {"semantic-scholar": "s2", "openalex": "oa"}
# Jobs submitted before the source label existed are Semantic Scholar jobs.
LEGACY_SOURCE: SourceName = "semantic-scholar"
CONVERT_JOB_PREFIX = "specter2-"
# Conversion reads the Semantic Scholar archive, so it shares that source's Batch slot.
CONVERT_SOURCE: SourceName = "semantic-scholar"


class _JobExists(Exception):
    """The job ID holds the operation's UUID, so an earlier attempt of it created the job."""


class _JobGone(BatchRejected):
    """Batch has no such job, for instance once it was deleted."""


class CloudBatchJobs:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        token: Token,
        config: BatchConfig,
        *,
        api_root: str,
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._session = session
        self._token = token
        self._config = config
        self._root = api_root
        self._sleep = sleep
        self._parent = f"projects/{config.project}/locations/{config.region}"

    async def submit(self, job: ShardJob) -> str:
        return await self._submit(job_id(job), job.operation_id, job_spec(self._config, job))

    async def submit_convert(self, job: ConvertJob) -> str:
        return await self._submit(
            convert_job_id(job), job.operation_id, convert_spec(self._config, job)
        )

    async def _submit(self, job: str, operation_id: UUID, spec: dict[str, Any]) -> str:
        name = f"{self._parent}/jobs/{job}"
        try:
            await self._call(
                "POST",
                f"{self._parent}/jobs",
                params={"jobId": job, "requestId": str(operation_id)},
                json=spec,
            )
        except _JobExists:
            pass
        except BatchUnavailable as exc:
            raise BatchUnavailable(
                f"Cloud Batch job {name} submission is unconfirmed; it may be running. {exc}"
            ) from None
        return name

    async def running(self, family: str = ArchiveFamily.CORE) -> dict[str, bool]:
        """Inspect all pages before allowing family cleanup or a new submission."""
        source = family.source if isinstance(family, ArchiveFamily) else CONVERT_SOURCE
        found: dict[str, bool] = {}
        params: dict[str, str] = {}
        while True:
            body = await self._call("GET", f"{self._parent}/jobs", params=params)
            for job in body.get("jobs", []):
                state = job.get("status", {}).get("state")
                if "operation-id" in job.get("labels", {}) and state not in ENDED:
                    labels = job["labels"]
                    if labels.get("source", LEGACY_SOURCE) != source:
                        continue
                    job_family = labels.get("archive-family")
                    if (
                        job_family is None
                        and source == LEGACY_SOURCE
                        and ARCHIVE_DATASETS.get(labels.get("dataset", "")) == ArchiveFamily.CORE
                    ):
                        job_family = ArchiveFamily.CORE.value
                    if job_family != family:
                        blocker = (
                            f"Active Batch job {job['name']} of another family blocks conversion"
                            if family == CONVERT_FAMILY
                            else f"Active foreign or legacy download job {job['name']}"
                        )
                        raise SharedCapacityError(
                            f"{blocker}; drain it to a terminal state before retrying. "
                            "This lookup did not cancel or wait on jobs"
                        )
                    found[str(job["name"])] = state in QUEUED
            if not body.get("nextPageToken"):
                return found
            params = {"pageToken": body["nextPageToken"]}

    async def delete(self, name: str) -> None:
        await self._call("DELETE", name)

    async def wait(self, name: str) -> JobOutcome:
        """Bound the whole wait, including failed or stalled polls."""
        try:
            async with asyncio.timeout(self._config.wait_timeout_seconds):
                while True:
                    try:
                        state = (await self._call("GET", name)).get("status", {}).get("state")
                    except BatchUnavailable:
                        state = None
                    except _JobGone:
                        return JobOutcome.FAILED
                    if state in ENDED:
                        return ENDED[state]
                    await self._sleep(self._config.poll_seconds)
        except TimeoutError:
            waiters = (
                "a rerun of okp embeddings convert waits for it; archive downloads refuse to "
                "start until it ends"
                if name.rsplit("/", 1)[-1].startswith(CONVERT_JOB_PREFIX)
                else "same-family downloads wait for it once started; "
                "other families refuse to start"
            )
            raise BatchTimedOut(
                f"Cloud Batch job {name} did not finish within "
                f"{self._config.wait_timeout_seconds:g}s and may still be running; {waiters}. "
                "Inspect and drain it if it hangs"
            ) from None

    async def _call(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        try:
            token = await self._token.get()
            headers = {"Authorization": f"Bearer {token}"}
            async with self._session.request(
                method, f"{self._root}/{path}", headers=headers, **kwargs
            ) as response:
                status = response.status
                body = cast(dict[str, Any], await response.json(content_type=None) or {})
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            raise BatchUnavailable(
                f"Cloud Batch did not answer ({type(exc).__name__}); rerun later"
            ) from None
        if status < 300:
            return body
        if status in TRANSIENT_STATUSES:
            raise BatchUnavailable(f"Cloud Batch answered HTTP {status}; rerun later")
        if status == 409 and method == "POST":
            raise _JobExists
        message = str(body.get("error", {}).get("message", "no message"))
        error = _JobGone if status == 404 else BatchRejected
        raise error(f"Cloud Batch refused {method} {path} with HTTP {status}: {message}")


def job_id(job: ShardJob) -> str:
    """At most 63 lowercase characters, stable for retries of the same operation."""
    part = f"-{job.part}" if job.part else ""
    name = f"{JOB_PREFIXES[job.source]}-{job.dataset.replace('_', '-')}-{job.release_id}{part}"[
        :30
    ].rstrip("-")
    return f"{name}-{job.operation_id.hex}"


def job_spec(config: BatchConfig, job: ShardJob) -> dict[str, Any]:
    command = [
        CLI_GROUPS[job.source],
        "download-shard",
        "--release",
        job.release_id,
        "--dataset",
        job.dataset,
    ]
    command += ["--bucket", job.bucket, "--prefix", job.prefix]
    if job.from_release is not None and job.part is not None:
        command += ["--from-release", job.from_release, "--part", job.part]
    labels = {
        "source": job.source,
        "archive-family": archive_family(job.dataset, job.source).value,
        "dataset": job.dataset.replace("_", "-"),
        "release": job.release_id,
        "part": job.part or "base",
    }
    # Each task makes one listing call with the S2 key, which allows one request
    # per second; the retries' jitter spreads the tasks that start together.
    return _spec(config, command, job.shards, job.operation_id, labels, config.secrets)


def convert_job_id(job: ConvertJob) -> str:
    return f"{CONVERT_JOB_PREFIX}{job.release}-{job.part}-{job.operation_id.hex}"


def convert_spec(config: BatchConfig, job: ConvertJob) -> dict[str, Any]:
    command = ["embeddings", "convert-shard", "--release", job.release]
    command += ["--raw-bucket", job.raw_bucket, "--prefix", job.prefix]
    command += ["--curated-bucket", job.curated_bucket]
    if job.from_release is not None and job.part != "base":
        command += ["--from-release", job.from_release, "--part", job.part]
    labels = {
        "source": CONVERT_SOURCE,
        "archive-family": CONVERT_FAMILY,
        "dataset": "embeddings-specter-v2",
        "release": job.release,
        "part": job.part,
    }
    # The tasks read and write GCS only, so they get no secret.
    return _spec(config, command, job.shards, job.operation_id, labels, {})


def _spec(
    config: BatchConfig,
    command: list[str],
    shards: tuple[int, ...],
    operation_id: UUID,
    labels: dict[str, str],
    secrets: dict[str, str],
) -> dict[str, Any]:
    return {
        "taskGroups": [
            {
                # One task per entry; okp reads its shard from OKP_SHARD.
                "taskEnvironments": [{"variables": {"OKP_SHARD": str(s)}} for s in shards],
                "parallelism": min(config.parallelism, len(shards)),
                "taskSpec": {
                    "runnables": [{"container": {"imageUri": config.image, "commands": command}}],
                    "environment": {
                        "variables": {
                            **config.environment,
                            "OKP_BATCH_OPERATION_ID": operation_id.hex,
                        },
                        "secretVariables": secrets,
                    },
                    "maxRunDuration": f"{config.max_run_seconds}s",
                },
            }
        ],
        "allocationPolicy": {
            "serviceAccount": {"email": config.service_account},
            "instances": [
                {
                    "policy": {
                        "machineType": config.machine_type,
                        "provisioningModel": config.provisioning_model,
                    }
                }
            ],
            "network": {
                "networkInterfaces": [{"network": config.network, "subnetwork": config.subnetwork}]
            },
        },
        "labels": {**labels, "operation-id": operation_id.hex},
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
    }
