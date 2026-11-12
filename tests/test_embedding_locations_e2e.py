"""okp embeddings load-index against ClickHouse and fake GCS in Docker, which CI must have."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import date

import httpx
import pytest
from click.testing import CliRunner
from pydantic import HttpUrl, SecretStr

from openknowledge_adapters import clickhouse, gcs
from openknowledge_adapters.clickhouse.embedding_index import write_index
from openknowledge_core.embeddings import (
    ConvertEmbeddingShard,
    EmbeddingConversion,
    ShardPart,
    output_names,
)
from openknowledge_core.embeddings.errors import NoIndexLoaded
from openknowledge_core.ingestion import DownloadDataset, DownloadDatasetService, DownloadDiff
from openknowledge_core.ingestion.archive_progress import ArchiveProgress, completion_path
from openknowledge_pipelines import cli
from tests import test_load_e2e as load_e2e
from tests.fakes.ports import PRODUCER, FakeSource, clock
from tests.test_load_e2e import BUCKET, packed

# The same compose stack, started once more for this module, and the same skip without Docker.
ports = load_e2e.ports
pytestmark = load_e2e.pytestmark

CURATED = "openknowledge-508216-curated"
DATASET = "embeddings-specter_v2"
BASE, NEXT = "2026-10-06", "2026-10-13"


def paper(corpusid: int, value: float | None = None) -> str:
    if value is None:
        return json.dumps({"corpusid": corpusid})
    vector = json.dumps([value] * 768)
    return json.dumps({"corpusid": corpusid, "model": "specter_v2", "vector": vector})


def on_gcs(ports: dict[str, str], work: Callable[[gcs.GcsArtifactStore], Awaitable[None]]) -> None:
    config = gcs.GcsConfig(
        request_timeout_seconds=60,
        min_transfer_rate=1024,
        api_root=f"http://127.0.0.1:{ports['GCS_PORT']}",
    )

    async def run() -> None:
        async with gcs.connect(config) as store:
            await work(store)

    asyncio.run(run())


def archive(
    ports: dict[str, str], source: FakeSource, command: DownloadDataset | DownloadDiff
) -> None:
    """Archive and finalize ``source``'s shards, as okp s2 archive-specter2 would."""

    async def work(store: gcs.GcsArtifactStore) -> None:
        service = DownloadDatasetService(
            catalog=source, downloads=source, store=store, producer=PRODUCER, clock=clock
        )
        progress = ArchiveProgress(store, service, PRODUCER, clock)
        predecessor = None
        if isinstance(command, DownloadDiff):
            pinned = await service.pin_diff(command)
            parts = (pinned.updates, pinned.deletes)
            base = command.model_dump(exclude={"from_release"}) | {"release": command.from_release}
            predecessor = await progress.reference(completion_path(DownloadDataset(**base)))
        else:
            parts = (await service.pin_dataset(command),)
        for part in parts:
            for shard in part.shards:
                await service.archive_shard(part, shard)
        await progress.finalize(command, predecessor)

    on_gcs(ports, work)


def convert(
    ports: dict[str, str],
    release: str,
    shards: dict[ShardPart, range],
    from_release: str | None = None,
) -> None:
    async def work(store: gcs.GcsArtifactStore) -> None:
        conversion = EmbeddingConversion(store, write_index)
        for part, numbers in shards.items():
            for shard in numbers:
                await conversion.convert_shard(
                    ConvertEmbeddingShard(
                        release=release,
                        from_release=from_release,
                        part=None if part == "base" else part,
                        raw_bucket=BUCKET,
                        curated_bucket=CURATED,
                        shard=shard,
                    )
                )

    on_gcs(ports, work)


def load_index(ports: dict[str, str], *args: str) -> tuple[int, str]:
    env = {
        "OKP_CLICKHOUSE_URL": f"http://127.0.0.1:{ports['CLICKHOUSE_PORT']}",
        "CLICKHOUSE_PASSWORD": "openknowledge",
        "OKP_CLICKHOUSE_RAW_BUCKET": BUCKET,
        "OKP_GCS_API_ROOT": f"http://127.0.0.1:{ports['GCS_PORT']}",
    }
    command = [
        "embeddings",
        "load-index",
        *args,
        "--raw-bucket",
        BUCKET,
        "--curated-bucket",
        CURATED,
    ]
    result = CliRunner().invoke(cli.main, command, env=env)
    return result.exit_code, result.output


def loaded(ports: dict[str, str], *args: str) -> dict[str, object]:
    code, output = load_index(ports, *args)
    assert code == 0, output
    return json.loads(output.splitlines()[-1])


def locate(ports: dict[str, str], corpusids: list[int]) -> tuple[date, dict[int, tuple[str, int]]]:
    config = clickhouse.ClickHouseConfig(
        url=HttpUrl(f"http://127.0.0.1:{ports['CLICKHOUSE_PORT']}"),
        user="openknowledge",
        password=SecretStr("openknowledge"),
        raw_bucket=BUCKET,
        timeout_seconds=60,
    )

    async def run() -> tuple[date, dict[int, tuple[str, int]]]:
        async with clickhouse.connect(config) as ch:
            return await ch.locations.locate(corpusids)

    return asyncio.run(run())


def blob(release: str, part: ShardPart, shard: int) -> str:
    return output_names(release, part, f"part-{shard}.gz")[0]


def test_locate_before_load_is_refused(ports: dict[str, str]) -> None:
    with pytest.raises(NoIndexLoaded, match="no embedding index loaded"):
        locate(ports, [1])


def test_base_then_diff_locates_latest_and_hides_deleted(ports: dict[str, str]) -> None:
    httpx.post(
        f"http://127.0.0.1:{ports['GCS_PORT']}/storage/v1/b",
        params={"project": "e2e"},
        json={"name": CURATED},
    ).raise_for_status()
    base = FakeSource()
    base.releases = [BASE]
    for shard, line in enumerate([paper(1, 0.5), paper(2, 1.5), paper(3, 2.5)]):
        base.add(DATASET, f"part-{shard}.gz", packed([line]))
    archive(ports, base, DownloadDataset(release=BASE, dataset=DATASET, bucket=BUCKET))
    convert(ports, BASE, {"base": range(2)})
    code, output = load_index(ports, "--release", BASE)
    assert code == 1
    assert "1 of 3" in output

    convert(ports, BASE, {"base": range(2, 3)})
    assert loaded(ports, "--release", BASE) == {"release": BASE, "rows": 3, "repeated": 0}
    assert locate(ports, [1, 2, 3]) == (
        date.fromisoformat(BASE),
        {
            1: (blob(BASE, "base", 0), 0),
            2: (blob(BASE, "base", 1), 0),
            3: (blob(BASE, "base", 2), 0),
        },
    )

    diff = FakeSource()
    diff.releases = [NEXT]
    diff.add_diff(
        DATASET,
        BASE,
        NEXT,
        {"part-0.gz": packed([paper(2, 9.5)])},
        {"part-0.gz": packed([paper(3), paper(2)])},
    )
    command = DownloadDiff(from_release=BASE, release=NEXT, dataset=DATASET, bucket=BUCKET)
    archive(ports, diff, command)
    convert(ports, NEXT, {"updates": range(1), "deletes": range(1)}, from_release=BASE)
    expected = {"release": NEXT, "rows": 3, "repeated": 1}
    assert loaded(ports, "--release", NEXT, "--from-release", BASE) == expected
    # A rerun converges on the same rows.
    assert loaded(ports, "--release", NEXT, "--from-release", BASE) == expected
    assert locate(ports, [1, 2, 3]) == (date.fromisoformat(NEXT), {1: (blob(BASE, "base", 0), 0)})
    # The most one lookup asks for, with ids as long as any.
    assert locate(ports, [10**12 + i for i in range(20_000)]) == (date.fromisoformat(NEXT), {})
