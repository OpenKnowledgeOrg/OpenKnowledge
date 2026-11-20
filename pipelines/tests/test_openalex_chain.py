from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import partial

import pytest
from prefect.concurrency.asyncio import ConcurrencySlotAcquisitionError

from openknowledge_contracts.manifests import ObjectPath
from openknowledge_contracts.tables.openalex import CONTRACTS
from openknowledge_core.ingestion import (
    LoadDataset,
    LoadDatasetResult,
    OpenAlexLoad,
    OpenAlexLoadResult,
)
from openknowledge_core.ingestion.errors import IngestionError, TableRejected
from openknowledge_core.ingestion.read_tables import BuildReadTablesResult, ReadTablesPath
from openknowledge_pipelines import admission, bootstrap, deployments
from openknowledge_pipelines.flows.openalex_load import (
    CORE,
    ReleaseState,
    check_release,
    load_release,
)
from openknowledge_pipelines.settings import LoadSettings

RELEASE = "2027-01-13"
COMMAND = OpenAlexLoad(release=RELEASE, bucket="raw")


def loader(
    events: list[str], failing: str | None = None
) -> Callable[[LoadDataset], Awaitable[LoadDatasetResult]]:
    async def load(command: LoadDataset) -> LoadDatasetResult:
        events.append(command.dataset)
        if command.dataset == failing:
            raise TableRejected("synthetic load failure")
        return LoadDatasetResult(
            release_id=command.release,
            dataset=command.dataset,
            table=command.dataset,
            objects=1,
            loaded=1,
            already_loaded=0,
            live_rows=1,
            identical_duplicates=0,
            removed=0,
            manifest=ObjectPath.under("raw", "openalex", command.release, "manifest.json"),
        )

    return load


def builder(
    events: list[str], failure: str | None = None
) -> Callable[[], Awaitable[BuildReadTablesResult]]:
    async def build() -> BuildReadTablesResult:
        events.append("build")
        if failure is not None:
            raise TableRejected(failure)
        return BuildReadTablesResult(
            source_releases={"openalex_works": RELEASE},
            tables={},
            paths={"openalex": ReadTablesPath.FULL},
        )

    return build


async def test_a_release_builds_after_its_core_tables_until_a_build_succeeds() -> None:
    events: list[str] = []
    held: set[str] = set()
    built: list[bool] = [False]
    failure: list[str | None] = ["synthetic refusal"]

    async def latest() -> str:
        return RELEASE

    async def state(release: str) -> ReleaseState:
        return ReleaseState(tuple(name for name in CONTRACTS if name not in held), built[0])

    async def tidy(release: str) -> None:
        events.append("tidy")

    async def take(release: str) -> OpenAlexLoadResult:
        command = OpenAlexLoad(release=release, bucket="raw")
        result = await load_release(loader(events), command, builder(events, failure[0]), state)
        held.update(CONTRACTS)
        built[0] = built[0] or result.read_tables is not None
        return result

    # A failed build leaves the core tables loaded and the others loading after it.
    first = await check_release(latest, state, take, tidy)
    build = events.index("build")
    assert events[:build] == list(CORE)
    assert events[build + 1 :] == [name for name in CONTRACTS if name not in CORE]
    assert events[-6:] == [
        "openalex-authors",
        "awards",
        "works-authorships",
        "works-locations",
        "works-references",
        "works-classifications",
    ]
    assert first is not None and first.read_tables_failure == "synthetic refusal"
    assert first.works is not None and "awards" in first.additional
    # The next check builds from the loaded release without loading anything.
    events.clear()
    failure[0] = None
    rebuilt = await check_release(latest, state, take, tidy)
    assert events == ["build"] and rebuilt is not None and rebuilt.read_tables is not None
    # A loaded and built release only tidies, and a run queued behind it does nothing.
    events.clear()
    assert await check_release(latest, state, take, tidy) is None
    assert await take(RELEASE) == OpenAlexLoadResult() and events == ["tidy"]


@pytest.mark.parametrize("busy", [True, False])
async def test_a_current_release_drops_leftovers_only_under_a_free_loader_lease(
    monkeypatch: pytest.MonkeyPatch, busy: bool
) -> None:
    events: list[str] = []

    @asynccontextmanager
    async def lease(*, wait: bool = True) -> AsyncGenerator[None]:
        assert not wait
        if busy:
            raise ConcurrencySlotAcquisitionError("a load holds ClickHouse")
        events.append("lease")
        yield

    @asynccontextmanager
    async def admitted() -> AsyncGenerator[None]:
        yield

    @asynccontextmanager
    async def held(
        settings: LoadSettings, *, prefix: str, tidy: bool = False
    ) -> AsyncGenerator[
        tuple[Callable[[str, str], Awaitable[bool]], Callable[[str], Awaitable[bool]]]
    ]:
        events.append(f"state, tidy={tidy}")

        async def holds(dataset: str, release: str) -> bool:
            return True

        async def built(release: str) -> bool:
            return True

        yield holds, built

    async def latest() -> str:
        return RELEASE

    async def take(release: str) -> OpenAlexLoadResult:
        raise AssertionError("a current release is not taken")

    monkeypatch.setattr(deployments, "one_clickhouse_loader", lease)
    monkeypatch.setattr(admission, "execution", admitted)
    monkeypatch.setattr(bootstrap, "open_held_releases", held)
    settings = LoadSettings.model_construct()
    state = partial(deployments._release_state, settings, prefix="openalex")  # pyright: ignore[reportPrivateUsage]
    tidy = partial(deployments._tidy, settings, prefix="openalex")  # pyright: ignore[reportPrivateUsage]
    assert await check_release(latest, state, take, tidy) is None
    assert events == ["state, tidy=False"] + ([] if busy else ["lease", "state, tidy=True"])


async def test_a_failed_new_table_neither_blocks_the_build_nor_the_tables_after_it() -> None:
    events: list[str] = []
    with pytest.raises(IngestionError, match="did not load into awards: synthetic load failure"):
        await load_release(loader(events, failing="awards"), COMMAND, builder(events))
    assert events.index("build") < events.index("awards") < events.index("works-references")
    # Only the lagging table is taken up again, without a build.
    events.clear()
    again = await load_release(
        loader(events), OpenAlexLoad(release=RELEASE, bucket="raw", datasets=("awards",)),
        builder(events),
    )  # fmt: skip
    assert events == ["awards"] and again.read_tables is None
    assert tuple(again.additional) == ("awards",)
