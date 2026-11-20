"""One OpenAlex release into every OpenAlex table, each through the dataset load flow."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import NamedTuple

from openknowledge_contracts.datasets import DatasetName, ReleaseId
from openknowledge_contracts.tables.openalex import CONTRACTS
from openknowledge_core.ingestion import (
    LoadDataset,
    LoadDatasetResult,
    OpenAlexLoad,
    OpenAlexLoadResult,
)
from openknowledge_core.ingestion.errors import IngestionError
from openknowledge_core.ingestion.read_tables import BuildReadTablesResult

log = logging.getLogger(__name__)

CORE: tuple[DatasetName, ...] = ("sources", "works", "works-abstracts")
"""The tables the read tables are built from. The other tables load after the build."""


class ReleaseState(NamedTuple):
    """Where OpenAlex's tables stand against one release."""

    behind: tuple[DatasetName, ...]
    """The tables that do not hold the release, or hold it from SQL they no longer load with."""
    built: bool
    """Whether the read tables were built from the release."""

    @property
    def current(self) -> bool:
        return not self.behind and self.built


async def load_release(
    load: Callable[[LoadDataset], Awaitable[LoadDatasetResult]],
    command: OpenAlexLoad,
    build: Callable[[], Awaitable[BuildReadTablesResult]] | None = None,
    state: Callable[[ReleaseId], Awaitable[ReleaseState]] | None = None,
) -> OpenAlexLoadResult:
    """The core tables, then ``build`` if one of them was loaded, then the other tables.

    A core table that fails stops the run. Another table that fails leaves the core tables and
    the build standing and the tables after it loading; the run fails at the end, naming each.
    Each table takes the release only once all its objects loaded.

    With ``state``, read once the caller holds the load limit, only the tables behind the
    release load, and the build also runs when the read tables were not built from it; so a
    run queued behind another that took the release does nothing.
    """

    def dataset(name: str) -> LoadDataset:
        return LoadDataset(
            release=command.release, dataset=name, bucket=command.bucket, prefix=command.prefix
        )

    names = [name for name in CONTRACTS if not command.datasets or name in command.datasets]
    unbuilt = False
    if state is not None:
        found = await state(command.release)
        names = [name for name in names if name in found.behind]
        unbuilt = not found.built
    core = {name: await load(dataset(name)) for name in names if name in CORE}
    result = OpenAlexLoadResult(
        sources=core.get("sources"), works=core.get("works"), abstracts=core.get("works-abstracts")
    )
    if (core or unbuilt) and build is not None:
        result = await _build(build, command.release, result)
    additional: dict[DatasetName, LoadDatasetResult] = {}
    failed: dict[DatasetName, str] = {}
    for name in names:
        if name in CORE:
            continue
        try:
            additional[name] = await load(dataset(name))
        except IngestionError as exc:
            log.warning("OpenAlex %s did not load into %s: %s", command.release, name, exc)
            failed[name] = str(exc)
    if failed:
        reasons = "; ".join(f"{name}: {reason}" for name, reason in failed.items())
        raise IngestionError(f"OpenAlex {command.release} did not load into {reasons}")
    return result.model_copy(update={"additional": additional})


async def _build(
    build: Callable[[], Awaitable[BuildReadTablesResult]],
    release: ReleaseId,
    loaded: OpenAlexLoadResult,
) -> OpenAlexLoadResult:
    """Bring the read tables current, as an S2 sync does after its loads.

    A failed build leaves the release applied; the next release check builds from it.
    """
    try:
        built = await build()
    except IngestionError as exc:
        log.warning("read tables were not built after OpenAlex %s: %s", release, exc)
        return loaded.model_copy(update={"read_tables_failure": str(exc)})
    return loaded.model_copy(update={"read_tables": built})


async def check_release(
    latest: Callable[[], Awaitable[ReleaseId]],
    state: Callable[[ReleaseId], Awaitable[ReleaseState]],
    take: Callable[[ReleaseId], Awaitable[OpenAlexLoadResult]],
) -> OpenAlexLoadResult | None:
    """Take OpenAlex's release into the tables behind it, and build from it; then start nothing.

    ``take`` reads the state again under the load limit. Archive and load resume from their
    receipts, so a release whose archive, load or build failed is taken up again, and only the
    missing objects are downloaded or loaded.
    """
    release = await latest()
    found = await state(release)
    if found.current:
        log.info("OpenAlex release %s is loaded and built already", release)
        return None
    log.info(
        "OpenAlex release %s: behind in %s; read tables built: %s",
        release,
        ", ".join(found.behind) or "no table",
        found.built,
    )
    return await take(release)
