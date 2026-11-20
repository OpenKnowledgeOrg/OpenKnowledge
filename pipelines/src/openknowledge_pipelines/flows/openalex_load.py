"""One OpenAlex release into every OpenAlex table, each through the dataset load flow."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

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


async def load_release(
    load: Callable[[LoadDataset], Awaitable[LoadDatasetResult]],
    command: OpenAlexLoad,
    build: Callable[[], Awaitable[BuildReadTablesResult]] | None = None,
) -> OpenAlexLoadResult:
    """The core tables, then ``build`` if one of them was loaded, then the other tables.

    A core table that fails stops the run. Another table that fails leaves the core tables and
    the build standing and the tables after it loading; the run fails at the end, naming each.
    Each table takes the release only once all its objects loaded.
    """

    def dataset(name: str) -> LoadDataset:
        return LoadDataset(
            release=command.release, dataset=name, bucket=command.bucket, prefix=command.prefix
        )

    names = [name for name in CONTRACTS if not command.datasets or name in command.datasets]
    core = {name: await load(dataset(name)) for name in names if name in CORE}
    result = OpenAlexLoadResult(
        sources=core.get("sources"), works=core.get("works"), abstracts=core.get("works-abstracts")
    )
    if core and build is not None:
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

    A failed build leaves the release applied; the next sync builds the read tables from it.
    """
    try:
        built = await build()
    except IngestionError as exc:
        log.warning("read tables were not built after OpenAlex %s: %s", release, exc)
        return loaded.model_copy(update={"read_tables_failure": str(exc)})
    return loaded.model_copy(update={"read_tables": built})


async def check_release(
    latest: Callable[[], Awaitable[ReleaseId]],
    lagging: Callable[[ReleaseId], Awaitable[tuple[DatasetName, ...]]],
    take: Callable[[ReleaseId, tuple[DatasetName, ...]], Awaitable[OpenAlexLoadResult]],
) -> OpenAlexLoadResult | None:
    """Take OpenAlex's release into the tables that do not hold it yet; then start nothing.

    Only a lagging core table brings a build. Archive and load resume from their receipts, so
    a release whose archive or load failed is taken up again, and only the missing objects are
    downloaded or loaded.
    """
    release = await latest()
    behind = await lagging(release)
    if not behind:
        log.info("OpenAlex release %s is loaded already", release)
        return None
    log.info("OpenAlex release %s is not loaded into %s yet", release, ", ".join(behind))
    return await take(release, behind)
