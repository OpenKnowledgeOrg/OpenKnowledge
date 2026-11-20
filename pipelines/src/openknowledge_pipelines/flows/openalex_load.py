"""One OpenAlex release into every OpenAlex table, each through the dataset load flow."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from openknowledge_contracts.datasets import ReleaseId
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


async def load_release(
    load: Callable[[LoadDataset], Awaitable[LoadDatasetResult]], command: OpenAlexLoad
) -> OpenAlexLoadResult:
    """One table after another, sources first since it takes seconds.

    Each table takes the release only once all its objects loaded.
    """

    def dataset(name: str) -> LoadDataset:
        return LoadDataset(
            release=command.release, dataset=name, bucket=command.bucket, prefix=command.prefix
        )

    results = {
        name: await load(dataset(name))
        for name in CONTRACTS
        if not command.datasets or name in command.datasets
    }
    return OpenAlexLoadResult(
        sources=results.pop("sources", None),
        works=results.pop("works", None),
        abstracts=results.pop("works-abstracts", None),
        additional=results,
    )


async def load_then_build(
    load: Callable[[LoadDataset], Awaitable[LoadDatasetResult]],
    build: Callable[[], Awaitable[BuildReadTablesResult]],
    command: OpenAlexLoad,
) -> OpenAlexLoadResult:
    """Load the release, then bring the read tables current, as an S2 sync does after its loads.

    A failed build leaves the release applied; the next sync builds the read tables from it.
    """
    loaded = await load_release(load, command)
    try:
        built = await build()
    except IngestionError as exc:
        log.warning("read tables were not built after OpenAlex %s: %s", command.release, exc)
        return loaded.model_copy(update={"read_tables_failure": str(exc)})
    return loaded.model_copy(update={"read_tables": built})


async def check_release(
    latest: Callable[[], Awaitable[ReleaseId]],
    loaded: Callable[[ReleaseId], Awaitable[bool]],
    take: Callable[[ReleaseId], Awaitable[OpenAlexLoadResult]],
) -> OpenAlexLoadResult | None:
    """Take OpenAlex's release in until every OpenAlex table holds it; then start nothing.

    Archive and load resume from their receipts, so a release whose archive or load failed is
    taken up again, and only the missing objects are downloaded or loaded.
    """
    release = await latest()
    if await loaded(release):
        log.info("OpenAlex release %s is loaded already", release)
        return None
    return await take(release)
