"""One OpenAlex release into every OpenAlex table, each through the dataset load flow."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from openknowledge_core.ingestion import (
    LoadDataset,
    LoadDatasetResult,
    OpenAlexLoad,
    OpenAlexLoadResult,
)


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

    sources = await load(dataset("sources"))
    works = await load(dataset("works"))
    abstracts = await load(dataset("works-abstracts"))
    return OpenAlexLoadResult(sources=sources, works=works, abstracts=abstracts)
