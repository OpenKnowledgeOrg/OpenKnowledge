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
    """Each table takes the release only after all its objects loaded, one table after another."""

    def dataset(name: str) -> LoadDataset:
        return LoadDataset(
            release=command.release, dataset=name, bucket=command.bucket, prefix=command.prefix
        )

    works = await load(dataset("works"))
    abstracts = await load(dataset("works-abstracts"))
    return OpenAlexLoadResult(works=works, abstracts=abstracts)
