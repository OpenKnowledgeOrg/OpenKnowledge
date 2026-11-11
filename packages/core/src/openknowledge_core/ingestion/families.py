"""Archive routing shared by controllers and Batch workers."""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType

from openknowledge_contracts.sources import SOURCES, SourceName
from openknowledge_contracts.tables.s2 import CONTRACTS


class ArchiveFamily(StrEnum):
    CORE = "core"
    FULLTEXT = "fulltext"
    EMBEDDINGS = "embeddings"
    OPENALEX = "openalex"

    @property
    def source(self) -> SourceName:
        return "openalex" if self is ArchiveFamily.OPENALEX else "semantic-scholar"


ARCHIVE_DATASETS: Mapping[str, ArchiveFamily] = MappingProxyType(
    {
        **dict.fromkeys(CONTRACTS, ArchiveFamily.CORE),
        "s2orc_v2": ArchiveFamily.FULLTEXT,
        "embeddings-specter_v2": ArchiveFamily.EMBEDDINGS,
    }
)


_FAMILIES: Mapping[SourceName, Mapping[str, ArchiveFamily]] = MappingProxyType(
    {
        "semantic-scholar": ARCHIVE_DATASETS,
        "openalex": MappingProxyType(
            dict.fromkeys(SOURCES["openalex"].datasets, ArchiveFamily.OPENALEX)
        ),
    }
)


def archive_family(dataset: str, source: SourceName = "semantic-scholar") -> ArchiveFamily:
    """A dataset name is unique only within its source, so the source decides."""
    try:
        return _FAMILIES[source][dataset]
    except KeyError:
        raise ValueError(f"Unsupported archive dataset: {dataset}") from None


def require_family(dataset: str, family: ArchiveFamily) -> None:
    actual = archive_family(dataset, family.source)
    if actual != family:
        raise ValueError(f"Dataset {dataset} belongs to {actual}, not {family}")
