"""The sources the Hub ingests, each with its archived datasets and the contracts it loads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from openknowledge_contracts.datasets import DatasetName
from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.openalex import CONTRACTS as OPENALEX_CONTRACTS
from openknowledge_contracts.tables.s2 import CONTRACTS as S2_CONTRACTS

type SourceName = Literal["semantic-scholar", "openalex"]
type ObjectFormat = Literal["jsonl-gz", "parquet"]


@dataclass(frozen=True)
class ArchivedDataset:
    """How a source dataset is stored in the archive.

    Attributes:
        format: The format of its objects.
        folders: Trailing source-path folders kept in the archive name.
    """

    format: ObjectFormat
    folders: int = 0


@dataclass(frozen=True)
class Source:
    """One source: its bucket prefix, archived datasets and the contracts loaded from them."""

    name: SourceName
    prefix: str
    datasets: Mapping[DatasetName, ArchivedDataset]
    contracts: Mapping[DatasetName, DatasetContract]


_S2_DATASETS = (*S2_CONTRACTS, "s2orc_v2", "embeddings-specter_v2")

# Every entity of the OpenAlex manifest is archived, since only its latest release stays online.
_OPENALEX_ENTITIES = (
    "works",
    "authors",
    "institutions",
    "sources",
    "publishers",
    "funders",
    "topics",
    "subfields",
    "fields",
    "domains",
    "concepts",
    "keywords",
    "awards",
    "continents",
    "countries",
    "institution-types",
    "languages",
    "licenses",
    "sdgs",
    "source-types",
    "work-types",
)

SOURCES: Mapping[SourceName, Source] = MappingProxyType(
    {
        "semantic-scholar": Source(
            name="semantic-scholar",
            prefix="semantic-scholar",
            datasets=MappingProxyType({name: ArchivedDataset("jsonl-gz") for name in _S2_DATASETS}),
            contracts=S2_CONTRACTS,
        ),
        "openalex": Source(
            name="openalex",
            prefix="openalex",
            datasets=MappingProxyType(
                {name: ArchivedDataset("parquet", folders=1) for name in _OPENALEX_ENTITIES}
                | {"works-deleted": ArchivedDataset("jsonl-gz")}
            ),
            contracts=OPENALEX_CONTRACTS,
        ),
    }
)


def check_sources(sources: Mapping[SourceName, Source]) -> None:
    seen: set[DatasetName] = set()
    for source in sources.values():
        for name, contract in source.contracts.items():
            if name in seen:
                raise ValueError(f"contract {name!r} appears in more than one source")
            seen.add(name)
            if contract.archived not in source.datasets:
                raise ValueError(
                    f"{source.name}: {name!r} reads {contract.archived!r}, which is not archived"
                )


check_sources(SOURCES)


def contract_for(name: DatasetName) -> tuple[Source, DatasetContract]:
    """The source and contract of a dataset; raises ``KeyError`` for an unknown name."""
    for source in SOURCES.values():
        if name in source.contracts:
            return source, source.contracts[name]
    raise KeyError(name)


def all_contracts() -> tuple[DatasetContract, ...]:
    """Every contract of every source."""
    return tuple(contract for source in SOURCES.values() for contract in source.contracts.values())
