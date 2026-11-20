"""OpenAlex tables, each projected from an archived Parquet entity.

Unlike the Semantic Scholar contracts, these describe the stored projection, not the delivered
record: the load derives each column from the Parquet columns in
``openknowledge_adapters.clickhouse.parquet_sql``.
"""

from collections.abc import Mapping
from types import MappingProxyType

from openknowledge_contracts.datasets import DatasetName
from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.openalex.abstracts import WORKS_ABSTRACTS
from openknowledge_contracts.tables.openalex.entities import ENTITIES, SOURCES_FULL
from openknowledge_contracts.tables.openalex.sources import OPENALEX_SOURCES
from openknowledge_contracts.tables.openalex.work_projections import WORK_PROJECTIONS
from openknowledge_contracts.tables.openalex.works import WORKS

_WIDE = {"openalex-authors", "awards"}

CONTRACTS: Mapping[DatasetName, DatasetContract] = MappingProxyType(
    {
        contract.name: contract
        for contract in (
            OPENALEX_SOURCES,
            WORKS,
            WORKS_ABSTRACTS,
            SOURCES_FULL,
            *(contract for contract in ENTITIES if contract.name not in _WIDE),
            *(contract for contract in ENTITIES if contract.name in _WIDE),
            *WORK_PROJECTIONS,
        )
    }
)

__all__ = ["CONTRACTS", "OPENALEX_SOURCES", "WORKS", "WORKS_ABSTRACTS"]
