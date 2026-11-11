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
from openknowledge_contracts.tables.openalex.works import WORKS

CONTRACTS: Mapping[DatasetName, DatasetContract] = MappingProxyType(
    {contract.name: contract for contract in (WORKS, WORKS_ABSTRACTS)}
)

__all__ = ["CONTRACTS", "WORKS", "WORKS_ABSTRACTS"]
