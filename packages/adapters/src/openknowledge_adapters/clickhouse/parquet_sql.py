"""SQL that loads one archived Parquet object into a table projected from it.

The Parquet columns are typed, so a load reads only the columns it names and derives each table
column with one expression. Values a column cannot hold stop the insert with
``<table>: <field> breaks the contract``, as the JSON loads do. The SELECT list is positional and
carries no aliases, so no alias can shadow a Parquet column of the same name.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from openknowledge_adapters.clickhouse.load_sql import (
    Check,
    Source,
    identifier,
    literal,
    provenance,
)
from openknowledge_contracts.datasets import DatasetName
from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.fields import fields_of

# 'https://openalex.org/W' is 22 characters; 19 digits always fit UInt64.
_WORK_ID = "toUInt64(substring(id, 23))"
_IS_WORK = "id IS NOT NULL AND match(id, '^https://openalex\\\\.org/W[1-9][0-9]{0,18}$')"


def _ids(name: str) -> str:
    return f"toUInt64OrNull(extract(ifNull(ids['{name}'], ''), '(\\\\d+)/?$'))"


_WORKS: dict[str, str] = {
    "work_id": _WORK_ID,
    "doi": "doi",
    "doi_norm": ("if(doi IS NULL, NULL, if(startsWith(doi_stripped, '10.'), doi_stripped, NULL))"),
    "pmid": _ids("pmid"),
    "mag": _ids("mag"),
    "title": "title",
    "publication_year": "accurateCastOrNull(publication_year, 'UInt16')",
    "type": "type",
    "language": "language",
    "is_xpac": "is_xpac",
    "is_retracted": "is_retracted",
    "is_paratext": "is_paratext",
    # The abstracts load checks the index; parsing it here too would read it a second time.
    "has_abstract": "ifNull(abstract_inverted_index, '') NOT IN ('', 'null', '{}')",
    "oa_status": "tupleElement(open_access, 'oa_status')",
    "is_oa": "tupleElement(open_access, 'is_oa')",
    "oa_url": "tupleElement(open_access, 'oa_url')",
    "any_repository_has_fulltext": "tupleElement(open_access, 'any_repository_has_fulltext')",
    "best_oa_pdf_url": "tupleElement(best_oa_location, 'pdf_url')",
    "best_oa_landing_page_url": "tupleElement(best_oa_location, 'landing_page_url')",
    "best_oa_license": "tupleElement(best_oa_location, 'license')",
    "best_oa_version": "tupleElement(best_oa_location, 'version')",
    "best_oa_source_id": "tupleElement(tupleElement(best_oa_location, 'source'), 'id')",
    "primary_landing_page_url": "tupleElement(primary_location, 'landing_page_url')",
    "primary_source_id": "tupleElement(tupleElement(primary_location, 'source'), 'id')",
    "has_pdf": "tupleElement(has_content, 'pdf')",
    "has_grobid_xml": "tupleElement(has_content, 'grobid_xml')",
    "has_fulltext": "has_fulltext",
    "updated_date": "toDateTime(updated_date, 'UTC')",
}

SELECTS: Mapping[DatasetName, Mapping[str, str]] = MappingProxyType(
    {"works": MappingProxyType(_WORKS)}
)

_WITH: Mapping[DatasetName, tuple[tuple[str, str], ...]] = MappingProxyType(
    {
        "works": (
            (
                "replaceRegexpOne(lower(trimBoth(assumeNotNull(doi))), "
                "'^(https?://(dx\\\\.)?doi\\\\.org/|doi:)', '')",
                "doi_stripped",
            ),
        ),
    }
)

CHECKS: Mapping[DatasetName, tuple[Check, ...]] = MappingProxyType(
    {
        "works": (
            (_IS_WORK, "openalex_works: id breaks the contract"),
            *(
                (f"{name} IS NOT NULL", f"openalex_works: {name} breaks the contract")
                for name in ("is_xpac", "is_retracted", "is_paratext", "updated_date")
            ),
        ),
    }
)


def parquet_upsert_query(contract: DatasetContract, source: Source = "s3") -> str:
    """Insert every row of one Parquet object as a current row of ``contract.table``."""
    selects = SELECTS[contract.name]
    names = [field.name for field in fields_of(contract.source)]
    if list(selects) != names:
        raise ValueError(f"{contract.name}: the Parquet projection does not match the contract")
    values = [*selects.items(), *provenance(0)]
    columns = ", ".join(identifier(name) for name, _ in values)
    selected = ",\n    ".join(expression for _, expression in values)
    where = "\n    AND ".join(
        f"NOT throwIf(NOT ({condition}), {literal(message)})"
        for condition, message in CHECKS[contract.name]
    )
    return (
        f"INSERT INTO {identifier(contract.table)} ({columns})\n"
        "SETTINGS insert_null_as_default = 0\n"
        f"{_with(contract.name)}"
        f"SELECT\n    {selected}\n"
        f"FROM {_source(source)}\n"
        f"WHERE {where}"
    )


def _with(dataset: DatasetName) -> str:
    aliases = _WITH.get(dataset, ())
    if not aliases:
        return ""
    return "WITH\n    " + ",\n    ".join(f"{e} AS {a}" for e, a in aliases) + "\n"


def _source(source: Source, parameter: str = "source_file") -> str:
    name = "{" + parameter + ":String}"
    if source == "s3":
        return f"s3(raw, filename = {name}, format = 'Parquet')"
    return f"file({name}, 'Parquet')"
