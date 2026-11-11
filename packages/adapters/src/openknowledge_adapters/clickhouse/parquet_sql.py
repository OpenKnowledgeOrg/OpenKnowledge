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
    EXAMPLES,
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
_IS_SOURCE = "id IS NOT NULL AND match(id, '^https://openalex\\\\.org/S[1-9][0-9]{0,18}$')"


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

# Each word paired with each of its positions, sorted by position, then by word bytes; gaps close.
_ABSTRACT = (
    "arrayStringConcat(arrayMap(t -> tupleElement(t, 2), arraySort(arrayFlatten(arrayMap("
    "kv -> arrayMap(p -> (p, tupleElement(kv, 1)), tupleElement(kv, 2)), "
    "JSONExtractKeysAndValues(index_json, 'Array(UInt32)'))))), ' ')"
)
# Positions are checked to fit UInt32, so the typed extraction above cannot clamp one silently.
_IS_INDEX = (
    "index_json IN ('', 'null') OR (JSONType(index_json) = 'Object' "
    "AND length(JSONExtractKeys(index_json)) = length(arrayDistinct(JSONExtractKeys(index_json))) "
    "AND arrayAll(kv -> JSONType(tupleElement(kv, 2)) = 'Array' "
    "AND arrayAll(p -> JSONType(p) = 'Int64' AND JSONExtractInt(p) BETWEEN 0 AND 4294967295, "
    "JSONExtractArrayRaw(tupleElement(kv, 2))), JSONExtractKeysAndValuesRaw(index_json)))"
)

SELECTS: Mapping[DatasetName, Mapping[str, str]] = MappingProxyType(
    {
        "works": MappingProxyType(_WORKS),
        "works-abstracts": MappingProxyType({"work_id": _WORK_ID, "abstract": _ABSTRACT}),
        "sources": MappingProxyType(
            {
                "source_id": "toUInt64(substring(id, 23))",
                **{
                    name: name
                    for name in (
                        "display_name",
                        "type",
                        "issn_l",
                        "host_organization",
                        "host_organization_name",
                        "homepage_url",
                        "is_oa",
                        "is_in_doaj",
                        "is_core",
                        "country_code",
                    )
                },
                "updated_date": "toDateTime(updated_date, 'UTC')",
            }
        ),
    }
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
        "works-abstracts": (("ifNull(abstract_inverted_index, '')", "index_json"),),
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
        "works-abstracts": (
            (_IS_WORK, "openalex_abstracts: id breaks the contract"),
            (_IS_INDEX, "openalex_abstracts: abstract_inverted_index breaks the contract"),
        ),
        "sources": (
            (_IS_SOURCE, "openalex_sources: id breaks the contract"),
            ("updated_date IS NOT NULL", "openalex_sources: updated_date breaks the contract"),
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


def parquet_duplicates_query(contract: DatasetContract, objects: int, source: Source = "s3") -> str:
    """Keys that repeat across the objects ``source_file_0`` to ``source_file_<objects - 1>``.

    Rows repeat identically when every stored value is the same. Each object is read with
    ``SELECT *`` so the query stays short for thousands of objects; ClickHouse still reads only
    the Parquet columns the hash uses.
    """
    selects = SELECTS[contract.name]
    key = ", ".join(selects[name] for name in contract.key)
    files = "\n    UNION ALL\n    ".join(
        f"SELECT * FROM {_source(source, f'source_file_{index}')}" for index in range(objects)
    )
    records = (
        f"{_with(contract.name)}SELECT tuple({key}) AS record_key, "
        f"cityHash64(tuple({', '.join(selects.values())})) AS record_hash\n"
        f"FROM (\n    {files}\n)"
    )
    keys = (
        "SELECT record_key, count() AS n, min(record_hash) != max(record_hash) AS conflicting\n"
        f"FROM (\n{records}\n)\nGROUP BY record_key\nHAVING n > 1"
    )
    return (
        "SELECT\n"
        "    sumIf(n - 1, NOT conflicting) AS identical_rows,\n"
        "    countIf(conflicting) AS conflicting_keys,\n"
        f"    groupArrayIf({EXAMPLES})(toString(record_key), conflicting) AS examples\n"
        f"FROM (\n{keys}\n)"
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
