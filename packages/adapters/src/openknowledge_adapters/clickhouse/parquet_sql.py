"""SQL that loads one archived Parquet object into a table projected from it.

The Parquet columns are typed, so a load reads only the columns it names and derives each table
column with one expression. Hive partitioning stays off: archived names keep OpenAlex's
``updated_date=<day>`` folder, which ClickHouse would otherwise read as the ``updated_date``
column in place of the file's own. Values a column cannot hold stop the insert with
``<table>: <field> breaks the contract``, as the JSON loads do, except an abstract's inverted
index: one that cannot be read loads as an empty abstract with ``index_broken`` set. The SELECT
list is positional and carries no aliases, so no alias can shadow a Parquet column of the same
name.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import cast

import pyarrow as pa

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
from openknowledge_contracts.tables.clickhouse import table_columns
from openknowledge_contracts.tables.fields import ArrowField, fields_of
from openknowledge_contracts.tables.openalex import CONTRACTS, WORKS
from openknowledge_contracts.tables.openalex.entities import ID_PREFIXES
from openknowledge_contracts.tables.openalex.fields import id_prefix

_NO_HIVE = "use_hive_partitioning = 0"
BROKEN_INDEX = "index_broken"
# 'https://openalex.org/W' is 22 characters; 19 digits always fit UInt64.
_WORK_ID = "toUInt64(substring(id, 23))"
_IS_WORK = "id IS NOT NULL AND match(id, '^https://openalex\\\\.org/W[1-9][0-9]{0,18}$')"
_IS_SOURCE = "id IS NOT NULL AND match(id, '^https://openalex\\\\.org/S[1-9][0-9]{0,18}$')"


def strip_doi(column: str) -> str:
    """``column`` lower-cased and trimmed, without a doi.org or ``doi:`` prefix."""
    return (
        f"replaceRegexpOne(lower(trimBoth(assumeNotNull({column}))), "
        "'^(https?://(dx\\\\.)?doi\\\\.org/|doi:)', '')"
    )


def _ids(name: str) -> str:
    return f"toUInt64OrNull(extract(ifNull(ids['{name}'], ''), '(\\\\d+)/?$'))"


def _id_pattern(prefix: str, expression: str) -> str:
    pattern = literal("^https://openalex[.]org/" + prefix + "[1-9][0-9]{0,18}$")
    return f"ifNull(match({expression}, {pattern}), 0)"


def _item(field: ArrowField) -> ArrowField | None:
    """The item of a list field; ``None`` for any other field."""
    type_ = field.type
    if isinstance(type_, pa.ListType):
        return cast("pa.ListType[pa.DataType]", type_).value_field
    return None


def _members(field: ArrowField) -> list[ArrowField]:
    type_ = field.type
    return fields_of(type_) if isinstance(type_, pa.StructType) else []


def _holds_ids(field: ArrowField) -> bool:
    item = _item(field)
    return (
        id_prefix(field) is not None
        or (item is not None and _holds_ids(item))
        or any(_holds_ids(member) for member in _members(field))
    )


def _stored(value: str, field: ArrowField, depth: int = 0) -> str:
    """``value`` as ``field`` stores it: OpenAlex IDs parsed, struct members it lacks dropped.

    A part without IDs is passed through whole. The check from ``_valid`` runs first, so a
    parse here never meets a malformed ID.
    """
    if id_prefix(field) is not None:
        if field.nullable:
            return f"toUInt64OrNull(substring({value}, 23))"
        return f"toUInt64OrZero(substring(assumeNotNull({value}), 23))"
    if not _holds_ids(field):
        return value
    if (item := _item(field)) is not None:
        name = f"x{depth}"
        return f"arrayMap({name} -> {_stored(name, item, depth + 1)}, {value})"
    members = (
        _stored(f"tupleElement({value}, {literal(member.name)})", member, depth)
        for member in _members(field)
    )
    return f"tuple({', '.join(members)})"


def _valid(value: str, field: ArrowField, depth: int = 0) -> str:
    """Whether every OpenAlex ID in ``value`` is a URL of its entity; null only where allowed."""
    prefix = id_prefix(field)
    if prefix is not None:
        valid = _id_pattern(prefix, value)
        return f"(isNull({value}) OR {valid})" if field.nullable else valid
    if (item := _item(field)) is not None:
        name = f"x{depth}"
        return f"arrayAll({name} -> {_valid(name, item, depth + 1)}, {value})"
    return " AND ".join(
        _valid(f"tupleElement({value}, {literal(member.name)})", member, depth)
        for member in _members(field)
        if _holds_ids(member)
    )


def _record_selects(contract: DatasetContract, fields: list[ArrowField]) -> dict[str, str]:
    """``fields`` of ``contract`` read from the Parquet columns of the same names.

    A ``has_<struct>`` flag says whether the struct was not null.
    """
    types = {column.name: column.type for column in table_columns(contract)}
    structs = {field.name for field in fields if isinstance(field.type, pa.StructType)}
    selects: dict[str, str] = {}
    for field in fields:
        value = identifier(field.name)
        if field.name.startswith("has_") and field.name[4:] in structs:
            value = identifier(field.name[4:])
            selects[field.name] = (
                f"NOT isNotDistinctFrom({value}, defaultValueOfArgumentType({value}))"
            )
        elif _holds_ids(field):
            selects[field.name] = f"CAST({_stored(value, field)}, {literal(types[field.name])})"
        else:
            selects[field.name] = value
    return selects


def _record_checks(contract: DatasetContract, fields: list[ArrowField]) -> tuple[Check, ...]:
    return tuple(
        (
            _valid(identifier(field.name), field),
            f"{contract.table}: {field.name} breaks the contract",
        )
        for field in fields
        if _holds_ids(field)
    )


# The columns of a work's own record beyond its scalar metadata, which end the works contract.
_WORK_RECORD = fields_of(WORKS.source)[
    [field.name for field in fields_of(WORKS.source)].index("updated_date") + 1 :
]


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
    # Whether OpenAlex sent an index, not parsed here, which would read every index a second
    # time. A readable index beyond these spellings of nothing rebuilds to a non-empty abstract,
    # so an empty abstract beside a true has_abstract is one the abstracts load flagged broken.
    "has_abstract": (
        "NOT match(ifNull(abstract_inverted_index, ''), '^\\\\s*(null|\\\\{\\\\s*\\\\})?\\\\s*$')"
    ),
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
# An index failing this loads as an empty abstract. Positions are checked to fit UInt32, so the
# typed extraction above cannot clamp one silently.
_IS_INDEX = (
    "match(index_json, '^\\\\s*(null)?\\\\s*$') OR (JSONType(index_json) = 'Object' "
    "AND length(JSONExtractKeys(index_json)) = length(arrayDistinct(JSONExtractKeys(index_json))) "
    "AND arrayAll(kv -> tupleElement(kv, 1) != '' AND JSONType(tupleElement(kv, 2)) = 'Array' "
    "AND length(JSONExtractArrayRaw(tupleElement(kv, 2))) > 0 "
    "AND arrayAll(p -> JSONType(p) = 'Int64' AND JSONExtractInt(p) BETWEEN 0 AND 4294967295, "
    "JSONExtractArrayRaw(tupleElement(kv, 2))), JSONExtractKeysAndValuesRaw(index_json)))"
)

_PROJECTED: Mapping[DatasetName, Mapping[str, str]] = MappingProxyType(
    {
        "works": MappingProxyType(_WORKS | _record_selects(WORKS, _WORK_RECORD)),
        "works-abstracts": MappingProxyType(
            {
                "work_id": _WORK_ID,
                "abstract": f"if(index_ok, {_ABSTRACT}, '')",
                BROKEN_INDEX: "NOT index_ok",
            }
        ),
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
        "works": ((strip_doi("doi"), "doi_stripped"),),
        "works-abstracts": (
            ("ifNull(abstract_inverted_index, '')", "index_json"),
            (_IS_INDEX, "index_ok"),
        ),
    }
)

_PROJECTED_CHECKS: Mapping[DatasetName, tuple[Check, ...]] = MappingProxyType(
    {
        "works": (
            (_IS_WORK, "openalex_works: id breaks the contract"),
            *(
                (f"{name} IS NOT NULL", f"openalex_works: {name} breaks the contract")
                for name in ("is_xpac", "is_paratext", "updated_date")
            ),
            *_record_checks(WORKS, _WORK_RECORD),
        ),
        "works-abstracts": ((_IS_WORK, "openalex_abstracts: id breaks the contract"),),
        "sources": (
            (_IS_SOURCE, "openalex_sources: id breaks the contract"),
            ("updated_date IS NOT NULL", "openalex_sources: updated_date breaks the contract"),
        ),
    }
)


_FULL_RECORDS = tuple(contract for name, contract in CONTRACTS.items() if name not in _PROJECTED)

# Bound both the reader's prefetch and the blocks buffered for sorting and insertion.
_WIDE_READ_SETTINGS = "max_threads = 1, max_memory_usage = 6000000000"
_WIDE_SETTINGS = (
    f"{_WIDE_READ_SETTINGS}, max_download_threads = 1, "
    "input_format_parquet_max_block_size = 8192, "
    "input_format_parquet_prefer_block_bytes = 8388608, "
    "input_format_parquet_memory_high_watermark = 268435456"
)
# Blocks, and so parts, close at 256 MB of data, whatever the row count; a wide table's insert
# peaks near 2.3 GB with them.
_WIDE_INSERT_SETTINGS = (
    "max_insert_threads = 1, min_insert_block_size_rows = 0, "
    "min_insert_block_size_bytes = 268435456"
)


def _wide(contract: DatasetContract) -> bool:
    if contract.name == "works":
        return True
    return contract in _FULL_RECORDS and contract.archived in {"works", "authors", "awards"}


def wide_read_settings(contract: DatasetContract) -> str:
    """Return a trailing SETTINGS clause for reads of wide contracts."""
    return f"\nSETTINGS {_WIDE_READ_SETTINGS}" if _wide(contract) else ""


def _full_projection(contract: DatasetContract) -> dict[str, str]:
    selects = _record_selects(contract, fields_of(contract.source))
    if contract.key != ("id",):
        selects[contract.key[0]] = _WORK_ID
    return selects


def _full_checks(contract: DatasetContract) -> tuple[Check, ...]:
    prefix = "W" if contract.archived == "works" else ID_PREFIXES.get(contract.archived)
    key = _id_pattern(prefix, "id") if prefix else "notEmpty(ifNull(id, ''))"
    return (
        (key, f"{contract.table}: id breaks the contract"),
        *_record_checks(contract, fields_of(contract.source)),
    )


SELECTS: Mapping[DatasetName, Mapping[str, str]] = MappingProxyType(
    dict(_PROJECTED) | {c.name: MappingProxyType(_full_projection(c)) for c in _FULL_RECORDS}
)
CHECKS: Mapping[DatasetName, tuple[Check, ...]] = MappingProxyType(
    dict(_PROJECTED_CHECKS) | {c.name: _full_checks(c) for c in _FULL_RECORDS}
)


def _no_subcolumns(contract: DatasetContract) -> str:
    """Read whole structs where IDs are parsed out of them.

    ClickHouse 26.8 otherwise reads a struct member as a Parquet subcolumn beside the whole
    struct and fails with NOT_FOUND_COLUMN_IN_BLOCK.
    """
    if any(_holds_ids(field) for field in fields_of(contract.source)):
        return ", optimize_functions_to_subcolumns = 0"
    return ""


def parquet_upsert_query(
    contract: DatasetContract, source: Source = "s3", into: str | None = None
) -> str:
    """Insert every row of one Parquet object as a current row of ``contract.table``.

    ``into`` names the staging copy to insert into instead; the receipts hash the query without.
    """
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
    settings = f"insert_null_as_default = 0, {_NO_HIVE}{_no_subcolumns(contract)}"
    if _wide(contract):
        settings += f", {_WIDE_SETTINGS}, {_WIDE_INSERT_SETTINGS}"
    return (
        f"INSERT INTO {identifier(into or contract.table)} ({columns})\n"
        f"SETTINGS {settings}\n"
        f"{_with(contract.name)}"
        f"SELECT\n    {selected}\n"
        f"FROM {_source(source)}\n"
        f"WHERE {where}"
    )


def broken_indexes_query(contract: DatasetContract) -> str | None:
    """The rows of ``release`` in ``table`` loaded from a broken index, where they are flagged."""
    if BROKEN_INDEX not in contract.source.names:
        return None
    return (
        f"SELECT countIf({BROKEN_INDEX}) AS rows FROM {{table:Identifier}} FINAL "
        "WHERE source_release = {release:Date} AND NOT removed"
        f"{wide_read_settings(contract)}"
    )


def parquet_duplicates_query(contract: DatasetContract, source: Source = "s3") -> str:
    """Keys that repeat across the objects ``source_files`` names, as one glob.

    Rows repeat identically when every stored value is the same. One read of all objects keeps
    the plan small for thousands of them, where one read per object exceeds ClickHouse's
    limit on plan optimizations. The repeated keys are found from the key column alone; only
    their rows are hashed whole, so a release's abstracts are not rebuilt again. A rerun of
    the same query, under the same query id, replaces one its client stopped waiting for.
    """
    selects = SELECTS[contract.name]
    key = f"tuple({', '.join(selects[name] for name in contract.key)})"
    files = _source(source, "source_files")
    records = (
        f"{_with(contract.name)}SELECT {key} AS record_key, "
        f"cityHash64(tuple({', '.join(selects.values())})) AS record_hash\n"
        f"FROM {files}\n"
        f"WHERE {key} IN (SELECT {key} FROM {files} GROUP BY 1 HAVING count() > 1)"
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
        f"FROM (\n{keys}\n)\n"
        f"SETTINGS {_NO_HIVE}, max_bytes_ratio_before_external_group_by = 0.5, "
        f"replace_running_query = 1{_no_subcolumns(contract)}"
        + (f", {_WIDE_SETTINGS}" if _wide(contract) else "")
    )


def parquet_duplicates_parameters(source_files: Sequence[str]) -> dict[str, str]:
    """``source_files`` as one glob: their common folder, then each rest as an alternative.

    S3 lists only under that folder, and the alternatives match exactly these objects.
    """
    if not source_files or any(set(name) & set("{},*?[]\\") for name in source_files):
        raise ValueError("Parquet duplicates need object names without glob characters")
    folder = posixpath.commonpath([name.rpartition("/")[0] for name in source_files]) + "/"
    rests = ",".join(name.removeprefix(folder) for name in source_files)
    return {"source_files": f"{folder}{{{rests}}}"}


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
