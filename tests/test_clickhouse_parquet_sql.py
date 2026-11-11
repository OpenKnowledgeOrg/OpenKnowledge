"""The Parquet load SQL, run with ``clickhouse local`` on files in OpenAlex's own Arrow types.

The types copy the footer of the 2026-10-14 works files; a fixture names only the columns the
load reads, since ClickHouse reads no other.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openknowledge_adapters.clickhouse.load_sql import query_parameters
from openknowledge_adapters.clickhouse.parquet_sql import SELECTS, parquet_upsert_query
from openknowledge_contracts.sources import all_contracts
from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.clickhouse import table_columns
from openknowledge_contracts.tables.fields import fields_of
from openknowledge_contracts.tables.openalex import WORKS, WORKS_ABSTRACTS
from tests.clickhouse_local import clickhouse_local, needs_clickhouse, run, up

RELEASE = "2026-10-14"

_S, _B, _LIST = pa.string(), pa.bool_(), pa.list_(pa.string())
_SOURCE = pa.struct(
    [
        ("id", _S), ("display_name", _S), ("issn_l", _S), ("issn", _LIST), ("is_oa", _B),
        ("is_in_doaj", _B), ("is_core", _B), ("listed_in", _LIST), ("host_organization", _S),
        ("host_organization_name", _S), ("host_organization_lineage", _LIST),
        ("host_organization_lineage_names", _LIST), ("type", _S),
    ]
)  # fmt: skip
_LOCATION = pa.struct(
    [
        ("id", _S), ("doi", _S), ("source", _SOURCE), ("is_oa", _B), ("is_published", _B),
        ("landing_page_url", _S), ("pdf_url", _S), ("raw_source_name", _S), ("raw_type", _S),
        ("provenance", _S), ("license", _S), ("license_id", _S), ("version", _S),
        ("is_accepted", _B),
    ]
)  # fmt: skip
WORKS_FILE = pa.schema(
    [
        ("id", _S),
        ("doi", _S),
        ("title", _S),
        ("ids", pa.map_(_S, _S)),
        ("publication_year", pa.int32()),
        ("language", _S),
        ("type", _S),
        ("primary_location", _LOCATION),
        ("best_oa_location", _LOCATION),
        ("open_access", pa.struct([("is_oa", _B), ("oa_status", _S),
                                   ("any_repository_has_fulltext", _B), ("oa_url", _S)])),
        ("is_paratext", _B),
        ("is_retracted", _B),
        ("is_xpac", _B),
        ("abstract_inverted_index", _S),
        ("has_content", pa.struct([("pdf", _B), ("grobid_xml", _B)])),
        ("has_fulltext", _B),
        ("updated_date", pa.timestamp("us", tz="UTC")),
    ]
)  # fmt: skip

FULL_LOCATION = {
    "landing_page_url": "https://example.org/landing",
    "pdf_url": "https://example.org/a.pdf",
    "license": "cc-by",
    "version": "publishedVersion",
    "source": {"id": "https://openalex.org/S123", "issn": ["1234-5678"]},
}


def work(
    id: str | None,
    *,
    doi: str | None = "https://doi.org/10.1/x",
    title: str = "A title",
    ids: dict[str, str] | None = None,
    updated_date: str = "2026-10-01T00:00:00",
    best_oa: dict[str, Any] | None = FULL_LOCATION,
    has_content: tuple[bool, bool] | None = (False, False),
    abstract: str | None = None,
) -> dict[str, Any]:
    return {
        "id": id,
        "doi": doi,
        "title": title,
        "ids": list((ids or {}).items()),
        "publication_year": 2020,
        "language": "en",
        "type": "article",
        "primary_location": best_oa,
        "best_oa_location": best_oa,
        "open_access": {"is_oa": best_oa is not None, "oa_status": "gold"},
        "is_paratext": False,
        "is_retracted": False,
        "is_xpac": False,
        "abstract_inverted_index": abstract,
        "has_content": None
        if has_content is None
        else {"pdf": has_content[0], "grobid_xml": has_content[1]},
        "has_fulltext": False,
        "updated_date": datetime.fromisoformat(updated_date).replace(tzinfo=UTC),
    }


def write_works(path: Path, rows: list[dict[str, Any]]) -> None:
    table = pa.Table.from_pylist(rows, schema=WORKS_FILE)
    pq.write_table(table, path)  # pyright: ignore[reportUnknownMemberType]


def params(path: Path, release: str = RELEASE) -> dict[str, str]:
    return query_parameters(
        source_file=str(path), release=release, source_object=f"gs://raw/openalex/{path.name}"
    )


def load_and_select(
    workdir: Path, contract: DatasetContract, name: str
) -> dict[str, dict[str, Any]]:
    """Every row loaded from ``name``, by work ID as text, with times printed in UTC."""
    columns = ", ".join(c.name for c in table_columns(contract) if not c.materialized)
    sql = f"""
        SET session_timezone = 'UTC';
        {up()}
        {parquet_upsert_query(contract, "file")};
        SELECT {columns} FROM {contract.table} ORDER BY work_id FORMAT JSONEachRow;
    """
    rows = clickhouse_local(sql, workdir / "db", **params(workdir / name))
    return {str(row["work_id"]): row for row in rows}


def test_each_parquet_projection_fills_its_contract_in_order() -> None:
    for contract in all_contracts():
        if contract.name in SELECTS:
            assert list(SELECTS[contract.name]) == [f.name for f in fields_of(contract.source)]


@needs_clickhouse
def test_a_works_file_lands_with_normalised_ids(tmp_path: Path) -> None:
    write_works(
        tmp_path / "w.parquet",
        [
            work(
                "https://openalex.org/W2741809807",
                doi="https://doi.org/10.1000/ABC.Def",
                ids={
                    "openalex": "https://openalex.org/W2741809807",
                    "doi": "https://doi.org/10.1000/ABC.Def",
                    "pmid": "https://pubmed.ncbi.nlm.nih.gov/12345678",
                    "mag": "2741809807",
                },
                updated_date="2026-10-12T03:04:05.123456",
                has_content=(True, False),
                abstract='{"a":[0]}',
            ),
            work(
                "https://openalex.org/W5",
                doi=None,
                ids={"openalex": "https://openalex.org/W5"},
                best_oa=None,
                has_content=None,
                abstract=None,
            ),
            work("https://openalex.org/W6", doi="doi:10.5555/X"),
            work("https://openalex.org/W7", doi="https://dx.doi.org/10.1/Y"),
            work("https://openalex.org/W8", doi="not-a-doi"),
        ],
    )
    rows = load_and_select(tmp_path, WORKS, "w.parquet")
    assert (
        rows["2741809807"].items()
        >= {
            "doi_norm": "10.1000/abc.def",
            "pmid": 12345678,
            "mag": 2741809807,
            "updated_date": "2026-10-12 03:04:05",
            "best_oa_source_id": "https://openalex.org/S123",
            "best_oa_license": "cc-by",
            "has_pdf": True,
            "has_grobid_xml": False,
            "has_abstract": True,
            "source_release": RELEASE,
            "removed": False,
        }.items()
    )
    assert (
        rows["5"].items()
        >= {
            "doi_norm": None,
            "pmid": None,
            "mag": None,
            "best_oa_pdf_url": None,
            "best_oa_source_id": None,
            "is_oa": False,
            "has_pdf": None,
            "has_abstract": False,
        }.items()
    )
    assert [rows[k]["doi_norm"] for k in ("6", "7", "8")] == ["10.5555/x", "10.1/y", None]


@needs_clickhouse
@pytest.mark.parametrize(
    "bad",
    [
        "https://openalex.org/A123",
        "W123",
        "https://openalex.org/W",
        "https://openalex.org/W" + "1" * 20,
        None,
    ],
)
@pytest.mark.parametrize("contract", [WORKS, WORKS_ABSTRACTS], ids=lambda c: c.name)
def test_an_id_that_is_not_a_work_stops_the_insert(
    tmp_path: Path, bad: str | None, contract: DatasetContract
) -> None:
    write_works(tmp_path / "w.parquet", [work("https://openalex.org/W1"), work(bad)])
    result = run(
        f"{up()}\n{parquet_upsert_query(contract, 'file')};", tmp_path / "db",
        **params(tmp_path / "w.parquet"),
    )  # fmt: skip
    assert result.returncode != 0
    assert f"{contract.table}: id breaks the contract" in result.stderr


def decode(index: str | None) -> str:
    if index in (None, "", "null"):
        return ""
    words: dict[str, list[int]] = json.loads(index)
    return " ".join(w for _, w in sorted((p, w) for w, ps in words.items() for p in ps))


INDEXES = {
    1: '{"Results":[0],"show,":[1],"that":[2],"it":[3],"works.":[4]}',
    2: '{"the":[0,3],"cat":[1],"saw":[2],"dog":[4]}',
    3: '{"\\u00dcber":[0],"die":[1],"Größe":[2],"von":[3],"细胞":[4]}',
    4: "{}",
    5: None,
    6: '{"gap":[0],"here":[5],"then":[9]}',
    7: '{"b":[1],"a":[1],"x":[0]}',
}


@needs_clickhouse
def test_the_rebuilt_abstract_matches_a_python_decode(tmp_path: Path) -> None:
    write_works(
        tmp_path / "a.parquet",
        [work(f"https://openalex.org/W{k}", abstract=v) for k, v in INDEXES.items()],
    )
    rows = load_and_select(tmp_path, WORKS_ABSTRACTS, "a.parquet")
    got = {int(key): row["abstract"] for key, row in rows.items()}
    assert got == {k: decode(v) for k, v in INDEXES.items()}
    assert got[1] == "Results show, that it works." and got[2] == "the cat saw the dog"
    assert got[3] == "Über die Größe von 细胞" and got[6] == "gap here then" and got[7] == "x a b"
    assert got[4] == got[5] == ""


@needs_clickhouse
@pytest.mark.parametrize(
    "bad",
    [
        '{"neg":[-1]}',
        '{"big":[4294967296]}',
        '{"half":[1.5]}',
        '{"a":[0],"a":[1]}',
        '{"a":0}',
        "[1, 2]",
        "not json",
    ],
)
def test_a_malformed_index_stops_the_insert(tmp_path: Path, bad: str) -> None:
    write_works(tmp_path / "a.parquet", [work("https://openalex.org/W1", abstract=bad)])
    result = run(
        f"{up()}\n{parquet_upsert_query(WORKS_ABSTRACTS, 'file')};", tmp_path / "db",
        **params(tmp_path / "a.parquet"),
    )  # fmt: skip
    assert result.returncode != 0
    assert "openalex_abstracts: abstract_inverted_index breaks the contract" in result.stderr
