"""The read-table rebuild SQL, run with ``clickhouse local`` against the migrations."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from openknowledge_adapters.clickhouse.crosswalk import (
    BEST_COLUMNS,
    XWALK_COLUMNS,
    best_select,
    xwalk_select,
)
from openknowledge_adapters.clickhouse.read_tables import (
    COLUMNS,
    DELTA,
    IN,
    KEY,
    OUT,
    PAPERS,
    SOURCES,
    apply_sql,
    delta_sql,
    in_only_sql,
)
from openknowledge_core.ingestion.read_tables import GROUPS
from tests.clickhouse_local import clickhouse_local, needs_clickhouse, up


@needs_clickhouse
def test_edge_rebuild_preserves_live_citation_ids(tmp_path: Path) -> None:
    sql = f"""
        {up()}
        SYSTEM STOP MERGES citations;
        INSERT INTO citations
            (citationid, citingcorpusid, citedcorpusid, source_release, is_deleted)
        VALUES (11, 1, 2, '2026-10-01', 0), (12, 1, 2, '2026-10-01', 0),
               (13, 1, 3, '2026-10-01', 0), (14, 1, NULL, '2026-10-01', 0);
        INSERT INTO citations
            (citationid, citingcorpusid, citedcorpusid, source_release, is_deleted)
        VALUES (11, 1, 9, '2026-09-01', 0);
        INSERT INTO citations (citationid, source_release, is_deleted)
        VALUES (13, '2026-10-02', 1);
        CREATE TABLE citation_edges_out_new AS citation_edges_out;
        INSERT INTO citation_edges_out_new ({COLUMNS[OUT]}) {SOURCES[OUT]};
        CREATE TABLE citation_edges_in_new AS citation_edges_in;
        INSERT INTO citation_edges_in_new ({COLUMNS[IN]}) {SOURCES[IN]};
        SELECT citing, cited, citationid FROM citation_edges_out_new
        ORDER BY citationid FORMAT JSONEachRow;
        SELECT citing, cited, citationid FROM citation_edges_in_new
        ORDER BY citationid FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == [
        {"citing": 1, "cited": 2, "citationid": 11},
        {"citing": 1, "cited": 2, "citationid": 12},
        {"citing": 1, "cited": 2, "citationid": 11},
        {"citing": 1, "cited": 2, "citationid": 12},
    ]


S2_TABLES = GROUPS["papers"] + GROUPS["citations"]


def _full_build(suffix: str, tables: tuple[str, ...] = S2_TABLES) -> str:
    return "".join(
        f"CREATE TABLE {table}{suffix} AS {table};\n"
        f"INSERT INTO {table}{suffix} ({COLUMNS[table]}) {SOURCES[table]};\n"
        for table in tables
    )


def _publish(tables: tuple[str, ...]) -> str:
    return "".join(
        f"EXCHANGE TABLES {table}_new AND {table}; DROP TABLE {table}_new;\n" for table in tables
    )


def _copies(sql: str, tmp_path: Path) -> dict[str, list[dict[str, Any]]]:
    copies: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in clickhouse_local(sql, tmp_path):
        copies[row.pop("copy")].append(row)
    return copies


def _compare(tables: tuple[str, ...]) -> str:
    return "".join(
        f"SELECT '{name}' AS copy, * FROM {name} ORDER BY all FORMAT JSONEachRow;\n"
        for table in tables
        for name in (table, f"{table}_new")
    )


@needs_clickhouse
def test_incremental_update_equals_full_rebuild(tmp_path: Path) -> None:
    run = [
        *delta_sql("citations", "2026-10-01", "2026-10-03", 100),
        *apply_sql("citations"),
        *delta_sql("papers", "2026-10-01", "2026-10-03", 100),
        *apply_sql("papers"),
    ]
    # A run that stopped before its last insert, then two complete reruns.
    statements = ";\n".join([*run[:-1], *run, *run])
    sql = f"""
        {up()}
        SYSTEM STOP MERGES citations;
        SYSTEM STOP MERGES papers;
        INSERT INTO citations (citationid, citingcorpusid, citedcorpusid, isinfluential, intents,
            source_release, is_deleted)
        VALUES (11, 1, 2, 1, [['background']], '2026-10-01', 0),
               (12, 1, 2, 0, [], '2026-10-01', 0), (13, 1, 3, 0, [], '2026-10-01', 0),
               (14, 1, NULL, 0, [], '2026-10-01', 0), (15, 2, 3, 0, [], '2026-10-01', 0),
               (17, 2, 4, 0, [], '2026-10-01', 0);
        INSERT INTO papers (corpusid, title, year, citationcount, source_release, is_deleted)
        VALUES (1, 'a', 2020, 1, '2026-10-01', 0), (2, 'b', 2021, 2, '2026-10-01', 0),
               (3, 'c', NULL, 3, '2026-10-01', 0), (4, 'd', 2023, 4, '2026-10-01', 0);
        {_full_build("_new")}
        {_publish(S2_TABLES)}
        INSERT INTO citations (citationid, citingcorpusid, citedcorpusid, isinfluential, intents,
            source_release, is_deleted)
        VALUES (11, 1, 9, 1, [['background']], '2026-10-02', 0),
               (14, 1, 3, 0, [], '2026-10-02', 0), (17, 2, NULL, 0, [], '2026-10-02', 0),
               (16, 3, 1, 1, [['methodology'], ['result']], '2026-10-02', 0),
               (15, 2, 3, 1, [['result']], '2026-10-03', 0);
        INSERT INTO citations (citationid, source_release, is_deleted)
        VALUES (13, '2026-10-02', 1), (15, '2026-10-02', 1);
        INSERT INTO papers (corpusid, title, year, citationcount, source_release, is_deleted)
        VALUES (2, 'b2', 2021, 5, '2026-10-02', 0), (5, 'e', 2026, 0, '2026-10-02', 0),
               (4, 'd2', 2023, 6, '2026-10-03', 0);
        INSERT INTO papers (corpusid, source_release, is_deleted)
        VALUES (3, '2026-10-02', 1), (4, '2026-10-02', 1);
        {statements};
        {_full_build("_new")}
        {_compare(S2_TABLES)}
    """
    copies = _copies(sql, tmp_path)
    for table in S2_TABLES:
        assert copies[table] == copies[f"{table}_new"], table
    assert sorted(row["citationid"] for row in copies[IN]) == [11, 12, 14, 15, 16]
    assert [row["corpusid"] for row in copies[PAPERS]] == [1, 2, 4, 5]


@needs_clickhouse
def test_in_only_rebuild_fills_ids_from_out(tmp_path: Path) -> None:
    rebuild = ";\n".join(in_only_sql())
    sql = f"""
        {up()}
        INSERT INTO citation_edges_out (citing, cited, isinfluential, intents, citationid)
        VALUES (2, 1, 0, 1, 21), (1, 2, 1, 4, 22);
        INSERT INTO citation_edges_in (citing, cited, isinfluential, intents)
        VALUES (2, 1, 0, 1), (1, 2, 1, 4);
        {rebuild};
        SELECT * FROM citation_edges_in_new ORDER BY cited, citing FORMAT JSONEachRow;
    """
    assert clickhouse_local(sql, tmp_path) == [
        {"citing": 2, "cited": 1, "isinfluential": False, "intents": 1, "citationid": 21},
        {"citing": 1, "cited": 2, "isinfluential": True, "intents": 4, "citationid": 22},
    ]


@needs_clickhouse
@pytest.mark.parametrize("source", ["citations", "papers"])
def test_delta_takes_keys_after_start_up_to_end(source: str, tmp_path: Path) -> None:
    complete, capped = (
        ";\n".join(delta_sql(source, "2026-10-01", "2026-10-03", max_keys)) for max_keys in (3, 1)
    )
    sql = f"""
        {up()}
        SYSTEM STOP MERGES {source};
        INSERT INTO {source} ({KEY[source]}, source_release, is_deleted)
        VALUES (1, '2026-10-01', 0), (2, '2026-10-02', 0), (3, '2026-10-03', 0),
               (4, '2026-10-04', 0), (5, '2026-10-02', 1);
        INSERT INTO {source} ({KEY[source]}, source_release, is_deleted)
        VALUES (2, '2026-10-03', 0), (5, '2026-10-03', 1);
        {complete};
        SELECT key FROM {DELTA[source]} ORDER BY key FORMAT JSONEachRow;
        {capped};
        SELECT count() AS keys FROM {DELTA[source]} FORMAT JSONEachRow;
    """
    # Keys 2 and 5 come twice in range but count once; a capped delta stops one key past its cap.
    assert clickhouse_local(sql, tmp_path) == [{"key": 2}, {"key": 3}, {"key": 5}, {"keys": 2}]


@needs_clickhouse
def test_xwalk_full_build_ranks_every_candidate(tmp_path: Path) -> None:
    sql = f"""
        {up()}
        INSERT INTO papers (corpusid, externalids, title, year, source_release, is_deleted)
        VALUES (1, {{'DOI': 'https://doi.org/10.1000/ABC'}}, '', NULL, '2026-10-01', 0),
               (2, {{'DOI': '10.1000/x', 'PubMed': '77'}}, '', NULL, '2026-10-01', 0),
               (3, {{'DOI': '10.1000/pub', 'ArXiv': '2101.00001v2'}}, '', NULL, '2026-10-01', 0),
               (4, {{'MAG': '500'}}, 'Deep Nets: A Study', 2020, '2026-10-01', 0),
               (5, {{'MAG': '600'}}, 'Other', 2020, '2026-10-01', 0),
               (7, {{'DOI': '10.1000/dup'}}, '', NULL, '2026-10-01', 0),
               (8, {{'DOI': '10.1000/dup'}}, '', NULL, '2026-10-01', 0),
               (9, {{'DOI': '10.1000/gone'}}, '', NULL, '2026-10-01', 1),
               (10, {{'DOI': '10.1000/removed'}}, '', NULL, '2026-10-01', 0),
               (11, {{'DOI': '10.1000/many'}}, '', NULL, '2026-10-01', 0),
               (12, {{'DOI': '10.1000/tie'}}, '', NULL, '2026-10-01', 0),
               (13, {{'DOI': '10.1000/broken'}}, '', NULL, '2026-10-01', 0);
        INSERT INTO openalex_works (work_id, doi_norm, pmid, mag, title, publication_year, type,
            has_abstract, is_xpac, removed, source_release, is_deleted)
        VALUES (10, '10.1000/abc', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (20, '10.1000/x', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (21, NULL, 77, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (30, '10.48550/arxiv.2101.00001', NULL, NULL, NULL, NULL, 'preprint', 1, 0, 0,
                '2026-10-01', 0),
               (31, '10.1000/pub', NULL, NULL, NULL, NULL, 'article', 1, 0, 0, '2026-10-01', 0),
               (40, NULL, NULL, 500, 'deep nets — a study', 2021, NULL, 1, 0, 0, '2026-10-01', 0),
               (60, NULL, NULL, 600, 'Other', 2023, NULL, 1, 0, 0, '2026-10-01', 0),
               (70, '10.1000/dup', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (90, '10.1000/gone', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (100, '10.1000/removed', NULL, NULL, NULL, NULL, NULL, 1, 0, 1, '2026-10-01', 0),
               (110, '10.1000/many', NULL, NULL, NULL, NULL, NULL, 0, 0, 0, '2026-10-01', 0),
               (111, '10.1000/many', NULL, NULL, NULL, NULL, NULL, 1, 1, 0, '2026-10-01', 0),
               (112, '10.1000/many', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (120, '10.1000/tie', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (121, '10.1000/tie', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (130, '10.1000/broken', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0),
               (131, '10.1000/broken', NULL, NULL, NULL, NULL, NULL, 1, 0, 0, '2026-10-01', 0);
        INSERT INTO openalex_abstracts (work_id, abstract, index_broken, source_release, is_deleted)
        VALUES (130, '', 1, '2026-10-01', 0), (131, 'text', 0, '2026-10-01', 0);
        CREATE TABLE paper_xwalk_new AS paper_xwalk;
        INSERT INTO paper_xwalk_new ({XWALK_COLUMNS}) {xwalk_select(None)};
        SELECT * FROM paper_xwalk_new ORDER BY corpusid, rank FORMAT JSONEachRow;
    """
    rows = [tuple(row.values()) for row in clickhouse_local(sql, tmp_path)]
    # corpusid, work_id, match_key, rank, chosen, shared_work
    assert rows == [
        (1, 10, "doi", 1, True, False),
        (2, 20, "doi", 1, True, False),
        (2, 21, "pmid", 2, False, False),
        (3, 31, "doi", 1, True, False),
        (3, 30, "doi", 2, False, False),
        (4, 40, "mag", 1, True, False),
        (7, 70, "doi", 1, True, True),
        (8, 70, "doi", 1, True, True),
        (11, 112, "doi", 1, True, False),
        (11, 111, "doi", 2, False, False),
        (11, 110, "doi", 3, False, False),
        (12, 120, "doi", 1, True, False),
        (12, 121, "doi", 2, False, False),
        (13, 131, "doi", 1, True, False),
        (13, 130, "doi", 2, False, False),
    ]


@needs_clickhouse
def test_xwalk_incremental_equals_full_rebuild(tmp_path: Path) -> None:
    run = [*delta_sql("openalex", "2026-10-01", "2026-10-03", 100), *apply_sql("openalex")]
    # A run that stopped before its last insert, then two complete reruns on the kept delta.
    statements = ";\n".join([*run[:-1], *run, *run])
    xwalk = GROUPS["openalex"]
    sql = f"""
        {up()}
        SYSTEM STOP MERGES papers;
        INSERT INTO papers (corpusid, externalids, source_release, is_deleted)
        VALUES (1, {{'DOI': '10.1/d10'}}, '2026-10-01', 0),
               (2, {{'DOI': '10.1/d20'}}, '2026-10-01', 0),
               (4, {{'DOI': '10.1/d40'}}, '2026-10-01', 0),
               (5, {{'DOI': '10.1/d40'}}, '2026-10-01', 0);
        INSERT INTO openalex_works (work_id, doi_norm, has_abstract, source_release, is_deleted)
        VALUES (10, '10.1/d10', 1, '2026-10-01', 0), (20, '10.1/d20', 1, '2026-10-01', 0),
               (30, '10.1/d30', 1, '2026-10-01', 0), (40, '10.1/d40', 1, '2026-10-01', 0);
        {_full_build("_new", xwalk)}
        {_publish(xwalk)}
        INSERT INTO papers (corpusid, externalids, source_release, is_deleted)
        VALUES (2, {{'DOI': '10.1/d10'}}, '2026-10-02', 0),
               (3, {{'DOI': '10.1/d30'}}, '2026-10-03', 0);
        INSERT INTO papers (corpusid, source_release, is_deleted) VALUES (4, '2026-10-02', 1);
        {statements};
        {_full_build("_new", xwalk)}
        {_compare(xwalk)}
    """
    copies = _copies(sql, tmp_path)
    assert copies["paper_xwalk"] == copies["paper_xwalk_new"]
    assert [(r["corpusid"], r["work_id"], r["shared_work"]) for r in copies["paper_xwalk"]] == [
        (1, 10, True),
        (2, 10, True),
        (3, 30, False),
        (5, 40, False),
    ]


@needs_clickhouse
def test_best_abstract_prefers_live_s2_then_the_chosen_work(tmp_path: Path) -> None:
    sql = f"""
        {up()}
        SYSTEM STOP MERGES abstracts;
        INSERT INTO papers (corpusid, source_release, is_deleted)
        VALUES (1, '2026-10-01', 0), (2, '2026-10-01', 0), (3, '2026-10-01', 0),
               (4, '2026-10-01', 0), (5, '2026-10-01', 0), (6, '2026-10-01', 1);
        INSERT INTO abstracts (corpusid, abstract, source_release, is_deleted)
        VALUES (1, 'S2 one', '2026-10-01', 0), (3, 'S2 three', '2026-10-01', 0),
               (6, 'S2 six', '2026-10-01', 0);
        INSERT INTO abstracts (corpusid, abstract, source_release, is_deleted)
        VALUES (3, '', '2026-10-02', 1);
        INSERT INTO paper_xwalk (corpusid, work_id, match_key, rank, chosen, shared_work)
        VALUES (1, 10, 'doi', 1, 1, 0), (2, 20, 'doi', 1, 1, 0), (2, 21, 'pmid', 2, 0, 0),
               (3, 30, 'doi', 1, 1, 0), (4, 40, 'doi', 1, 1, 0), (5, 50, 'doi', 1, 1, 0);
        INSERT INTO openalex_abstracts (work_id, abstract, removed, source_release, is_deleted)
        VALUES (10, 'OA one', 0, '2026-10-01', 0), (20, 'OA two', 0, '2026-10-01', 0),
               (21, 'OA wrong', 0, '2026-10-01', 0), (30, 'OA three', 0, '2026-10-01', 0),
               (40, '', 0, '2026-10-01', 0), (50, 'OA five', 1, '2026-10-01', 0);
        CREATE TABLE paper_abstract_best_new AS paper_abstract_best;
        INSERT INTO paper_abstract_best_new ({BEST_COLUMNS}) {best_select("paper_xwalk", None)};
        SELECT * FROM paper_abstract_best_new ORDER BY corpusid FORMAT JSONEachRow;
    """
    # Plan A writes a row for every work, so an empty OpenAlex abstract must not win (40).
    assert clickhouse_local(sql, tmp_path) == [
        {"corpusid": 1, "abstract": "S2 one", "source": "s2", "work_id": None},
        {"corpusid": 2, "abstract": "OA two", "source": "openalex", "work_id": 20},
        {"corpusid": 3, "abstract": "OA three", "source": "openalex", "work_id": 30},
    ]
