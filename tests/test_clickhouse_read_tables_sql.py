"""The read-table rebuild SQL, run with ``clickhouse local`` against the migrations."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from openknowledge_adapters.clickhouse.crosswalk import (
    BEST_COLUMNS,
    MAX_TITLE_WORKS,
    XWALK_COLUMNS,
    best_select,
    title_sql,
    xwalk_select,
)
from openknowledge_adapters.clickhouse.read_tables import (
    COLUMNS,
    DELTA,
    GRACE_HASH,
    IN,
    KEY,
    OUT,
    PAPERS,
    SOURCES,
    XWALK,
    apply_sql,
    build_budget,
    delta_sql,
    in_only_sql,
)
from openknowledge_adapters.clickhouse.search import BASE_QUERY, DIFF_QUERY
from openknowledge_core.ingestion.read_tables import GROUPS
from tests.clickhouse_local import clickhouse_local, needs_clickhouse, up

BUDGET = build_budget(14_760_000_000, 0.9, 2)


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


def _titles(xwalk: str) -> str:
    return "".join(f"{statement};\n" for statement in title_sql(xwalk, f"SETTINGS {GRACE_HASH}"))


def _full_build(suffix: str, tables: tuple[str, ...] = S2_TABLES) -> str:
    return "".join(
        f"CREATE TABLE {table}{suffix} AS {table};\n"
        f"INSERT INTO {table}{suffix} ({COLUMNS[table]}) {SOURCES[table]};\n"
        + (_titles(f"{table}{suffix}") if table == XWALK else "")
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
        *delta_sql("citations", "2026-10-01", "2026-10-03", 100, BUDGET),
        *apply_sql("citations", BUDGET),
        *delta_sql("papers", "2026-10-01", "2026-10-03", 100, BUDGET),
        *apply_sql("papers", BUDGET),
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
    rebuild = ";\n".join(in_only_sql(BUDGET))
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
@pytest.mark.parametrize("source", ["citations", "papers", "openalex"])
def test_delta_takes_keys_after_start_up_to_end(source: str, tmp_path: Path) -> None:
    complete, capped = (
        ";\n".join(delta_sql(source, "2026-10-01", "2026-10-03", max_keys, BUDGET))
        for max_keys in (3, 1)
    )
    # The openalex keys come from papers and abstracts together.
    first, second = ("papers", "abstracts") if source == "openalex" else (source, source)
    sql = f"""
        {up()}
        SYSTEM STOP MERGES {first};
        SYSTEM STOP MERGES {second};
        INSERT INTO {first} ({KEY[source]}, source_release, is_deleted)
        VALUES (1, '2026-10-01', 0), (2, '2026-10-02', 0), (3, '2026-10-03', 0);
        INSERT INTO {second} ({KEY[source]}, source_release, is_deleted)
        VALUES (4, '2026-10-04', 0), (5, '2026-10-02', 1);
        INSERT INTO {second} ({KEY[source]}, source_release, is_deleted)
        VALUES (2, '2026-10-03', 0), (5, '2026-10-03', 1);
        {complete};
        SELECT key FROM {DELTA[source]} ORDER BY key FORMAT JSONEachRow;
        DROP TABLE {DELTA[source]};
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
    run = [
        *delta_sql("openalex", "2026-10-01", "2026-10-03", 100, BUDGET),
        *apply_sql("openalex", BUDGET),
    ]
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


@needs_clickhouse
def test_best_abstract_incremental_equals_full_rebuild(tmp_path: Path) -> None:
    run = [
        *delta_sql("openalex", "2026-10-01", "2026-10-02", 100, BUDGET),
        *apply_sql("openalex", BUDGET),
    ]
    statements = ";\n".join([*run[:-1], *run, *run])
    group = GROUPS["openalex"]
    sql = f"""
        {up()}
        SYSTEM STOP MERGES papers;
        SYSTEM STOP MERGES abstracts;
        INSERT INTO papers (corpusid, externalids, source_release, is_deleted)
        SELECT number, map('DOI', concat('10.1/', toString(number * 10))), '2026-10-01', 0
        FROM numbers(1, 5);
        INSERT INTO openalex_works (work_id, doi_norm, has_abstract, source_release, is_deleted)
        SELECT number * 10, concat('10.1/', toString(number * 10)), 1, '2026-10-01', 0
        FROM numbers(1, 6);
        INSERT INTO openalex_abstracts (work_id, abstract, source_release, is_deleted)
        SELECT number * 10, concat('OA ', ['one', 'two', 'three', 'four', 'five', 'six'][number]),
               '2026-10-01', 0
        FROM numbers(1, 6);
        INSERT INTO abstracts (corpusid, abstract, source_release, is_deleted)
        VALUES (1, 'S2 one', '2026-10-01', 0), (3, 'S2 three', '2026-10-01', 0);
        {_full_build("_new", group)}
        {_publish(group)}
        INSERT INTO abstracts (corpusid, abstract, source_release, is_deleted)
        VALUES (3, '', '2026-10-02', 1), (2, 'S2 two', '2026-10-02', 0);
        INSERT INTO papers (corpusid, externalids, source_release, is_deleted)
        VALUES (4, map(), '2026-10-02', 1), (5, map('DOI', '10.1/60'), '2026-10-02', 0);
        {statements};
        {_full_build("_new", group)}
        {_compare(group)}
    """
    copies = _copies(sql, tmp_path)
    assert copies["paper_abstract_best"] == copies["paper_abstract_best_new"]
    assert copies["paper_xwalk"] == copies["paper_xwalk_new"]
    assert [
        (r["corpusid"], r["source"], r["abstract"], r["work_id"])
        for r in copies["paper_abstract_best"]
    ] == [
        (1, "s2", "S2 one", None),
        (2, "s2", "S2 two", None),
        (3, "openalex", "OA three", 30),
        (5, "openalex", "OA six", 60),
    ]


@needs_clickhouse
def test_apply_keeps_the_best_abstract_of_papers_that_only_shared_a_work(tmp_path: Path) -> None:
    group = GROUPS["openalex"]
    # Stop before the last insert, as a search pass reading in between would see it.
    statements = ";\n".join(
        [
            *delta_sql("openalex", "2026-10-01", "2026-10-02", 100, BUDGET),
            *apply_sql("openalex", BUDGET),
        ][:-1]
    )
    sql = f"""
        {up()}
        INSERT INTO papers (corpusid, externalids, source_release, is_deleted)
        VALUES (1, {{'DOI': '10.1/d10'}}, '2026-10-01', 0),
               (2, {{'DOI': '10.1/d10'}}, '2026-10-01', 0);
        INSERT INTO openalex_works (work_id, doi_norm, has_abstract, source_release, is_deleted)
        VALUES (10, '10.1/d10', 1, '2026-10-01', 0), (20, '10.1/d20', 1, '2026-10-01', 0);
        INSERT INTO openalex_abstracts (work_id, abstract, source_release, is_deleted)
        VALUES (10, 'OA ten', '2026-10-01', 0), (20, 'OA twenty', '2026-10-01', 0);
        {_full_build("_new", group)}
        {_publish(group)}
        INSERT INTO papers (corpusid, externalids, source_release, is_deleted)
        VALUES (1, {{'DOI': '10.1/d20'}}, '2026-10-02', 0);
        {statements};
        SELECT corpusid, shared_work FROM paper_xwalk ORDER BY corpusid FORMAT JSONEachRow;
        SELECT corpusid FROM paper_abstract_best ORDER BY corpusid FORMAT JSONEachRow;
    """
    # Paper 2 is updated only for shared_work: the search diff never sees it.
    assert clickhouse_local(sql, tmp_path) == [
        {"corpusid": 1, "shared_work": False},
        {"corpusid": 2, "shared_work": False},
        {"corpusid": 2},
    ]


@needs_clickhouse
@pytest.mark.parametrize("query", [BASE_QUERY, DIFF_QUERY])
def test_search_leaves_out_abstracts_of_shared_works(query: str, tmp_path: Path) -> None:
    sql = f"""
        {up()}
        INSERT INTO papers (corpusid, source_release, is_deleted)
        VALUES (1, '2026-10-08', 0), (2, '2026-10-08', 0), (3, '2026-10-08', 0);
        INSERT INTO paper_xwalk (corpusid, work_id, match_key, rank, chosen, shared_work)
        VALUES (1, 10, 'doi', 1, 1, 1), (2, 10, 'doi', 1, 1, 1), (3, 30, 'doi', 1, 1, 0);
        INSERT INTO paper_abstract_best (corpusid, abstract, source, work_id)
        VALUES (1, 'OA ten', 'openalex', 10), (2, 'S2 two', 's2', NULL),
               (3, 'OA three', 'openalex', 30);
        {query} FORMAT JSONEachRow;
    """
    rows = clickhouse_local(sql, tmp_path, low="0", high="9", from_release="2026-10-01")
    assert {row["corpusid"]: row["best_abstract"] for row in rows} == {
        1: None,
        2: "S2 two",
        3: "OA three",
    }


# Titles of at least 20 characters once normalised, each its own block.
TITLES = {key: f"a study of case {key} in depth" for key in range(1, 30)}


def _paper(corpusid: int, key: int | None, year: int | None, doi: str | None = None) -> str:
    title = "" if key is None else TITLES[key].title()
    ids = f"{{'DOI': '{doi}'}}" if doi else "{}"
    return f"({corpusid}, {ids}, '{title}', {'NULL' if year is None else year}, '2026-10-01', 0)"


def _work(
    work_id: int,
    key: int,
    year: int | None,
    kind: str | None = "article",
    *,
    xpac: bool = False,
    doi: str | None = None,
    paratext: bool = False,
) -> str:
    return (
        f"({work_id}, {f"'{doi}'" if doi else 'NULL'}, '{TITLES[key].upper()}.', "
        f"{'NULL' if year is None else year}, {'NULL' if kind is None else f"'{kind}'"}, "
        f"{int(xpac)}, {int(paratext)}, 1, 0, '2026-10-01', 0)"
    )


PAPER_COLUMNS = "corpusid, externalids, title, year, source_release, is_deleted"
WORK_COLUMNS = (
    "work_id, doi_norm, title, publication_year, type, is_xpac, is_paratext, has_abstract, "
    "removed, source_release, is_deleted"
)


@needs_clickhouse
def test_title_matches_take_unique_free_works_only(tmp_path: Path) -> None:
    long_title = "a much longer title of forty characters or more"
    papers = [
        _paper(1, 1, 2020),  # unique title, year one apart: matched
        _paper(2, 2, 2020),  # two works in the window
        _paper(3, 3, 2019),  # two papers in the work's window
        _paper(4, 3, 2021),
        _paper(5, 4, 2020),  # another paper of the title holds a DOI match
        _paper(6, 4, 2020, doi="10.1/six"),
        _paper(7, 5, 2020),  # generic: more than MAX_TITLE_WORKS works, all years apart
        _paper(8, 6, 2020),  # serial: works in three years
        _paper(9, 7, 2020),  # the work is taken by an ID match
        _paper(10, 29, 2020, doi="10.1/ten"),
        _paper(11, 8, 2020),  # non-XPAC work beside an XPAC one: matched to the non-XPAC
        _paper(12, 9, 2020),  # only one XPAC work: matched
        _paper(13, 10, 2020),  # two XPAC works
        _paper(14, 11, 2020),  # data deposits, paratext and a dataset copy are ignored
        _paper(16, 13, 2020),  # year two apart
        _paper(17, 14, 2020),  # NULL type and another year
        _paper(18, 15, 2020),  # 'other' type, same year: matched
        _paper(19, 16, 2020, doi="10.1/nineteen"),  # has an ID candidate already
        _paper(21, 18, None),  # no year and a title under 40 characters
        _paper(22, 21, 2020, doi="10.1/twentytwo"),  # an ID match: no title match on top
        _paper(23, 23, 2020),  # empty-string type and another year
        _paper(24, 24, 2020),  # empty-string type, same year: matched
        "(15, {}, 'Short Title', 2020, '2026-10-01', 0)",  # under 20 characters
        f"(20, {{}}, '{long_title.title()}', NULL, '2026-10-01', 0)",  # no year: matched
    ]
    works = [
        _work(101, 1, 2021),
        _work(201, 2, 2020),
        _work(202, 2, 2021),
        _work(301, 3, 2020),
        _work(401, 4, 2020),
        _work(402, 28, 2020, doi="10.1/six"),
        _work(501, 5, 2020),
        _work(601, 6, 2016),
        _work(602, 6, 2018),
        _work(603, 6, 2020),
        _work(701, 7, 2020, doi="10.1/ten"),
        _work(801, 8, 2020, xpac=True),
        _work(802, 8, 2020),
        _work(901, 9, 2021, xpac=True),
        _work(1001, 10, 2020, xpac=True),
        _work(1002, 10, 2020, xpac=True),
        _work(1101, 11, 2020),
        _work(1102, 11, 2020, "dataset"),
        _work(1103, 11, 2020, doi="10.5281/zenodo.1"),
        _work(1104, 11, 2020, doi="10.6084/m9.figshare.1"),
        _work(1105, 11, 2020, doi="10.17632/abc.1"),
        _work(1106, 11, 2020, paratext=True),
        _work(1301, 13, 2018),
        _work(1401, 14, 2021, None),
        _work(1501, 15, 2020, "other"),
        _work(1601, 16, 2020, doi="10.1/nineteen"),
        _work(1602, 16, 2020),
        _work(1801, 18, 2020),
        _work(2101, 21, 2020),
        _work(2201, 22, 2020, doi="10.1/twentytwo"),
        _work(2301, 23, 2021, ""),
        _work(2401, 24, 2020, ""),
        _work(15010, 1, None),  # no year: outside every dated window
        "(150, NULL, 'Short title', 2020, 'article', 0, 0, 1, 0, '2026-10-01', 0)",
        f"(2001, NULL, '{long_title}', 2010, 'article', 0, 0, 1, 0, '2026-10-01', 0)",
    ]
    sql = f"""
        {up()}
        INSERT INTO papers ({PAPER_COLUMNS}) VALUES {", ".join(papers)};
        INSERT INTO openalex_works ({WORK_COLUMNS}) VALUES {", ".join(works)};
        INSERT INTO openalex_works ({WORK_COLUMNS})
        SELECT 50000 + number, NULL, '{TITLES[5]}', 1900, 'article', 0, 0, 1, 0, '2026-10-01', 0
        FROM numbers({MAX_TITLE_WORKS});
        {_full_build("_new", (XWALK,))}
        SELECT corpusid, work_id, match_key, rank, chosen, shared_work FROM paper_xwalk_new
        ORDER BY corpusid, rank FORMAT JSONEachRow;
    """
    rows = [tuple(row.values()) for row in clickhouse_local(sql, tmp_path)]
    assert rows == [
        (1, 101, "title", 1, True, False),
        (6, 402, "doi", 1, True, False),
        (10, 701, "doi", 1, True, False),
        (11, 802, "title", 1, True, False),
        (12, 901, "title", 1, True, False),
        (14, 1101, "title", 1, True, False),
        (18, 1501, "title", 1, True, False),
        (19, 1601, "doi", 1, True, False),
        (20, 2001, "title", 1, True, False),
        (22, 2201, "doi", 1, True, False),
        (24, 2401, "title", 1, True, False),
    ]


@needs_clickhouse
def test_an_update_drops_a_title_match_whose_work_an_id_match_takes(tmp_path: Path) -> None:
    run = [
        *delta_sql("openalex", "2026-10-01", "2026-10-02", 100, BUDGET),
        *apply_sql("openalex", BUDGET, "2026-10-02"),
    ]
    statements = ";\n".join([*run[:-1], *run, *run])
    group = GROUPS["openalex"]
    sql = f"""
        {up()}
        SYSTEM STOP MERGES papers;
        INSERT INTO papers ({PAPER_COLUMNS})
        VALUES {_paper(1, 1, 2020)}, {_paper(2, 2, 2020)}, {_paper(3, 3, 2020)};
        INSERT INTO openalex_works ({WORK_COLUMNS})
        VALUES {_work(10, 1, 2020)}, {_work(20, 2, 2020, doi="10.1/d20")}, {_work(30, 3, 2020)};
        INSERT INTO openalex_abstracts (work_id, abstract, source_release, is_deleted)
        VALUES (10, 'OA ten', '2026-10-01', 0), (20, 'OA twenty', '2026-10-01', 0),
               (30, 'OA thirty', '2026-10-01', 0);
        {_full_build("_new", group)}
        {_publish(group)}
        SELECT corpusid, work_id, match_key FROM paper_xwalk ORDER BY corpusid FORMAT JSONEachRow;
        INSERT INTO papers ({PAPER_COLUMNS})
        VALUES (4, {{'DOI': '10.1/d20'}}, 'Another Title Of Some Length', 2020, '2026-10-02', 0);
        {statements};
        {_full_build("_new", group)}
        {_compare(group)}
    """
    rows = clickhouse_local(sql, tmp_path)
    before = [(row["corpusid"], row["work_id"], row["match_key"]) for row in rows[:3]]
    assert before == [(1, 10, "title"), (2, 20, "title"), (3, 30, "title")]
    copies: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows[3:]:
        copies[row.pop("copy")].append(row)
    # Paper 4 takes work 20 by DOI, so paper 2 loses it and its abstract, as in a full build.
    for table in group:
        assert copies[table] == copies[f"{table}_new"], table
    assert [(r["corpusid"], r["work_id"], r["match_key"]) for r in copies[XWALK]] == [
        (1, 10, "title"),
        (3, 30, "title"),
        (4, 20, "doi"),
    ]
    assert [(r["corpusid"], r["work_id"]) for r in copies["paper_abstract_best"]] == [
        (1, 10),
        (3, 30),
        (4, 20),
    ]
    # The weekly search diff must also pick up displaced paper 2 so its stale abstract is cleared.
    diff_sql = f"""
        {up()}
        SYSTEM STOP MERGES papers;
        INSERT INTO papers ({PAPER_COLUMNS})
        VALUES {_paper(1, 1, 2020)}, {_paper(2, 2, 2020)}, {_paper(3, 3, 2020)};
        INSERT INTO openalex_works ({WORK_COLUMNS})
        VALUES {_work(10, 1, 2020)}, {_work(20, 2, 2020, doi="10.1/d20")}, {_work(30, 3, 2020)};
        INSERT INTO openalex_abstracts (work_id, abstract, source_release, is_deleted)
        VALUES (10, 'OA ten', '2026-10-01', 0), (20, 'OA twenty', '2026-10-01', 0),
               (30, 'OA thirty', '2026-10-01', 0);
        {_full_build("_new", group)}
        {_publish(group)}
        INSERT INTO papers ({PAPER_COLUMNS})
        VALUES (4, {{'DOI': '10.1/d20'}}, 'Another Title Of Some Length', 2020, '2026-10-02', 0);
        {";\n".join(run)};
        {DIFF_QUERY} FORMAT JSONEachRow;
    """
    diff_rows = clickhouse_local(
        diff_sql, tmp_path / "diff", low="0", high="9", from_release="2026-10-01"
    )
    assert {row["corpusid"]: row["best_abstract"] for row in diff_rows} == {
        2: None,
        4: "OA twenty",
    }
