"""The indexing flow against ClickHouse and OpenSearch in Docker, which CI must have."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from typing import Any

import httpx
import pytest
from click.testing import CliRunner

from openknowledge_pipelines import cli
from openknowledge_pipelines.index_deployment import search_index
from tests import test_load_e2e as load_e2e
from tests.test_load_e2e import BUCKET, FIXTURE, archive, archive_diff, ran

# The same compose stack, started once more for this module, and the same skip without Docker.
ports = load_e2e.ports
pytestmark = load_e2e.pytestmark

PAPERS = FIXTURE.with_name("papers.jsonl")
ABSTRACTS = FIXTURE.with_name("abstracts.jsonl")
INDEX = "papers"
BASE, NEXT = "2026-10-13", "2026-10-20"


def indexed(ports: dict[str, str]) -> dict[str, Any]:
    env = {
        "OKP_CLICKHOUSE_URL": f"http://127.0.0.1:{ports['CLICKHOUSE_PORT']}",
        "CLICKHOUSE_PASSWORD": "openknowledge",
        "OKP_CLICKHOUSE_RAW_BUCKET": BUCKET,
        "OKP_OPENSEARCH_URL": f"http://127.0.0.1:{ports['OPENSEARCH_PORT']}",
        "OPENSEARCH_PASSWORD": "unused-without-the-security-plugin",
        "OKP_INDEX_SHARDS": "1",
        "OKP_INDEX_BASE_PARTS": "2",
        "OKP_INDEX_DIFF_PARTS": "2",
        "OKP_INDEX_PARALLELISM": "2",
    }
    with pytest.MonkeyPatch.context() as patch:
        for name, value in env.items():
            patch.setenv(name, value)
        return asyncio.run(search_index()).model_dump(mode="json")


def build_read_tables(ports: dict[str, str]) -> None:
    """Bring the read tables current, ``paper_abstract_best`` among them, which search reads."""
    env = {
        "OKP_CLICKHOUSE_URL": f"http://127.0.0.1:{ports['CLICKHOUSE_PORT']}",
        "CLICKHOUSE_PASSWORD": "openknowledge",
        "OKP_CLICKHOUSE_RAW_BUCKET": BUCKET,
        # The diff deletes one of five papers, more than a full rebuild may drop.
        "OKP_READ_TABLES_MAX_DELTA_SHARE": "1",
    }
    result = CliRunner().invoke(cli.main, ["s2", "build-read-tables"], env=env)
    assert result.exit_code == 0, result.output


def document(ports: dict[str, str], corpusid: str) -> dict[str, Any] | None:
    """The stored document with its ``_version``, or None once the index no longer has it."""
    response = httpx.get(f"http://127.0.0.1:{ports['OPENSEARCH_PORT']}/{INDEX}/_doc/{corpusid}")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def search(ports: dict[str, str], query: dict[str, Any]) -> list[dict[str, Any]]:
    response = httpx.post(
        f"http://127.0.0.1:{ports['OPENSEARCH_PORT']}/{INDEX}/_search", json={"query": query}
    )
    response.raise_for_status()
    return response.json()["hits"]["hits"]


def version_of(release: str) -> int:
    """The table's ``version`` of a live row loaded at ``release``."""
    return (date.fromisoformat(release) - date(1970, 1, 1)).days * 2


def test_a_base_pass_then_a_diff_pass_keep_the_index_current(ports: dict[str, str]) -> None:
    papers = PAPERS.read_text().splitlines()
    abstracts = ABSTRACTS.read_text().splitlines()
    archive(ports, BASE, papers, "papers")
    archive(ports, BASE, abstracts, "abstracts")
    ran(ports, "load", "--release", BASE, "--dataset", "papers")
    ran(ports, "load", "--release", BASE, "--dataset", "abstracts")

    archive(ports, BASE, FIXTURE.with_name("tldrs.jsonl").read_text().splitlines(), "tldrs")
    ran(ports, "load", "--release", BASE, "--dataset", "tldrs")
    citations = FIXTURE.with_name("citations.jsonl").read_text().splitlines()
    archive(ports, BASE, citations, "citations")
    ran(ports, "load", "--release", BASE, "--dataset", "citations")
    assert indexed(ports)["outcome"].startswith("papers, abstracts, tldrs and paper_abstract_best")
    build_read_tables(ports)
    result = indexed(ports)
    assert result["outcome"] == "base"
    physical = result["index"]
    assert physical.startswith("papers-v3-") and physical != INDEX
    assert httpx.get(
        f"http://127.0.0.1:{ports['OPENSEARCH_PORT']}/_alias/papers"
    ).json().keys() == {physical}
    hits = search(ports, {"match_phrase": {"title": "A Synthetic Paper"}})
    assert [hit["_source"]["corpusid"] for hit in hits] == ["101"]
    hits = search(ports, {"match": {"title": "papers"}})  # stemmed, so it finds each "Paper"
    titled = {"101", "103", "104", "18446744073709551615"}
    assert {hit["_source"]["corpusid"] for hit in hits} == titled
    since = {"gte": "01/01/2000", "format": "dd/MM/yyyy"}  # a keyword field would match 1665 too
    hits = search(ports, {"range": {"publicationdate": since}})
    assert [hit["_source"]["corpusid"] for hit in hits] == ["101"]  # the others: 1665 and none
    expected = {
        str(json.loads(line)["corpusid"]): json.loads(line)["abstract"] for line in abstracts
    }
    for line in papers:
        corpusid = str(json.loads(line)["corpusid"])
        found = document(ports, corpusid)
        assert found is not None, corpusid
        assert found["_source"].get("abstract") == expected.get(corpusid), corpusid
        # No OpenAlex release is loaded, so every abstract is S2's and versions are S2's.
        source = "s2" if corpusid in expected else None
        assert found["_source"].get("abstract_source") == source, corpusid
        assert (found["_version"], found["_source"]["contract_version"]) == (version_of(BASE), 3)
    index_settings = httpx.get(f"http://127.0.0.1:{ports['OPENSEARCH_PORT']}/{INDEX}/_settings")
    assert "refresh_interval" not in index_settings.json()[physical]["settings"]["index"]

    retitled = {**json.loads(papers[4]), "title": "A Retitled Paper"}
    assert retitled["corpusid"] == 104
    archive_diff(ports, BASE, NEXT, [json.dumps(retitled)], ['{"corpusid": 103}'], dataset="papers")
    ran(ports, "load-diff", "--from-release", BASE, "--release", NEXT, "--dataset", "papers")
    assert indexed(ports)["outcome"].startswith("papers, abstracts")
    for dataset in ("abstracts", "tldrs"):
        archive_diff(ports, BASE, NEXT, [], [], dataset=dataset)
        ran(ports, "load-diff", "--from-release", BASE, "--release", NEXT, "--dataset", dataset)
    build_read_tables(ports)
    result = indexed(ports)
    assert result == {"outcome": "diff", "index": physical}
    assert indexed(ports) == {"outcome": "current", "index": physical}
    assert document(ports, "103") is None
    found = document(ports, "104")
    assert found is not None
    assert (found["_source"]["title"], found["_version"]) == ("A Retitled Paper", version_of(NEXT))
    assert all(document(ports, key) is not None for key in ("101", "102", "18446744073709551615"))
