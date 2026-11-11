from dataclasses import replace

import pytest

from openknowledge_contracts.sources import SOURCES, check_sources, contract_for
from openknowledge_contracts.tables.s2 import CONTRACTS, PAPERS


def test_the_s2_source_holds_every_s2_contract_as_gzip_lines() -> None:
    s2 = SOURCES["semantic-scholar"]
    assert s2.prefix == "semantic-scholar"
    assert dict(s2.contracts) == dict(CONTRACTS)
    assert {s2.datasets[name].format for name in s2.datasets} == {"jsonl-gz"}
    assert set(s2.datasets) == set(CONTRACTS) | {"s2orc_v2", "embeddings-specter_v2"}
    assert contract_for("papers") == (s2, PAPERS)
    assert PAPERS.archived == "papers"
    with pytest.raises(KeyError):
        contract_for("works-deleted")


def test_a_registry_refuses_a_repeated_contract_and_an_unarchived_dataset() -> None:
    s2 = SOURCES["semantic-scholar"]
    twin = replace(s2, name="openalex")
    with pytest.raises(ValueError, match="more than one source"):
        check_sources({"semantic-scholar": s2, "openalex": twin})
    lost = replace(s2, contracts={"papers": replace(PAPERS, archive="works")})
    with pytest.raises(ValueError, match="not archived"):
        check_sources({"semantic-scholar": lost})
