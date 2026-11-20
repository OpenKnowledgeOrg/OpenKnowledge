from openknowledge_contracts.sources import SOURCES, contract_for
from openknowledge_contracts.tables.clickhouse import table_columns
from openknowledge_contracts.tables.openalex import WORKS, WORKS_ABSTRACTS
from openknowledge_contracts.tables.s2 import AUTHORS as S2_AUTHORS


def test_the_works_tables_have_the_agreed_columns() -> None:
    columns = [(c.name, c.type) for c in table_columns(WORKS)]
    assert columns[:4] == [
        ("work_id", "UInt64"),
        ("doi", "Nullable(String)"),
        ("doi_norm", "Nullable(String)"),
        ("pmid", "Nullable(UInt64)"),
    ]
    assert ("type", "LowCardinality(Nullable(String))") in columns
    assert ("updated_date", "DateTime") in columns
    assert columns[-5:] == [
        ("source_release", "Date"),
        ("source_object", "LowCardinality(String)"),
        ("is_deleted", "UInt8"),
        ("version", "UInt64"),
        ("removed", "Bool"),
    ]
    assert [(c.name, c.type) for c in table_columns(WORKS_ABSTRACTS)][:2] == [
        ("work_id", "UInt64"),
        ("abstract", "String"),
    ]
    assert WORKS_ABSTRACTS.archived == "works"
    assert contract_for("works-abstracts") == (SOURCES["openalex"], WORKS_ABSTRACTS)


def test_openalex_archives_have_contracts_and_authors_keep_their_own_name() -> None:
    source = SOURCES["openalex"]
    assert {c.archived for c in source.contracts.values()} == source.datasets.keys() - {
        "works-deleted"
    }
    assert contract_for("authors") == (SOURCES["semantic-scholar"], S2_AUTHORS)
    _, authors = contract_for("openalex-authors")
    assert (authors.table, authors.archived, authors.key) == (
        "openalex_authors",
        "authors",
        ("author_id",),
    )
