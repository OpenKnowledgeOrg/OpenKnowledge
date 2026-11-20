"""Stored OpenAlex works projections from the archive footer schemas.

A work refers to sources, authors, institutions and other entities by their numeric OpenAlex ID.
What an entity's own table holds, such as a source's name or an institution's ROR, is not
repeated per work; what varies per work, such as an author's position, stays.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import (
    ArrowField,
    list_of,
    optional,
    required,
    struct_of,
)
from openknowledge_contracts.tables.openalex.fields import (
    LABEL,
    openalex_id,
    openalex_ids,
    stored_fields,
)

_STRING = pa.string()
_BOOL = pa.bool_()
_INT = pa.int32()
_FLOAT = pa.float32()
_DOUBLE = pa.float64()


def _work(group: str, fields: list[ArrowField]) -> DatasetContract:
    return DatasetContract(
        name=f"works-{group}",
        table=f"openalex_work_{group}",
        key=("work_id",),
        source=pa.schema([required("work_id", pa.uint64()), *stored_fields(fields)]),
        updates="snapshot",
        archive="works",
    )


_LOCATION = struct_of(
    optional("id", _STRING),
    optional("doi", _STRING),
    required("source", struct_of(openalex_id("id", "S"))),
    optional("is_oa", _BOOL),
    optional("is_published", _BOOL),
    optional("landing_page_url", _STRING),
    optional("pdf_url", _STRING),
    optional("raw_source_name", _STRING),
    optional("raw_type", LABEL),
    optional("provenance", LABEL),
    optional("license", LABEL),
    optional("license_id", LABEL),
    optional("version", LABEL),
    optional("is_accepted", _BOOL),
)

_SUBFIELD = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
)

_PRIMARY_TOPIC = struct_of(
    openalex_id("id", "T"),
    optional("display_name", _STRING),
    optional("score", _FLOAT),
    required("subfield", _SUBFIELD),
    required("field", _SUBFIELD),
    required("domain", _SUBFIELD),
)

_INSTITUTIONS = required("institutions", list_of(struct_of(openalex_id("id", "I"))))

WORK_PROJECTIONS = (
    _work(
        "authorships",
        [
            required(
                "authorships",
                list_of(
                    struct_of(
                        required(
                            "author",
                            struct_of(
                                optional("display_name", _STRING),
                                openalex_id("id", "A"),
                                optional("orcid", _STRING),
                                required("observed_orcids", list_of(_STRING, nullable_items=True)),
                            ),
                        ),
                        optional("author_position", LABEL),
                        required(
                            "affiliations",
                            list_of(
                                struct_of(
                                    openalex_ids("institution_ids", "I"),
                                    optional("raw_affiliation_string", _STRING),
                                )
                            ),
                        ),
                        required("countries", list_of(LABEL, nullable_items=True)),
                        optional("raw_author_name", _STRING),
                        optional("raw_orcid", _STRING),
                        optional("is_corresponding", _BOOL),
                        required("raw_affiliation_strings", list_of(_STRING, nullable_items=True)),
                        _INSTITUTIONS,
                    )
                ),
            ),
            optional("authors_count", _INT),
            openalex_ids("corresponding_author_ids", "A"),
            openalex_ids("corresponding_institution_ids", "I"),
            _INSTITUTIONS,
            optional("countries_distinct_count", _INT),
            optional("institutions_distinct_count", _INT),
        ],
    ),
    _work(
        "locations",
        [
            required("locations", list_of(_LOCATION)),
            optional("locations_count", _INT),
            optional("primary_location", _LOCATION),
            optional("best_oa_location", _LOCATION),
            required("indexed_in", list_of(LABEL, nullable_items=True)),
        ],
    ),
    _work(
        "references",
        [
            openalex_ids("referenced_works", "W"),
            openalex_ids("related_works", "W"),
            optional("referenced_works_count", _INT),
        ],
    ),
    _work(
        "classifications",
        [
            optional("primary_topic", _PRIMARY_TOPIC),
            required("topics", list_of(_PRIMARY_TOPIC)),
            required(
                "keywords",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("score", _FLOAT),
                    )
                ),
            ),
            required(
                "concepts",
                list_of(
                    struct_of(
                        openalex_id("id", "C"),
                        optional("wikidata", _STRING),
                        optional("display_name", _STRING),
                        optional("level", _INT),
                        optional("score", _FLOAT),
                    )
                ),
            ),
            required(
                "mesh",
                list_of(
                    struct_of(
                        optional("descriptor_ui", _STRING),
                        optional("descriptor_name", _STRING),
                        optional("qualifier_ui", _STRING),
                        optional("qualifier_name", _STRING),
                        optional("is_major_topic", _BOOL),
                    )
                ),
            ),
            required(
                "sustainable_development_goals",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("score", _DOUBLE),
                    )
                ),
            ),
            required("study_designs", list_of(_SUBFIELD)),
        ],
    ),
)
