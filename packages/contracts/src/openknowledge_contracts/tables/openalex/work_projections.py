"""Stored OpenAlex works projections from the archive footer schemas."""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import (
    ArrowField,
    list_of,
    optional,
    required,
    string_map,
    struct_of,
)
from openknowledge_contracts.tables.openalex.fields import stored_fields

_STRING = pa.string()
_BOOL = pa.bool_()
_INT = pa.int32()
_FLOAT = pa.float32()
_DOUBLE = pa.float64()
_TIMESTAMP = pa.timestamp("us", tz="UTC")


def _work(group: str, fields: list[ArrowField]) -> DatasetContract:
    return DatasetContract(
        name=f"works-{group}",
        table=f"openalex_work_{group}",
        key=("work_id",),
        source=pa.schema([required("work_id", pa.uint64()), *stored_fields(fields)]),
        updates="snapshot",
        archive="works",
    )


_SOURCE = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    optional("issn_l", _STRING),
    required("issn", list_of(_STRING, nullable_items=True)),
    optional("is_oa", _BOOL),
    optional("is_in_doaj", _BOOL),
    optional("is_core", _BOOL),
    required("listed_in", list_of(_STRING, nullable_items=True)),
    optional("host_organization", _STRING),
    optional("host_organization_name", _STRING),
    required("host_organization_lineage", list_of(_STRING, nullable_items=True)),
    required("host_organization_lineage_names", list_of(_STRING, nullable_items=True)),
    optional("type", _STRING),
)

_LOCATION = struct_of(
    optional("id", _STRING),
    optional("doi", _STRING),
    required("source", _SOURCE),
    optional("is_oa", _BOOL),
    optional("is_published", _BOOL),
    optional("landing_page_url", _STRING),
    optional("pdf_url", _STRING),
    optional("raw_source_name", _STRING),
    optional("raw_type", _STRING),
    optional("provenance", _STRING),
    optional("license", _STRING),
    optional("license_id", _STRING),
    optional("version", _STRING),
    optional("is_accepted", _BOOL),
)

_SUBFIELD = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
)

_PRIMARY_TOPIC = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    optional("score", _FLOAT),
    required("subfield", _SUBFIELD),
    required("field", _SUBFIELD),
    required("domain", _SUBFIELD),
)

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
                                optional("id", _STRING),
                                optional("orcid", _STRING),
                                required("observed_orcids", list_of(_STRING, nullable_items=True)),
                            ),
                        ),
                        optional("author_position", _STRING),
                        required(
                            "affiliations",
                            list_of(
                                struct_of(
                                    required(
                                        "institution_ids", list_of(_STRING, nullable_items=True)
                                    ),
                                    optional("raw_affiliation_string", _STRING),
                                )
                            ),
                        ),
                        required("countries", list_of(_STRING, nullable_items=True)),
                        optional("raw_author_name", _STRING),
                        optional("raw_orcid", _STRING),
                        optional("is_corresponding", _BOOL),
                        required("raw_affiliation_strings", list_of(_STRING, nullable_items=True)),
                        required(
                            "institutions",
                            list_of(
                                struct_of(
                                    optional("country_code", _STRING),
                                    optional("display_name", _STRING),
                                    optional("id", _STRING),
                                    required("lineage", list_of(_STRING, nullable_items=True)),
                                    optional("ror", _STRING),
                                    optional("type", _STRING),
                                )
                            ),
                        ),
                    )
                ),
            ),
            optional("authors_count", _INT),
            required("corresponding_author_ids", list_of(_STRING, nullable_items=True)),
            required("corresponding_institution_ids", list_of(_STRING, nullable_items=True)),
            required(
                "institutions",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("ror", _STRING),
                        optional("country_code", _STRING),
                        optional("type", _STRING),
                        required("lineage", list_of(_STRING, nullable_items=True)),
                    )
                ),
            ),
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
            required("indexed_in", list_of(_STRING, nullable_items=True)),
        ],
    ),
    _work(
        "references",
        [
            required("referenced_works", list_of(pa.uint64())),
            required("related_works", list_of(pa.uint64())),
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
                        optional("id", _STRING),
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
    _work(
        "details",
        [
            optional("display_name", _STRING),
            required("ids", string_map(nullable_values=True)),
            optional("publication_date", pa.date32()),
            optional("created_date", _TIMESTAMP),
            optional(
                "biblio",
                struct_of(
                    optional("volume", _STRING),
                    optional("issue", _STRING),
                    optional("first_page", _STRING),
                    optional("last_page", _STRING),
                ),
            ),
            optional("volume", _SUBFIELD),
            optional("cited_by_count", _INT),
            required(
                "counts_by_year",
                list_of(
                    struct_of(
                        optional("year", _INT),
                        optional("cited_by_count", _INT),
                    )
                ),
            ),
            required(
                "awards",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("funder_award_id", _STRING),
                        optional("funder_id", _STRING),
                        optional("funder_display_name", _STRING),
                    )
                ),
            ),
            required(
                "funders",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("ror", _STRING),
                    )
                ),
            ),
            optional(
                "apc_list",
                struct_of(
                    optional("value", _INT),
                    optional("currency", _STRING),
                    optional("value_usd", _INT),
                ),
            ),
            optional(
                "apc_paid",
                struct_of(
                    optional("value", _DOUBLE),
                    optional("currency", _STRING),
                    optional("value_usd", _DOUBLE),
                ),
            ),
            optional("fwci", _DOUBLE),
            optional(
                "citation_normalized_percentile",
                struct_of(
                    optional("value", _DOUBLE),
                    optional("is_in_top_1_percent", _BOOL),
                    optional("is_in_top_10_percent", _BOOL),
                ),
            ),
            optional(
                "cited_by_percentile_year",
                struct_of(
                    optional("min", _INT),
                    optional("max", _INT),
                ),
            ),
            optional(
                "open_access",
                struct_of(
                    optional("is_oa", _BOOL),
                    optional("oa_status", _STRING),
                    optional("any_repository_has_fulltext", _BOOL),
                    optional("oa_url", _STRING),
                ),
            ),
            optional(
                "has_content",
                struct_of(
                    optional("pdf", _BOOL),
                    optional("grobid_xml", _BOOL),
                ),
            ),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
)
