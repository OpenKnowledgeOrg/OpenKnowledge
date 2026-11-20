"""Stored OpenAlex entity records from the archive footer schemas."""

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


def _entity(name: str, fields: list[ArrowField], *, archive: str | None = None) -> DatasetContract:
    entity = archive or name
    key = (
        required(f"{entity.removesuffix('s')}_id", pa.uint64())
        if entity in ID_PREFIXES
        else required("id", pa.string())
    )
    return DatasetContract(
        name=name,
        table=f"openalex_{name.removeprefix('openalex-').replace('-', '_')}",
        key=(key.name,),
        source=pa.schema([key, *stored_fields(fields)]),
        updates="snapshot",
        archive=archive,
    )


_SUMMARY_STATS = struct_of(
    optional("2yr_mean_citedness", _DOUBLE),
    optional("h_index", _INT),
    optional("i10_index", _INT),
)

_INSTITUTION = struct_of(
    optional("id", _STRING),
    optional("ror", _STRING),
    optional("display_name", _STRING),
    optional("country_code", _STRING),
    optional("type", _STRING),
    required("lineage", list_of(_STRING, nullable_items=True)),
)

_SUBFIELD = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
)

_TOPIC_COUNT = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    optional("count", _INT),
    required("subfield", _SUBFIELD),
    required("field", _SUBFIELD),
    required("domain", _SUBFIELD),
)

_TOPIC_SHARE = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    optional("value", _DOUBLE),
    required("subfield", _SUBFIELD),
    required("field", _SUBFIELD),
    required("domain", _SUBFIELD),
)

_COUNTS_BY_YEAR = struct_of(
    optional("year", _INT),
    optional("works_count", _INT),
    optional("oa_works_count", _INT),
    optional("cited_by_count", _INT),
)

_ROLE = struct_of(
    optional("role", _STRING),
    optional("id", _STRING),
    optional("works_count", _INT),
)

_IDS = struct_of(
    optional("openalex", _STRING),
    optional("wikidata", _STRING),
    optional("wikipedia", _STRING),
)

_PRIMARY_TOPIC = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    optional("score", _DOUBLE),
    required("subfield", _SUBFIELD),
    required("field", _SUBFIELD),
    required("domain", _SUBFIELD),
)

_ID = struct_of(
    optional("id", _STRING),
    optional("type", _STRING),
    optional("asserted_by", _STRING),
)

_AFFILIATION = struct_of(
    optional("name", _STRING),
    optional("country", _STRING),
    required("ids", list_of(_ID)),
)

_LEAD_INVESTIGATOR = struct_of(
    optional("given_name", _STRING),
    optional("family_name", _STRING),
    optional("orcid", _STRING),
    optional("role_start", pa.date32()),
    required("affiliation", _AFFILIATION),
)

_INSTITUTION_AWARDED = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    optional("ror", _STRING),
    optional("country_code", _STRING),
    optional("type", _STRING),
    required("lineage", list_of(_STRING, nullable_items=True)),
)

_PARENT_AWARD = struct_of(
    optional("id", _STRING),
    optional("display_name", _STRING),
    required("funder", _SUBFIELD),
    optional("relationship", _STRING),
    optional("asserted_by", _STRING),
)

ID_PREFIXES = {
    "sources": "S",
    "authors": "A",
    "institutions": "I",
    "publishers": "P",
    "funders": "F",
    "topics": "T",
    "concepts": "C",
    "awards": "G",
}

ENTITIES = (
    _entity(
        "openalex-authors",
        [
            optional("display_name", _STRING),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            required("raw_author_names", list_of(_STRING, nullable_items=True)),
            optional("full_name", _STRING),
            optional("orcid", _STRING),
            required("observed_orcids", list_of(_STRING, nullable_items=True)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("summary_stats", _SUMMARY_STATS),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("orcid", _STRING),
                    required("observed_orcids", list_of(_STRING, nullable_items=True)),
                ),
            ),
            required(
                "affiliations",
                list_of(
                    struct_of(
                        required("institution", _INSTITUTION),
                        required("years", list_of(_INT, nullable_items=True)),
                    )
                ),
            ),
            required("last_known_institutions", list_of(_INSTITUTION)),
            required("topics", list_of(_TOPIC_COUNT)),
            required("topic_share", list_of(_TOPIC_SHARE)),
            required(
                "x_concepts",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("wikidata", _STRING),
                        optional("display_name", _STRING),
                        optional("level", _INT),
                        optional("score", _FLOAT),
                        optional("count", _INT),
                    )
                ),
            ),
            required(
                "sources",
                list_of(
                    struct_of(
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
                        required(
                            "host_organization_lineage", list_of(_STRING, nullable_items=True)
                        ),
                        required(
                            "host_organization_lineage_names", list_of(_STRING, nullable_items=True)
                        ),
                        optional("type", _STRING),
                    )
                ),
            ),
            required("counts_by_year", list_of(_COUNTS_BY_YEAR)),
            optional("works_api_url", _STRING),
            optional("updated_date", _TIMESTAMP),
            optional("created_date", _TIMESTAMP),
        ],
        archive="authors",
    ),
    _entity(
        "institutions",
        [
            optional("ror", _STRING),
            optional("display_name", _STRING),
            optional("country_code", _STRING),
            optional("type", _STRING),
            required("lineage", list_of(_STRING, nullable_items=True)),
            optional("is_super_system", _BOOL),
            optional("type_id", _STRING),
            optional("homepage_url", _STRING),
            optional("image_url", _STRING),
            optional("image_thumbnail_url", _STRING),
            required("display_name_acronyms", list_of(_STRING, nullable_items=True)),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("ror", _STRING),
                    optional("grid", _STRING),
                    optional("wikipedia", _STRING),
                    optional("wikidata", _STRING),
                ),
            ),
            required("roles", list_of(_ROLE)),
            required(
                "repositories",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("host_organization", _STRING),
                        optional("host_organization_name", _STRING),
                        required(
                            "host_organization_lineage", list_of(_STRING, nullable_items=True)
                        ),
                    )
                ),
            ),
            optional(
                "geo",
                struct_of(
                    optional("city", _STRING),
                    optional("geonames_city_id", _STRING),
                    optional("region", _STRING),
                    optional("country_code", _STRING),
                    optional("country", _STRING),
                    optional("latitude", _DOUBLE),
                    optional("longitude", _DOUBLE),
                ),
            ),
            required(
                "topics",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("count", _INT),
                        optional("score", _FLOAT),
                        required("subfield", _SUBFIELD),
                        required("field", _SUBFIELD),
                        required("domain", _SUBFIELD),
                    )
                ),
            ),
            required("topic_share", list_of(_TOPIC_SHARE)),
            required(
                "associated_institutions",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("ror", _STRING),
                        optional("display_name", _STRING),
                        optional("country_code", _STRING),
                        optional("type", _STRING),
                        optional("relationship", _STRING),
                    )
                ),
            ),
            required("counts_by_year", list_of(_COUNTS_BY_YEAR)),
            optional("summary_stats", _SUMMARY_STATS),
            optional("status", _STRING),
            optional("works_api_url", _STRING),
            optional("updated_date", _TIMESTAMP),
            optional("created_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "publishers",
        [
            required("lineage", list_of(_STRING, nullable_items=True)),
            optional("display_name", _STRING),
            required("alternate_titles", list_of(_STRING, nullable_items=True)),
            required("country_codes", list_of(_STRING, nullable_items=True)),
            optional("hierarchy_level", _INT),
            optional("parent_publisher", _SUBFIELD),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("ror", _STRING),
                    optional("wikidata", _STRING),
                ),
            ),
            optional("ror_id", _STRING),
            optional("image_url", _STRING),
            optional("image_thumbnail_url", _STRING),
            optional("wikidata_id", _STRING),
            optional("homepage_url", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("summary_stats", _SUMMARY_STATS),
            required("roles", list_of(_ROLE)),
            required(
                "counts_by_year",
                list_of(
                    struct_of(
                        optional("year", _INT),
                        optional("works_count", pa.int64()),
                        optional("cited_by_count", pa.int64()),
                    )
                ),
            ),
            optional("sources_api_url", _STRING),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "funders",
        [
            optional("display_name", _STRING),
            required("alternate_titles", list_of(_STRING, nullable_items=True)),
            optional("country_code", _STRING),
            optional("description", _STRING),
            optional("homepage_url", _STRING),
            optional("image_url", _STRING),
            optional("image_thumbnail_url", _STRING),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("ror", _STRING),
                    optional("wikidata", _STRING),
                    optional("crossref", pa.int64()),
                    optional("doi", _STRING),
                ),
            ),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("awards_count", pa.int64()),
            required("roles", list_of(_ROLE)),
            required("counts_by_year", list_of(_COUNTS_BY_YEAR)),
            optional("summary_stats", _SUMMARY_STATS),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "topics",
        [
            optional("display_name", _STRING),
            optional("description", _STRING),
            required(
                "keywords",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("score", _DOUBLE),
                    )
                ),
            ),
            optional("legacy_keywords", _STRING),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("wikipedia", _STRING),
                ),
            ),
            optional("subfield", _SUBFIELD),
            optional("field", _SUBFIELD),
            optional("domain", _SUBFIELD),
            required("siblings", list_of(_SUBFIELD)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("works_api_url", _STRING),
            optional("updated_date", _TIMESTAMP),
            optional("created_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "subfields",
        [
            optional("display_name", _STRING),
            optional("description", _STRING),
            optional("ids", _IDS),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            optional("field", _SUBFIELD),
            optional("domain", _SUBFIELD),
            required("topics", list_of(_SUBFIELD)),
            required("siblings", list_of(_SUBFIELD)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("works_api_url", _STRING),
            optional("updated_date", _TIMESTAMP),
            optional("created_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "fields",
        [
            optional("display_name", _STRING),
            optional("description", _STRING),
            optional("ids", _IDS),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            optional("domain", _SUBFIELD),
            required("subfields", list_of(_SUBFIELD)),
            required("siblings", list_of(_SUBFIELD)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("works_api_url", _STRING),
            optional("updated_date", _TIMESTAMP),
            optional("created_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "domains",
        [
            optional("display_name", _STRING),
            optional("description", _STRING),
            optional("ids", _IDS),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            required("fields", list_of(_SUBFIELD)),
            required("siblings", list_of(_SUBFIELD)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("works_api_url", _STRING),
            optional("updated_date", _TIMESTAMP),
            optional("created_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "concepts",
        [
            optional("display_name", _STRING),
            optional("level", _INT),
            optional("description", _STRING),
            optional("wikidata", _STRING),
            optional("image_url", _STRING),
            optional("image_thumbnail_url", _STRING),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("wikidata", _STRING),
                    optional("wikipedia", _STRING),
                    required("umls_aui", list_of(_STRING, nullable_items=True)),
                    required("umls_cui", list_of(_STRING, nullable_items=True)),
                    optional("mag", _STRING),
                ),
            ),
            optional("works_api_url", _STRING),
            optional("summary_stats", _SUMMARY_STATS),
            required("international", string_map(nullable_values=True)),
            required("ancestors", list_of(_STRING, nullable_items=True)),
            required("related_concepts", list_of(_STRING, nullable_items=True)),
            required("counts_by_year", list_of(_STRING, nullable_items=True)),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "keywords",
        [
            optional("display_name", _STRING),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            optional("description", _STRING),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("wikidata", _STRING),
                ),
            ),
            optional("primary_topic", _PRIMARY_TOPIC),
            required("topics", list_of(_PRIMARY_TOPIC)),
            optional("works_count", _INT),
            optional("cited_by_count", _INT),
            optional("works_api_url", _STRING),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "awards",
        [
            optional("display_name", _STRING),
            optional("description", _STRING),
            optional("funder_award_id", _STRING),
            optional("amount", _DOUBLE),
            optional("currency", _STRING),
            optional(
                "funder",
                struct_of(
                    optional("id", _STRING),
                    optional("display_name", _STRING),
                    optional("ror_id", _STRING),
                    optional("doi", _STRING),
                ),
            ),
            optional("funding_type", _STRING),
            optional("funder_scheme", _STRING),
            optional("provenance", _STRING),
            optional("start_date", pa.date32()),
            optional("end_date", pa.date32()),
            optional("start_year", pa.int64()),
            optional("end_year", pa.int64()),
            optional("lead_investigator", _LEAD_INVESTIGATOR),
            optional("co_lead_investigator", _LEAD_INVESTIGATOR),
            required("investigators", list_of(_LEAD_INVESTIGATOR)),
            optional("landing_page_url", _STRING),
            optional("doi", _STRING),
            optional("works_api_url", _STRING),
            optional("created_date", _TIMESTAMP),
            required("funded_outputs", list_of(_STRING, nullable_items=True)),
            optional("funded_outputs_count", pa.int64()),
            optional(
                "primary_topic",
                struct_of(
                    optional("id", _STRING),
                    optional("display_name", _STRING),
                    optional("score", _FLOAT),
                    required("subfield", _SUBFIELD),
                    required("field", _SUBFIELD),
                    required("domain", _SUBFIELD),
                ),
            ),
            required(
                "topics",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("score", _FLOAT),
                        required("subfield", _SUBFIELD),
                        required("field", _SUBFIELD),
                        required("domain", _SUBFIELD),
                    )
                ),
            ),
            required("institution_awarded", list_of(_INSTITUTION_AWARDED)),
            required("institution_awarded_full", list_of(_INSTITUTION_AWARDED)),
            optional(
                "primary_topic_full",
                struct_of(
                    optional("id", _STRING),
                    optional("display_name", _STRING),
                    optional("score", _FLOAT),
                    required("subfield", _SUBFIELD),
                    required("field", _SUBFIELD),
                    required("domain", _SUBFIELD),
                ),
            ),
            required(
                "topics_full",
                list_of(
                    struct_of(
                        optional("id", _STRING),
                        optional("display_name", _STRING),
                        optional("score", _FLOAT),
                        required("subfield", _SUBFIELD),
                        required("field", _SUBFIELD),
                        required("domain", _SUBFIELD),
                    )
                ),
            ),
            required("parent_awards", list_of(_PARENT_AWARD)),
            required("sub_awards", list_of(_PARENT_AWARD)),
            optional("sub_awards_count", pa.int64()),
            required("parent_awards_full", list_of(_PARENT_AWARD)),
            required("sub_awards_full", list_of(_PARENT_AWARD)),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "continents",
        [
            optional("display_name", _STRING),
            optional("wikidata_id", _STRING),
            optional("wikidata_url", _STRING),
            optional("wikipedia_url", _STRING),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            optional("description", _STRING),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("wikidata", _STRING),
                ),
            ),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
            required("countries", list_of(_SUBFIELD)),
        ],
    ),
    _entity(
        "countries",
        [
            optional("country_code", _STRING),
            optional("display_name", _STRING),
            optional("continent_id", _INT),
            optional("is_global_south", _BOOL),
            optional("wikidata_url", _STRING),
            optional("wikipedia_url", _STRING),
            required("display_name_alternatives", list_of(_STRING, nullable_items=True)),
            optional("description", _STRING),
            optional("alpha_3", _STRING),
            optional("numeric", _INT),
            optional("full_name", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("authors_api_url", _STRING),
            optional("institutions_api_url", _STRING),
            optional("works_api_url", _STRING),
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("iso", _STRING),
                    optional("wikidata", _STRING),
                    optional("wikipedia", _STRING),
                ),
            ),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
            optional("continent", _SUBFIELD),
        ],
    ),
    _entity(
        "institution-types",
        [
            optional("display_name", _STRING),
            optional("works_api_url", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "languages",
        [
            optional("works_api_url", _STRING),
            optional("display_name", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "licenses",
        [
            optional("display_name", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("url", _STRING),
            optional("description", _STRING),
            optional("works_api_url", _STRING),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "sdgs",
        [
            optional(
                "ids",
                struct_of(
                    optional("openalex", _STRING),
                    optional("un", _STRING),
                    optional("wikidata", _STRING),
                ),
            ),
            optional("display_name", _STRING),
            optional("description", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("image_url", _STRING),
            optional("image_thumbnail_url", _STRING),
            optional("works_api_url", _STRING),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "source-types",
        [
            optional("display_name", _STRING),
            optional("works_api_url", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
    _entity(
        "work-types",
        [
            optional("works_api_url", _STRING),
            optional("display_name", _STRING),
            optional("description", _STRING),
            optional("works_count", pa.int64()),
            optional("cited_by_count", pa.int64()),
            optional("created_date", _TIMESTAMP),
            optional("updated_date", _TIMESTAMP),
        ],
    ),
)


SOURCES_FULL = _entity(
    "sources-full",
    [
        optional("issn_l", _STRING),
        required("issn", list_of(_STRING, nullable_items=True)),
        optional("display_name", _STRING),
        optional("host_organization", _STRING),
        optional("host_organization_name", _STRING),
        required("host_organization_lineage", list_of(_STRING, nullable_items=True)),
        optional("works_count", _INT),
        optional("oa_works_count", _INT),
        optional("cited_by_count", _INT),
        optional("summary_stats", _SUMMARY_STATS),
        optional("is_oa", _BOOL),
        optional("is_in_doaj", _BOOL),
        optional("is_in_doaj_since_year", _INT),
        optional("is_high_oa_rate", _BOOL),
        optional("is_high_oa_rate_since_year", pa.int64()),
        optional("is_in_scielo", _BOOL),
        optional("is_ojs", _BOOL),
        optional("is_core", _BOOL),
        required("listed_in", list_of(_STRING, nullable_items=True)),
        optional("is_preprint_repository", _BOOL),
        optional("oa_flip_year", _INT),
        optional("first_publication_year", _INT),
        optional("last_publication_year", _INT),
        optional(
            "ids",
            struct_of(
                optional("openalex", _STRING),
                optional("issn_l", _STRING),
                required("issn", list_of(_STRING, nullable_items=True)),
                optional("mag", _STRING),
                optional("wikidata", _STRING),
            ),
        ),
        optional("homepage_url", _STRING),
        required(
            "apc_prices",
            list_of(struct_of(optional("price", _INT), optional("currency", _STRING))),
        ),
        optional("apc_usd", _INT),
        required(
            "apc_usd_by_year",
            list_of(struct_of(optional("year", _INT), optional("price", _INT))),
        ),
        optional("country_code", _STRING),
        required(
            "societies",
            list_of(struct_of(optional("url", _STRING), optional("organization", _STRING))),
        ),
        required("alternate_titles", list_of(_STRING, nullable_items=True)),
        optional("type", _STRING),
        required("topics", list_of(_TOPIC_COUNT)),
        required("topic_share", list_of(_TOPIC_SHARE)),
        required("counts_by_year", list_of(_COUNTS_BY_YEAR)),
        optional("works_api_url", _STRING),
        optional("updated_date", _TIMESTAMP),
        optional("created_date", _TIMESTAMP),
    ],
    archive="sources",
)
