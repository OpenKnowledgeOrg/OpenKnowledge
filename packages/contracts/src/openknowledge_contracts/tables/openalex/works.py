"""``works``: one row per work of the archived ``works`` entity.

The scalar metadata comes first; the columns after ``updated_date`` hold the rest of a work's own
record, with funders and awards referred to by numeric OpenAlex ID.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import (
    list_of,
    optional,
    required,
    string_map,
    struct_of,
)
from openknowledge_contracts.tables.openalex.fields import (
    LABEL,
    openalex_id,
    stored_fields,
)

_STRING = pa.string()
_INT = pa.int32()
_DOUBLE = pa.float64()

WORKS = DatasetContract(
    name="works",
    table="openalex_works",
    key=("work_id",),
    updates="snapshot",
    source=pa.schema(
        [
            required("work_id", pa.uint64()),
            optional("doi", _STRING),
            optional("doi_norm", _STRING),
            optional("pmid", pa.uint64()),
            optional("mag", pa.uint64()),
            optional("title", _STRING),
            optional("publication_year", pa.uint16()),
            optional("type", LABEL),
            optional("language", LABEL),
            required("is_xpac", pa.bool_()),
            optional("is_retracted", pa.bool_()),
            required("is_paratext", pa.bool_()),
            required("has_abstract", pa.bool_()),
            optional("oa_status", LABEL),
            optional("is_oa", pa.bool_()),
            optional("oa_url", _STRING),
            optional("any_repository_has_fulltext", pa.bool_()),
            optional("best_oa_pdf_url", _STRING),
            optional("best_oa_landing_page_url", _STRING),
            optional("best_oa_license", _STRING),
            optional("best_oa_version", _STRING),
            optional("best_oa_source_id", _STRING),
            optional("primary_landing_page_url", _STRING),
            optional("primary_source_id", _STRING),
            optional("has_pdf", pa.bool_()),
            optional("has_grobid_xml", pa.bool_()),
            optional("has_fulltext", pa.bool_()),
            required("updated_date", pa.timestamp("s")),
            *stored_fields(
                [
                    required("ids", string_map(nullable_values=True)),
                    optional("publication_date", pa.date32()),
                    optional("created_date", pa.timestamp("us", tz="UTC")),
                    optional(
                        "biblio",
                        struct_of(
                            optional("volume", _STRING),
                            optional("issue", _STRING),
                            optional("first_page", _STRING),
                            optional("last_page", _STRING),
                        ),
                    ),
                    optional(
                        "volume",
                        struct_of(optional("id", _STRING), optional("display_name", _STRING)),
                    ),
                    optional("cited_by_count", _INT),
                    required(
                        "counts_by_year",
                        list_of(
                            struct_of(optional("year", _INT), optional("cited_by_count", _INT))
                        ),
                    ),
                    required(
                        "awards",
                        list_of(
                            struct_of(
                                openalex_id("id", "G"),
                                optional("display_name", _STRING),
                                optional("funder_award_id", _STRING),
                                openalex_id("funder_id", "F"),
                                optional("funder_display_name", _STRING),
                            )
                        ),
                    ),
                    required(
                        "funders",
                        list_of(
                            struct_of(
                                openalex_id("id", "F"),
                                optional("display_name", _STRING),
                                optional("ror", _STRING),
                            )
                        ),
                    ),
                    optional(
                        "apc_list",
                        struct_of(
                            optional("value", _INT),
                            optional("currency", LABEL),
                            optional("value_usd", _INT),
                        ),
                    ),
                    optional(
                        "apc_paid",
                        struct_of(
                            optional("value", _DOUBLE),
                            optional("currency", LABEL),
                            optional("value_usd", _DOUBLE),
                        ),
                    ),
                    optional("fwci", _DOUBLE),
                    optional(
                        "citation_normalized_percentile",
                        struct_of(
                            optional("value", _DOUBLE),
                            optional("is_in_top_1_percent", pa.bool_()),
                            optional("is_in_top_10_percent", pa.bool_()),
                        ),
                    ),
                    optional(
                        "cited_by_percentile_year",
                        struct_of(optional("min", _INT), optional("max", _INT)),
                    ),
                ]
            ),
        ]
    ),
)
