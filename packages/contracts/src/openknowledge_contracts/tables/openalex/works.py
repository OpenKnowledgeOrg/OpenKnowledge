"""``works``: one row of scalar work metadata per work of the archived ``works`` entity."""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import optional, required

_LABEL = pa.dictionary(pa.int32(), pa.string())

WORKS = DatasetContract(
    name="works",
    table="openalex_works",
    key=("work_id",),
    updates="snapshot",
    source=pa.schema(
        [
            required("work_id", pa.uint64()),
            optional("doi", pa.string()),
            optional("doi_norm", pa.string()),
            optional("pmid", pa.uint64()),
            optional("mag", pa.uint64()),
            optional("title", pa.string()),
            optional("publication_year", pa.uint16()),
            optional("type", _LABEL),
            optional("language", _LABEL),
            required("is_xpac", pa.bool_()),
            required("is_retracted", pa.bool_()),
            required("is_paratext", pa.bool_()),
            required("has_abstract", pa.bool_()),
            optional("oa_status", _LABEL),
            optional("is_oa", pa.bool_()),
            optional("oa_url", pa.string()),
            optional("any_repository_has_fulltext", pa.bool_()),
            optional("best_oa_pdf_url", pa.string()),
            optional("best_oa_landing_page_url", pa.string()),
            optional("best_oa_license", pa.string()),
            optional("best_oa_version", pa.string()),
            optional("best_oa_source_id", pa.string()),
            optional("primary_landing_page_url", pa.string()),
            optional("primary_source_id", pa.string()),
            optional("has_pdf", pa.bool_()),
            optional("has_grobid_xml", pa.bool_()),
            optional("has_fulltext", pa.bool_()),
            required("updated_date", pa.timestamp("s")),
        ]
    ),
)
