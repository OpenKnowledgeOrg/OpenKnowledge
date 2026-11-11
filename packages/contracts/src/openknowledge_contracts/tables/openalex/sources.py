"""``sources``: one row per journal, repository or other venue OpenAlex lists as a source."""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import optional, required

_LABEL = pa.dictionary(pa.int32(), pa.string())

OPENALEX_SOURCES = DatasetContract(
    name="sources",
    table="openalex_sources",
    key=("source_id",),
    updates="snapshot",
    source=pa.schema(
        [
            required("source_id", pa.uint64()),
            optional("display_name", pa.string()),
            optional("type", _LABEL),
            optional("issn_l", pa.string()),
            optional("host_organization", pa.string()),
            optional("host_organization_name", pa.string()),
            optional("homepage_url", pa.string()),
            optional("is_oa", pa.bool_()),
            optional("is_in_doaj", pa.bool_()),
            optional("is_core", pa.bool_()),
            optional("country_code", _LABEL),
            required("updated_date", pa.timestamp("s")),
        ]
    ),
)
