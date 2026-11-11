"""``works-abstracts``: each work's abstract, rebuilt from the inverted index of ``works``.

One row per work, with an empty abstract where the work has none, so the table writes one row
per Parquet row as the load's row counts require.
"""

import pyarrow as pa

from openknowledge_contracts.tables.contract import DatasetContract
from openknowledge_contracts.tables.fields import required

WORKS_ABSTRACTS = DatasetContract(
    name="works-abstracts",
    table="openalex_abstracts",
    key=("work_id",),
    updates="snapshot",
    archive="works",
    source=pa.schema([required("work_id", pa.uint64()), required("abstract", pa.string())]),
)
