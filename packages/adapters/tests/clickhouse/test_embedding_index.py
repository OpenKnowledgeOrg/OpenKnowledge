import io

import pyarrow as pa
import pyarrow.parquet as pq

from openknowledge_adapters.clickhouse.embedding_index import read_index, write_index
from openknowledge_core.embeddings import IndexRow


def test_index_round_trips_through_parquet_with_the_loaded_column_types() -> None:
    rows = [IndexRow(3, 0, False), IndexRow(2**64 - 1, 2**32 - 1, False), IndexRow(7, 0, True)]
    sink = io.BytesIO()

    write_index(rows, sink)

    sink.seek(0)
    assert read_index(sink) == rows
    assert pq.ParquetFile(sink).schema_arrow == pa.schema(
        [
            pa.field("corpusid", pa.uint64(), nullable=False),
            pa.field("row", pa.uint32(), nullable=False),
            pa.field("is_deleted", pa.uint8(), nullable=False),
        ]
    )
