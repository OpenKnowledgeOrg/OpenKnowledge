"""SPECTER2 vectors as flat Float32 shards, found through a corpusid index."""

from openknowledge_core.embeddings.convert import convert_shard
from openknowledge_core.embeddings.models import ROW_BYTES, VECTOR_DIM, IndexRow, ShardPart

__all__ = ["ROW_BYTES", "VECTOR_DIM", "IndexRow", "ShardPart", "convert_shard"]
