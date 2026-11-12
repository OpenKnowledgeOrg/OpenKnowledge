"""Failures of the embeddings feature."""

from openknowledge_core.ingestion.errors import IngestionError


class ShardInvalid(IngestionError):
    """A raw record cannot be converted, so the shard writes nothing."""


class ConversionFailed(IngestionError):
    """A Batch job ended with shards still unconverted."""
