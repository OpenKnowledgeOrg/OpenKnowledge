"""Failures of the embeddings feature."""

from openknowledge_core.ingestion.errors import IngestionError


class ShardInvalid(IngestionError):
    """A raw record cannot be converted, so the shard writes nothing."""


class ConversionFailed(IngestionError):
    """A Batch job ended with shards still unconverted."""


class IndexIncomplete(IngestionError):
    """Some archived shards lack their converted output, so no index loads."""


class NoIndexLoaded(IngestionError):
    """The location table holds no loaded release, so nothing can be located."""


class TooManyIds(IngestionError):
    """A lookup asked for more papers than one call reads."""
