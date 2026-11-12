-- +goose NO TRANSACTION
-- +goose Up
-- Where each paper's SPECTER2 vector sits: row ``row`` of the Float32 file ``object`` in the
-- curated bucket. A delete keeps an empty ``object`` and outranks an update of the same release.
CREATE TABLE IF NOT EXISTS paper_embedding_locations
(
    corpusid UInt64,
    object LowCardinality(String),
    row UInt32,
    source_release Date,
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY corpusid
SETTINGS index_granularity = 1024;

-- +goose Down
DROP TABLE IF EXISTS paper_embedding_locations;
