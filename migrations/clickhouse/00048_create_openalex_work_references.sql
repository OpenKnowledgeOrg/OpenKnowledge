-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_work_references
(
    work_id UInt64,
    referenced_works Array(Nullable(UInt64)) CODEC(ZSTD(1)),
    related_works Array(Nullable(UInt64)) CODEC(ZSTD(1)),
    referenced_works_count Nullable(Int32),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY work_id
SETTINGS index_granularity = 1024;

-- +goose Down
DROP TABLE IF EXISTS openalex_work_references;
