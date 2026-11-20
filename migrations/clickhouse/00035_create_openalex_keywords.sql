-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_keywords
(
    id String,
    display_name Nullable(String),
    display_name_alternatives Array(Nullable(String)) CODEC(ZSTD(1)),
    description Nullable(String),
    ids Tuple(openalex Nullable(String), wikidata Nullable(String)) CODEC(ZSTD(1)),
    has_ids Bool,
    primary_topic Tuple(id Nullable(UInt64), display_name Nullable(String), score Nullable(Float64), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String))) CODEC(ZSTD(1)),
    has_primary_topic Bool,
    topics Array(Tuple(id Nullable(UInt64), display_name Nullable(String), score Nullable(Float64), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))) CODEC(ZSTD(1)),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    works_api_url Nullable(String),
    created_date Nullable(DateTime64(6, 'UTC')),
    updated_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id;

-- +goose Down
DROP TABLE IF EXISTS openalex_keywords;
