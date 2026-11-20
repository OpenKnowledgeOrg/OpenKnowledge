-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_fields
(
    id String,
    display_name Nullable(String),
    description Nullable(String),
    ids Tuple(openalex Nullable(String), wikidata Nullable(String), wikipedia Nullable(String)) CODEC(ZSTD(1)),
    has_ids Bool,
    display_name_alternatives Array(Nullable(String)) CODEC(ZSTD(1)),
    domain Tuple(id Nullable(String), display_name Nullable(String)) CODEC(ZSTD(1)),
    has_domain Bool,
    subfields Array(Tuple(id Nullable(String), display_name Nullable(String))) CODEC(ZSTD(1)),
    siblings Array(Tuple(id Nullable(String), display_name Nullable(String))) CODEC(ZSTD(1)),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    works_api_url Nullable(String),
    updated_date Nullable(DateTime64(6, 'UTC')),
    created_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id;

-- +goose Down
DROP TABLE IF EXISTS openalex_fields;
