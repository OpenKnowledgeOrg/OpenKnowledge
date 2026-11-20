-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_continents
(
    id String,
    display_name Nullable(String),
    wikidata_id Nullable(String),
    wikidata_url Nullable(String),
    wikipedia_url Nullable(String),
    display_name_alternatives Array(Nullable(String)) CODEC(ZSTD(1)),
    description Nullable(String),
    ids Tuple(openalex Nullable(String), wikidata Nullable(String)) CODEC(ZSTD(1)),
    has_ids Bool,
    created_date Nullable(DateTime64(6, 'UTC')),
    updated_date Nullable(DateTime64(6, 'UTC')),
    countries Array(Tuple(id Nullable(String), display_name Nullable(String))) CODEC(ZSTD(1)),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id;

-- +goose Down
DROP TABLE IF EXISTS openalex_continents;
