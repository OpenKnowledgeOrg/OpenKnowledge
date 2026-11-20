-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_sdgs
(
    id String,
    ids Tuple(openalex Nullable(String), un Nullable(String), wikidata Nullable(String)),
    has_ids Bool,
    display_name Nullable(String),
    description Nullable(String),
    works_count Nullable(Int64),
    cited_by_count Nullable(Int64),
    image_url Nullable(String),
    image_thumbnail_url Nullable(String),
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
DROP TABLE IF EXISTS openalex_sdgs;
