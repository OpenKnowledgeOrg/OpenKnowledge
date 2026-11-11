-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_sources
(
    source_id UInt64,
    display_name Nullable(String),
    `type` LowCardinality(Nullable(String)),
    issn_l Nullable(String),
    host_organization Nullable(String),
    host_organization_name Nullable(String),
    homepage_url Nullable(String),
    is_oa Nullable(Bool),
    is_in_doaj Nullable(Bool),
    is_core Nullable(Bool),
    country_code LowCardinality(Nullable(String)),
    updated_date DateTime,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY source_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_sources;
