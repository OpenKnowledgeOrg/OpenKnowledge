-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_work_locations
(
    work_id UInt64,
    locations Array(Tuple(id Nullable(String), doi Nullable(String), source Tuple(id Nullable(UInt64)), is_oa Nullable(Bool), is_published Nullable(Bool), landing_page_url Nullable(String), pdf_url Nullable(String), raw_source_name Nullable(String), raw_type LowCardinality(Nullable(String)), provenance LowCardinality(Nullable(String)), license LowCardinality(Nullable(String)), license_id LowCardinality(Nullable(String)), version LowCardinality(Nullable(String)), is_accepted Nullable(Bool))) CODEC(ZSTD(1)),
    locations_count Nullable(Int32),
    primary_location Tuple(id Nullable(String), doi Nullable(String), source Tuple(id Nullable(UInt64)), is_oa Nullable(Bool), is_published Nullable(Bool), landing_page_url Nullable(String), pdf_url Nullable(String), raw_source_name Nullable(String), raw_type LowCardinality(Nullable(String)), provenance LowCardinality(Nullable(String)), license LowCardinality(Nullable(String)), license_id LowCardinality(Nullable(String)), version LowCardinality(Nullable(String)), is_accepted Nullable(Bool)) CODEC(ZSTD(1)),
    has_primary_location Bool,
    best_oa_location Tuple(id Nullable(String), doi Nullable(String), source Tuple(id Nullable(UInt64)), is_oa Nullable(Bool), is_published Nullable(Bool), landing_page_url Nullable(String), pdf_url Nullable(String), raw_source_name Nullable(String), raw_type LowCardinality(Nullable(String)), provenance LowCardinality(Nullable(String)), license LowCardinality(Nullable(String)), license_id LowCardinality(Nullable(String)), version LowCardinality(Nullable(String)), is_accepted Nullable(Bool)) CODEC(ZSTD(1)),
    has_best_oa_location Bool,
    indexed_in Array(LowCardinality(Nullable(String))) CODEC(ZSTD(1)),
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
DROP TABLE IF EXISTS openalex_work_locations;
