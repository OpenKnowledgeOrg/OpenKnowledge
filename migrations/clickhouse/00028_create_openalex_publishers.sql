-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_publishers
(
    publisher_id UInt64,
    lineage Array(Nullable(String)),
    display_name Nullable(String),
    alternate_titles Array(Nullable(String)),
    country_codes Array(Nullable(String)),
    hierarchy_level Nullable(Int32),
    parent_publisher Tuple(id Nullable(String), display_name Nullable(String)),
    has_parent_publisher Bool,
    ids Tuple(openalex Nullable(String), ror Nullable(String), wikidata Nullable(String)),
    has_ids Bool,
    ror_id Nullable(String),
    image_url Nullable(String),
    image_thumbnail_url Nullable(String),
    wikidata_id Nullable(String),
    homepage_url Nullable(String),
    works_count Nullable(Int64),
    cited_by_count Nullable(Int64),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)),
    has_summary_stats Bool,
    roles Array(Tuple(role Nullable(String), id Nullable(String), works_count Nullable(Int32))),
    counts_by_year Array(Tuple(year Nullable(Int32), works_count Nullable(Int64), cited_by_count Nullable(Int64))),
    sources_api_url Nullable(String),
    created_date Nullable(DateTime64(6, 'UTC')),
    updated_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY publisher_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_publishers;
