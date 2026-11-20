-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_funders
(
    funder_id UInt64,
    display_name Nullable(String),
    alternate_titles Array(Nullable(String)) CODEC(ZSTD(1)),
    country_code Nullable(String),
    description Nullable(String),
    homepage_url Nullable(String),
    image_url Nullable(String),
    image_thumbnail_url Nullable(String),
    ids Tuple(openalex Nullable(String), ror Nullable(String), wikidata Nullable(String), crossref Nullable(Int64), doi Nullable(String)) CODEC(ZSTD(1)),
    has_ids Bool,
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    awards_count Nullable(Int64),
    roles Array(Tuple(role Nullable(String), id Nullable(String), works_count Nullable(Int32))) CODEC(ZSTD(1)),
    counts_by_year Array(Tuple(year Nullable(Int32), works_count Nullable(Int32), oa_works_count Nullable(Int32), cited_by_count Nullable(Int32))) CODEC(ZSTD(1)),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)) CODEC(ZSTD(1)),
    has_summary_stats Bool,
    created_date Nullable(DateTime64(6, 'UTC')),
    updated_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY funder_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_funders;
