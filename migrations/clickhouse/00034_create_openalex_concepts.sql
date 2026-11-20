-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_concepts
(
    concept_id UInt64,
    display_name Nullable(String),
    level Nullable(Int32),
    description Nullable(String),
    wikidata Nullable(String),
    image_url Nullable(String),
    image_thumbnail_url Nullable(String),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    ids Tuple(openalex Nullable(String), wikidata Nullable(String), wikipedia Nullable(String), umls_aui Array(Nullable(String)), umls_cui Array(Nullable(String)), mag Nullable(String)) CODEC(ZSTD(1)),
    has_ids Bool,
    works_api_url Nullable(String),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)) CODEC(ZSTD(1)),
    has_summary_stats Bool,
    international Map(String, Nullable(String)) CODEC(ZSTD(1)),
    ancestors Array(Nullable(String)) CODEC(ZSTD(1)),
    related_concepts Array(Nullable(String)) CODEC(ZSTD(1)),
    counts_by_year Array(Nullable(String)) CODEC(ZSTD(1)),
    created_date Nullable(DateTime64(6, 'UTC')),
    updated_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY concept_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_concepts;
