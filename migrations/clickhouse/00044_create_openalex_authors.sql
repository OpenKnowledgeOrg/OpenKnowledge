-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_authors
(
    author_id UInt64,
    display_name Nullable(String),
    display_name_alternatives Array(Nullable(String)) CODEC(ZSTD(1)),
    raw_author_names Array(Nullable(String)) CODEC(ZSTD(1)),
    full_name Nullable(String),
    orcid Nullable(String),
    observed_orcids Array(Nullable(String)) CODEC(ZSTD(1)),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)) CODEC(ZSTD(1)),
    has_summary_stats Bool,
    ids Tuple(openalex Nullable(String), orcid Nullable(String), observed_orcids Array(Nullable(String))) CODEC(ZSTD(1)),
    has_ids Bool,
    affiliations Array(Tuple(institution Tuple(id Nullable(UInt64)), years Array(Nullable(Int32)))) CODEC(ZSTD(1)),
    last_known_institutions Array(Tuple(id Nullable(UInt64))) CODEC(ZSTD(1)),
    topics Array(Tuple(id Nullable(UInt64), display_name Nullable(String), count Nullable(Int32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))) CODEC(ZSTD(1)),
    topic_share Array(Tuple(id Nullable(UInt64), display_name Nullable(String), value Nullable(Float64), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))) CODEC(ZSTD(1)),
    x_concepts Array(Tuple(id Nullable(UInt64), wikidata Nullable(String), display_name Nullable(String), level Nullable(Int32), score Nullable(Float32), count Nullable(Int32))) CODEC(ZSTD(1)),
    sources Array(Tuple(id Nullable(UInt64))) CODEC(ZSTD(1)),
    counts_by_year Array(Tuple(year Nullable(Int32), works_count Nullable(Int32), oa_works_count Nullable(Int32), cited_by_count Nullable(Int32))) CODEC(ZSTD(1)),
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
ORDER BY author_id
SETTINGS index_granularity = 1024;

-- +goose Down
DROP TABLE IF EXISTS openalex_authors;
