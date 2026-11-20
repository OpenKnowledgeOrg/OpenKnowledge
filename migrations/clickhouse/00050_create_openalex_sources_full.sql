-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_sources_full
(
    source_id UInt64,
    issn_l Nullable(String),
    issn Array(Nullable(String)) CODEC(ZSTD(1)),
    display_name Nullable(String),
    host_organization Nullable(String),
    host_organization_name Nullable(String),
    host_organization_lineage Array(Nullable(String)) CODEC(ZSTD(1)),
    works_count Nullable(Int32),
    oa_works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)) CODEC(ZSTD(1)),
    has_summary_stats Bool,
    is_oa Nullable(Bool),
    is_in_doaj Nullable(Bool),
    is_in_doaj_since_year Nullable(Int32),
    is_high_oa_rate Nullable(Bool),
    is_high_oa_rate_since_year Nullable(Int64),
    is_in_scielo Nullable(Bool),
    is_ojs Nullable(Bool),
    is_core Nullable(Bool),
    listed_in Array(Nullable(String)) CODEC(ZSTD(1)),
    is_preprint_repository Nullable(Bool),
    oa_flip_year Nullable(Int32),
    first_publication_year Nullable(Int32),
    last_publication_year Nullable(Int32),
    ids Tuple(openalex Nullable(String), issn_l Nullable(String), issn Array(Nullable(String)), mag Nullable(String), wikidata Nullable(String)) CODEC(ZSTD(1)),
    has_ids Bool,
    homepage_url Nullable(String),
    apc_prices Array(Tuple(price Nullable(Int32), currency Nullable(String))) CODEC(ZSTD(1)),
    apc_usd Nullable(Int32),
    apc_usd_by_year Array(Tuple(year Nullable(Int32), price Nullable(Int32))) CODEC(ZSTD(1)),
    country_code Nullable(String),
    societies Array(Tuple(url Nullable(String), organization Nullable(String))) CODEC(ZSTD(1)),
    alternate_titles Array(Nullable(String)) CODEC(ZSTD(1)),
    type Nullable(String),
    topics Array(Tuple(id Nullable(UInt64), display_name Nullable(String), count Nullable(Int32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))) CODEC(ZSTD(1)),
    topic_share Array(Tuple(id Nullable(UInt64), display_name Nullable(String), value Nullable(Float64), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))) CODEC(ZSTD(1)),
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
ORDER BY source_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_sources_full;
