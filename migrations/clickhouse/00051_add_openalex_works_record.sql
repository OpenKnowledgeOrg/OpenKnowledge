-- +goose NO TRANSACTION
-- +goose Up
-- Metadata only: existing rows read the defaults until the next works load rewrites them.
ALTER TABLE openalex_works
    ADD COLUMN IF NOT EXISTS ids Map(String, Nullable(String)) CODEC(ZSTD(1)) AFTER updated_date,
    ADD COLUMN IF NOT EXISTS publication_date Nullable(Date32) AFTER ids,
    ADD COLUMN IF NOT EXISTS created_date Nullable(DateTime64(6, 'UTC')) AFTER publication_date,
    ADD COLUMN IF NOT EXISTS biblio Tuple(volume Nullable(String), issue Nullable(String), first_page Nullable(String), last_page Nullable(String)) CODEC(ZSTD(1)) AFTER created_date,
    ADD COLUMN IF NOT EXISTS has_biblio Bool AFTER biblio,
    ADD COLUMN IF NOT EXISTS volume Tuple(id Nullable(String), display_name Nullable(String)) CODEC(ZSTD(1)) AFTER has_biblio,
    ADD COLUMN IF NOT EXISTS has_volume Bool AFTER volume,
    ADD COLUMN IF NOT EXISTS cited_by_count Nullable(Int32) AFTER has_volume,
    ADD COLUMN IF NOT EXISTS counts_by_year Array(Tuple(year Nullable(Int32), cited_by_count Nullable(Int32))) CODEC(ZSTD(1)) AFTER cited_by_count,
    ADD COLUMN IF NOT EXISTS awards Array(Tuple(id Nullable(UInt64), display_name Nullable(String), funder_award_id Nullable(String), funder_id Nullable(UInt64), funder_display_name Nullable(String))) CODEC(ZSTD(1)) AFTER counts_by_year,
    ADD COLUMN IF NOT EXISTS funders Array(Tuple(id Nullable(UInt64), display_name Nullable(String), ror Nullable(String))) CODEC(ZSTD(1)) AFTER awards,
    ADD COLUMN IF NOT EXISTS apc_list Tuple(value Nullable(Int32), currency LowCardinality(Nullable(String)), value_usd Nullable(Int32)) CODEC(ZSTD(1)) AFTER funders,
    ADD COLUMN IF NOT EXISTS has_apc_list Bool AFTER apc_list,
    ADD COLUMN IF NOT EXISTS apc_paid Tuple(value Nullable(Float64), currency LowCardinality(Nullable(String)), value_usd Nullable(Float64)) CODEC(ZSTD(1)) AFTER has_apc_list,
    ADD COLUMN IF NOT EXISTS has_apc_paid Bool AFTER apc_paid,
    ADD COLUMN IF NOT EXISTS fwci Nullable(Float64) AFTER has_apc_paid,
    ADD COLUMN IF NOT EXISTS citation_normalized_percentile Tuple(value Nullable(Float64), is_in_top_1_percent Nullable(Bool), is_in_top_10_percent Nullable(Bool)) CODEC(ZSTD(1)) AFTER fwci,
    ADD COLUMN IF NOT EXISTS has_citation_normalized_percentile Bool AFTER citation_normalized_percentile,
    ADD COLUMN IF NOT EXISTS cited_by_percentile_year Tuple(min Nullable(Int32), max Nullable(Int32)) CODEC(ZSTD(1)) AFTER has_citation_normalized_percentile,
    ADD COLUMN IF NOT EXISTS has_cited_by_percentile_year Bool AFTER cited_by_percentile_year;

-- +goose Down
ALTER TABLE openalex_works
    DROP COLUMN IF EXISTS has_cited_by_percentile_year,
    DROP COLUMN IF EXISTS cited_by_percentile_year,
    DROP COLUMN IF EXISTS has_citation_normalized_percentile,
    DROP COLUMN IF EXISTS citation_normalized_percentile,
    DROP COLUMN IF EXISTS fwci,
    DROP COLUMN IF EXISTS has_apc_paid,
    DROP COLUMN IF EXISTS apc_paid,
    DROP COLUMN IF EXISTS has_apc_list,
    DROP COLUMN IF EXISTS apc_list,
    DROP COLUMN IF EXISTS funders,
    DROP COLUMN IF EXISTS awards,
    DROP COLUMN IF EXISTS counts_by_year,
    DROP COLUMN IF EXISTS cited_by_count,
    DROP COLUMN IF EXISTS has_volume,
    DROP COLUMN IF EXISTS volume,
    DROP COLUMN IF EXISTS has_biblio,
    DROP COLUMN IF EXISTS biblio,
    DROP COLUMN IF EXISTS created_date,
    DROP COLUMN IF EXISTS publication_date,
    DROP COLUMN IF EXISTS ids;
