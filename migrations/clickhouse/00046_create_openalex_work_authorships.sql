-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_work_authorships
(
    work_id UInt64,
    authorships Array(Tuple(author Tuple(display_name Nullable(String), id Nullable(UInt64), orcid Nullable(String), observed_orcids Array(Nullable(String))), author_position LowCardinality(Nullable(String)), affiliations Array(Tuple(institution_ids Array(UInt64), raw_affiliation_string Nullable(String))), countries Array(LowCardinality(Nullable(String))), raw_author_name Nullable(String), raw_orcid Nullable(String), is_corresponding Nullable(Bool), raw_affiliation_strings Array(Nullable(String)), institutions Array(Tuple(id Nullable(UInt64))))) CODEC(ZSTD(1)),
    authors_count Nullable(Int32),
    corresponding_author_ids Array(UInt64) CODEC(ZSTD(1)),
    corresponding_institution_ids Array(UInt64) CODEC(ZSTD(1)),
    institutions Array(Tuple(id Nullable(UInt64))) CODEC(ZSTD(1)),
    countries_distinct_count Nullable(Int32),
    institutions_distinct_count Nullable(Int32),
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
DROP TABLE IF EXISTS openalex_work_authorships;
