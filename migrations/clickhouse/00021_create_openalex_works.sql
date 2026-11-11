-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_works
(
    work_id UInt64,
    doi Nullable(String),
    doi_norm Nullable(String),
    pmid Nullable(UInt64),
    mag Nullable(UInt64),
    title Nullable(String),
    publication_year Nullable(UInt16),
    `type` LowCardinality(Nullable(String)),
    language LowCardinality(Nullable(String)),
    is_xpac Bool,
    is_retracted Bool,
    is_paratext Bool,
    has_abstract Bool,
    oa_status LowCardinality(Nullable(String)),
    is_oa Nullable(Bool),
    oa_url Nullable(String),
    any_repository_has_fulltext Nullable(Bool),
    best_oa_pdf_url Nullable(String),
    best_oa_landing_page_url Nullable(String),
    best_oa_license Nullable(String),
    best_oa_version Nullable(String),
    best_oa_source_id Nullable(String),
    primary_landing_page_url Nullable(String),
    primary_source_id Nullable(String),
    has_pdf Nullable(Bool),
    has_grobid_xml Nullable(Bool),
    has_fulltext Nullable(Bool),
    updated_date DateTime,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY work_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_works;
