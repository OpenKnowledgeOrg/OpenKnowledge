-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_topics
(
    topic_id UInt64,
    display_name Nullable(String),
    description Nullable(String),
    keywords Array(Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float64))),
    legacy_keywords Nullable(String),
    ids Tuple(openalex Nullable(String), wikipedia Nullable(String)),
    has_ids Bool,
    subfield Tuple(id Nullable(String), display_name Nullable(String)),
    has_subfield Bool,
    field Tuple(id Nullable(String), display_name Nullable(String)),
    has_field Bool,
    domain Tuple(id Nullable(String), display_name Nullable(String)),
    has_domain Bool,
    siblings Array(Tuple(id Nullable(String), display_name Nullable(String))),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
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
ORDER BY topic_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_topics;
