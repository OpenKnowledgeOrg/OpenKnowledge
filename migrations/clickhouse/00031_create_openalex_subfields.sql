-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_subfields
(
    id String,
    display_name Nullable(String),
    description Nullable(String),
    ids Tuple(openalex Nullable(String), wikidata Nullable(String), wikipedia Nullable(String)),
    has_ids Bool,
    display_name_alternatives Array(Nullable(String)),
    field Tuple(id Nullable(String), display_name Nullable(String)),
    has_field Bool,
    domain Tuple(id Nullable(String), display_name Nullable(String)),
    has_domain Bool,
    topics Array(Tuple(id Nullable(String), display_name Nullable(String))),
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
ORDER BY id;

-- +goose Down
DROP TABLE IF EXISTS openalex_subfields;
