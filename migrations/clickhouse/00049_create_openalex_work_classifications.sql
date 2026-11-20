-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_work_classifications
(
    work_id UInt64,
    primary_topic Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String))),
    has_primary_topic Bool,
    topics Array(Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    keywords Array(Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32))),
    concepts Array(Tuple(id Nullable(String), wikidata Nullable(String), display_name Nullable(String), level Nullable(Int32), score Nullable(Float32))),
    mesh Array(Tuple(descriptor_ui Nullable(String), descriptor_name Nullable(String), qualifier_ui Nullable(String), qualifier_name Nullable(String), is_major_topic Nullable(Bool))),
    sustainable_development_goals Array(Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float64))),
    study_designs Array(Tuple(id Nullable(String), display_name Nullable(String))),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY work_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_work_classifications;
