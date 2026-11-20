-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_countries
(
    id String,
    country_code Nullable(String),
    display_name Nullable(String),
    continent_id Nullable(Int32),
    is_global_south Nullable(Bool),
    wikidata_url Nullable(String),
    wikipedia_url Nullable(String),
    display_name_alternatives Array(Nullable(String)),
    description Nullable(String),
    alpha_3 Nullable(String),
    numeric Nullable(Int32),
    full_name Nullable(String),
    works_count Nullable(Int64),
    cited_by_count Nullable(Int64),
    authors_api_url Nullable(String),
    institutions_api_url Nullable(String),
    works_api_url Nullable(String),
    ids Tuple(openalex Nullable(String), iso Nullable(String), wikidata Nullable(String), wikipedia Nullable(String)),
    has_ids Bool,
    created_date Nullable(DateTime64(6, 'UTC')),
    updated_date Nullable(DateTime64(6, 'UTC')),
    continent Tuple(id Nullable(String), display_name Nullable(String)),
    has_continent Bool,
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id;

-- +goose Down
DROP TABLE IF EXISTS openalex_countries;
