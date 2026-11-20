-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_institutions
(
    institution_id UInt64,
    ror Nullable(String),
    display_name Nullable(String),
    country_code Nullable(String),
    `type` Nullable(String),
    lineage Array(Nullable(String)),
    is_super_system Nullable(Bool),
    type_id Nullable(String),
    homepage_url Nullable(String),
    image_url Nullable(String),
    image_thumbnail_url Nullable(String),
    display_name_acronyms Array(Nullable(String)),
    display_name_alternatives Array(Nullable(String)),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    ids Tuple(openalex Nullable(String), ror Nullable(String), grid Nullable(String), wikipedia Nullable(String), wikidata Nullable(String)),
    has_ids Bool,
    roles Array(Tuple(role Nullable(String), id Nullable(String), works_count Nullable(Int32))),
    repositories Array(Tuple(id Nullable(String), display_name Nullable(String), host_organization Nullable(String), host_organization_name Nullable(String), host_organization_lineage Array(Nullable(String)))),
    geo Tuple(city Nullable(String), geonames_city_id Nullable(String), region Nullable(String), country_code Nullable(String), country Nullable(String), latitude Nullable(Float64), longitude Nullable(Float64)),
    has_geo Bool,
    topics Array(Tuple(id Nullable(String), display_name Nullable(String), count Nullable(Int32), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    topic_share Array(Tuple(id Nullable(String), display_name Nullable(String), value Nullable(Float64), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    associated_institutions Array(Tuple(id Nullable(String), ror Nullable(String), display_name Nullable(String), country_code Nullable(String), type Nullable(String), relationship Nullable(String))),
    counts_by_year Array(Tuple(year Nullable(Int32), works_count Nullable(Int32), oa_works_count Nullable(Int32), cited_by_count Nullable(Int32))),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)),
    has_summary_stats Bool,
    status Nullable(String),
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
ORDER BY institution_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_institutions;
