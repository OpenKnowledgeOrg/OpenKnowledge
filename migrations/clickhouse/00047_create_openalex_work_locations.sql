-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_work_locations
(
    work_id UInt64,
    locations Array(Tuple(id Nullable(String), doi Nullable(String), source Tuple(id Nullable(String), display_name Nullable(String), issn_l Nullable(String), issn Array(Nullable(String)), is_oa Nullable(Bool), is_in_doaj Nullable(Bool), is_core Nullable(Bool), listed_in Array(Nullable(String)), host_organization Nullable(String), host_organization_name Nullable(String), host_organization_lineage Array(Nullable(String)), host_organization_lineage_names Array(Nullable(String)), type Nullable(String)), is_oa Nullable(Bool), is_published Nullable(Bool), landing_page_url Nullable(String), pdf_url Nullable(String), raw_source_name Nullable(String), raw_type Nullable(String), provenance Nullable(String), license Nullable(String), license_id Nullable(String), version Nullable(String), is_accepted Nullable(Bool))),
    locations_count Nullable(Int32),
    primary_location Tuple(id Nullable(String), doi Nullable(String), source Tuple(id Nullable(String), display_name Nullable(String), issn_l Nullable(String), issn Array(Nullable(String)), is_oa Nullable(Bool), is_in_doaj Nullable(Bool), is_core Nullable(Bool), listed_in Array(Nullable(String)), host_organization Nullable(String), host_organization_name Nullable(String), host_organization_lineage Array(Nullable(String)), host_organization_lineage_names Array(Nullable(String)), type Nullable(String)), is_oa Nullable(Bool), is_published Nullable(Bool), landing_page_url Nullable(String), pdf_url Nullable(String), raw_source_name Nullable(String), raw_type Nullable(String), provenance Nullable(String), license Nullable(String), license_id Nullable(String), version Nullable(String), is_accepted Nullable(Bool)),
    has_primary_location Bool,
    best_oa_location Tuple(id Nullable(String), doi Nullable(String), source Tuple(id Nullable(String), display_name Nullable(String), issn_l Nullable(String), issn Array(Nullable(String)), is_oa Nullable(Bool), is_in_doaj Nullable(Bool), is_core Nullable(Bool), listed_in Array(Nullable(String)), host_organization Nullable(String), host_organization_name Nullable(String), host_organization_lineage Array(Nullable(String)), host_organization_lineage_names Array(Nullable(String)), type Nullable(String)), is_oa Nullable(Bool), is_published Nullable(Bool), landing_page_url Nullable(String), pdf_url Nullable(String), raw_source_name Nullable(String), raw_type Nullable(String), provenance Nullable(String), license Nullable(String), license_id Nullable(String), version Nullable(String), is_accepted Nullable(Bool)),
    has_best_oa_location Bool,
    indexed_in Array(Nullable(String)),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY work_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_work_locations;
