-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_work_details
(
    work_id UInt64,
    display_name Nullable(String),
    ids Map(String, Nullable(String)),
    publication_date Nullable(Date32),
    created_date Nullable(DateTime64(6, 'UTC')),
    biblio Tuple(volume Nullable(String), issue Nullable(String), first_page Nullable(String), last_page Nullable(String)),
    has_biblio Bool,
    volume Tuple(id Nullable(String), display_name Nullable(String)),
    has_volume Bool,
    cited_by_count Nullable(Int32),
    counts_by_year Array(Tuple(year Nullable(Int32), cited_by_count Nullable(Int32))),
    awards Array(Tuple(id Nullable(String), display_name Nullable(String), funder_award_id Nullable(String), funder_id Nullable(String), funder_display_name Nullable(String))),
    funders Array(Tuple(id Nullable(String), display_name Nullable(String), ror Nullable(String))),
    apc_list Tuple(value Nullable(Int32), currency Nullable(String), value_usd Nullable(Int32)),
    has_apc_list Bool,
    apc_paid Tuple(value Nullable(Float64), currency Nullable(String), value_usd Nullable(Float64)),
    has_apc_paid Bool,
    fwci Nullable(Float64),
    citation_normalized_percentile Tuple(value Nullable(Float64), is_in_top_1_percent Nullable(Bool), is_in_top_10_percent Nullable(Bool)),
    has_citation_normalized_percentile Bool,
    cited_by_percentile_year Tuple(min Nullable(Int32), max Nullable(Int32)),
    has_cited_by_percentile_year Bool,
    open_access Tuple(is_oa Nullable(Bool), oa_status Nullable(String), any_repository_has_fulltext Nullable(Bool), oa_url Nullable(String)),
    has_open_access Bool,
    has_content Tuple(pdf Nullable(Bool), grobid_xml Nullable(Bool)),
    has_has_content Bool,
    updated_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY work_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_work_details;
