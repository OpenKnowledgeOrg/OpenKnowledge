-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_authors
(
    author_id UInt64,
    display_name Nullable(String),
    display_name_alternatives Array(Nullable(String)),
    raw_author_names Array(Nullable(String)),
    full_name Nullable(String),
    orcid Nullable(String),
    observed_orcids Array(Nullable(String)),
    works_count Nullable(Int32),
    cited_by_count Nullable(Int32),
    summary_stats Tuple(`2yr_mean_citedness` Nullable(Float64), h_index Nullable(Int32), i10_index Nullable(Int32)),
    has_summary_stats Bool,
    ids Tuple(openalex Nullable(String), orcid Nullable(String), observed_orcids Array(Nullable(String))),
    has_ids Bool,
    affiliations Array(Tuple(institution Tuple(id Nullable(String), ror Nullable(String), display_name Nullable(String), country_code Nullable(String), type Nullable(String), lineage Array(Nullable(String))), years Array(Nullable(Int32)))),
    last_known_institutions Array(Tuple(id Nullable(String), ror Nullable(String), display_name Nullable(String), country_code Nullable(String), type Nullable(String), lineage Array(Nullable(String)))),
    topics Array(Tuple(id Nullable(String), display_name Nullable(String), count Nullable(Int32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    topic_share Array(Tuple(id Nullable(String), display_name Nullable(String), value Nullable(Float64), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    x_concepts Array(Tuple(id Nullable(String), wikidata Nullable(String), display_name Nullable(String), level Nullable(Int32), score Nullable(Float32), count Nullable(Int32))),
    sources Array(Tuple(id Nullable(String), display_name Nullable(String), issn_l Nullable(String), issn Array(Nullable(String)), is_oa Nullable(Bool), is_in_doaj Nullable(Bool), is_core Nullable(Bool), listed_in Array(Nullable(String)), host_organization Nullable(String), host_organization_name Nullable(String), host_organization_lineage Array(Nullable(String)), host_organization_lineage_names Array(Nullable(String)), type Nullable(String))),
    counts_by_year Array(Tuple(year Nullable(Int32), works_count Nullable(Int32), oa_works_count Nullable(Int32), cited_by_count Nullable(Int32))),
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
ORDER BY author_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_authors;
