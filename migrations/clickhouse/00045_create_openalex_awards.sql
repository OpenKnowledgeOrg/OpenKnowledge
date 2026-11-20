-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS openalex_awards
(
    award_id UInt64,
    display_name Nullable(String),
    description Nullable(String),
    funder_award_id Nullable(String),
    amount Nullable(Float64),
    currency Nullable(String),
    funder Tuple(id Nullable(String), display_name Nullable(String), ror_id Nullable(String), doi Nullable(String)),
    has_funder Bool,
    funding_type Nullable(String),
    funder_scheme Nullable(String),
    provenance Nullable(String),
    start_date Nullable(Date32),
    end_date Nullable(Date32),
    start_year Nullable(Int64),
    end_year Nullable(Int64),
    lead_investigator Tuple(given_name Nullable(String), family_name Nullable(String), orcid Nullable(String), role_start Nullable(Date32), affiliation Tuple(name Nullable(String), country Nullable(String), ids Array(Tuple(id Nullable(String), type Nullable(String), asserted_by Nullable(String))))),
    has_lead_investigator Bool,
    co_lead_investigator Tuple(given_name Nullable(String), family_name Nullable(String), orcid Nullable(String), role_start Nullable(Date32), affiliation Tuple(name Nullable(String), country Nullable(String), ids Array(Tuple(id Nullable(String), type Nullable(String), asserted_by Nullable(String))))),
    has_co_lead_investigator Bool,
    investigators Array(Tuple(given_name Nullable(String), family_name Nullable(String), orcid Nullable(String), role_start Nullable(Date32), affiliation Tuple(name Nullable(String), country Nullable(String), ids Array(Tuple(id Nullable(String), type Nullable(String), asserted_by Nullable(String)))))),
    landing_page_url Nullable(String),
    doi Nullable(String),
    works_api_url Nullable(String),
    created_date Nullable(DateTime64(6, 'UTC')),
    funded_outputs Array(Nullable(String)),
    funded_outputs_count Nullable(Int64),
    primary_topic Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String))),
    has_primary_topic Bool,
    topics Array(Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    institution_awarded Array(Tuple(id Nullable(String), display_name Nullable(String), ror Nullable(String), country_code Nullable(String), type Nullable(String), lineage Array(Nullable(String)))),
    institution_awarded_full Array(Tuple(id Nullable(String), display_name Nullable(String), ror Nullable(String), country_code Nullable(String), type Nullable(String), lineage Array(Nullable(String)))),
    primary_topic_full Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String))),
    has_primary_topic_full Bool,
    topics_full Array(Tuple(id Nullable(String), display_name Nullable(String), score Nullable(Float32), subfield Tuple(id Nullable(String), display_name Nullable(String)), field Tuple(id Nullable(String), display_name Nullable(String)), domain Tuple(id Nullable(String), display_name Nullable(String)))),
    parent_awards Array(Tuple(id Nullable(String), display_name Nullable(String), funder Tuple(id Nullable(String), display_name Nullable(String)), relationship Nullable(String), asserted_by Nullable(String))),
    sub_awards Array(Tuple(id Nullable(String), display_name Nullable(String), funder Tuple(id Nullable(String), display_name Nullable(String)), relationship Nullable(String), asserted_by Nullable(String))),
    sub_awards_count Nullable(Int64),
    parent_awards_full Array(Tuple(id Nullable(String), display_name Nullable(String), funder Tuple(id Nullable(String), display_name Nullable(String)), relationship Nullable(String), asserted_by Nullable(String))),
    sub_awards_full Array(Tuple(id Nullable(String), display_name Nullable(String), funder Tuple(id Nullable(String), display_name Nullable(String)), relationship Nullable(String), asserted_by Nullable(String))),
    updated_date Nullable(DateTime64(6, 'UTC')),
    source_release Date,
    source_object LowCardinality(String),
    is_deleted UInt8,
    version UInt64 MATERIALIZED (toUInt64(source_release) * 2) + is_deleted,
    removed Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY award_id;

-- +goose Down
DROP TABLE IF EXISTS openalex_awards;
