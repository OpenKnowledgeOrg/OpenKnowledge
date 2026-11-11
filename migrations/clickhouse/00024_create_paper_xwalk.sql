-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS paper_xwalk
(
    corpusid UInt64 CODEC(Delta, ZSTD(1)),
    work_id UInt64 CODEC(ZSTD(1)),
    match_key Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3),
    rank UInt8,
    chosen Bool,
    shared_work Bool
)
ENGINE = MergeTree
ORDER BY (corpusid, rank)
SETTINGS index_granularity = 1024;

-- +goose Down
DROP TABLE IF EXISTS paper_xwalk;
