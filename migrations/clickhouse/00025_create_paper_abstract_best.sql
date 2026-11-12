-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE IF NOT EXISTS paper_abstract_best
(
    corpusid UInt64 CODEC(Delta, ZSTD(1)),
    abstract String CODEC(ZSTD(3)),
    source Enum8('s2' = 1, 'openalex' = 2),
    work_id Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY corpusid;

-- +goose Down
DROP TABLE IF EXISTS paper_abstract_best;
