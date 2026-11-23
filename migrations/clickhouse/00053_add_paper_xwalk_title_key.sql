-- +goose NO TRANSACTION
-- +goose Up
-- Title matches of papers with no ID candidate. Appending an Enum value changes only metadata;
-- 4 stays free for a relaxed MAG key, so ID keys still sort first.
ALTER TABLE paper_xwalk MODIFY COLUMN match_key Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3, 'title' = 5);
-- Papers whose best abstract changed in an incremental update without a row in papers/abstracts/tldrs
-- (e.g. a title match displaced by a new ID match), so search diffs rewrite them.
CREATE TABLE IF NOT EXISTS paper_abstract_best_invalidations (
    corpusid UInt64,
    source_release Date
) ENGINE = MergeTree
ORDER BY (corpusid, source_release);

-- +goose Down
-- Refuse before changing metadata while title matches exist; a full build without them
-- replaces the table.
SELECT throwIf(countIf(match_key = 'title') > 0, 'paper_xwalk holds title matches; rollback refused')
FROM paper_xwalk FORMAT Null;
DROP TABLE IF EXISTS paper_abstract_best_invalidations;
ALTER TABLE paper_xwalk MODIFY COLUMN match_key Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3);
