-- +goose NO TRANSACTION
-- +goose Up
-- Title matches of papers with no ID candidate. Appending an Enum value changes only metadata;
-- 4 stays free for a relaxed MAG key, so ID keys still sort first.
ALTER TABLE paper_xwalk MODIFY COLUMN match_key Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3, 'title' = 5);

-- +goose Down
-- Refuse before changing metadata while title matches exist; a full build without them
-- replaces the table.
SELECT throwIf(countIf(match_key = 'title') > 0, 'paper_xwalk holds title matches; rollback refused')
FROM paper_xwalk FORMAT Null;
ALTER TABLE paper_xwalk MODIFY COLUMN match_key Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3);
