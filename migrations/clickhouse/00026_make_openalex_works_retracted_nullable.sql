-- +goose NO TRANSACTION
-- +goose Up
-- OpenAlex leaves is_retracted empty for some works (1.69M of 464.8M in 2026-10-14).
-- A failed rollback leaves its conversion pending; removing its guard below would let it
-- turn NULL into false. Refuse until an operator has killed that mutation.
SELECT throwIf(count() > 0, 'openalex_works has an unfinished mutation; kill it first')
FROM system.mutations
WHERE database = currentDatabase() AND table = 'openalex_works' AND NOT is_done FORMAT Null;
ALTER TABLE openalex_works MODIFY COLUMN is_retracted Nullable(Bool) DEFAULT NULL;
-- Clear the guard if a previous rollback stopped before removing its default.
ALTER TABLE openalex_works MODIFY COLUMN is_retracted REMOVE DEFAULT;

-- +goose Down
-- Refuse before changing metadata if unknown values exist. The throwing default also
-- prevents a concurrent NULL from being silently replaced during conversion.
SELECT throwIf(countIf(isNull(is_retracted)) > 0, 'openalex_works.is_retracted contains NULL; rollback refused')
FROM openalex_works FORMAT Null;
ALTER TABLE openalex_works MODIFY COLUMN is_retracted Bool
DEFAULT throwIf(1, 'openalex_works.is_retracted contains NULL; rollback refused') = 1
SETTINGS mutations_sync = 2;
ALTER TABLE openalex_works MODIFY COLUMN is_retracted REMOVE DEFAULT;
