-- Universal Search is a projection, and a change to its source never waits for
-- it (ADR 0017). These foreign keys tied the two together: deleting a Saved
-- Function cascaded into the search tables inside the deleting transaction.
-- That delete waited for any index write that held the search row, and the two
-- could deadlock: the write's `doc_len` UPDATE re-checks the key of a row its
-- own transaction wrote, which needs the function row the delete already
-- removed. The reconcile task now removes what a source no longer holds. Each
-- table keeps its Workspace key.
--
-- Dropping a key changes only the catalog, but it takes an exclusive lock.
-- lock_timeout makes the step fail instead of queueing every read behind it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE search_entities
    DROP CONSTRAINT search_entities_generation_fk,
    DROP CONSTRAINT search_entities_saved_function_fk;

ALTER TABLE search_index_states
    DROP CONSTRAINT search_index_states_generation_fk,
    DROP CONSTRAINT search_index_states_saved_function_fk;
