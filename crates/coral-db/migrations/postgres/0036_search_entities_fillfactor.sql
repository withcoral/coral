-- Each index write inserts or updates a row, then sets its `doc_len` in a
-- second UPDATE. No index holds `doc_len`, so that UPDATE can be a HOT update
-- and add no index entry, but only when the row's page has room for the new
-- row version. With the default fillfactor of 100 the insert fills each page,
-- so every `doc_len` UPDATE writes a new entry in each index, and the trigram
-- index extracts the keys of `all_text` again.
--
-- A fillfactor of 50 leaves half of each page free for that new version.
-- The setting controls where later writes put rows. A page written before
-- this step stays full until its rows move: the next write of a row puts
-- its new version on a page with room.
--
-- lock_timeout makes the step fail instead of queueing reads and writes
-- behind its lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE search_entities SET (fillfactor = 50);
