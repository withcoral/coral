-- SQLite finds each row's FTS5 copy by rowid from this step on.
--
-- The triggers of step 30 found the copy by its two UNINDEXED key columns,
-- which FTS5 cannot look up, so each update or delete scanned the whole FTS5
-- table. Removing or reindexing a Provider writes all its rows in one
-- transaction, so its cost grew with the square of its row count, and it held
-- the write lock all that time.
--
-- `search_entities_fts_keys` maps each row's key to the rowid of its copy.
-- FTS5 keeps a row's rowid. `search_entities` has no INTEGER PRIMARY KEY, so
-- `VACUUM` may renumber its rowids, and they cannot link the two tables.
CREATE TABLE search_entities_fts_keys (
    workspace_id TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    fts_rowid INTEGER NOT NULL,
    CONSTRAINT search_entities_fts_keys_pk PRIMARY KEY (workspace_id, doc_id)
) WITHOUT ROWID;

-- An `INSERT OR REPLACE` before this step leaves a second copy of its row,
-- and the key table links only one. So the step copies each row again instead of
-- trusting the copies already there. That rewrites each row's copy once.
DELETE FROM search_entities_fts;

INSERT INTO search_entities_fts (
    workspace_id, doc_id, qualified_name, title, description, provider_name,
    searchable_text
)
SELECT
    workspace_id, doc_id, qualified_name, title, description,
    replace(provider_name, '.', ' '), searchable_text
FROM search_entities;

INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
SELECT workspace_id, doc_id, rowid FROM search_entities_fts;

DROP TRIGGER search_entities_fts_after_insert;
DROP TRIGGER search_entities_fts_after_update;
DROP TRIGGER search_entities_fts_after_delete;

-- Inside a trigger, `last_insert_rowid()` returns the rowid of the trigger's
-- own last insert.
CREATE TRIGGER search_entities_fts_after_insert
AFTER INSERT ON search_entities
BEGIN
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
    INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
    VALUES (NEW.workspace_id, NEW.doc_id, last_insert_rowid());
END;

CREATE TRIGGER search_entities_fts_after_update
AFTER UPDATE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE rowid = (
        SELECT fts_rowid FROM search_entities_fts_keys
        WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id
    );
    DELETE FROM search_entities_fts_keys
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
    INSERT INTO search_entities_fts_keys (workspace_id, doc_id, fts_rowid)
    VALUES (NEW.workspace_id, NEW.doc_id, last_insert_rowid());
END;

CREATE TRIGGER search_entities_fts_after_delete
AFTER DELETE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE rowid = (
        SELECT fts_rowid FROM search_entities_fts_keys
        WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id
    );
    DELETE FROM search_entities_fts_keys
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
END;
