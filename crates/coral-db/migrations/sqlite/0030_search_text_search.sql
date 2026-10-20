-- SQLite text search for Universal Search.
--
-- This step takes over version 1 of Universal Search's own migration stream,
-- which recorded itself in `search_migrations`. On a database that stream
-- reached, the objects exist already, so the step creates each one only if it
-- is missing. Only the search steps use `IF NOT EXISTS`, and only for objects
-- the stream built.
--
-- The FTS5 table keeps its own copy of the five text columns, found through
-- the two unindexed key columns. Triggers copy every insert, update, and
-- delete on `search_entities` to it. So the indexer's statements are the same
-- on both backends, and the foreign-key cascades reach this table too. The
-- Provider name is name-like and gets the same dot replacement as on Postgres.
--
-- Write `search_entities` with `INSERT ... ON CONFLICT ... DO UPDATE`, never
-- `INSERT OR REPLACE`: a replace does not fire the delete trigger, so the old
-- copy stays in this table beside the new one.
CREATE VIRTUAL TABLE IF NOT EXISTS search_entities_fts USING fts5(
    workspace_id UNINDEXED,
    doc_id UNINDEXED,
    qualified_name,
    title,
    description,
    provider_name,
    searchable_text,
    tokenize = 'trigram'
);

CREATE TRIGGER IF NOT EXISTS search_entities_fts_after_insert
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
END;

CREATE TRIGGER IF NOT EXISTS search_entities_fts_after_update
AFTER UPDATE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
    INSERT INTO search_entities_fts (
        workspace_id, doc_id, qualified_name, title, description, provider_name,
        searchable_text
    )
    VALUES (
        NEW.workspace_id, NEW.doc_id, NEW.qualified_name, NEW.title, NEW.description,
        replace(NEW.provider_name, '.', ' '), NEW.searchable_text
    );
END;

CREATE TRIGGER IF NOT EXISTS search_entities_fts_after_delete
AFTER DELETE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
END;
