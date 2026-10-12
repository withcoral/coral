-- SQLite text search for Universal Search, stream version 1.
--
-- This step runs once, in one immediate transaction with its row in
-- `search_migrations`. It does not skip objects that already exist: such an
-- object is unexpected state, so the step fails and the ledger records nothing.
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
CREATE VIRTUAL TABLE search_entities_fts USING fts5(
    workspace_id UNINDEXED,
    doc_id UNINDEXED,
    qualified_name,
    title,
    description,
    provider_name,
    searchable_text,
    tokenize = 'trigram'
);

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
END;

CREATE TRIGGER search_entities_fts_after_update
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

CREATE TRIGGER search_entities_fts_after_delete
AFTER DELETE ON search_entities
BEGIN
    DELETE FROM search_entities_fts
    WHERE workspace_id = OLD.workspace_id AND doc_id = OLD.doc_id;
END;
