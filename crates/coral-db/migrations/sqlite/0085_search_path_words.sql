-- The words of the Provider Tool paths of each Generation. The Authored
-- Tool retriever reads them: an Authored Tool is a strong match by a path
-- word only when few Provider Tool paths of the Workspace hold a word that
-- starts with it.
--
-- A Tool's path here is the last segment of its Provider Call Root and its
-- Tool-relative Path. Each row holds one word of one Generation and the
-- number of the Generation's Tools whose path holds the word. The Tool
-- indexer writes a Generation's rows in the transaction that writes its Tool
-- rows. It deletes them with those rows. A read adds the rows of the
-- Workspace's complete Generations.
--
-- `word` compares byte by byte (the BINARY collation). So the primary key
-- serves the read's range of the words that start with a query word.
--
-- search_index_states.tool_count holds the number of Tools that a complete
-- Generation attempt wrote. The column is nullable. A failed attempt writes
-- no count. A row that an earlier indexer wrote has no count until a
-- reconcile pass indexes its Generation again under the new indexer
-- revision. Until then, that Generation adds no path and no word.
CREATE TABLE search_path_words (
    workspace_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    provider_generation_id TEXT NOT NULL,
    word TEXT NOT NULL,
    tool_count BIGINT NOT NULL,

    CONSTRAINT search_path_words_pk
        PRIMARY KEY (workspace_id, word, provider_generation_id),
    CONSTRAINT search_path_words_tool_count_valid CHECK (tool_count > 0),
    CONSTRAINT search_path_words_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE
);

CREATE INDEX search_path_words_generation_idx
    ON search_path_words (provider_generation_id);

ALTER TABLE search_index_states
    ADD COLUMN tool_count BIGINT;
