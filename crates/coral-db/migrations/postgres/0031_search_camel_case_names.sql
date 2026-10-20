-- Postgres text search for Universal Search, as the search stream's version 2.
-- Step 30 says why this step builds what the stream built. On a database the
-- stream already took to version 2 or 3, the step rebuilds the same `tsv`
-- column and recounts the same statistics.
-- squawk-ignore-file adding-field-with-default, ban-drop-column, require-concurrent-index-creation
--
-- The `simple` parser keeps a camelCase name as one lexeme, so no query word
-- reached `comments` in `listComments`. `tsv` now splits camelCase in the name
-- field only, before the dot replacement: a lowercase letter or digit before a
-- capital starts a new word, and so does the last capital of a run before a
-- lowercase letter. `getSSHKey` gives `get`, `ssh` and `key`. The title, the
-- description, the Provider name and the schema text keep their words: prose
-- holds brand names such as `GitHub`. The compact identifier variants in the
-- schema text stay as the indexer writes them.
--
-- Postgres cannot change a generated column's expression before version 17,
-- so the step drops `tsv` with its index and adds it again. Adding a `STORED`
-- column rewrites every row, so the vectors of rows indexed already change
-- here. `doc_len` and the statistics are written only at index time, so the
-- step recomputes them from the new vectors, by the rules the indexer uses:
-- `doc_len` counts lexeme positions, `search_terms` counts the rows that hold
-- each lexeme, and `search_stats` holds each Workspace's row count and mean
-- `doc_len`.
SET LOCAL lock_timeout = '5s';

ALTER TABLE search_entities DROP COLUMN tsv;

ALTER TABLE search_entities
    ADD COLUMN tsv TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', replace(
            regexp_replace(
                regexp_replace(qualified_name, '([A-Z]+)([A-Z][a-z])', '\1 \2', 'g'),
                '([a-z0-9])([A-Z])', '\1 \2', 'g'
            ),
            '.', ' '
        )), 'A')
        || setweight(to_tsvector('simple', replace(title, '.', ' ')), 'B')
        || setweight(to_tsvector('simple', description), 'C')
        || setweight(to_tsvector('simple', replace(provider_name, '.', ' ')), 'C')
        || setweight(to_tsvector('simple', replace(searchable_text, '.', ' ')), 'D')
    ) STORED;

CREATE INDEX search_entities_tsv
    ON search_entities USING gin (tsv);

UPDATE search_entities AS d
SET doc_len = coalesce((
    SELECT sum(coalesce(array_length(u.positions, 1), 1))
    FROM unnest(d.tsv) AS u(lexeme, positions, weights)
), 0);

DELETE FROM search_terms;

INSERT INTO search_terms (workspace_id, term, ndoc)
SELECT d.workspace_id, u.lexeme, count(*)
FROM search_entities AS d
CROSS JOIN LATERAL unnest(d.tsv) AS u(lexeme, positions, weights)
GROUP BY d.workspace_id, u.lexeme;

UPDATE search_stats AS s
SET total_docs = (
        SELECT count(*) FROM search_entities AS d WHERE d.workspace_id = s.workspace_id
    ),
    avgdl = (
        SELECT coalesce(avg(d.doc_len), 1.0)
        FROM search_entities AS d WHERE d.workspace_id = s.workspace_id
    );

INSERT INTO search_stats (workspace_id, total_docs, avgdl)
SELECT d.workspace_id, count(*), coalesce(avg(d.doc_len), 1.0)
FROM search_entities AS d
WHERE NOT EXISTS (
    SELECT 1 FROM search_stats AS s WHERE s.workspace_id = d.workspace_id
)
GROUP BY d.workspace_id;
