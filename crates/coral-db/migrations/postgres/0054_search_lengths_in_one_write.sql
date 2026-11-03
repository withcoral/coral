-- Each index write upserted a row, then counted its vector's length in a
-- second UPDATE of the same row. `doc_len` could not be generated: a
-- generated column cannot read another one, and its expression cannot hold
-- the subquery that counts the positions. The two updates of one row held it
-- across a statement boundary, which is where #2684's deadlock met a delete.
--
-- `search_entity_vector` now builds the weighted vector that step 31 wrote
-- inline, and `search_vector_length` counts its lexeme positions, a lexeme
-- without positions as one, as the indexer counted `doc_len`. `tsv` and the
-- new `tsv_length` both call the first function, so one write sets both and
-- the two cannot drift. Both functions read their arguments alone, so they
-- are immutable, and every name they call resolves in `pg_catalog`.
--
-- `doc_len` stays as a plain column that no release reads from this step on.
-- A process of an earlier release, still running during a rolling update,
-- still clears and counts it. A later step drops it.
--
-- Step 36 left half of each page free for the second UPDATE's row version.
-- No write updates a row twice now, so the table returns to the default
-- fillfactor before the rewrite below packs its pages.
--
-- Postgres cannot change a generated column's expression before version 17,
-- so `tsv` is dropped with its index and added again. Adding a `STORED` column
-- rewrites every row and rebuilds every index, in one pass for both columns.
-- `STORED` is spelled out: Postgres 18 changed the default to VIRTUAL.
-- `tsv_length` is an INTEGER like `doc_len`: a vector holds far fewer
-- positions than that type holds.
-- squawk-ignore-file adding-field-with-default, ban-drop-column, prefer-bigint-over-int, require-concurrent-index-creation
SET LOCAL lock_timeout = '5s';

CREATE FUNCTION search_entity_vector(
    qualified_name TEXT,
    title TEXT,
    description TEXT,
    provider_name TEXT,
    searchable_text TEXT
) RETURNS TSVECTOR
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $$
    SELECT setweight(to_tsvector('simple', replace(
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
$$;

CREATE FUNCTION search_vector_length(vector TSVECTOR) RETURNS INTEGER
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $$
    SELECT coalesce(sum(coalesce(array_length(u.positions, 1), 1)), 0)::integer
    FROM unnest(vector) AS u(lexeme, positions, weights)
$$;

ALTER TABLE search_entities RESET (fillfactor);

ALTER TABLE search_entities
    DROP COLUMN tsv,
    ADD COLUMN tsv TSVECTOR GENERATED ALWAYS AS (
        search_entity_vector(
            qualified_name, title, description, provider_name, searchable_text
        )
    ) STORED,
    ADD COLUMN tsv_length INTEGER GENERATED ALWAYS AS (
        search_vector_length(search_entity_vector(
            qualified_name, title, description, provider_name, searchable_text
        ))
    ) STORED;

CREATE INDEX search_entities_tsv
    ON search_entities USING gin (tsv);
