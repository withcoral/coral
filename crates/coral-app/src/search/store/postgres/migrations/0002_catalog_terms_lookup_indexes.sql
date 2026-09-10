-- Lexicon lookups for query-word rescue. A typed word that matches no lexeme
-- exactly and prefixes none is asked two cheaper questions against the
-- lexicon (`catalog_terms`, tens of thousands of rows) rather than against
-- the documents: does any lexeme start with this (or with its stem), and is
-- any lexeme similar to it. `term LIKE 'word%'` needs `text_pattern_ops`
-- under a non-C collation to use a btree; `term % 'word'` needs a trigram
-- GIN.
--
-- Runs with `search_path` set to the Workspace's surrogate schema plus the
-- schema that holds `pg_trgm`, so names are unqualified on purpose. Not
-- idempotent on purpose (see 0001).
CREATE INDEX catalog_terms_term_pattern
    ON catalog_terms (term text_pattern_ops);

CREATE INDEX catalog_terms_term_trgm
    ON catalog_terms USING gin (term gin_trgm_ops);
