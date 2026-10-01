-- The catalog projection stored a second copy of the schema -> owner topology
-- that #1791 removed: one installed source publishes one SQL namespace, so
-- `catalog_documents.source_name` already is the installed source name.
--
-- Catalog documents and FTS rows are deliberately left in place. The refresh is
-- forced by bumping `CATALOG_SEARCH_SNAPSHOT_VERSION`, which guarantees every
-- stored fingerprint mismatches and triggers exactly one lazy refresh per
-- workspace -- and, unlike a DELETE here, does nothing extra on repair replays.

DROP INDEX IF EXISTS idx_catalog_source_owners_workspace_owner;
DROP TABLE IF EXISTS catalog_source_owners;
