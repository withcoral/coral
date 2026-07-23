ALTER TABLE catalog_documents
ADD COLUMN searchable_text TEXT NOT NULL DEFAULT '';

-- Catalog projections are disposable. Invalidate existing rows so their
-- stored searchable text is rebuilt from the current snapshot.
DELETE FROM catalog_documents_fts;
DELETE FROM catalog_documents;
DELETE FROM catalog_source_owners;
DELETE FROM search_meta WHERE key GLOB 'catalog_snapshot_fingerprint:*';
