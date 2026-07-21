ALTER TABLE catalog_documents
ADD COLUMN is_workspace_function INTEGER NOT NULL DEFAULT 0
CHECK (is_workspace_function IN (0, 1));
