-- The Workspace a sign-in at the Organization MCP Endpoint chose. It is null
-- for every other resource, whose URL already names its Workspace.
ALTER TABLE oauth_authorization_codes ADD COLUMN workspace_id TEXT;
