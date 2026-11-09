-- The Built-in Provider's Tools now sit directly on `coral`, and Private
-- Tools, which were Saved Functions, are called at `coral.private` (ADR 0032).
-- Each Provider row stores its full Call Root, so the Built-in Provider's
-- moves from `coral.workspace` to `coral`. Universal Search stores each
-- indexed Call Path as it returns it, so the rows of Built-in Tools and
-- Private Tools move to the new roots too. Retained Run history keeps the
-- paths it recorded.
UPDATE provider_records
SET namespace = 'coral'
WHERE namespace = 'coral.workspace';

UPDATE search_entities
SET call_path = 'coral.' || substr(call_path, length('coral.workspace.') + 1)
WHERE call_path LIKE 'coral.workspace.%';

UPDATE search_entities
SET call_path = 'coral.private.' || substr(call_path, length('coral.functions.') + 1)
WHERE call_path LIKE 'coral.functions.%';
