-- Keep first-visibility evidence across failed projection rebuilds.
-- NULL history means an older release wrote a failed row whose prior
-- visibility cannot be reconstructed. Never invent publication latency for it.
ALTER TABLE search_index_states ADD COLUMN ever_visible BOOLEAN;
ALTER TABLE search_index_states ADD COLUMN unavailable_since_unix_nanos BIGINT;
UPDATE search_index_states SET ever_visible = TRUE
WHERE kind = 'provider_tool' AND state = 'complete';
UPDATE search_index_states SET unavailable_since_unix_nanos = attempted_at_unix_nanos
WHERE kind = 'provider_tool' AND state = 'failed';
