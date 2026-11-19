-- Why an Upstream Outcome rejects its Tool call, when it does: a Run stream
-- that moves to another Coral Instance answers a call again from its row.
-- Nullable: a declared success has none, and a row written before this
-- release has none either.
ALTER TABLE tool_invocations ADD COLUMN upstream_rejection TEXT;
