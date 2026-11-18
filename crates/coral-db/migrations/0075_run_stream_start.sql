-- What a Coral Instance needs to start a Run that another one accepted: the
-- time limit, which the start adds to its own instant as the deadline. Each
-- Private and Shared Tool the Run linked keeps the revision a call performs
-- and the default Tool Access State that applies to it. An Invocation that a
-- Run stream request opened records that request's number. Nullable: a row
-- written before this release has none, and that release keeps its queries.
ALTER TABLE program_run_records ADD COLUMN time_limit_nanos BIGINT;
ALTER TABLE program_run_private_tool_snapshots ADD COLUMN revision BIGINT;
ALTER TABLE program_run_private_tool_snapshots ADD COLUMN default_access_state TEXT;
ALTER TABLE program_run_shared_tool_snapshots ADD COLUMN default_access_state TEXT;
ALTER TABLE tool_invocations ADD COLUMN request_number BIGINT;
