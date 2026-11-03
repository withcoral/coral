-- The Run Trace's accept span as one W3C traceparent, written once with
-- acceptance. Nullable: a Run accepted before this release has none.
ALTER TABLE program_run_records ADD COLUMN trace_parent TEXT;
