-- The Coral Instance that holds a Run's claim (ADR 0025). Nullable: a Run a
-- previous release accepted has none, and that release keeps its queries.
ALTER TABLE program_run_records ADD COLUMN holder_instance_id TEXT;
