-- The Run Executor that served a relayed compile: its id, and the kind of
-- credential that admitted it. The Coral Instance that served the row writes
-- them with its answer, and the requester names them on its compile span.
-- Nullable: a probe's answer names none, and nor does an answer an earlier
-- release wrote.
ALTER TABLE compile_requests ADD COLUMN run_executor_id TEXT;
ALTER TABLE compile_requests ADD COLUMN run_executor_kind TEXT;
