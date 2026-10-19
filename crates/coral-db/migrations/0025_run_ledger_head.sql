-- The Run Ledger head (ADR 0018). Every chunk commit that writes a Run's
-- Events or Run-scoped rows is predicated on the execution claim id and this
-- head, and moves the head by one. Event numbering stays data on the Event rows.
--
-- next_event_sequence stays: a rolling update runs the previous release against
-- this schema, and that release numbers its Events from it. Acceptance keeps
-- writing its initial value and every chunk commit advances it by its Event
-- count, so the two releases can write one Run in turn; a later release drops
-- the column.
ALTER TABLE program_run_records
    ADD COLUMN chunk_sequence BIGINT NOT NULL DEFAULT 0
        CONSTRAINT program_run_records_chunk_sequence_nonnegative CHECK (chunk_sequence >= 0);
