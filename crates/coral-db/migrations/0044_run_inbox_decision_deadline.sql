-- A decision in the Run Inbox carries its Approval Request's deadline as the
-- requester read it (ADR 0025): the Run Owner applies the expiry first when
-- it applies the decision past that deadline. A cancellation carries none.
ALTER TABLE program_run_inbox
    ADD COLUMN deadline_at_unix_nanos BIGINT
        CONSTRAINT program_run_inbox_decision_deadline
            CHECK ((approval_id IS NULL) = (deadline_at_unix_nanos IS NULL));
