-- When a Turn started, and when it ended. A running Turn has no end, and a
-- Turn stored before this migration has neither.
ALTER TABLE sub_agent_turns ADD COLUMN started_at_unix_nanos BIGINT;
ALTER TABLE sub_agent_turns ADD COLUMN ended_at_unix_nanos BIGINT;
