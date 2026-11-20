-- The lead's messages that a running Turn has not read yet, joined by a
-- blank line. The Turn adds them to its thread before its next model call.
ALTER TABLE sub_agent_turns ADD COLUMN unread_messages TEXT;
