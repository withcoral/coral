CREATE TABLE agent_turns (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL CHECK (principal_kind IN ('user', 'agent')),
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    state TEXT NOT NULL
        CHECK (state IN ('running', 'needs_action', 'completed', 'failed', 'canceled', 'interrupted')),
    message TEXT NOT NULL CHECK (LENGTH(message) BETWEEN 1 AND 32768),
    final_message TEXT,
    evidence_json TEXT,
    failure TEXT,
    required_action_json TEXT,
    next_event_sequence BIGINT NOT NULL CHECK (next_event_sequence >= 1),
    started_at_unix_nanos BIGINT NOT NULL,
    ended_at_unix_nanos BIGINT,
    UNIQUE (workspace_id, id),
    UNIQUE (workspace_id, task_id, sequence),
    FOREIGN KEY (workspace_id, task_id) REFERENCES task_records(workspace_id, id) ON DELETE CASCADE,
    CHECK (
        (state IN ('running', 'needs_action') AND ended_at_unix_nanos IS NULL)
        OR (state NOT IN ('running', 'needs_action') AND ended_at_unix_nanos >= started_at_unix_nanos)
    ),
    CHECK (state <> 'completed' OR final_message IS NOT NULL),
    CHECK ((state = 'needs_action') = (required_action_json IS NOT NULL)),
    CHECK (state NOT IN ('failed', 'interrupted') OR failure IS NOT NULL)
);
-- One open turn per Task: a thread runs one turn at a time.
CREATE UNIQUE INDEX idx_agent_turns_one_open
    ON agent_turns (workspace_id, task_id)
    WHERE state IN ('running', 'needs_action');

CREATE TABLE agent_turn_events (
    workspace_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (kind IN ('step', 'needs_action')),
    payload_json TEXT NOT NULL,
    occurred_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (turn_id, sequence),
    FOREIGN KEY (workspace_id, turn_id) REFERENCES agent_turns(workspace_id, id) ON DELETE CASCADE
);

-- The model transcript of one Task's thread, in the model provider's message format.
CREATE TABLE agent_thread_messages (
    workspace_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    turn_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content_json TEXT NOT NULL,
    -- An assistant message is one model call: its wall time, retries
    -- included, and the token counts the provider reported. NULL otherwise.
    model_latency_millis BIGINT CHECK (model_latency_millis >= 0),
    model_usage_json TEXT,
    PRIMARY KEY (task_id, sequence),
    FOREIGN KEY (workspace_id, turn_id) REFERENCES agent_turns(workspace_id, id) ON DELETE CASCADE
);
CREATE INDEX idx_agent_thread_messages_thread ON agent_thread_messages (workspace_id, task_id, sequence);
