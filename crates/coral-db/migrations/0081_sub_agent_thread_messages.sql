-- A Sub-agent's conversation with its model, numbered per Sub-agent. The
-- content is JSON in Coral's own block format, which the model client
-- converts on every call. A message that a model call produced records the
-- model that answered, the call's latency and the tokens it used.
CREATE TABLE sub_agent_thread_messages (
    workspace_id TEXT NOT NULL,
    sub_agent_id TEXT NOT NULL,
    position BIGINT NOT NULL,
    turn_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content_json TEXT NOT NULL,
    model TEXT,
    latency_ms BIGINT,
    input_tokens BIGINT,
    cache_read_input_tokens BIGINT,
    output_tokens BIGINT,
    PRIMARY KEY (sub_agent_id, position),
    FOREIGN KEY (workspace_id, sub_agent_id) REFERENCES sub_agent_records(workspace_id, id) ON DELETE CASCADE,
    CHECK ((model IS NULL) = (latency_ms IS NULL))
);
