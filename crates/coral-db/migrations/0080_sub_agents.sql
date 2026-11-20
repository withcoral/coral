-- A Sub-agent is a model that Coral runs for one caller, inside that caller's
-- Task. Its row never changes, and it is deleted with its Task. The caller is
-- a copy of the Task's Principal: only that Principal reads or stops it.
CREATE TABLE sub_agent_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL CHECK (principal_kind IN ('user', 'agent')),
    UNIQUE (workspace_id, id),
    FOREIGN KEY (workspace_id, task_id) REFERENCES task_records(workspace_id, id) ON DELETE CASCADE
);
CREATE INDEX idx_sub_agent_records_task ON sub_agent_records (workspace_id, task_id);

-- A Turn is a Sub-agent's work from one message to its response. The Coral
-- Instance that runs a Turn renews its lease while the Turn is 'running'.
CREATE TABLE sub_agent_turns (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    sub_agent_id TEXT NOT NULL,
    state TEXT NOT NULL,
    answer TEXT,
    failure TEXT,
    lease_expires_at_unix_nanos BIGINT NOT NULL,
    FOREIGN KEY (workspace_id, sub_agent_id) REFERENCES sub_agent_records(workspace_id, id) ON DELETE CASCADE,
    CHECK (state <> 'completed' OR answer IS NOT NULL),
    CHECK (state <> 'failed' OR failure IS NOT NULL)
);
CREATE INDEX idx_sub_agent_turns_sub_agent ON sub_agent_turns (workspace_id, sub_agent_id);
-- A Sub-agent has at most one open Turn.
CREATE UNIQUE INDEX idx_sub_agent_turns_one_open
    ON sub_agent_turns (workspace_id, sub_agent_id)
    WHERE state = 'running';
