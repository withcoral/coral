-- The one model the Organization's Sub-agents use (coral-agent-model).
-- `singleton` keeps it to one row, and an Organization with no model has
-- none. `id` names the destination: a new base URL gives the row a new `id`,
-- and so does a model that is removed and set again. The API key is sealed
-- for that `id` and base URL.
CREATE TABLE sub_agent_models (
    id TEXT NOT NULL PRIMARY KEY,
    singleton BIGINT NOT NULL
        CONSTRAINT sub_agent_models_singleton UNIQUE
        CONSTRAINT sub_agent_models_one_row CHECK (singleton = 1),
    version BIGINT NOT NULL CONSTRAINT sub_agent_models_version CHECK (version > 0),
    protocol TEXT NOT NULL,
    base_url TEXT NOT NULL,
    model TEXT NOT NULL,
    sealed_api_key TEXT,
    context_window BIGINT,
    request_args_json TEXT NOT NULL,
    changed_by_user_id TEXT NOT NULL,
    changed_at_unix_nanos BIGINT NOT NULL
);
