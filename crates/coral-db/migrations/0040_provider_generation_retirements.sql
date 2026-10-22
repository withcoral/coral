-- When a collection pass first found a Provider Generation unused: no
-- Provider selected it and nothing else held it. A later pass deletes a
-- Generation that has stayed unused for the retirement grace, with its
-- Operations and shared types; its search rows cascade.
--
-- The mark lives apart from the Generation row, so marking a Generation
-- never rewrites its pinned document and catalog.
CREATE TABLE provider_generation_retirements (
    provider_generation_id TEXT NOT NULL PRIMARY KEY,
    retired_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT provider_generation_retirements_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id)
        ON DELETE CASCADE
);

CREATE INDEX idx_provider_generation_retirements_retired_at
    ON provider_generation_retirements (retired_at_unix_nanos, provider_generation_id);
