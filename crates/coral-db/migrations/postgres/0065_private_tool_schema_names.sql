-- Saved Functions are Private Tools (ADR 0033). This step renames the tables,
-- columns, constraints and indexes named after Saved Functions, and the stored
-- values that a check constraint names: the Search Entity and search state
-- kind `saved_function`, the source document `saved_function` that a Run
-- recorded for a location, the lifecycle actions `saved_function.*`, and the
-- declaration fields format. Each is now `private_tool`.

-- Like step 45, this one breaks the previous release's queries, so the
-- upgrade stops every earlier process before the first one on this release
-- boots. The step rewrites the rows that the new checks read, so it
-- validates each check as it adds it. lock_timeout makes the migration fail
-- instead of queueing reads and writes behind its locks.
-- squawk-ignore-file renaming-table, renaming-column, renaming-object, constraint-missing-not-valid
SET LOCAL lock_timeout = '5s';

ALTER TABLE saved_function_records RENAME TO private_tool_records;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_pkey TO private_tool_records_pkey;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_owner_path_unique TO private_tool_records_owner_path_unique;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_workspace_fk TO private_tool_records_workspace_fk;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_principal_kind_valid TO private_tool_records_principal_kind_valid;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_path_not_empty TO private_tool_records_path_not_empty;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_description_not_empty TO private_tool_records_description_not_empty;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_source_not_empty TO private_tool_records_source_not_empty;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_declaration_not_empty TO private_tool_records_declaration_not_empty;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_revision_positive TO private_tool_records_revision_positive;
ALTER TABLE private_tool_records RENAME CONSTRAINT saved_function_records_default_access_state_valid TO private_tool_records_default_access_state_valid;
ALTER INDEX idx_saved_function_records_owner_path RENAME TO idx_private_tool_records_owner_path;
ALTER TABLE private_tool_records DROP CONSTRAINT saved_function_records_declaration_fields_valid;
UPDATE private_tool_records SET declaration_fields_format = 'private-tool-declaration-fields-v1';
ALTER TABLE private_tool_records
    ADD CONSTRAINT private_tool_records_declaration_fields_valid
        CHECK (
            declaration_fields_format = 'private-tool-declaration-fields-v1'
            AND (
                (declaration_fields_status = 'ready'
                    AND declaration_fields_json IS NOT NULL
                    AND declaration_fields_failure IS NULL)
                OR
                (declaration_fields_status = 'failed'
                    AND declaration_fields_json IS NULL
                    AND declaration_fields_failure IS NOT NULL
                    AND declaration_fields_failure <> '')
            )
        );

ALTER TABLE saved_function_lifecycle_events RENAME TO private_tool_lifecycle_events;
ALTER TABLE private_tool_lifecycle_events RENAME COLUMN target_saved_function_id TO target_private_tool_id;
ALTER TABLE private_tool_lifecycle_events RENAME CONSTRAINT saved_function_lifecycle_events_pkey TO private_tool_lifecycle_events_pkey;
ALTER TABLE private_tool_lifecycle_events RENAME CONSTRAINT saved_function_lifecycle_events_workspace_fk TO private_tool_lifecycle_events_workspace_fk;
ALTER TABLE private_tool_lifecycle_events RENAME CONSTRAINT saved_function_lifecycle_events_actor_kind_valid TO private_tool_lifecycle_events_actor_kind_valid;
ALTER TABLE private_tool_lifecycle_events RENAME CONSTRAINT saved_function_lifecycle_events_target_path_not_empty TO private_tool_lifecycle_events_target_path_not_empty;
ALTER TABLE private_tool_lifecycle_events RENAME CONSTRAINT saved_function_lifecycle_events_outcome_valid TO private_tool_lifecycle_events_outcome_valid;
ALTER TABLE private_tool_lifecycle_events RENAME CONSTRAINT saved_function_lifecycle_events_trace_id_shape TO private_tool_lifecycle_events_trace_id_shape;
ALTER INDEX idx_saved_function_lifecycle_events_owner_order RENAME TO idx_private_tool_lifecycle_events_owner_order;
ALTER TABLE private_tool_lifecycle_events DROP CONSTRAINT saved_function_lifecycle_events_action_valid;
UPDATE private_tool_lifecycle_events SET action = 'private_tool.' || SUBSTR(action, 16);
ALTER TABLE private_tool_lifecycle_events
    ADD CONSTRAINT private_tool_lifecycle_events_action_valid
        CHECK (action IN (
            'private_tool.created',
            'private_tool.replaced',
            'private_tool.deleted'
        ));

ALTER TABLE program_run_saved_function_snapshots RENAME TO program_run_private_tool_snapshots;
ALTER TABLE program_run_private_tool_snapshots RENAME COLUMN saved_function_id TO private_tool_id;
ALTER TABLE program_run_private_tool_snapshots RENAME CONSTRAINT program_run_saved_function_snapshots_pkey TO program_run_private_tool_snapshots_pkey;
ALTER TABLE program_run_private_tool_snapshots RENAME CONSTRAINT program_run_saved_function_snapshots_run_fk TO program_run_private_tool_snapshots_run_fk;
ALTER TABLE program_run_private_tool_snapshots RENAME CONSTRAINT program_run_saved_function_snapshots_run_path_unique TO program_run_private_tool_snapshots_run_path_unique;
ALTER TABLE program_run_private_tool_snapshots RENAME CONSTRAINT program_run_saved_function_snapshots_path_not_empty TO program_run_private_tool_snapshots_path_not_empty;
ALTER TABLE program_run_private_tool_snapshots RENAME CONSTRAINT program_run_saved_function_snapshots_source_not_empty TO program_run_private_tool_snapshots_source_not_empty;

ALTER TABLE tool_invocations RENAME COLUMN source_saved_function_id TO source_private_tool_id;
ALTER TABLE tool_invocations DROP CONSTRAINT tool_invocations_source_location_consistent;
UPDATE tool_invocations SET source_document = 'private_tool' WHERE source_document = 'saved_function';
ALTER TABLE tool_invocations
    ADD CONSTRAINT tool_invocations_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_private_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    ));

ALTER TABLE program_run_events RENAME COLUMN source_saved_function_id TO source_private_tool_id;
ALTER TABLE program_run_events DROP CONSTRAINT program_run_events_source_location_consistent;
UPDATE program_run_events SET source_document = 'private_tool' WHERE source_document = 'saved_function';
ALTER TABLE program_run_events
    ADD CONSTRAINT program_run_events_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (event_type IN ('program.emitted', 'program.logged')
            AND (
                (source_document = 'program' AND source_private_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0
                    AND source_line > 0 AND source_column > 0)
            )),
        FALSE
    ));

ALTER TABLE search_entities RENAME COLUMN saved_function_id TO private_tool_id;
ALTER TABLE search_entities
    DROP CONSTRAINT search_entities_kind_valid,
    DROP CONSTRAINT search_entities_owner_keys_valid;
UPDATE search_entities SET kind = 'private_tool' WHERE kind = 'saved_function';
ALTER TABLE search_entities
    ADD CONSTRAINT search_entities_kind_valid
        CHECK (kind IN ('provider_tool', 'private_tool')),
    ADD CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND tool_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || tool_key
            AND private_tool_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'private_tool'
            AND private_tool_id IS NOT NULL
            AND doc_id = private_tool_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND tool_key IS NULL)
    );

ALTER TABLE search_index_states RENAME COLUMN saved_function_id TO private_tool_id;
ALTER TABLE search_index_states RENAME COLUMN failed_saved_function_revision TO failed_private_tool_revision;
ALTER TABLE search_index_states
    DROP CONSTRAINT search_index_states_kind_valid,
    DROP CONSTRAINT search_index_states_target_valid;
UPDATE search_index_states SET kind = 'private_tool' WHERE kind = 'saved_function';
ALTER TABLE search_index_states
    ADD CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('provider_tool', 'private_tool')),
    ADD CONSTRAINT search_index_states_target_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND target_id = provider_generation_id
            AND provider_display_name IS NOT NULL
            AND private_tool_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'private_tool'
            AND state = 'failed'
            AND private_tool_id IS NOT NULL
            AND target_id = private_tool_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND provider_display_name IS NULL)
    );

ALTER TABLE task_interactions RENAME COLUMN function_id TO private_tool_id;
