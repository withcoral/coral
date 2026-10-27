-- Operations are Provider Tools (ADR 0024). This step renames the tables,
-- columns, constraints and indexes named after Operations, and the two stored
-- values that a check constraint names: the Search Entity kind `operation`,
-- now `provider_tool`, and the policy lifecycle action
-- `operation_policy.access_state_updated`, now `tool_policy.*`.

-- Unlike other steps, this one breaks the previous release's queries, so the
-- upgrade stops every earlier process before the first one on this release
-- boots (ADR 0024). The step rewrites the rows that the new checks read, so it
-- validates each check as it adds it. lock_timeout makes the migration fail
-- instead of queueing reads and writes behind its locks.
-- squawk-ignore-file renaming-table, renaming-column, renaming-object, constraint-missing-not-valid
SET LOCAL lock_timeout = '5s';

ALTER TABLE materialized_operations RENAME TO materialized_tools;
ALTER TABLE materialized_tools RENAME COLUMN operation_key TO tool_key;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_availability_fields_consistent TO materialized_tools_availability_fields_consistent;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_availability_valid TO materialized_tools_availability_valid;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_definition_valid TO materialized_tools_definition_valid;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_generation_fk TO materialized_tools_generation_fk;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_hints_declared_by_source TO materialized_tools_hints_declared_by_source;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_pk TO materialized_tools_pk;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_presentation_valid TO materialized_tools_presentation_valid;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_protocol_index_valid TO materialized_tools_protocol_index_valid;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_protocol_valid TO materialized_tools_protocol_valid;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_relative_path_valid TO materialized_tools_relative_path_valid;
ALTER TABLE materialized_tools RENAME CONSTRAINT materialized_operations_summary_valid TO materialized_tools_summary_valid;

ALTER TABLE operation_invocations RENAME TO tool_invocations;
ALTER TABLE tool_invocations RENAME COLUMN operation_key TO tool_key;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_account_setting_version_positive TO tool_invocations_account_setting_version_positive;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_arguments_not_empty TO tool_invocations_arguments_not_empty;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_call_path_not_empty TO tool_invocations_call_path_not_empty;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_lifecycle_consistent TO tool_invocations_lifecycle_consistent;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_operation_key_not_empty TO tool_invocations_tool_key_not_empty;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_pkey TO tool_invocations_pkey;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_result_bytes_check TO tool_invocations_result_bytes_check;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_result_kind_valid TO tool_invocations_result_kind_valid;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_route_valid TO tool_invocations_route_valid;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_run_fk TO tool_invocations_run_fk;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_source_location_consistent TO tool_invocations_source_location_consistent;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_state_valid TO tool_invocations_state_valid;
ALTER TABLE tool_invocations RENAME CONSTRAINT operation_invocations_workspace_id_unique TO tool_invocations_workspace_id_unique;
ALTER INDEX idx_operation_invocations_provider_history RENAME TO idx_tool_invocations_provider_history;
ALTER INDEX idx_operation_invocations_run RENAME TO idx_tool_invocations_run;

ALTER TABLE operation_access_rules RENAME TO tool_access_rules;
ALTER TABLE tool_access_rules RENAME COLUMN operation_key TO tool_key;
ALTER TABLE tool_access_rules RENAME CONSTRAINT operation_access_rules_operation_key_not_empty TO tool_access_rules_tool_key_not_empty;
ALTER TABLE tool_access_rules RENAME CONSTRAINT operation_access_rules_pk TO tool_access_rules_pk;
ALTER TABLE tool_access_rules RENAME CONSTRAINT operation_access_rules_provider_fk TO tool_access_rules_provider_fk;
ALTER TABLE tool_access_rules RENAME CONSTRAINT operation_access_rules_state_valid TO tool_access_rules_state_valid;
ALTER TABLE tool_access_rules RENAME CONSTRAINT operation_access_rules_workspace_fk TO tool_access_rules_workspace_fk;

ALTER TABLE operation_policy_lifecycle_events RENAME TO tool_policy_lifecycle_events;
ALTER TABLE tool_policy_lifecycle_events RENAME COLUMN target_operation_key TO target_tool_key;
ALTER TABLE tool_policy_lifecycle_events RENAME CONSTRAINT operation_policy_lifecycle_events_authority_source_valid TO tool_policy_lifecycle_events_authority_source_valid;
ALTER TABLE tool_policy_lifecycle_events RENAME CONSTRAINT operation_policy_lifecycle_events_outcome_valid TO tool_policy_lifecycle_events_outcome_valid;
ALTER TABLE tool_policy_lifecycle_events RENAME CONSTRAINT operation_policy_lifecycle_events_pkey TO tool_policy_lifecycle_events_pkey;
ALTER TABLE tool_policy_lifecycle_events RENAME CONSTRAINT operation_policy_lifecycle_events_target_key_not_empty TO tool_policy_lifecycle_events_target_key_not_empty;
ALTER TABLE tool_policy_lifecycle_events RENAME CONSTRAINT operation_policy_lifecycle_events_trace_id_shape TO tool_policy_lifecycle_events_trace_id_shape;
ALTER TABLE tool_policy_lifecycle_events RENAME CONSTRAINT operation_policy_lifecycle_events_workspace_fk TO tool_policy_lifecycle_events_workspace_fk;
ALTER INDEX idx_operation_policy_lifecycle_events_workspace_order RENAME TO idx_tool_policy_lifecycle_events_workspace_order;

ALTER TABLE tool_policy_lifecycle_events
    DROP CONSTRAINT operation_policy_lifecycle_events_action_valid;
UPDATE tool_policy_lifecycle_events
SET action = 'tool_policy.access_state_updated'
WHERE action = 'operation_policy.access_state_updated';
ALTER TABLE tool_policy_lifecycle_events
    ADD CONSTRAINT tool_policy_lifecycle_events_action_valid
        CHECK (action = 'tool_policy.access_state_updated');

ALTER TABLE search_entities RENAME COLUMN operation_key TO tool_key;
ALTER TABLE search_entities
    DROP CONSTRAINT search_entities_kind_valid,
    DROP CONSTRAINT search_entities_owner_keys_valid;
UPDATE search_entities SET kind = 'provider_tool' WHERE kind = 'operation';
ALTER TABLE search_entities
    ADD CONSTRAINT search_entities_kind_valid
        CHECK (kind IN ('provider_tool', 'saved_function')),
    ADD CONSTRAINT search_entities_owner_keys_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND tool_key IS NOT NULL
            AND doc_id = provider_generation_id || ':' || tool_key
            AND saved_function_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'saved_function'
            AND saved_function_id IS NOT NULL
            AND doc_id = saved_function_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND tool_key IS NULL)
    );

ALTER TABLE search_index_states
    DROP CONSTRAINT search_index_states_kind_valid,
    DROP CONSTRAINT search_index_states_target_valid;
UPDATE search_index_states SET kind = 'provider_tool' WHERE kind = 'operation';
ALTER TABLE search_index_states
    ADD CONSTRAINT search_index_states_kind_valid
        CHECK (kind IN ('provider_tool', 'saved_function')),
    ADD CONSTRAINT search_index_states_target_valid CHECK (
        (kind = 'provider_tool'
            AND provider_id IS NOT NULL
            AND provider_generation_id IS NOT NULL
            AND target_id = provider_generation_id
            AND provider_display_name IS NOT NULL
            AND saved_function_id IS NULL
            AND principal_id IS NULL
            AND principal_kind IS NULL)
        OR (kind = 'saved_function'
            AND state = 'failed'
            AND saved_function_id IS NOT NULL
            AND target_id = saved_function_id
            AND principal_id IS NOT NULL
            AND principal_kind IS NOT NULL
            AND principal_kind IN ('user', 'agent')
            AND provider_id IS NULL
            AND provider_generation_id IS NULL
            AND provider_display_name IS NULL)
    );
