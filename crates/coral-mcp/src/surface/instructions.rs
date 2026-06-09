use crate::McpRuntimeExposure;

static INITIAL_INSTRUCTIONS_WITH_SQL: &str = "You are connected to Coral, a provider capability system. Use `search` to find ranked refs, `describe` for JSON input/output schemas, and `exec`/`wait` for JavaScript Code Mode. Search hits expose `ref`, `call`, `sql`, `score`, `matched_terms`, and `matched_fields`; use `call` to invoke generated `tools.*` methods in Code Mode, or typed refs such as `typescript:*`, `sql_table:*`, and `sql_function:*` with `describe`. Generated calls return `{ ok, complete, partial, errors, source_status, value, error, envelope }`; after checking `response.ok`, read provider data from `response.value`. Calls reject by default on provider/transport failure; use `{ allowErrorResult: true }` only to collect raw `{ ok: false }` diagnostics. Use `coral.sql.query` for SQL projections. `exec`/`wait` return `{ run, result, events }`; call `wait` while `events.has_more` is true, passing `events.next_after_event_id`. Use `describe` with `view: \"detailed\"` only when full provider artifacts are needed.";
static INITIAL_INSTRUCTIONS_TYPESCRIPT_ONLY: &str = "You are connected to Coral, a provider capability system. Use `search` to find ranked refs, `describe` for JSON input/output schemas, and `exec`/`wait` for JavaScript Code Mode. Search hits expose `ref`, `call`, `score`, `matched_terms`, and `matched_fields`; use `call` to invoke generated `tools.*` methods in Code Mode, or typed refs such as `typescript:*` with `describe`. Generated calls return `{ ok, complete, partial, errors, source_status, value, error, envelope }`; after checking `response.ok`, read provider data from `response.value`. Calls reject by default on provider/transport failure; use `{ allowErrorResult: true }` only to collect raw `{ ok: false }` diagnostics. `exec`/`wait` return `{ run, result, events }`; call `wait` while `events.has_more` is true, passing `events.next_after_event_id`. SQL projection refs and `coral.sql.query` are hidden by runtime exposure. Use `describe` with `view: \"detailed\"` only when full provider artifacts are needed.";
static INITIAL_INSTRUCTIONS_SQL_ONLY: &str = "You are connected to Coral, a provider capability system. Use `search` to find ranked SQL refs, `describe` for JSON schemas/projection metadata, and `exec`/`wait` for JavaScript Code Mode. Search hits expose `ref`, `sql`, `score`, `matched_terms`, and `matched_fields`; use typed refs such as `sql_table:*` and `sql_function:*` with `describe`, then query SQL projections through `coral.sql.query`. `exec`/`wait` return `{ run, result, events }`; call `wait` while `events.has_more` is true, passing `events.next_after_event_id`. Generated provider methods under `tools.*` and TypeScript refs are hidden by runtime exposure. Use `describe` with `view: \"detailed\"` only when full provider artifacts are needed.";

pub(crate) fn initial_instructions(exposure: McpRuntimeExposure) -> &'static str {
    match (exposure.typescript_enabled, exposure.sql_enabled) {
        (true, true) => INITIAL_INSTRUCTIONS_WITH_SQL,
        (true | false, false) => INITIAL_INSTRUCTIONS_TYPESCRIPT_ONLY,
        (false, true) => INITIAL_INSTRUCTIONS_SQL_ONLY,
    }
}

#[cfg(test)]
mod tests {
    use super::initial_instructions;
    use crate::McpRuntimeExposure;

    #[test]
    fn initial_instructions_frame_coral_as_capability_system() {
        let instructions = initial_instructions(McpRuntimeExposure::both());
        assert!(instructions.contains("provider capability system"));
        assert!(instructions.contains("search"));
        assert!(instructions.contains("describe"));
        assert!(instructions.contains("exec"));
        assert!(instructions.contains("wait"));
        assert!(instructions.contains("`sql_table:*`"));
        assert!(instructions.contains("`sql_function:*`"));
        assert!(instructions.contains("coral.sql.query"));
        assert!(!instructions.contains(&format!("read-only {} database", "SQL")));
    }

    #[test]
    fn initial_instructions_omit_sql_when_sql_exposure_is_disabled() {
        let instructions = initial_instructions(McpRuntimeExposure::typescript_only());
        assert!(instructions.contains("provider capability system"));
        assert!(instructions.contains("`typescript:*`"));
        assert!(instructions.contains("SQL projection refs"));
        assert!(instructions.contains("hidden by runtime exposure"));
        assert!(!instructions.contains("`sql_table:*`"));
        assert!(!instructions.contains("`sql_function:*`"));
        assert!(instructions.contains("coral.sql.query"));
        assert!(!instructions.contains("when a SQL projection is available"));
    }

    #[test]
    fn initial_instructions_omit_typescript_when_typescript_exposure_is_disabled() {
        let instructions = initial_instructions(McpRuntimeExposure::sql_only());
        assert!(instructions.contains("provider capability system"));
        assert!(instructions.contains("`sql_table:*`"));
        assert!(instructions.contains("`sql_function:*`"));
        assert!(instructions.contains("coral.sql.query"));
        assert!(instructions.contains("Generated provider methods under `tools.*`"));
        assert!(instructions.contains("hidden by runtime exposure"));
        assert!(!instructions.contains("`typescript:*`"));
        assert!(!instructions.contains("Generated TypeScript tools return"));
    }
}
