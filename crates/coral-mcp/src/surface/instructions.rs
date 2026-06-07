use crate::McpRuntimeExposure;

static INITIAL_INSTRUCTIONS_WITH_SQL: &str = "You are connected to Coral, a provider capability system. Discover available capabilities with `search`, inspect compact invocation/projection metadata with `describe`, and run JavaScript Code Mode with `exec` and `wait`. If Coral blocks or confuses you, call the MCP `feedback` tool directly instead of searching for it as a capability. Generated provider methods are global under `tools.*`; use `full_path` from `search` or `describe` to call them from Code Mode. Coral helpers are global under `coral.*`. Generated provider methods throw on provider or transport failure by default; successful calls return `{ ok, complete, partial, errors, source_status, value, error, envelope }`; after checking `response.ok`, read provider data from `response.value`. Use typed refs such as `typescript:*`, `sql_table:*`, and `sql_function:*`; when a SQL projection is available, call it from Code Mode through `coral.sql.query`. Do not guess ambiguous untyped names: retry `describe` with a typed ref or full_path when candidates are returned. Use `describe` with `view: \"detailed\"` only when full provider schemas are needed.";
static INITIAL_INSTRUCTIONS_TYPESCRIPT_ONLY: &str = "You are connected to Coral, a provider capability system. Discover available capabilities with `search`, inspect compact invocation metadata with `describe`, and run JavaScript Code Mode with `exec` and `wait`. If Coral blocks or confuses you, call the MCP `feedback` tool directly instead of searching for it as a capability. Generated provider methods are global under `tools.*`; use `full_path` from `search` or `describe` to call them from Code Mode. Coral helpers are global under `coral.*`. Generated provider methods throw on provider or transport failure by default; successful calls return `{ ok, complete, partial, errors, source_status, value, error, envelope }`; after checking `response.ok`, read provider data from `response.value`. Use typed refs such as `typescript:*`. SQL projection refs and `coral.sql.query` are hidden by runtime exposure. Do not guess ambiguous untyped names: retry `describe` with a typed ref or full_path when candidates are returned. Use `describe` with `view: \"detailed\"` only when full provider schemas are needed.";
static INITIAL_INSTRUCTIONS_SQL_ONLY: &str = "You are connected to Coral, a provider capability system. Discover SQL-capable exports with `search`, inspect compact projection metadata with `describe`, and run JavaScript Code Mode with `exec` and `wait`. If Coral blocks or confuses you, call the MCP `feedback` tool directly instead of searching for it as a capability. Coral helpers are global under `coral.*`; generated provider methods under `tools.*` and TypeScript refs are hidden by runtime exposure. Use typed refs such as `sql_table:*` and `sql_function:*`; when a SQL projection is available, call it from Code Mode through `coral.sql.query`. Do not guess ambiguous untyped names: retry `describe` with a typed ref when candidates are returned. Use `describe` with `view: \"detailed\"` only when full provider schemas are needed.";

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
        assert!(instructions.contains("generated provider methods under `tools.*`"));
        assert!(instructions.contains("hidden by runtime exposure"));
        assert!(!instructions.contains("`typescript:*`"));
        assert!(!instructions.contains("Generated TypeScript tools return"));
    }
}
