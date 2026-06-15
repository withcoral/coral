use crate::McpRuntimeExposure;

static INITIAL_INSTRUCTIONS_WITH_SQL: &str = "You are connected to Coral, a provider capability system. Use `search` to find capabilities, `describe` to inspect schemas, and `exec`/`wait` to run JavaScript Code Mode. Search hits carry a preferred `ref` (typed refs such as `typescript:*`, `sql_table:*`, and `sql_function:*`), a generated `call` path, a one-line call `signature`, and a SQL `sql` ref — when the `signature` makes the arguments obvious, skip `describe` and call the tool directly in `exec`. Exact-reference queries (or `expand_top: true`) inline the top hit's compact describe entry as `top`, replacing a separate `describe` call. `intent: \"read\"|\"write\"|\"any\"` is a soft ranking hint, never a filter; read-shaped queries already rank reads first. `describe` schemas may be size-bounded with elided subtrees marked `x-coral-truncated`; expand a renderer-elided subtree with `path` (e.g. `path: \"filter.team\"`, `output.` prefix for the value schema, ~1-2KB) before using `schemas: \"full\"`; source/importer-level truncation stubs are final in the current artifact. Use `view: \"detailed\"` only for raw provider artifacts. Generated `tools.*` calls resolve directly to the provider value and reject on provider/transport failure with diagnostics (catchable with try/catch); pass `{ allowErrorResult: true }` to get `{ ok, value, partial, error }` as data, or `{ envelope: true }` to add transport metadata (status, headers) for pagination and rate limits. `coral.search`, `coral.describe`, and `coral.sql.query` are callable inside `exec` — prefer one exec that refines and calls over separate search/describe/exec round trips, e.g. `const top = (await coral.search({ query: \"github list issues\" })).items[0]; const rows = await tools.github.rest.issues.listForRepo({ owner: \"o\", repo: \"r\", per_page: 20 }); return rows.map(r => r.title);`. `exec`/`wait` return `{ run, result, output, cursor }`: `result` is the script's return value (with `result_truncated` metadata when bounded), `output` is joined console text, and while `run.status` is \"running\", call `wait { run_id, cursor }`. Be economical: describe only tools you will call, return only the fields you need from exec scripts (never whole raw responses), fetch only the pages you need, and prefer SQL refs via `coral.sql.query` for cross-source joins and aggregation — Code Mode is the orchestration engine, SQL the join engine.";
static INITIAL_INSTRUCTIONS_TYPESCRIPT_ONLY: &str = "You are connected to Coral, a provider capability system. Use `search` to find capabilities, `describe` to inspect schemas, and `exec`/`wait` to run JavaScript Code Mode. Search hits carry a preferred `ref` (typed refs such as `typescript:*`), a generated `call` path, and a one-line call `signature` — when the `signature` makes the arguments obvious, skip `describe` and call the tool directly in `exec`. Exact-reference queries (or `expand_top: true`) inline the top hit's compact describe entry as `top`, replacing a separate `describe` call. `intent: \"read\"|\"write\"|\"any\"` is a soft ranking hint, never a filter; read-shaped queries already rank reads first. `describe` schemas may be size-bounded with elided subtrees marked `x-coral-truncated`; expand a renderer-elided subtree with `path` (e.g. `path: \"filter.team\"`, `output.` prefix for the value schema, ~1-2KB) before using `schemas: \"full\"`; source/importer-level truncation stubs are final in the current artifact. Use `view: \"detailed\"` only for raw provider artifacts. Generated `tools.*` calls resolve directly to the provider value and reject on provider/transport failure with diagnostics (catchable with try/catch); pass `{ allowErrorResult: true }` to get `{ ok, value, partial, error }` as data, or `{ envelope: true }` to add transport metadata (status, headers) for pagination and rate limits. `coral.search` and `coral.describe` are callable inside `exec` — prefer one exec that refines and calls over separate search/describe/exec round trips, e.g. `const top = (await coral.search({ query: \"github list issues\" })).items[0]; const rows = await tools.github.rest.issues.listForRepo({ owner: \"o\", repo: \"r\", per_page: 20 }); return rows.map(r => r.title);`. `exec`/`wait` return `{ run, result, output, cursor }`: `result` is the script's return value (with `result_truncated` metadata when bounded), `output` is joined console text, and while `run.status` is \"running\", call `wait { run_id, cursor }`. Be economical: describe only tools you will call, return only the fields you need from exec scripts (never whole raw responses), and fetch only the pages you need. SQL projection refs and `coral.sql.query` are hidden by runtime exposure.";
static INITIAL_INSTRUCTIONS_SQL_ONLY: &str = "You are connected to Coral, a provider capability system. Use `search` to find ranked SQL refs, `describe` for JSON schemas and projection metadata, and `exec`/`wait` for JavaScript Code Mode. Search hits carry typed refs such as `sql_table:*` and `sql_function:*`; query them through `coral.sql.query` inside `exec`. Exact-reference queries (or `expand_top: true`) inline the top hit's compact describe entry as `top`, replacing a separate `describe` call. `intent: \"read\"|\"write\"|\"any\"` is a soft ranking hint, never a filter; read-shaped queries already rank reads first. `describe` schemas may be size-bounded with elided subtrees marked `x-coral-truncated`; expand a renderer-elided subtree with `path` before using `schemas: \"full\"`; source/importer-level truncation stubs are final in the current artifact. Use `view: \"detailed\"` only for raw provider artifacts. `coral.search`, `coral.describe`, and `coral.sql.query` are callable inside `exec` — prefer one exec that refines and queries over separate search/describe/exec round trips, e.g. `return (await coral.sql.query(\"select title from github_rest.issues limit 20\")).rows;`. `exec`/`wait` return `{ run, result, output, cursor }`: `result` is the script's return value (with `result_truncated` metadata when bounded), `output` is joined console text, and while `run.status` is \"running\", call `wait { run_id, cursor }`. Be economical: describe only projections you will query, return only the fields you need from exec scripts (never whole raw responses), fetch only the rows you need, and push joins and aggregation into SQL. Generated provider methods under `tools.*` and TypeScript refs are hidden by runtime exposure.";

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
    fn initial_instructions_teach_the_compact_discovery_and_exec_contract() {
        let instructions = initial_instructions(McpRuntimeExposure::both());
        assert!(instructions.contains("provider capability system"));
        assert!(instructions.contains("search"));
        assert!(instructions.contains("describe"));
        assert!(instructions.contains("exec"));
        assert!(instructions.contains("wait"));
        assert!(instructions.contains("`sql_table:*`"));
        assert!(instructions.contains("`sql_function:*`"));
        assert!(instructions.contains("coral.sql.query"));
        assert!(instructions.contains("`signature`"));
        assert!(instructions.contains("`expand_top: true`"));
        assert!(instructions.contains("intent"));
        assert!(instructions.contains("x-coral-truncated"));
        assert!(instructions.contains("allowErrorResult"));
        assert!(instructions.contains("envelope: true"));
        assert!(instructions.contains("callable inside `exec`"));
        assert!(instructions.contains("{ run, result, output, cursor }"));
        assert!(instructions.contains("`wait { run_id, cursor }`"));
        assert!(instructions.contains("only the fields you need"));
        assert!(instructions.contains("SQL the join engine"));
        assert!(!instructions.contains("has_more"));
        assert!(!instructions.contains("next_after_event_id"));
        assert!(!instructions.contains(&format!("read-only {} database", "SQL")));
    }

    #[test]
    fn initial_instructions_omit_sql_when_sql_exposure_is_disabled() {
        let instructions = initial_instructions(McpRuntimeExposure::typescript_only());
        assert!(instructions.contains("provider capability system"));
        assert!(instructions.contains("`typescript:*`"));
        assert!(instructions.contains("SQL projection refs"));
        assert!(instructions.contains("hidden by runtime exposure"));
        assert!(instructions.contains("`signature`"));
        assert!(instructions.contains("`expand_top: true`"));
        assert!(instructions.contains("intent"));
        assert!(instructions.contains("{ run, result, output, cursor }"));
        assert!(instructions.contains("`wait { run_id, cursor }`"));
        assert!(!instructions.contains("`sql_table:*`"));
        assert!(!instructions.contains("`sql_function:*`"));
        assert!(instructions.contains("coral.sql.query"));
        assert!(!instructions.contains("join engine"));
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
        assert!(instructions.contains("`expand_top: true`"));
        assert!(instructions.contains("intent"));
        assert!(instructions.contains("{ run, result, output, cursor }"));
        assert!(instructions.contains("`wait { run_id, cursor }`"));
        assert!(!instructions.contains("`signature`"));
        assert!(!instructions.contains("`typescript:*`"));
        assert!(!instructions.contains("tools.github"));
        assert!(!instructions.contains("Generated TypeScript tools return"));
    }
}
