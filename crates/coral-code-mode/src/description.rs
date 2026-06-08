use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

use crate::PUBLIC_TOOL_NAME;

const MAX_JS_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;
const DEFERRED_NESTED_TOOLS_GUIDANCE: &str = r#"Some nested MCP/app tools may be omitted from this description. They are still available on the global `tools` object and listed in `ALL_TOOLS`.
To find one, filter `ALL_TOOLS` by `name` and `description`."#;
const CORAL_SQL_GUIDANCE: &str = r#"Coral SQL-first guidance:
- Code Mode is a JavaScript orchestration layer around Coral SQL and generated provider tools, not a replacement for SQL. For relational questions, default to one `coral.sql.query` query.
- Put filtering, projection, joins across tables or sources, grouping, ordering, and limits in SQL so DataFusion can optimize the plan and push source-aware filters/limits into scans.
- Do not fetch multiple tables into JavaScript and join, filter, sort, or aggregate them there unless SQL cannot express the operation or you are post-processing a deliberately small result.
- Use JavaScript for dynamic query construction, schema inspection, small result shaping, generated provider calls, branching/retry orchestration, or combining a few results that cannot be expressed as one SQL statement.
- Discover capabilities with `coral.search` and `coral.describe`, inspect SQL tables with `information_schema` or `LIMIT 0`, then query with `coral.sql.query`.

- Return the JSON-serializable value you want `exec` to return; `console.*` is captured as text output for debugging, but do not rely on a bare final expression or a bare awaited tool call.
- Use `information_schema` and `LIMIT 0` queries for schema inspection when you need the concrete output columns before fetching rows.
- Use registered JSON SQL functions such as `json_get_str`, `json_get_int`, `json_get_bool`, and `json_as_text` for filtering and projection over JSON payloads.
- `coral.sql.query("SELECT 1 AS n")` and `coral.sql.query({ sql: "SELECT 1 AS n" })` return `{ rows, row_count }`.
- SQL results are runtime JSON values; inspect `columns` when you need to validate the returned shape.
- Generated provider API operations are available under `tools.<source>...`; Coral host helpers are available under `coral.*`.
"#;
const CORAL_SQL_TYPESCRIPT_DECLARATIONS: &str = r"type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };
type SqlValue = JsonValue;
type SqlRow = Record<string, SqlValue>;
type SqlType = { kind: string; [key: string]: JsonValue };
type SqlColumn<TName extends string = string, TType extends SqlType = SqlType> = {
  name: TName;
  data_type: TType;
  nullable: boolean;
};
type SqlInput = string | { sql: string };
type SqlResult<TRow extends SqlRow = SqlRow> = {
  columns: SqlColumn[];
  rows: TRow[];
  row_count: number;
};
type SqlFunction = {
  <TRow extends SqlRow = SqlRow>(input: SqlInput): Promise<SqlResult<TRow>>;
};
declare const coral: { sql: { query: SqlFunction } };";
const EXEC_DESCRIPTION_TEMPLATE: &str = r#"Run JavaScript code to orchestrate/compose tool calls
- Executes the provided JavaScript code in a fresh V8 isolate through Coral's async entrypoint wrapper.
- Generated provider tools are available on the global `tools` object, for example `await tools.github.rest.search.issuesAndPullRequests(...)`.
- Coral host helpers are available on the global `coral` object; inspect the enabled tool declarations below for available helpers.
- Use `full_path` from `coral.search(...)` or `coral.describe(...)` to call generated provider tools under `tools.*`.
- Tool path segments that are not valid JavaScript identifiers are exposed with normalized identifiers.
- Nested tool methods take the input shape described in their tool declaration.
- Nested tools return JSON-serializable structured values.
- Generated provider tools reject on provider or transport failure by default; the rejection is catchable with JavaScript `try`/`catch`, and an uncaught rejection fails the run.
- Successful generated provider calls return `{ ok, complete, partial, errors, source_status, value, error, envelope }`; after checking `response.ok`, read provider data from `response.value`. REST calls also expose transport details such as `response.envelope.provider.status` and lowercase response headers at `response.envelope.provider.headers`.
- Pass `{ allowErrorResult: true }` as a second argument only when you deliberately want the raw `{ ok: false }` error result with structured provider diagnostics.
- Runs JavaScript in Coral's V8 isolate. Supported globals are `tools.*`, `coral.*`, standard JavaScript, `console.*`, and the documented helpers listed below.
- Accepts raw JavaScript source text, not JSON, quoted strings, or markdown code fences.
- Return the JSON-serializable value you want `exec` to return. A bare final expression or bare `await tools...` statement is ignored unless it is returned.
- Returned object properties with `undefined` values are omitted; `undefined` array items become `null`; top-level `undefined` means no returned value.
- If the source looks like an async/function expression, Coral invokes it and uses its returned value. Otherwise the source is treated as an async function body, so `return value;` is valid.
- You may optionally start the tool input with a first-line pragma like `// @exec: {"yield_time_ms": 10000, "max_output_tokens": 1000}`.
- `exec` waits up to `yield_time_ms` before returning; if the script finishes in that window, `exec` returns `status.name: "CODE_MODE_RUN_STATUS_COMPLETED"` and no immediate `wait` call is needed.
- `yield_time_ms` asks `exec` to yield early after that many milliseconds if the script is still running.
- `max_output_tokens` sets the preview token budget for direct `exec` results. Oversized direct results return a bounded preview with truncation metadata and, when possible, `truncation.full_output_path` for the complete spilled JSON result. Spilled direct results are capped by `max_output_result_spill_bytes=1048576`; larger results still return the preview plus `truncation.max_spill_bytes`, but no full-output path.
- Verbose `console.*` diagnostics are bounded as content events; when Coral truncates, drops, or spills console text, it emits an `output_shaping` content item with `limit_name`, observed/dropped counts, and `full_output_path` when available. Spilled console text is also capped by `max_output_text_spill_bytes=1048576`; after that, diagnostics are dropped with metadata instead of failing the run or growing the spill file indefinitely.
- When the JS code is fully evaluated, the isolate's lifetime ends and unawaited unresolved/fulfilled promises are discarded; unhandled rejected promises fail the run.

- Global helpers:
- `exit()`: Immediately ends the current script successfully (like an early return from the top level).
- `image(imageUrlOrItem: string | { image_url: string; detail?: "high" | "original" | null } | ImageContent, detail?: "high" | "original" | null)`: Appends an image item. `image_url` can be an HTTPS URL or a base64-encoded `data:` URL. To forward an MCP tool image, pass an individual `ImageContent` block from `result.content`, for example `image(result.content[0])`. MCP image blocks may request detail with `_meta: { "coral/imageDetail": "original" }`. When provided, the second `detail` argument overrides any detail embedded in the first argument.
- `store(key: string, value: any)`: stores a serializable value under a string key for later `exec` calls in the same session.
- `load(key: string)`: returns the stored value for a string key, or `undefined` if it is missing.
- `setTimeout(callback: () => void, delayMs?: number)`: schedules a callback to run later and returns a timeout id. Pending timeouts do not keep `exec` alive by themselves; await an explicit promise if you need to wait for one.
- `clearTimeout(timeoutId?: number)`: cancels a timeout created by `setTimeout`.
- `ALL_TOOLS`: metadata for the enabled nested tools as `{ name, description }` entries.
- `yield_control()`: returns `status.name: "CODE_MODE_RUN_STATUS_RUNNING"` immediately while the script keeps running."#;
const WAIT_DESCRIPTION_TEMPLATE: &str = r#"- Use `wait` only after `exec` returns `status.name: "CODE_MODE_RUN_STATUS_RUNNING"`.
- `cell_id` identifies the running `exec` cell to resume or terminate.
- `yield_time_ms` controls how long to wait for more output before yielding again. If omitted, `wait` uses its default wait timeout.
- `terminate: true` stops the running cell instead of waiting for more output.
- `wait` returns typed run events after the requested cursor, or the final completion or termination status for that cell.
- If the cell is still running, `wait` may yield again with the same `cell_id`.
- If the cell finishes after an earlier running response but before `wait`, `wait` returns the buffered final result and closes the cell.
- If the cell was already closed or is unknown, `wait` returns a failed result."#;
// Based off of https://modelcontextprotocol.io/specification/draft/schema#calltoolresult
const MCP_TYPESCRIPT_PREAMBLE: &str = r#"type Role = "user" | "assistant";
type MetaObject = Record<string, unknown>;
type Annotations = {
  audience?: Role[];
  priority?: number;
  lastModified?: string;
};
type Icon = {
  src: string;
  mimeType?: string;
  sizes?: string[];
  theme?: "light" | "dark";
};
type TextResourceContents = {
  uri: string;
  mimeType?: string;
  _meta?: MetaObject;
  text: string;
};
type BlobResourceContents = {
  uri: string;
  mimeType?: string;
  _meta?: MetaObject;
  blob: string;
};
type TextContent = {
  type: "text";
  text: string;
  annotations?: Annotations;
  _meta?: MetaObject;
};
type ImageContent = {
  type: "image";
  data: string;
  mimeType: string;
  annotations?: Annotations;
  _meta?: MetaObject;
};
type AudioContent = {
  type: "audio";
  data: string;
  mimeType: string;
  annotations?: Annotations;
  _meta?: MetaObject;
};
type ResourceLink = {
  icons?: Icon[];
  name: string;
  title?: string;
  uri: string;
  description?: string;
  mimeType?: string;
  annotations?: Annotations;
  size?: number;
  _meta?: MetaObject;
  type: "resource_link";
};
type EmbeddedResource = {
  type: "resource";
  resource: TextResourceContents | BlobResourceContents;
  annotations?: Annotations;
  _meta?: MetaObject;
};
type ContentBlock =
  | TextContent
  | ImageContent
  | AudioContent
  | ResourceLink
  | EmbeddedResource;
type CallToolResult<TStructured = { [key: string]: unknown }> = {
  _meta?: MetaObject;
  content: ContentBlock[];
  isError?: boolean;
  structuredContent?: TStructured;
  [key: string]: unknown;
};"#;

pub const CODE_MODE_PRAGMA_PREFIX: &str = "// @exec:";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodeModeToolKind {
    Function,
    Freeform,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolName {
    pub namespace: Option<String>,
    pub name: String,
}

impl ToolName {
    #[must_use]
    pub fn plain(name: impl Into<String>) -> Self {
        Self {
            namespace: None,
            name: name.into(),
        }
    }

    #[must_use]
    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub tool_name: ToolName,
    pub description: String,
    pub kind: CodeModeToolKind,
    pub input_schema: Option<JsonValue>,
    pub output_schema: Option<JsonValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolNamespaceDescription {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeModeSchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeModeSchemaTable {
    pub schema_name: String,
    pub name: String,
    pub columns: Vec<CodeModeSchemaColumn>,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CodeModeExecPragma {
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    max_output_tokens: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParsedExecSource {
    pub code: String,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

pub fn parse_exec_source(input: &str) -> Result<ParsedExecSource, String> {
    if input.trim().is_empty() {
        return Err(
            "exec expects raw JavaScript source text (non-empty). Provide JS only, optionally with first-line `// @exec: {\"yield_time_ms\": 10000, \"max_output_tokens\": 1000}`.".to_string(),
        );
    }

    let mut args = ParsedExecSource {
        code: input.to_string(),
        yield_time_ms: None,
        max_output_tokens: None,
    };

    let mut lines = input.splitn(2, '\n');
    let first_line = lines.next().unwrap_or_default();
    let rest = lines.next().unwrap_or_default();
    let trimmed = first_line.trim_start();
    let Some(pragma) = trimmed.strip_prefix(CODE_MODE_PRAGMA_PREFIX) else {
        return Ok(args);
    };

    if rest.trim().is_empty() {
        return Err(
            "exec pragma must be followed by JavaScript source on subsequent lines".to_string(),
        );
    }

    let directive = pragma.trim();
    if directive.is_empty() {
        return Err(
            "exec pragma must be a JSON object with supported fields `yield_time_ms` and `max_output_tokens`"
                .to_string(),
        );
    }

    let value: serde_json::Value = serde_json::from_str(directive).map_err(|err| {
        format!(
            "exec pragma must be valid JSON with supported fields `yield_time_ms` and `max_output_tokens`: {err}"
        )
    })?;
    let object = value.as_object().ok_or_else(|| {
        "exec pragma must be a JSON object with supported fields `yield_time_ms` and `max_output_tokens`"
            .to_string()
    })?;
    for key in object.keys() {
        match key.as_str() {
            "yield_time_ms" | "max_output_tokens" => {}
            _ => {
                return Err(format!(
                    "exec pragma only supports `yield_time_ms` and `max_output_tokens`; got `{key}`"
                ));
            }
        }
    }

    let pragma: CodeModeExecPragma = serde_json::from_value(value).map_err(|err| {
        format!(
            "exec pragma fields `yield_time_ms` and `max_output_tokens` must be non-negative safe integers: {err}"
        )
    })?;
    if pragma
        .yield_time_ms
        .is_some_and(|yield_time_ms| yield_time_ms > MAX_JS_SAFE_INTEGER)
    {
        return Err(
            "exec pragma field `yield_time_ms` must be a non-negative safe integer".to_string(),
        );
    }
    if pragma.max_output_tokens.is_some_and(|max_output_tokens| {
        u64::try_from(max_output_tokens)
            .map(|max_output_tokens| max_output_tokens > MAX_JS_SAFE_INTEGER)
            .unwrap_or(true)
    }) {
        return Err(
            "exec pragma field `max_output_tokens` must be a non-negative safe integer".to_string(),
        );
    }

    args.code = rest.to_string();
    args.yield_time_ms = pragma.yield_time_ms;
    args.max_output_tokens = pragma.max_output_tokens;
    Ok(args)
}

pub fn is_code_mode_nested_tool(tool_name: &str) -> bool {
    tool_name != crate::PUBLIC_TOOL_NAME && tool_name != crate::WAIT_TOOL_NAME
}

pub fn build_exec_tool_description(
    enabled_tools: &[ToolDefinition],
    namespace_descriptions: &BTreeMap<String, ToolNamespaceDescription>,
    include_nested_tools: bool,
    deferred_tools_available: bool,
) -> String {
    let mut sections = Vec::new();
    sections.push(EXEC_DESCRIPTION_TEMPLATE.to_string());
    if deferred_tools_available {
        sections.push(DEFERRED_NESTED_TOOLS_GUIDANCE.to_string());
    }
    if !include_nested_tools {
        return sections.join("\n\n");
    }

    if !enabled_tools.is_empty() {
        let mut current_namespace: Option<&str> = None;
        let mut nested_tool_sections = Vec::with_capacity(enabled_tools.len());
        let has_mcp_tools = enabled_tools
            .iter()
            .any(|tool| mcp_structured_content_schema(tool.output_schema.as_ref()).is_some());

        for tool in enabled_tools {
            let name = tool.name.as_str();
            let nested_description = render_code_mode_sample_for_definition(tool);
            let namespace_description = tool
                .tool_name
                .namespace
                .as_ref()
                .and_then(|namespace| namespace_descriptions.get(namespace));
            let next_namespace = namespace_description
                .map(|namespace_description| namespace_description.name.as_str());
            if next_namespace != current_namespace {
                if let Some(namespace_description) = namespace_description {
                    let namespace_description_text = namespace_description.description.trim();
                    if !namespace_description_text.is_empty() {
                        nested_tool_sections.push(format!(
                            "## {}\n{namespace_description_text}",
                            namespace_description.name
                        ));
                    }
                }
                current_namespace = next_namespace;
            }

            let global_name = code_mode_global_name(name);
            let nested_description = nested_description.trim();
            if nested_description.is_empty() {
                nested_tool_sections.push(render_tool_heading(&global_name, name));
            } else {
                nested_tool_sections.push(format!(
                    "{}\n{nested_description}",
                    render_tool_heading(&global_name, name)
                ));
            }
        }

        if has_mcp_tools {
            sections.push(format!(
                "Shared MCP Types:\n```ts\n{MCP_TYPESCRIPT_PREAMBLE}\n```"
            ));
        }
        let nested_tool_reference = nested_tool_sections.join("\n\n");
        sections.push(nested_tool_reference);
    }

    sections.join("\n\n")
}

pub fn build_coral_exec_tool_description(
    enabled_tools: &[ToolDefinition],
    schema_tables: &[CodeModeSchemaTable],
) -> String {
    let mut description = build_exec_tool_description(enabled_tools, &BTreeMap::new(), true, false);
    if !enabled_tools
        .iter()
        .any(|tool| tool.name == "coral.sql.query")
    {
        return description;
    }

    description = format!("{CORAL_SQL_GUIDANCE}\n{description}");
    description.push_str("\nCoral SQL TypeScript helpers:\n```ts\n");
    description.push_str(CORAL_SQL_TYPESCRIPT_DECLARATIONS);
    description.push_str("\n```");

    let schema_declarations = render_coral_schema_declarations(schema_tables);
    if !schema_declarations.is_empty() {
        description.push_str("\nCoral schema declarations:\n```ts\n");
        description.push_str(&schema_declarations);
        description.push_str("\n```");
    }
    description
}

pub fn build_wait_tool_description() -> &'static str {
    WAIT_DESCRIPTION_TEMPLATE
}

pub fn normalize_code_mode_identifier(tool_key: &str) -> String {
    let mut identifier = String::new();

    for (index, ch) in tool_key.chars().enumerate() {
        let is_valid = if index == 0 {
            ch == '_' || ch == '$' || ch.is_ascii_alphabetic()
        } else {
            ch == '_' || ch == '$' || ch.is_ascii_alphanumeric()
        };

        if is_valid {
            identifier.push(ch);
        } else {
            identifier.push('_');
        }
    }

    if identifier.is_empty() {
        "_".to_string()
    } else {
        identifier
    }
}

pub fn augment_tool_definition(mut definition: ToolDefinition) -> ToolDefinition {
    if definition.name != PUBLIC_TOOL_NAME {
        definition.description = render_code_mode_sample_for_definition(&definition);
    }
    definition
}

#[cfg(feature = "code-mode")]
pub(crate) fn enabled_tool_metadata(definition: &ToolDefinition) -> EnabledToolMetadata {
    EnabledToolMetadata {
        tool_name: definition.tool_name.clone(),
        global_name: code_mode_global_name(&definition.name),
        description: definition.description.clone(),
        kind: definition.kind,
    }
}

fn code_mode_global_name(name: &str) -> String {
    let mut segments = name.split('.');
    let Some(root) = segments.next() else {
        return normalize_code_mode_identifier(name);
    };
    if matches!(root, "tools" | "coral") && segments.clone().next().is_some() {
        std::iter::once(root.to_string())
            .chain(segments.map(normalize_code_mode_identifier))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        normalize_code_mode_identifier(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg(feature = "code-mode")]
pub(crate) struct EnabledToolMetadata {
    pub tool_name: ToolName,
    pub global_name: String,
    pub description: String,
    pub kind: CodeModeToolKind,
}

pub fn render_code_mode_sample(
    description: &str,
    tool_name: &str,
    input_name: &str,
    input_type: String,
    output_type: String,
) -> String {
    let usage_output_type = output_type.clone();
    let declaration = render_code_mode_declaration(tool_name, input_name, input_type, output_type);
    let usage =
        generated_provider_usage_example(tool_name, input_name, &declaration, &usage_output_type)
            .map_or_else(String::new, |usage| {
                format!("\n\nexec usage example:\n```js\n{usage}\n```")
            });
    format!(
        "{description}\n\nexec tool declaration (documentation-only TypeScript signature):\n```ts\n{declaration}\n```{usage}"
    )
}

fn generated_provider_usage_example(
    tool_name: &str,
    input_name: &str,
    declaration: &str,
    output_type: &str,
) -> Option<String> {
    if !tool_name.starts_with("tools.")
        || !output_type.contains("ok: boolean")
        || !output_type.contains("value")
    {
        return None;
    }
    let call_path = declaration
        .strip_prefix("declare const tools: ")
        .and_then(|_| tool_name.strip_prefix("tools."))
        .map(|path| format!("tools.{path}"))?;
    Some(format!(
        "const response = await {call_path}({input_name}, {{ allowErrorResult: true }});\nif (!response.ok) return response;\nconst {{ value }} = response;\nreturn value;"
    ))
}

fn render_code_mode_sample_for_definition(definition: &ToolDefinition) -> String {
    let input_name = match definition.kind {
        CodeModeToolKind::Function => "args",
        CodeModeToolKind::Freeform => "input",
    };
    let input_type = match definition.kind {
        CodeModeToolKind::Function => definition
            .input_schema
            .as_ref()
            .map(render_json_schema_to_typescript)
            .unwrap_or_else(|| "unknown".to_string()),
        CodeModeToolKind::Freeform => "string".to_string(),
    };
    let output_type = if let Some(structured_content_schema) =
        mcp_structured_content_schema(definition.output_schema.as_ref())
    {
        let structured_content_type = match structured_content_schema {
            McpStructuredContentSchema::Unknown => "unknown".to_string(),
            McpStructuredContentSchema::Schema(schema) => render_json_schema_to_typescript(schema),
        };
        if structured_content_type == "unknown" {
            "CallToolResult".to_string()
        } else {
            format!("CallToolResult<{structured_content_type}>")
        }
    } else {
        definition
            .output_schema
            .as_ref()
            .map(render_json_schema_to_typescript)
            .unwrap_or_else(|| "unknown".to_string())
    };
    render_code_mode_sample(
        &definition.description,
        &definition.name,
        input_name,
        input_type,
        output_type,
    )
}

fn render_code_mode_tool_declaration(
    tool_name: &str,
    input_name: &str,
    input_type: String,
    output_type: String,
) -> String {
    let tool_name = normalize_code_mode_identifier(tool_name);
    format!(
        "{tool_name}({input_name}: {input_type}, options?: {{ allowErrorResult?: boolean }}): Promise<{output_type}>;"
    )
}

fn render_code_mode_declaration(
    tool_name: &str,
    input_name: &str,
    input_type: String,
    output_type: String,
) -> String {
    let mut segments = tool_name.split('.');
    let Some(root) = segments.next() else {
        return format!(
            "declare const tools: {{ {} }};",
            render_code_mode_tool_declaration(tool_name, input_name, input_type, output_type)
        );
    };
    let path = segments
        .map(normalize_code_mode_identifier)
        .collect::<Vec<_>>();
    if matches!(root, "tools" | "coral") && !path.is_empty() {
        let declaration =
            render_nested_code_mode_declaration(&path, input_name, input_type, output_type);
        return format!("declare const {root}: {declaration};");
    }

    format!(
        "declare const tools: {{ {} }};",
        render_code_mode_tool_declaration(tool_name, input_name, input_type, output_type)
    )
}

fn render_nested_code_mode_declaration(
    path: &[String],
    input_name: &str,
    input_type: String,
    output_type: String,
) -> String {
    let Some((segment, rest)) = path.split_first() else {
        return "{}".to_string();
    };
    if rest.is_empty() {
        return format!(
            "{{ {} }}",
            render_code_mode_tool_declaration(segment, input_name, input_type, output_type)
        );
    }
    format!(
        "{{ {segment}: {} }}",
        render_nested_code_mode_declaration(rest, input_name, input_type, output_type)
    )
}

fn render_tool_heading(global_name: &str, raw_name: &str) -> String {
    if global_name == raw_name {
        format!("### `{global_name}`")
    } else {
        format!("### `{global_name}` (`{raw_name}`)")
    }
}

pub fn render_json_schema_to_typescript(schema: &JsonValue) -> String {
    render_json_schema_to_typescript_inner(schema)
}

pub fn render_coral_schema_declarations(tables: &[CodeModeSchemaTable]) -> String {
    if tables.is_empty() {
        return String::new();
    }

    let mut output = String::from(
        "declare namespace CoralSchema {\n  type Row<S extends keyof Tables, T extends keyof Tables[S]> = Tables[S][T];\n  interface Tables {\n",
    );
    let mut sorted_tables = tables.iter().collect::<Vec<_>>();
    sorted_tables.sort_unstable_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then_with(|| left.name.cmp(&right.name))
    });

    let mut current_schema: Option<&str> = None;
    for table in sorted_tables {
        if current_schema != Some(table.schema_name.as_str()) {
            if current_schema.is_some() {
                output.push_str("    };\n");
            }
            current_schema = Some(&table.schema_name);
            output.push_str("    ");
            output.push_str(&quoted_ts_key(&table.schema_name));
            output.push_str(": {\n");
        }
        output.push_str("      ");
        output.push_str(&quoted_ts_key(&table.name));
        output.push_str(": {\n");
        for column in &table.columns {
            output.push_str("        ");
            output.push_str(&quoted_ts_key(&column.name));
            output.push_str(": ");
            output.push_str(typescript_type_for_datafusion(&column.data_type));
            if column.nullable {
                output.push_str(" | null");
            }
            output.push_str(";\n");
        }
        output.push_str("      };\n");
    }
    output.push_str("    };\n  }\n}\n");
    output
}

fn quoted_ts_key(value: &str) -> String {
    serde_json::to_string(value).expect("string key serializes")
}

fn typescript_type_for_datafusion(data_type: &str) -> &'static str {
    let data_type = data_type.to_ascii_lowercase();
    if data_type.starts_with("int64")
        || data_type.starts_with("uint64")
        || data_type.starts_with("decimal")
    {
        "string"
    } else if data_type.contains("int")
        || data_type.contains("float")
        || data_type.contains("double")
    {
        "number"
    } else if data_type.contains("bool") {
        "boolean"
    } else if data_type.contains("utf8")
        || data_type.contains("string")
        || data_type.contains("json")
        || data_type.contains("date")
        || data_type.contains("time")
    {
        "string"
    } else {
        "JsonValue"
    }
}

enum McpStructuredContentSchema<'a> {
    Unknown,
    Schema(&'a JsonValue),
}

fn mcp_structured_content_schema(
    output_schema: Option<&JsonValue>,
) -> Option<McpStructuredContentSchema<'_>> {
    let output_schema = output_schema?;
    let properties = output_schema
        .get("properties")
        .and_then(JsonValue::as_object)?;
    let content_schema = properties.get("content").and_then(JsonValue::as_object)?;
    if content_schema.get("type").and_then(JsonValue::as_str) != Some("array") {
        return None;
    }

    if content_schema
        .get("items")
        .and_then(JsonValue::as_object)
        .is_none_or(|items| items.get("type").and_then(JsonValue::as_str) != Some("object"))
    {
        return None;
    }

    if properties
        .get("isError")
        .and_then(JsonValue::as_object)
        .is_none_or(|schema| schema.get("type").and_then(JsonValue::as_str) != Some("boolean"))
    {
        return None;
    }

    if properties
        .get("_meta")
        .and_then(JsonValue::as_object)
        .is_none_or(|schema| schema.get("type").and_then(JsonValue::as_str) != Some("object"))
    {
        return None;
    }

    Some(properties.get("structuredContent").map_or(
        McpStructuredContentSchema::Unknown,
        McpStructuredContentSchema::Schema,
    ))
}

fn render_json_schema_to_typescript_inner(schema: &JsonValue) -> String {
    match schema {
        JsonValue::Bool(true) => "unknown".to_string(),
        JsonValue::Bool(false) => "never".to_string(),
        JsonValue::Object(map) => {
            if let Some(value) = map.get("const") {
                return render_json_schema_literal(value);
            }

            if let Some(values) = map.get("enum").and_then(JsonValue::as_array) {
                let rendered = values
                    .iter()
                    .map(render_json_schema_literal)
                    .collect::<Vec<_>>();
                if !rendered.is_empty() {
                    return rendered.join(" | ");
                }
            }

            for key in ["anyOf", "oneOf"] {
                if let Some(variants) = map.get(key).and_then(JsonValue::as_array) {
                    let rendered = variants
                        .iter()
                        .map(render_json_schema_to_typescript_inner)
                        .collect::<Vec<_>>();
                    if !rendered.is_empty() {
                        return rendered.join(" | ");
                    }
                }
            }

            if let Some(schema_type) = map.get("type") {
                if let Some(types) = schema_type.as_array() {
                    let rendered = types
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(|schema_type| render_json_schema_type_keyword(map, schema_type))
                        .collect::<Vec<_>>();
                    if !rendered.is_empty() {
                        return rendered.join(" | ");
                    }
                }

                if let Some(schema_type) = schema_type.as_str() {
                    return render_json_schema_type_keyword(map, schema_type);
                }
            }

            if map.contains_key("properties")
                || map.contains_key("additionalProperties")
                || map.contains_key("required")
            {
                return render_json_schema_object(map);
            }

            if map.contains_key("items") || map.contains_key("prefixItems") {
                return render_json_schema_array(map);
            }

            if let Some(variants) = map.get("allOf").and_then(JsonValue::as_array) {
                let rendered = variants
                    .iter()
                    .map(render_json_schema_to_typescript_inner)
                    .collect::<Vec<_>>();
                if !rendered.is_empty() {
                    return rendered.join(" & ");
                }
            }

            "unknown".to_string()
        }
        _ => "unknown".to_string(),
    }
}

fn render_json_schema_type_keyword(
    map: &serde_json::Map<String, JsonValue>,
    schema_type: &str,
) -> String {
    match schema_type {
        "string" => "string".to_string(),
        "number" | "integer" => "number".to_string(),
        "boolean" => "boolean".to_string(),
        "null" => "null".to_string(),
        "array" => render_json_schema_array(map),
        "object" => render_json_schema_object(map),
        _ => "unknown".to_string(),
    }
}

fn render_json_schema_array(map: &serde_json::Map<String, JsonValue>) -> String {
    if let Some(items) = map.get("items") {
        let item_type = render_json_schema_to_typescript_inner(items);
        return format!("Array<{item_type}>");
    }

    if let Some(items) = map.get("prefixItems").and_then(JsonValue::as_array) {
        let item_types = items
            .iter()
            .map(render_json_schema_to_typescript_inner)
            .collect::<Vec<_>>();
        if !item_types.is_empty() {
            return format!("[{}]", item_types.join(", "));
        }
    }

    "unknown[]".to_string()
}

fn append_additional_properties_line(
    lines: &mut Vec<String>,
    map: &serde_json::Map<String, JsonValue>,
    properties: &serde_json::Map<String, JsonValue>,
    line_prefix: &str,
) {
    if let Some(additional_properties) = map.get("additionalProperties") {
        let property_type = match additional_properties {
            JsonValue::Bool(true) => Some("unknown".to_string()),
            JsonValue::Bool(false) => None,
            value => Some(render_json_schema_to_typescript_inner(value)),
        };

        if let Some(property_type) = property_type {
            lines.push(format!("{line_prefix}[key: string]: {property_type};"));
        }
    } else if properties.is_empty() {
        lines.push(format!("{line_prefix}[key: string]: unknown;"));
    }
}

fn has_property_description(value: &JsonValue) -> bool {
    value
        .get("description")
        .and_then(JsonValue::as_str)
        .is_some_and(|description| !description.is_empty())
}

fn render_json_schema_object_property(name: &str, value: &JsonValue, required: &[&str]) -> String {
    let optional = if required.iter().any(|required_name| required_name == &name) {
        ""
    } else {
        "?"
    };
    let property_name = render_json_schema_property_name(name);
    let property_type = render_json_schema_to_typescript_inner(value);
    format!("{property_name}{optional}: {property_type};")
}

fn render_json_schema_object(map: &serde_json::Map<String, JsonValue>) -> String {
    let required = map
        .get("required")
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let properties = map
        .get("properties")
        .and_then(JsonValue::as_object)
        .cloned()
        .unwrap_or_default();

    let mut sorted_properties = properties.iter().collect::<Vec<_>>();
    sorted_properties.sort_unstable_by(|(name_a, _), (name_b, _)| name_a.cmp(name_b));
    if sorted_properties
        .iter()
        .any(|(_, value)| has_property_description(value))
    {
        let mut lines = vec!["{".to_string()];
        for (name, value) in sorted_properties {
            if let Some(description) = value.get("description").and_then(JsonValue::as_str) {
                for description_line in description
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                {
                    lines.push(format!("  // {description_line}"));
                }
            }

            lines.push(format!(
                "  {}",
                render_json_schema_object_property(name, value, &required)
            ));
        }

        append_additional_properties_line(&mut lines, map, &properties, "  ");
        lines.push("}".to_string());
        return lines.join("\n");
    }

    let mut lines = sorted_properties
        .into_iter()
        .map(|(name, value)| render_json_schema_object_property(name, value, &required))
        .collect::<Vec<_>>();

    append_additional_properties_line(&mut lines, map, &properties, "");

    if lines.is_empty() {
        return "{}".to_string();
    }

    format!("{{ {} }}", lines.join(" "))
}

fn render_json_schema_property_name(name: &str) -> String {
    if normalize_code_mode_identifier(name) == name {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{}\"", name.replace('"', "\\\"")))
    }
}

fn render_json_schema_literal(value: &JsonValue) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::CodeModeSchemaColumn;
    use super::CodeModeSchemaTable;
    use super::CodeModeToolKind;
    use super::ParsedExecSource;
    use super::ToolDefinition;
    use super::ToolName;
    use super::ToolNamespaceDescription;
    use super::augment_tool_definition;
    use super::build_coral_exec_tool_description;
    use super::build_exec_tool_description;
    use super::normalize_code_mode_identifier;
    use super::parse_exec_source;
    use super::render_code_mode_declaration;
    use super::render_coral_schema_declarations;
    use super::render_json_schema_to_typescript;
    use pretty_assertions::assert_eq;
    use serde_json::Value as JsonValue;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn mcp_call_tool_result_schema(structured_content_schema: JsonValue) -> JsonValue {
        json!({
            "type": "object",
            "properties": {
                "content": {
                    "type": "array",
                    "items": {
                        "type": "object"
                    }
                },
                "structuredContent": structured_content_schema,
                "isError": { "type": "boolean" },
                "_meta": { "type": "object" }
            },
            "required": ["content"],
            "additionalProperties": false
        })
    }

    #[test]
    fn parse_exec_source_without_pragma() {
        assert_eq!(
            parse_exec_source("return 'hi';").unwrap(),
            ParsedExecSource {
                code: "return 'hi';".to_string(),
                yield_time_ms: None,
                max_output_tokens: None,
            }
        );
    }

    #[test]
    fn parse_exec_source_with_pragma() {
        assert_eq!(
            parse_exec_source("// @exec: {\"yield_time_ms\": 10}\nreturn 'hi';").unwrap(),
            ParsedExecSource {
                code: "return 'hi';".to_string(),
                yield_time_ms: Some(10),
                max_output_tokens: None,
            }
        );
    }

    #[test]
    fn normalize_identifier_rewrites_invalid_characters() {
        assert_eq!(
            "source_tool_get_profile",
            normalize_code_mode_identifier("source-tool/get profile")
        );
        assert_eq!(
            "hidden_dynamic_tool",
            normalize_code_mode_identifier("hidden-dynamic-tool")
        );
    }

    #[test]
    fn augment_tool_definition_appends_typed_declaration() {
        let definition = ToolDefinition {
            name: "hidden_dynamic_tool".to_string(),
            tool_name: ToolName::plain("hidden_dynamic_tool"),
            description: "Test tool".to_string(),
            kind: CodeModeToolKind::Function,
            input_schema: Some(json!({
                "type": "object",
                "properties": { "city": { "type": "string" } },
                "required": ["city"],
                "additionalProperties": false
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": { "ok": { "type": "boolean" } },
                "required": ["ok"]
            })),
        };

        let description = augment_tool_definition(definition).description;
        assert!(description.contains("declare const tools"));
        assert!(
            description.contains(
                "hidden_dynamic_tool(args: { city: string; }, options?: { allowErrorResult?: boolean }): Promise<{ ok: boolean; }>;"
            )
        );
    }

    #[test]
    fn nested_tool_declaration_includes_error_result_option() {
        assert_eq!(
            render_code_mode_declaration(
                "tools.github.rest.issues.list",
                "args",
                "{}".to_string(),
                "{ ok: boolean; }".to_string()
            ),
            "declare const tools: { github: { rest: { issues: { list(args: {}, options?: { allowErrorResult?: boolean }): Promise<{ ok: boolean; }>; } } } };"
        );
    }

    #[test]
    fn schema_renderer_preserves_object_properties_with_all_of_constraints() {
        let rendered = render_json_schema_to_typescript(&json!({
            "type": "object",
            "properties": {
                "body": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    },
                    "additionalProperties": false
                },
                "contentType": { "type": "string" },
                "json": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false,
            "allOf": [
                { "not": { "required": ["body", "json"] } },
                {
                    "anyOf": [
                        { "required": ["body"] },
                        { "required": ["json"] }
                    ]
                }
            ]
        }));

        assert!(rendered.contains("body?: { title: string; };"));
        assert!(rendered.contains("contentType?: string;"));
        assert!(rendered.contains("json?: { title: string; };"));
        assert!(
            !rendered.contains("unknown"),
            "object properties should not be hidden by allOf constraints: {rendered}"
        );
    }

    #[test]
    fn augment_tool_definition_includes_property_descriptions_as_comments() {
        let definition = ToolDefinition {
            name: "weather_tool".to_string(),
            tool_name: ToolName::plain("weather_tool"),
            description: "Weather tool".to_string(),
            kind: CodeModeToolKind::Function,
            input_schema: Some(json!({
                "type": "object",
                "properties": {
                    "weather": {
                        "type": "array",
                        "description": "look up weather for a given list of locations",
                        "items": {
                            "type": "object",
                            "properties": {
                                "location": { "type": "string" }
                            },
                            "required": ["location"]
                        }
                    }
                },
                "required": ["weather"]
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "forecast": {
                        "type": "string",
                        "description": "human readable weather forecast"
                    }
                },
                "required": ["forecast"]
            })),
        };

        let description = augment_tool_definition(definition).description;
        assert!(description.contains(
            r#"weather_tool(args: {
  // look up weather for a given list of locations
  weather: Array<{ location: string; }>;
}, options?: { allowErrorResult?: boolean }): Promise<{
  // human readable weather forecast
  forecast: string;
}>;"#
        ));
    }

    #[test]
    fn exec_description_includes_nested_tools_when_requested() {
        let description = build_exec_tool_description(
            &[ToolDefinition {
                name: "foo".to_string(),
                tool_name: ToolName::plain("foo"),
                description: "bar".to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: None,
                output_schema: None,
            }],
            &BTreeMap::new(),
            /*include_nested_tools*/ true,
            /*deferred_tools_available*/ false,
        );
        assert!(description.contains(
            "### `foo`
bar"
        ));
        assert!(!description.contains("do not attempt to use any other tools directly"));
    }

    #[test]
    fn exec_description_mentions_timeout_helpers() {
        let description = build_exec_tool_description(
            &[],
            &BTreeMap::new(),
            /*include_nested_tools*/ false,
            /*deferred_tools_available*/ false,
        );
        assert!(description.contains("`setTimeout(callback: () => void, delayMs?: number)`"));
        assert!(description.contains("`clearTimeout(timeoutId?: number)`"));
        assert!(description.contains("Coral's V8 isolate"));
        assert!(description.contains("Supported globals are `tools.*`, `coral.*`"));
    }

    #[test]
    fn generated_provider_tool_description_shows_wrapper_value_usage() {
        let description = build_exec_tool_description(
            &[ToolDefinition {
                name: "tools.github.rest.search.issuesAndPullRequests".to_string(),
                tool_name: ToolName::plain("tools.github.rest.search.issuesAndPullRequests"),
                description: "Search issues and pull requests".to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: Some(json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "object",
                            "properties": {
                                "q": { "type": "string" }
                            },
                            "required": ["q"]
                        }
                    },
                    "required": ["query"]
                })),
                output_schema: Some(json!({
                    "type": "object",
                    "required": ["ok", "value"],
                    "properties": {
                        "ok": { "type": "boolean" },
                        "value": {
                            "type": "object",
                            "properties": {
                                "items": { "type": "array" }
                            }
                        }
                    }
                })),
            }],
            &BTreeMap::new(),
            /*include_nested_tools*/ true,
            /*deferred_tools_available*/ false,
        );

        assert!(description.contains("exec usage example"));
        assert!(description.contains(
            "const response = await tools.github.rest.search.issuesAndPullRequests(args, { allowErrorResult: true });"
        ));
        assert!(description.contains("if (!response.ok) return response;"));
        assert!(description.contains("const { value } = response;"));
        assert!(description.contains("return value;"));
    }

    #[test]
    fn exec_description_groups_namespace_instructions_once() {
        let namespace_descriptions = BTreeMap::from([(
            "mcp__sample__".to_string(),
            ToolNamespaceDescription {
                name: "mcp__sample".to_string(),
                description: "Shared namespace guidance.".to_string(),
            },
        )]);
        let description = build_exec_tool_description(
            &[
                ToolDefinition {
                    name: "mcp__sample__alpha".to_string(),
                    tool_name: ToolName::namespaced("mcp__sample__", "alpha"),
                    description: "First tool".to_string(),
                    kind: CodeModeToolKind::Function,
                    input_schema: Some(json!({
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    })),
                    output_schema: Some(mcp_call_tool_result_schema(json!({
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    }))),
                },
                ToolDefinition {
                    name: "mcp__sample__beta".to_string(),
                    tool_name: ToolName::namespaced("mcp__sample__", "beta"),
                    description: "Second tool".to_string(),
                    kind: CodeModeToolKind::Function,
                    input_schema: Some(json!({
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    })),
                    output_schema: Some(mcp_call_tool_result_schema(json!({
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false
                    }))),
                },
            ],
            &namespace_descriptions,
            /*include_nested_tools*/ true,
            /*deferred_tools_available*/ false,
        );
        assert_eq!(description.matches("## mcp__sample").count(), 1);
        assert!(description.contains("## mcp__sample\nShared namespace guidance."));
        assert!(description.contains(
            "declare const tools: { mcp__sample__alpha(args: {}, options?: { allowErrorResult?: boolean }): Promise<CallToolResult<{}>>; };"
        ));
        assert!(description.contains(
            "declare const tools: { mcp__sample__beta(args: {}, options?: { allowErrorResult?: boolean }): Promise<CallToolResult<{}>>; };"
        ));
    }

    #[test]
    fn exec_description_omits_empty_namespace_sections() {
        let namespace_descriptions = BTreeMap::from([(
            "mcp__sample__".to_string(),
            ToolNamespaceDescription {
                name: "mcp__sample".to_string(),
                description: String::new(),
            },
        )]);
        let description = build_exec_tool_description(
            &[ToolDefinition {
                name: "mcp__sample__alpha".to_string(),
                tool_name: ToolName::namespaced("mcp__sample__", "alpha"),
                description: "First tool".to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: Some(json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                })),
                output_schema: Some(mcp_call_tool_result_schema(json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }))),
            }],
            &namespace_descriptions,
            /*include_nested_tools*/ true,
            /*deferred_tools_available*/ false,
        );

        assert!(!description.contains("## mcp__sample"));
        assert!(description.contains("### `mcp__sample__alpha`"));
    }

    #[test]
    fn exec_description_renders_shared_mcp_types_once() {
        let first_tool = augment_tool_definition(ToolDefinition {
            name: "mcp__sample__alpha".to_string(),
            tool_name: ToolName::namespaced("mcp__sample__", "alpha"),
            description: "First tool".to_string(),
            kind: CodeModeToolKind::Function,
            input_schema: Some(json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": {
                        "type": "array",
                        "items": {
                            "type": "object"
                        }
                    },
                    "structuredContent": {
                        "type": "object",
                        "properties": {
                            "echo": { "type": "string" }
                        },
                        "required": ["echo"],
                        "additionalProperties": false
                    },
                    "isError": { "type": "boolean" },
                    "_meta": { "type": "object" }
                },
                "required": ["content"],
                "additionalProperties": false
            })),
        });
        let second_tool = augment_tool_definition(ToolDefinition {
            name: "mcp__sample__beta".to_string(),
            tool_name: ToolName::namespaced("mcp__sample__", "beta"),
            description: "Second tool".to_string(),
            kind: CodeModeToolKind::Function,
            input_schema: Some(json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": {
                        "type": "array",
                        "items": {
                            "type": "object"
                        }
                    },
                    "structuredContent": {
                        "type": "object",
                        "properties": {
                            "count": { "type": "integer" }
                        },
                        "required": ["count"],
                        "additionalProperties": false
                    },
                    "isError": { "type": "boolean" },
                    "_meta": { "type": "object" }
                },
                "required": ["content"],
                "additionalProperties": false
            })),
        });

        let description = build_exec_tool_description(
            &[
                ToolDefinition {
                    name: first_tool.name,
                    tool_name: first_tool.tool_name,
                    description: "First tool".to_string(),
                    kind: first_tool.kind,
                    input_schema: first_tool.input_schema,
                    output_schema: first_tool.output_schema,
                },
                ToolDefinition {
                    name: second_tool.name,
                    tool_name: second_tool.tool_name,
                    description: "Second tool".to_string(),
                    kind: second_tool.kind,
                    input_schema: second_tool.input_schema,
                    output_schema: second_tool.output_schema,
                },
            ],
            &BTreeMap::new(),
            /*include_nested_tools*/ true,
            /*deferred_tools_available*/ false,
        );

        assert_eq!(
            description
                .matches("type CallToolResult<TStructured = { [key: string]: unknown }>")
                .count(),
            1
        );
        assert_eq!(description.matches("Shared MCP Types:").count(), 1);
    }

    #[test]
    fn exec_description_mentions_deferred_nested_tools_when_available() {
        let description = build_exec_tool_description(
            &[],
            &BTreeMap::new(),
            /*include_nested_tools*/ false,
            /*deferred_tools_available*/ true,
        );

        assert!(description.contains("Some nested MCP/app tools may be omitted"));
        assert!(description.contains("filter `ALL_TOOLS` by `name` and `description`"));
        assert!(!description.contains("do not print the full `ALL_TOOLS` array"));
    }

    #[test]
    fn coral_exec_description_includes_sql_guidance_and_schema_declarations() {
        let description = build_coral_exec_tool_description(
            &[ToolDefinition {
                name: "coral.sql.query".to_string(),
                tool_name: ToolName::plain("coral.sql.query"),
                description: "Run SQL".to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: Some(json!({
                    "type": "object",
                    "properties": { "sql": { "type": "string" } },
                    "required": ["sql"],
                    "additionalProperties": false
                })),
                output_schema: None,
            }],
            &[CodeModeSchemaTable {
                schema_name: "local_messages".to_string(),
                name: "messages".to_string(),
                columns: vec![
                    CodeModeSchemaColumn {
                        name: "id".to_string(),
                        data_type: "Int64".to_string(),
                        nullable: false,
                    },
                    CodeModeSchemaColumn {
                        name: "text".to_string(),
                        data_type: "Utf8".to_string(),
                        nullable: false,
                    },
                    CodeModeSchemaColumn {
                        name: "seen".to_string(),
                        data_type: "Boolean".to_string(),
                        nullable: true,
                    },
                ],
            }],
        );

        assert!(description.starts_with("Coral SQL-first guidance"));
        assert!(description.contains("default to one `coral.sql.query` query"));
        assert!(description.contains("declare const coral: { sql: { query: SqlFunction } };"));
        assert!(description.contains("### `coral.sql.query`"));
        assert!(description.contains("type SqlFunction"));
        assert!(description.contains("declare namespace CoralSchema"));
        assert!(description.contains("\"local_messages\""));
        assert!(description.contains("\"messages\""));
        assert!(description.contains("\"id\": string"));
        assert!(description.contains("\"text\": string"));
        assert!(description.contains("\"seen\": boolean | null"));
    }

    #[test]
    fn coral_exec_description_omits_sql_guidance_without_sql_tool() {
        let description = build_coral_exec_tool_description(
            &[ToolDefinition {
                name: "coral.search".to_string(),
                tool_name: ToolName::plain("coral.search"),
                description: "Search exports".to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: Some(json!({
                    "type": "object",
                    "properties": { "query": { "type": "string" } },
                    "required": ["query"],
                    "additionalProperties": false
                })),
                output_schema: None,
            }],
            &[],
        );

        assert!(!description.contains("Coral SQL-first guidance"));
        assert!(!description.contains("coral.sql.query"));
        assert!(description.contains("global `coral` object"));
        assert!(description.contains("### `coral.search`"));
    }

    #[test]
    fn exec_description_preserves_generated_tool_full_paths() {
        let description = build_exec_tool_description(
            &[ToolDefinition {
                name: "tools.github.rest.search.issuesAndPullRequests".to_string(),
                tool_name: ToolName::plain("tools.github.rest.search.issuesAndPullRequests"),
                description: "Search issues and pull requests".to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: Some(json!({
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "object",
                            "properties": {
                                "q": { "type": "string" }
                            },
                            "required": ["q"]
                        }
                    },
                    "required": ["query"],
                    "additionalProperties": false
                })),
                output_schema: None,
            }],
            &BTreeMap::new(),
            /*include_nested_tools*/ true,
            /*deferred_tools_available*/ false,
        );

        assert!(description.contains("### `tools.github.rest.search.issuesAndPullRequests`"));
        assert!(description.contains(
            "declare const tools: { github: { rest: { search: { issuesAndPullRequests(args:"
        ));
        assert!(!description.contains("tools_github_rest_search_issuesAndPullRequests"));
    }

    #[test]
    fn coral_schema_declarations_are_empty_without_tables() {
        assert_eq!(render_coral_schema_declarations(&[]), "");
    }

    #[test]
    fn coral_schema_declarations_group_interleaved_tables() {
        let declarations = render_coral_schema_declarations(&[
            CodeModeSchemaTable {
                schema_name: "z".to_string(),
                name: "later".to_string(),
                columns: Vec::new(),
            },
            CodeModeSchemaTable {
                schema_name: "a".to_string(),
                name: "first".to_string(),
                columns: Vec::new(),
            },
            CodeModeSchemaTable {
                schema_name: "z".to_string(),
                name: "earlier".to_string(),
                columns: Vec::new(),
            },
        ]);

        assert_eq!(declarations.matches("\"z\": {").count(), 1);
        assert!(declarations.find("\"a\": {").unwrap() < declarations.find("\"z\": {").unwrap());
        assert!(
            declarations.find("\"earlier\": {").unwrap()
                < declarations.find("\"later\": {").unwrap()
        );
    }
}
