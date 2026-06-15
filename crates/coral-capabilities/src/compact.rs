//! Shared compact describe-entry shaping.
//!
//! One renderer feeds both surfaces that describe capabilities to agents: the
//! MCP `describe` tool (compact view) and Code Mode's in-exec
//! `coral.describe`. Both call sites map their transport-specific entry data
//! into [`CompactEntryFacts`] and delegate every byte of output shaping here,
//! so the two compact entries stay identical for the same capability.

use serde_json::{Map, Value, json};

use crate::model::Capability;
use crate::schema::{
    bound_schema_to_budget, code_mode_tool_input_schema, provider_value_schema,
    schema_contains_truncation_marker, schema_path_segments, schema_subtree_at_segments,
    truncated_schema_paths,
};

/// Serialized-size budget for the compact `input_schema`.
pub const COMPACT_INPUT_SCHEMA_BUDGET_BYTES: usize = 8192;
/// Serialized-size budget for the compact `value_schema`.
pub const COMPACT_VALUE_SCHEMA_BUDGET_BYTES: usize = 8192;
/// Maximum SQL columns rendered inline per compact SQL binding.
pub const COMPACT_SQL_COLUMN_LIMIT: usize = 24;

const SCHEMA_NOTE: &str = "Schemas may be size-bounded; x-coral-truncated marks a subtree omitted \
     from this rendered view or from the materialized source artifact. Re-run describe with path \
     (e.g. \"filter.team\" or \"output.items\") to expand a renderer-elided subtree, or \
     schemas:\"full\" to skip renderer-size bounding. Source/importer-level stubs are final in \
     the current artifact.";
const OUTPUT_PATH_SEGMENT: &str = "output";

/// How compact entries render input/value schemas.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchemaRenderMode {
    /// Bound each schema to its compact budget (the default).
    #[default]
    Bounded,
    /// Render schemas without compact-renderer size bounding.
    Full,
}

impl SchemaRenderMode {
    /// Parses the wire value (`"bounded"` or `"full"`).
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bounded" => Some(Self::Bounded),
            "full" => Some(Self::Full),
            _ => None,
        }
    }
}

/// Transport-neutral facts for one describable capability entry.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CompactEntryFacts {
    pub refs: Vec<String>,
    /// Generated Code Mode call path (`tools.*`), empty when not invokable.
    pub call: String,
    pub source_key: String,
    pub capability_kind: String,
    pub effects: Vec<String>,
    pub title: String,
    pub description: String,
    pub sql_bindings: Vec<CompactSqlBindingFacts>,
    pub deprecated: bool,
    pub support_status: String,
    /// Pre-rendered diagnostic objects (empty when the entry has none).
    pub diagnostics: Vec<Value>,
}

/// Transport-neutral facts for one SQL binding on a compact entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactSqlBindingFacts {
    pub reference: String,
    pub sql_reference: String,
    pub row_shape: String,
    pub columns: Vec<CompactSqlColumnFacts>,
    pub inputs: Vec<CompactSqlInputFacts>,
}

/// One SQL column rendered as `name:type` in compact entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactSqlColumnFacts {
    pub name: String,
    pub data_type: String,
}

/// One SQL input rendered as `name[*]:type` in compact entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactSqlInputFacts {
    pub name: String,
    pub required: bool,
    pub data_type: String,
}

/// Renders the compact describe entry shared by MCP and Code Mode.
#[must_use]
pub fn compact_entry_value(
    facts: &CompactEntryFacts,
    capability: Option<&Capability>,
    schemas: SchemaRenderMode,
) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&facts.refs));
    insert_nonempty(&mut value, "call", &facts.call);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&facts.refs));
    insert_nonempty(&mut value, "source_key", &facts.source_key);
    insert_nonempty(&mut value, "capability_kind", &facts.capability_kind);
    insert_effect_fields(&mut value, &facts.effects);
    insert_nonempty(&mut value, "title", &facts.title);
    insert_nonempty(&mut value, "description", &facts.description);
    let mut any_truncated = false;
    let full_input_schema = capability.map(code_mode_tool_input_schema);
    if let Some(full_input_schema) = &full_input_schema {
        let (schema, truncated) = render_schema(
            full_input_schema.clone(),
            schemas,
            COMPACT_INPUT_SCHEMA_BUDGET_BYTES,
        );
        value.insert("input_schema".to_string(), schema);
        if truncated {
            any_truncated = true;
            value.insert("input_schema_truncated".to_string(), Value::Bool(true));
        }
    }
    if let Some(value_schema) =
        capability.and_then(|capability| provider_value_schema(&capability.output_contract))
    {
        let (schema, truncated) =
            render_schema(value_schema, schemas, COMPACT_VALUE_SCHEMA_BUDGET_BYTES);
        value.insert("value_schema".to_string(), schema);
        if truncated {
            any_truncated = true;
            value.insert("value_schema_truncated".to_string(), Value::Bool(true));
        }
    }
    if any_truncated {
        value.insert("schema_note".to_string(), json!(SCHEMA_NOTE));
    }
    // Examples consume the full (pre-bounding) input schema so required-arg
    // examples stay correct even when the rendered schema was truncated.
    if let Some(example) = full_input_schema
        .as_ref()
        .and_then(|schema| compact_call_example(&facts.call, schema))
    {
        value.insert("examples".to_string(), Value::Array(vec![example]));
    }
    let sql = facts
        .sql_bindings
        .iter()
        .map(compact_sql_binding_value)
        .collect::<Vec<_>>();
    if !sql.is_empty() {
        value.insert("sql_bindings".to_string(), Value::Array(sql));
    }
    if facts.deprecated {
        value.insert("deprecated".to_string(), Value::Bool(true));
    }
    if facts.support_status != "generated" && !facts.support_status.is_empty() {
        value.insert("support".to_string(), json!(&facts.support_status));
    }
    if !facts.diagnostics.is_empty() {
        value.insert(
            "diagnostics".to_string(),
            Value::Array(facts.diagnostics.clone()),
        );
    }
    Value::Object(value)
}

/// Renders one compact ambiguous-describe candidate shared by MCP and Code
/// Mode.
#[must_use]
pub fn compact_candidate_value(
    refs: &[String],
    call: &str,
    deprecated: bool,
    support_status: &str,
) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(refs));
    insert_nonempty(&mut value, "call", call);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(refs));
    if deprecated {
        value.insert("deprecated".to_string(), Value::Bool(true));
    }
    if support_status != "generated" && !support_status.is_empty() {
        value.insert("support".to_string(), json!(support_status));
    }
    Value::Object(value)
}

/// Renders the path drill-down response shared by MCP and Code Mode.
///
/// `path` addresses the input schema by default; an `output` first segment
/// (e.g. `"output.items"`) addresses the provider value schema. The response
/// is `{ ref, call, path, schema, elided }`, where `elided` lists the dot
/// paths of subtrees re-elided by bounding.
///
/// # Errors
///
/// Returns an error when the entry has no capability, the value schema is
/// addressed but absent, the path is empty, or a path segment does not exist
/// (the error lists the keys available at the deepest valid node).
pub fn compact_entry_path_value(
    facts: &CompactEntryFacts,
    capability: Option<&Capability>,
    path: &str,
    schemas: SchemaRenderMode,
) -> Result<Value, String> {
    let capability =
        capability.ok_or_else(|| "entry has no capability schemas to expand".to_string())?;
    let segments = schema_path_segments(path);
    let (target_schema, segments, budget) = if segments.first() == Some(&OUTPUT_PATH_SEGMENT) {
        let value_schema = provider_value_schema(&capability.output_contract)
            .ok_or_else(|| "capability has no provider value schema".to_string())?;
        (
            value_schema,
            segments.get(1..).unwrap_or_default(),
            COMPACT_VALUE_SCHEMA_BUDGET_BYTES,
        )
    } else if segments.is_empty() {
        return Err("describe path is empty".to_string());
    } else {
        (
            code_mode_tool_input_schema(capability),
            segments.as_slice(),
            COMPACT_INPUT_SCHEMA_BUDGET_BYTES,
        )
    };
    let subtree = if segments.is_empty() {
        target_schema
    } else {
        schema_subtree_at_segments(&target_schema, segments)?
    };
    let (schema, _truncated) = render_schema(subtree, schemas, budget);
    let elided = truncated_schema_paths(&schema);
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&facts.refs));
    insert_nonempty(&mut value, "call", &facts.call);
    value.insert("path".to_string(), json!(path));
    value.insert("schema".to_string(), schema);
    value.insert("elided".to_string(), json!(elided));
    Ok(Value::Object(value))
}

fn render_schema(schema: Value, mode: SchemaRenderMode, budget_bytes: usize) -> (Value, bool) {
    match mode {
        SchemaRenderMode::Bounded => bound_schema_to_budget(schema, budget_bytes),
        SchemaRenderMode::Full => {
            let truncated = schema_contains_truncation_marker(&schema);
            (schema, truncated)
        }
    }
}

/// Picks the preferred typed ref to surface for an entry: TypeScript, then
/// SQL table, then SQL function, then the first ref.
#[must_use]
pub fn preferred_ref(refs: &[String]) -> &str {
    refs.iter()
        .find(|ref_| ref_.starts_with("typescript:"))
        .or_else(|| refs.iter().find(|ref_| ref_.starts_with("sql_table:")))
        .or_else(|| refs.iter().find(|ref_| ref_.starts_with("sql_function:")))
        .or_else(|| refs.first())
        .map_or("", String::as_str)
}

/// Picks the preferred SQL ref to surface for an entry.
#[must_use]
pub fn preferred_sql_ref(refs: &[String]) -> &str {
    refs.iter()
        .find(|ref_| ref_.starts_with("sql_table:") || ref_.starts_with("sql_function:"))
        .map_or("", String::as_str)
}

fn compact_call_example(call: &str, input_schema: &Value) -> Option<Value> {
    if call.is_empty() {
        return None;
    }
    let args = example_args_from_schema(input_schema);
    let args_text = serde_json::to_string(&args).ok()?;
    Some(json!({
        "call": call,
        "args": args,
        "javascript": format!("await {call}({args_text});"),
    }))
}

fn example_args_from_schema(schema: &Value) -> Value {
    let required = schema
        .as_object()
        .and_then(|object| object.get("required"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|name| (name.to_string(), example_value_for_property(schema, name)))
        .collect::<Map<_, _>>();
    Value::Object(required)
}

fn example_value_for_property(schema: &Value, name: &str) -> Value {
    let Some(property_schema) = schema.pointer(&format!("/properties/{name}")) else {
        return Value::String(format!("<{name}>"));
    };
    let property_schema = dereference_local_schema(schema, property_schema);
    if let Some(default) = property_schema.get("default") {
        return default.clone();
    }
    if let Some(values) = property_schema.get("enum").and_then(Value::as_array)
        && let Some(value) = values.first()
    {
        return value.clone();
    }
    match property_schema.get("type").and_then(Value::as_str) {
        Some("integer" | "number") => json!(0),
        Some("boolean") => json!(false),
        Some("array") => Value::Array(Vec::new()),
        Some("object") => Value::Object(Map::new()),
        _ if property_schema.get("properties").is_some()
            || property_schema.get("additionalProperties").is_some() =>
        {
            Value::Object(Map::new())
        }
        _ => Value::String(format!("<{name}>")),
    }
}

fn dereference_local_schema<'a>(root: &'a Value, schema: &'a Value) -> &'a Value {
    let mut current = schema;
    for _ in 0..8 {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            return current;
        };
        let Some(pointer) = reference.strip_prefix('#') else {
            return current;
        };
        let Some(target) = root.pointer(pointer) else {
            return current;
        };
        current = target;
    }
    current
}

fn compact_sql_binding_value(binding: &CompactSqlBindingFacts) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", &binding.reference);
    insert_nonempty(&mut value, "sql", &binding.sql_reference);
    insert_nonempty(&mut value, "shape", &binding.row_shape);
    if binding.columns.len() > COMPACT_SQL_COLUMN_LIMIT {
        value.insert("column_count".to_string(), json!(binding.columns.len()));
    }
    let columns = binding
        .columns
        .iter()
        .take(COMPACT_SQL_COLUMN_LIMIT)
        .map(|column| format!("{}:{}", column.name, column.data_type))
        .collect::<Vec<_>>();
    if !columns.is_empty() {
        value.insert("columns".to_string(), json!(columns));
    }
    let inputs = binding
        .inputs
        .iter()
        .map(|input| {
            let required = if input.required { "*" } else { "" };
            format!("{}{required}:{}", input.name, input.data_type)
        })
        .collect::<Vec<_>>();
    if !inputs.is_empty() {
        value.insert("inputs".to_string(), json!(inputs));
    }
    Value::Object(value)
}

fn insert_effect_fields(value: &mut Map<String, Value>, effects: &[String]) {
    match effects {
        [] => {}
        [effect] => {
            value.insert("effect".to_string(), json!(effect));
        }
        effects => {
            value.insert("effects".to_string(), json!(effects));
        }
    }
}

fn insert_nonempty(value: &mut Map<String, Value>, key: &str, entry: impl AsRef<str>) {
    let entry = entry.as_ref();
    if !entry.is_empty() {
        value.insert(key.to_string(), json!(entry));
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::indexing_slicing,
        reason = "test code: assertion-style indexing is idiomatic in tests"
    )]

    use serde_json::json;

    use super::*;
    use crate::model::{
        Capability, HttpMethod, InvocationSchema, McpTaskSupport, McpToolUpstreamBinding,
        OutputContract, ProviderOrigin, ProviderOriginKind, SourceId, UpstreamBinding,
    };

    fn mcp_test_capability(input_schema: Value, output_contract: OutputContract) -> Capability {
        let mut capability = Capability::new(
            SourceId("src_demo".to_string()),
            "mcp",
            "list_items",
            ProviderOrigin {
                kind: ProviderOriginKind::McpTool,
                snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/list_items".to_string(),
                provider_name: "list_items".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::McpTool(McpToolUpstreamBinding {
                server_ref: "source/src_demo/interface/mcp/server/default".to_string(),
                tool_name: "list_items".to_string(),
                task_support: McpTaskSupport::Unknown,
            }),
        );
        capability.effect_profile = HttpMethod::Get.default_effect_profile();
        capability.input_schema = InvocationSchema::new(input_schema);
        capability.output_contract = output_contract;
        capability
    }

    fn test_facts() -> CompactEntryFacts {
        CompactEntryFacts {
            refs: vec!["typescript:demo.mcp.listItems".to_string()],
            call: "tools.demo.mcp.listItems".to_string(),
            source_key: "demo".to_string(),
            capability_kind: "query".to_string(),
            effects: vec!["read".to_string()],
            title: "List items".to_string(),
            description: "Lists demo items.".to_string(),
            sql_bindings: Vec::new(),
            deprecated: false,
            support_status: "generated".to_string(),
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn compact_entry_keeps_small_schemas_unbounded() {
        let capability = mcp_test_capability(
            json!({
                "type": "object",
                "required": ["limit"],
                "properties": {
                    "limit": { "type": "integer", "default": 10 }
                }
            }),
            OutputContract::Single {
                schema: InvocationSchema::new(json!({
                    "type": "array",
                    "items": { "type": "object" }
                })),
            },
        );

        let entry =
            compact_entry_value(&test_facts(), Some(&capability), SchemaRenderMode::Bounded);

        assert_eq!(entry["ref"], "typescript:demo.mcp.listItems");
        assert_eq!(entry["call"], "tools.demo.mcp.listItems");
        assert_eq!(entry["effect"], "read");
        assert_eq!(
            entry["input_schema"]["properties"]["limit"]["type"],
            "integer"
        );
        assert_eq!(entry["value_schema"]["type"], "array");
        assert!(entry.get("input_schema_truncated").is_none());
        assert!(entry.get("value_schema_truncated").is_none());
        assert!(entry.get("schema_note").is_none());
        assert_eq!(entry["examples"][0]["args"], json!({ "limit": 10 }));
    }

    #[test]
    fn compact_entry_bounds_oversized_schemas_and_keeps_full_examples() {
        let deep = json!({
            "type": "object",
            "properties": {
                "a": {
                    "type": "object",
                    "properties": {
                        "b": {
                            "type": "object",
                            "properties": {
                                "c": {
                                    "type": "object",
                                    "description": "x".repeat(3 * COMPACT_INPUT_SCHEMA_BUDGET_BYTES)
                                }
                            }
                        }
                    }
                }
            }
        });
        let capability = mcp_test_capability(
            json!({
                "type": "object",
                "required": ["limit"],
                "properties": {
                    "limit": { "type": "integer", "default": 10 },
                    "filter": deep
                }
            }),
            OutputContract::Unknown,
        );

        let bounded =
            compact_entry_value(&test_facts(), Some(&capability), SchemaRenderMode::Bounded);
        let full = compact_entry_value(&test_facts(), Some(&capability), SchemaRenderMode::Full);

        assert_eq!(bounded["input_schema_truncated"], true);
        assert!(bounded["schema_note"].is_string());
        assert!(
            serde_json::to_string(&bounded["input_schema"])
                .expect("bounded input schema json")
                .len()
                <= COMPACT_INPUT_SCHEMA_BUDGET_BYTES
        );
        // Output contract is unknown, so no value schema and no value flags.
        assert!(bounded.get("value_schema").is_none());
        assert!(bounded.get("value_schema_truncated").is_none());
        // Examples come from the full pre-bounding schema on both renders.
        assert_eq!(bounded["examples"], full["examples"]);
        assert!(full.get("input_schema_truncated").is_none());
        assert!(full.get("schema_note").is_none());
    }

    #[test]
    fn compact_entry_full_mode_reports_preexisting_truncation_markers() {
        let capability = mcp_test_capability(
            json!({
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "object",
                        "description": "type Filter omitted",
                        "x-coral-truncated": true
                    }
                }
            }),
            OutputContract::Unknown,
        );

        let full = compact_entry_value(&test_facts(), Some(&capability), SchemaRenderMode::Full);

        assert_eq!(full["input_schema_truncated"], true);
        assert!(full["schema_note"].is_string());
        assert_eq!(
            full["input_schema"]["properties"]["filter"]["description"],
            "type Filter omitted"
        );
    }

    #[test]
    fn compact_entry_examples_dereference_required_object_args() {
        let capability = mcp_test_capability(
            json!({
                "type": "object",
                "required": ["filter"],
                "properties": {
                    "filter": { "$ref": "#/$defs/IssueFilter" }
                },
                "$defs": {
                    "IssueFilter": {
                        "type": "object",
                        "properties": {
                            "team": { "type": "object", "additionalProperties": true }
                        }
                    }
                }
            }),
            OutputContract::Unknown,
        );

        let entry =
            compact_entry_value(&test_facts(), Some(&capability), SchemaRenderMode::Bounded);

        assert_eq!(entry["examples"][0]["args"], json!({ "filter": {} }));
        assert_eq!(
            entry["examples"][0]["javascript"],
            "await tools.demo.mcp.listItems({\"filter\":{}});"
        );
    }

    #[test]
    fn compact_entry_path_value_addresses_input_and_value_schemas() {
        let capability = mcp_test_capability(
            json!({
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "object",
                        "properties": {
                            "team": { "type": "string" }
                        }
                    }
                }
            }),
            OutputContract::Single {
                schema: InvocationSchema::new(json!({
                    "type": "object",
                    "properties": {
                        "items": { "type": "array", "items": { "type": "string" } }
                    }
                })),
            },
        );
        let facts = test_facts();

        let input = compact_entry_path_value(
            &facts,
            Some(&capability),
            "filter",
            SchemaRenderMode::Bounded,
        )
        .expect("input path");
        assert_eq!(input["path"], "filter");
        assert_eq!(input["ref"], "typescript:demo.mcp.listItems");
        assert_eq!(input["call"], "tools.demo.mcp.listItems");
        assert_eq!(input["schema"]["properties"]["team"]["type"], "string");
        assert_eq!(input["elided"], json!([]));

        let output = compact_entry_path_value(
            &facts,
            Some(&capability),
            "output.items",
            SchemaRenderMode::Bounded,
        )
        .expect("output path");
        assert_eq!(output["schema"]["type"], "array");

        let whole_value_schema = compact_entry_path_value(
            &facts,
            Some(&capability),
            "output",
            SchemaRenderMode::Bounded,
        )
        .expect("output root path");
        assert_eq!(
            whole_value_schema["schema"]["properties"]["items"]["type"],
            "array"
        );
    }

    #[test]
    fn compact_candidate_value_renders_minimal_fields() {
        let candidate = compact_candidate_value(
            &["typescript:demo.mcp.listItems".to_string()],
            "tools.demo.mcp.listItems",
            true,
            "deprecated",
        );

        assert_eq!(candidate["ref"], "typescript:demo.mcp.listItems");
        assert_eq!(candidate["call"], "tools.demo.mcp.listItems");
        assert_eq!(candidate["deprecated"], true);
        assert_eq!(candidate["support"], "deprecated");
        assert!(candidate.get("input_schema").is_none());
    }
}
