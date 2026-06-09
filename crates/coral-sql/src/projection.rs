use coral_capabilities::{
    Capability, CapabilityKind, Diagnostic, DiagnosticSeverity, DiagnosticStage, EffectKind,
    OutputContract, RestParameterLocation, ResultShapeHint, UpstreamBinding,
};
use coral_exports::{
    Binding, BindingBuildContext, BindingContribution, BindingContributor, BindingDiagnostic,
    ExportKind, ExportRef, FileScanProjection, SqlBinding, SqlBindingKind, SqlColumn, SqlInput,
    SqlProjectionV1, SqlRowShape,
};
use serde_json::json;

/// SQL binding contributor.
#[derive(Debug, Default)]
pub struct SqlBindingContributor;

impl SqlBindingContributor {
    /// Creates a SQL binding contributor.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl BindingContributor for SqlBindingContributor {
    fn name(&self) -> &'static str {
        "sql"
    }

    fn contribute(
        &self,
        capability: &Capability,
        ctx: &BindingBuildContext,
    ) -> coral_exports::Result<BindingContribution> {
        let bindings = generate_sql_bindings(capability, ctx)
            .into_iter()
            .map(Binding::Sql)
            .collect::<Vec<_>>();
        let binding_diagnostics = if bindings.is_empty() {
            sql_projection_diagnostics(capability)
                .into_iter()
                .map(|diagnostic| {
                    BindingDiagnostic::new(
                        vec![ExportKind::SqlTable, ExportKind::SqlFunction],
                        diagnostic,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(BindingContribution {
            bindings,
            search_text: Vec::new(),
            diagnostics: Vec::new(),
            binding_diagnostics,
        })
    }
}

/// Generates SQL bindings when a capability is executable by this SQL runtime.
#[must_use]
pub fn generate_sql_bindings(
    capability: &Capability,
    ctx: &BindingBuildContext,
) -> Vec<SqlBinding> {
    if !is_sql_suitable(capability) {
        return Vec::new();
    }

    let sql_schema = sql_schema_name(capability, ctx.source_key.as_str());
    let sql_name = sql_identifier(sql_binding_leaf(capability));
    let sql_reference = format!("{sql_schema}.{sql_name}");
    let projection = projection_for_capability(capability);
    let kind = SqlBindingKind::Table;
    let ref_ = ExportRef::sql_table(sql_reference.clone());
    vec![SqlBinding {
        kind,
        ref_,
        sql_reference,
        projection,
    }]
}

/// Marker proving `DataFusion` belongs to `coral-sql`.
#[must_use]
pub fn datafusion_runtime_type_name() -> &'static str {
    std::any::type_name::<datafusion::prelude::SessionContext>()
}

/// Marker proving SQL runtime receives upstream plans through `coral-upstream`.
#[must_use]
pub fn upstream_plan_type_name() -> &'static str {
    std::any::type_name::<coral_upstream::UpstreamInvocationPlan>()
}

fn is_sql_suitable(capability: &Capability) -> bool {
    if capability.effect_profile.capability_kind != CapabilityKind::Query {
        return false;
    }
    if !capability
        .effect_profile
        .effects
        .contains(&EffectKind::Read)
    {
        return false;
    }
    if !capability.shape_hints.stable_output_shape {
        return false;
    }
    if !matches!(
        capability.upstream_binding,
        UpstreamBinding::FileRead(_)
            | UpstreamBinding::Rest(_)
            | UpstreamBinding::Graphql(_)
            | UpstreamBinding::McpTool(_)
    ) {
        return false;
    }
    matches!(
        capability.shape_hints.result_shape,
        ResultShapeHint::List | ResultShapeHint::Singleton
    )
}

fn sql_projection_diagnostics(capability: &Capability) -> Vec<Diagnostic> {
    if !matches!(capability.upstream_binding, UpstreamBinding::McpTool(_))
        || capability.effect_profile.capability_kind != CapabilityKind::Query
        || !capability
            .effect_profile
            .effects
            .contains(&EffectKind::Read)
    {
        return Vec::new();
    }
    let OutputContract::McpStructuredContent { schema } = &capability.output_contract else {
        return Vec::new();
    };
    let (code, message) = if schema.is_none() {
        (
            "MCP_SQL_OUTPUT_SCHEMA_MISSING",
            "MCP read tool was not projected into SQL because the provider did not publish outputSchema for structured content",
        )
    } else if !capability.shape_hints.stable_output_shape
        || !matches!(
            capability.shape_hints.result_shape,
            ResultShapeHint::List | ResultShapeHint::Singleton
        )
    {
        (
            "MCP_SQL_OUTPUT_SCHEMA_UNSUPPORTED",
            "MCP read tool was not projected into SQL because its outputSchema does not describe a stable list or singleton row shape",
        )
    } else {
        return Vec::new();
    };
    vec![Diagnostic {
        source_id: Some(capability.source_id.clone()),
        interface_id: Some(capability.interface_id.clone()),
        capability_id: Some(capability.capability_id.clone()),
        details: json!({
            "provider_origin": capability.provider_origin.provider_name.as_str(),
            "result_shape": capability.shape_hints.result_shape,
            "stable_output_shape": capability.shape_hints.stable_output_shape,
        }),
        ..Diagnostic::new(
            code,
            DiagnosticSeverity::Info,
            DiagnosticStage::ExportGeneration,
            message,
        )
    }]
}

fn sql_binding_leaf(capability: &Capability) -> &str {
    if matches!(capability.upstream_binding, UpstreamBinding::FileRead(_))
        && capability.operation_id == "read_files"
    {
        capability.interface_id.as_str()
    } else {
        capability.operation_id.as_str()
    }
}

fn sql_schema_name(capability: &Capability, source_key: &str) -> String {
    let source = sql_identifier(source_key);
    if matches!(capability.upstream_binding, UpstreamBinding::FileRead(_)) {
        return source;
    }
    let interface_id = &capability.interface_id;
    let interface = sql_identifier(interface_id);
    if source == interface || source.ends_with(&format!("_{interface}")) {
        source
    } else {
        format!("{source}_{interface}")
    }
}

fn projection_for_capability(capability: &Capability) -> SqlProjectionV1 {
    let row_shape = match capability.shape_hints.result_shape {
        ResultShapeHint::Singleton => SqlRowShape::Singleton,
        _ => SqlRowShape::Collection,
    };
    let response_selection = response_selection_for_capability(capability);
    SqlProjectionV1 {
        row_shape,
        columns: columns_from_output_contract(
            &capability.output_contract,
            response_selection
                .as_ref()
                .map(|selection| selection.path.as_slice()),
        ),
        inputs: sql_inputs_from_capability(capability),
        response_selection,
        pagination: pagination_for_capability(capability),
        file_scan: file_scan_from_capability(capability),
        diagnostics: Vec::new(),
    }
}

/// Returns the root JSON schema for an output contract's primary variant, or
/// None when the contract carries no schema. Shared by column derivation and
/// response-selection so both read the same variant.
fn primary_output_schema(contract: &OutputContract) -> Option<&serde_json::Value> {
    match contract {
        OutputContract::Single { schema }
        | OutputContract::GraphqlData { schema }
        | OutputContract::McpStructuredContent {
            schema: Some(schema),
        } => Some(&schema.schema),
        OutputContract::RestResponseVariants { variants } => {
            variants.first().map(|variant| &variant.schema.schema)
        }
        OutputContract::McpStructuredContent { schema: None } | OutputContract::Unknown => None,
    }
}

fn columns_from_output_contract(
    output_contract: &OutputContract,
    response_path: Option<&[String]>,
) -> Vec<SqlColumn> {
    let Some(root_schema) = primary_output_schema(output_contract) else {
        return Vec::new();
    };
    let schema = row_schema(root_schema, response_path);
    let mut columns = schema_properties(root_schema, schema)
        .into_iter()
        .map(|(name, property)| {
            let property = resolve_local_schema_ref(root_schema, property);
            SqlColumn {
                name: sql_identifier(name),
                data_type: json_schema_type_to_sql(property),
                nullable: true,
                description: property
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            }
        })
        .collect::<Vec<_>>();
    if columns.is_empty() {
        columns.push(SqlColumn {
            name: "json".to_string(),
            data_type: "Utf8".to_string(),
            nullable: true,
            description: "JSON row value.".to_string(),
        });
    }
    columns
}

fn row_schema<'a>(
    root_schema: &'a serde_json::Value,
    response_path: Option<&[String]>,
) -> &'a serde_json::Value {
    let mut schema = resolve_local_schema_ref(root_schema, root_schema);
    if let Some(path) = response_path {
        for segment in path {
            if let Some(next) = schema_property(root_schema, schema, segment) {
                schema = resolve_local_schema_ref(root_schema, next);
            }
        }
    }
    schema = resolve_local_schema_ref(root_schema, schema);
    if schema_type(root_schema, schema) == Some("array")
        && let Some(items) = schema_array_items(root_schema, schema)
    {
        return items;
    }
    schema
}

fn resolve_local_schema_ref<'a>(
    root_schema: &'a serde_json::Value,
    schema: &'a serde_json::Value,
) -> &'a serde_json::Value {
    let mut current = schema;
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..32 {
        let Some(reference) = current.get("$ref").and_then(serde_json::Value::as_str) else {
            return current;
        };
        let Some(pointer) = reference.strip_prefix('#') else {
            return current;
        };
        if !pointer.starts_with('/') || !seen.insert(pointer) {
            return current;
        }
        let Some(next) = root_schema.pointer(pointer) else {
            return current;
        };
        current = next;
    }
    current
}

fn schema_property<'a>(
    root_schema: &'a serde_json::Value,
    schema: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Value> {
    schema_properties(root_schema, schema).remove(name)
}

fn schema_properties<'a>(
    root_schema: &'a serde_json::Value,
    schema: &'a serde_json::Value,
) -> std::collections::BTreeMap<&'a str, &'a serde_json::Value> {
    let mut properties = std::collections::BTreeMap::new();
    collect_schema_properties(root_schema, schema, 0, &mut properties);
    properties
}

fn collect_schema_properties<'a>(
    root_schema: &'a serde_json::Value,
    schema: &'a serde_json::Value,
    depth: usize,
    properties: &mut std::collections::BTreeMap<&'a str, &'a serde_json::Value>,
) {
    if depth >= 32 {
        return;
    }
    let schema = resolve_local_schema_ref(root_schema, schema);
    if let Some(all_of) = schema.get("allOf").and_then(serde_json::Value::as_array) {
        for subschema in all_of {
            collect_schema_properties(root_schema, subschema, depth + 1, properties);
        }
    }
    if let Some(schema_properties) = schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
    {
        for (name, property) in schema_properties {
            properties.insert(name.as_str(), property);
        }
    }
}

fn schema_type<'a>(
    root_schema: &'a serde_json::Value,
    schema: &'a serde_json::Value,
) -> Option<&'a str> {
    let schema = resolve_local_schema_ref(root_schema, schema);
    json_schema_primary_type(schema).or_else(|| {
        schema
            .get("allOf")
            .and_then(serde_json::Value::as_array)?
            .iter()
            .find_map(|subschema| schema_type(root_schema, subschema))
    })
}

fn schema_array_items<'a>(
    root_schema: &'a serde_json::Value,
    schema: &'a serde_json::Value,
) -> Option<&'a serde_json::Value> {
    let schema = resolve_local_schema_ref(root_schema, schema);
    schema
        .get("items")
        .map(|items| resolve_local_schema_ref(root_schema, items))
        .or_else(|| {
            schema
                .get("allOf")
                .and_then(serde_json::Value::as_array)?
                .iter()
                .find_map(|subschema| schema_array_items(root_schema, subschema))
        })
}

fn response_selection_for_capability(
    capability: &Capability,
) -> Option<coral_exports::ResponseSelection> {
    let schema_root = primary_output_schema(&capability.output_contract)?;
    let root_schema = resolve_local_schema_ref(schema_root, schema_root);
    for path in &capability.shape_hints.row_path_candidates {
        if path.is_empty() {
            if capability.shape_hints.result_shape == ResultShapeHint::List
                && schema_type(schema_root, root_schema) == Some("array")
            {
                return None;
            }
            continue;
        }
        return Some(coral_exports::ResponseSelection {
            status: "default".to_string(),
            media_type: "application/json".to_string(),
            path: path.clone(),
        });
    }
    if capability.shape_hints.result_shape != ResultShapeHint::List {
        return None;
    }
    if schema_type(schema_root, root_schema) == Some("array") {
        return None;
    }
    let properties = schema_properties(schema_root, root_schema);
    let selected = ["items", "nodes", "edges", "data"]
        .into_iter()
        .find(|name| property_is_array(schema_root, properties.get(*name).copied()))
        .or_else(|| {
            properties.iter().find_map(|(name, value)| {
                property_is_array(schema_root, Some(*value)).then_some(*name)
            })
        })?;
    Some(coral_exports::ResponseSelection {
        status: "default".to_string(),
        media_type: "application/json".to_string(),
        path: vec![selected.to_string()],
    })
}

fn property_is_array(root_schema: &serde_json::Value, value: Option<&serde_json::Value>) -> bool {
    value.is_some_and(|value| schema_type(root_schema, value) == Some("array"))
}

fn pagination_for_capability(capability: &Capability) -> Option<coral_exports::PaginationProfile> {
    let hint = capability.shape_hints.pagination_hint.as_ref()?;
    Some(coral_exports::PaginationProfile {
        kind: format!("{:?}", hint.kind).to_ascii_lowercase(),
        cursor_input: hint.cursor_arg.clone().map(|value| sql_identifier(&value)),
        page_size_input: hint
            .page_size_arg
            .clone()
            .map(|value| sql_identifier(&value)),
        cursor_path: hint.cursor_path.clone(),
    })
}

fn sql_inputs_from_capability(capability: &Capability) -> Vec<SqlInput> {
    match &capability.upstream_binding {
        UpstreamBinding::Rest(binding) => {
            return binding
                .parameter_bindings
                .iter()
                .filter(|parameter| {
                    matches!(
                        parameter.location,
                        RestParameterLocation::Path | RestParameterLocation::Query
                    )
                })
                .map(|parameter| SqlInput {
                    name: sql_identifier(&parameter.name),
                    required: parameter.required,
                    data_type: input_schema_type(capability, &parameter.name),
                })
                .collect();
        }
        UpstreamBinding::Graphql(binding) => {
            return binding
                .variable_bindings
                .iter()
                .filter_map(|binding| {
                    binding
                        .argument_path
                        .first()
                        .map(|argument| (binding, argument))
                })
                .map(|(binding, argument)| SqlInput {
                    name: sql_identifier(argument),
                    required: binding.required,
                    data_type: input_schema_type(capability, argument),
                })
                .collect();
        }
        _ => {}
    }
    let Some(properties) = capability
        .input_schema
        .schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
    else {
        return Vec::new();
    };
    let required = capability
        .input_schema
        .schema
        .get("required")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    properties
        .iter()
        .map(|(name, value)| SqlInput {
            name: sql_identifier(name),
            required: required.contains(name.as_str()),
            data_type: json_schema_type_to_sql(value),
        })
        .collect()
}

fn input_schema_type(capability: &Capability, name: &str) -> String {
    input_schema_property(capability, name)
        .map_or_else(|| "Utf8".to_string(), json_schema_type_to_sql)
}

fn input_schema_property<'a>(
    capability: &'a Capability,
    name: &str,
) -> Option<&'a serde_json::Value> {
    let root = &capability.input_schema.schema;
    root.get("properties")
        .and_then(|properties| properties.get(name))
        .or_else(|| {
            ["path", "query", "header", "cookie"]
                .iter()
                .find_map(|container| {
                    root.get("properties")
                        .and_then(|properties| properties.get(*container))
                        .and_then(|container| container.get("properties"))
                        .and_then(|properties| properties.get(name))
                })
        })
}

fn file_scan_from_capability(capability: &Capability) -> Option<FileScanProjection> {
    let UpstreamBinding::FileRead(binding) = &capability.upstream_binding else {
        return None;
    };
    Some(FileScanProjection {
        file_refs: binding
            .file_refs
            .iter()
            .map(|file| file.id.clone())
            .collect(),
        format: format!("{:?}", binding.format).to_ascii_lowercase(),
        schema_ref: binding.schema_ref.clone(),
    })
}

fn json_schema_type_to_sql(schema: &serde_json::Value) -> String {
    match json_schema_primary_type(schema) {
        Some("integer") => "Int64",
        Some("number") => "Float64",
        Some("boolean") => "Boolean",
        _ => "Utf8",
    }
    .to_string()
}

fn json_schema_primary_type(schema: &serde_json::Value) -> Option<&str> {
    match schema.get("type") {
        Some(serde_json::Value::String(value)) if value != "null" => Some(value.as_str()),
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .filter_map(serde_json::Value::as_str)
            .find(|value| *value != "null"),
        _ => None,
    }
}

pub(crate) fn sql_identifier(raw: &str) -> String {
    let mut out = String::new();
    let mut chars = raw.chars().peekable();
    let mut previous_lower_or_digit = false;
    while let Some(ch) = chars.next() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            if ch.is_ascii_uppercase() {
                let next_is_lower = chars.peek().is_some_and(char::is_ascii_lowercase);
                if !out.is_empty()
                    && !out.ends_with('_')
                    && (previous_lower_or_digit || next_is_lower)
                {
                    out.push('_');
                }
            }
            out.push(ch.to_ascii_lowercase());
            previous_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !out.ends_with('_') {
            out.push('_');
            previous_lower_or_digit = false;
        } else {
            previous_lower_or_digit = false;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "_".to_string()
    } else if trimmed.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        format!("_{trimmed}")
    } else {
        trimmed.to_string()
    }
}
