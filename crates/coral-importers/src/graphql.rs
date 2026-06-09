use std::collections::{BTreeMap, BTreeSet};

use coral_capabilities::{
    Capability, EffectProfile, GraphqlOperationBinding, GraphqlOperationKind,
    GraphqlVariableBinding, InvocationSchema, OutputContract, ProviderOrigin, ProviderOriginKind,
    ShapeHints, SourceId, SupportStatus, UpstreamBinding,
};
use coral_spec::{GraphqlInterface, GraphqlSchemaDescriptor, SourceSpec};
use serde_json::{Map, Value};

use crate::auth::credential_requirements;
use crate::hash::sha256_hex;
use crate::naming::{OperationIdAllocator, pascal};
use crate::{
    ImportedInterface, ImporterError, ProviderSnapshotArtifact, RawInterfaceInput, Result,
};

#[expect(
    clippy::too_many_lines,
    reason = "GraphQL import keeps root-field snapshot and generated capability construction in one deterministic pass"
)]
pub(super) fn import_graphql(
    source_id: &SourceId,
    spec: &SourceSpec,
    interface: &GraphqlInterface,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<ImportedInterface> {
    let raw = raw_inputs
        .get(&interface.id)
        .ok_or_else(|| ImporterError::MissingRawInput(interface.id.clone()))?;
    let (fields, schema_index) = match raw {
        RawInterfaceInput::GraphqlIntrospection { value } => {
            let schema_index = graphql_schema_index_from_introspection(value);
            (graphql_fields_from_introspection(value), schema_index)
        }
        RawInterfaceInput::GraphqlSchema { text } => {
            (graphql_fields_from_sdl(text), GraphqlSchemaIndex::default())
        }
        _ => {
            return Err(ImporterError::Parse {
                interface_id: interface.id.clone(),
                message: "expected GraphQL schema or introspection input".to_string(),
            });
        }
    };
    let mut capabilities = Vec::new();
    let mut root_fields = Vec::new();
    let mut operation_ids = OperationIdAllocator::default();
    for field in fields {
        let known_broken = is_known_broken_graphql_field(interface, &field);
        let deprecated = field.deprecated || known_broken;
        let deprecation_reason = if known_broken && field.deprecation_reason.is_none() {
            Some("Provider returns a deprecation error at runtime".to_string())
        } else {
            field.deprecation_reason.clone()
        };
        let unsupported = field.kind == GraphqlOperationKind::Subscription
            || field.return_type.return_kind() == GraphqlReturnKind::Unknown
            || known_broken;
        let selection_set = graphql_selection_set(&field.return_type, &schema_index);
        let unsupported_reason =
            if field.kind == GraphqlOperationKind::Subscription {
                Some("subscriptions are not invokable".to_string())
            } else if field.return_type.return_kind() == GraphqlReturnKind::Unknown {
                Some("return type is missing from the GraphQL schema".to_string())
            } else if known_broken {
                Some(deprecation_reason.clone().unwrap_or_else(|| {
                    "Provider returns a deprecation error at runtime".to_string()
                }))
            } else {
                None
            };
        root_fields.push(serde_json::json!({
            "operation_kind": format!("{:?}", field.kind).to_ascii_lowercase(),
            "name": field.name,
            "args": field.args.iter().map(graphql_arg_snapshot).collect::<Vec<_>>(),
            "return_type": field.return_type.graphql_type_name(),
            "deprecated": deprecated,
            "deprecation_reason": deprecation_reason,
            "selection_set": selection_set,
            "unsupported": unsupported,
            "unsupported_reason": unsupported_reason,
        }));
        if unsupported {
            continue;
        }
        let operation_id =
            operation_ids.allocate(&format!("{}_{}", field.kind.as_keyword(), field.name));
        let operation_name = format!(
            "{}{}",
            match field.kind {
                GraphqlOperationKind::Query => "Query",
                GraphqlOperationKind::Mutation => "Mutation",
                GraphqlOperationKind::Subscription => "Subscription",
            },
            pascal(&field.name)
        );
        let response_path = graphql_response_path(&field, &schema_index);
        let provider_ref = format!(
            "interfaces/{}/provider-snapshot.yaml#/root_fields/{operation_id}",
            interface.id
        );
        let mut capability = Capability::new(
            source_id.clone(),
            interface.id.clone(),
            operation_id.clone(),
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref: provider_ref,
                provider_name: field.name.clone(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(GraphqlOperationBinding {
                endpoint_ref: format!(
                    "source/{source_id}/interface/{}/endpoint/default",
                    interface.id
                ),
                operation_name,
                graphql_operation_kind: field.kind,
                document_ref: format!(
                    "source/{source_id}/interface/{}/generated/{operation_id}.graphql",
                    interface.id
                ),
                selection_set,
                variable_bindings: field
                    .args
                    .iter()
                    .map(|arg| GraphqlVariableBinding {
                        variable_name: arg.name.clone(),
                        graphql_type: arg
                            .type_ref
                            .graphql_type_name()
                            .or_else(|| Some("String".to_string())),
                        argument_path: vec![arg.name.clone()],
                        required: arg.type_ref.required,
                    })
                    .collect(),
                response_path: response_path.clone(),
            }),
        );
        capability.display.title = format!(
            "{} {}",
            if field.kind == GraphqlOperationKind::Query {
                "Query"
            } else {
                "Mutate"
            },
            field.name
        );
        capability.display.deprecated = deprecated;
        capability.display.support_status = if deprecated {
            SupportStatus::Deprecated
        } else {
            SupportStatus::GeneratedPartial
        };
        capability.effect_profile = match field.kind {
            GraphqlOperationKind::Query => EffectProfile::read(),
            GraphqlOperationKind::Mutation => EffectProfile {
                idempotency: coral_capabilities::IdempotencyKind::NonIdempotent,
                ..EffectProfile::write()
            },
            GraphqlOperationKind::Subscription => EffectProfile::unknown_action(),
        };
        capability.input_schema = graphql_input_schema(&field.args, &schema_index);
        capability.output_contract = OutputContract::GraphqlData {
            schema: graphql_output_schema(&field.name, &field.return_type, &schema_index),
        };
        capability.shape_hints = graphql_shape_hints(&field, &schema_index, response_path);
        capability.credential_requirements = credential_requirements(spec, interface.auth.as_ref());
        capabilities.push(capability);
    }
    let snapshot_bytes = match raw {
        RawInterfaceInput::GraphqlSchema { text } => text.as_bytes().to_vec(),
        RawInterfaceInput::GraphqlIntrospection { value } => value.to_string().into_bytes(),
        _ => Vec::new(),
    };
    let snapshot = ProviderSnapshotArtifact {
        artifact_schema_version: 1,
        source_id: source_id.clone(),
        interface_id: interface.id.clone(),
        interface_type: "graphql".to_string(),
        importer_version: "graphql-root-fields-v1".to_string(),
        source_document_sha256: sha256_hex(&snapshot_bytes),
        snapshot: serde_json::json!({
            "schema": graphql_schema_descriptor_name(&interface.schema),
            "root_fields": root_fields,
        }),
        diagnostics: Vec::new(),
    };
    Ok(ImportedInterface {
        snapshot,
        capabilities,
    })
}

fn is_known_broken_graphql_field(interface: &GraphqlInterface, field: &GraphqlField) -> bool {
    let endpoint = interface.endpoint.raw();
    endpoint.contains("linear.app")
        && field.kind == GraphqlOperationKind::Query
        && field.name == "issueSearch"
}

#[derive(Debug, Clone)]
struct GraphqlField {
    kind: GraphqlOperationKind,
    name: String,
    args: Vec<GraphqlArg>,
    return_type: GraphqlTypeRef,
    deprecated: bool,
    deprecation_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct GraphqlArg {
    name: String,
    type_ref: GraphqlTypeRef,
}

#[derive(Debug, Clone, Default)]
pub(super) struct GraphqlTypeRef {
    pub(super) named_type: Option<String>,
    pub(super) kind: Option<String>,
    pub(super) graphql_type: Option<String>,
    pub(super) required: bool,
    pub(super) is_list: bool,
}

impl GraphqlTypeRef {
    fn unknown() -> Self {
        Self::default()
    }

    fn graphql_type_name(&self) -> Option<String> {
        self.graphql_type.clone().or_else(|| {
            self.named_type.as_ref().map(|name| {
                if self.required {
                    format!("{name}!")
                } else {
                    name.clone()
                }
            })
        })
    }

    fn return_kind(&self) -> GraphqlReturnKind {
        match self.kind.as_deref() {
            Some("SCALAR" | "ENUM") => GraphqlReturnKind::Scalar,
            Some("OBJECT" | "INTERFACE" | "UNION") => GraphqlReturnKind::Composite,
            _ => GraphqlReturnKind::Unknown,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct GraphqlSchemaIndex {
    types: BTreeMap<String, GraphqlTypeInfo>,
}

#[derive(Debug, Clone, Default)]
struct GraphqlTypeInfo {
    kind: String,
    fields: BTreeMap<String, GraphqlTypeField>,
    input_fields: BTreeMap<String, GraphqlTypeRef>,
    enum_values: Vec<String>,
}

#[derive(Debug, Clone)]
struct GraphqlTypeField {
    name: String,
    args: Vec<GraphqlArg>,
    type_ref: GraphqlTypeRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphqlReturnKind {
    Scalar,
    Composite,
    Unknown,
}

fn graphql_fields_from_introspection(value: &Value) -> Vec<GraphqlField> {
    let schema = value
        .get("data")
        .and_then(|data| data.get("__schema"))
        .unwrap_or(value);
    let query_type = schema
        .get("queryType")
        .and_then(|query| query.get("name"))
        .and_then(Value::as_str);
    let mutation_type = schema
        .get("mutationType")
        .and_then(|query| query.get("name"))
        .and_then(Value::as_str);
    let subscription_type = schema
        .get("subscriptionType")
        .and_then(|query| query.get("name"))
        .and_then(Value::as_str);
    let types = schema
        .get("types")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut fields = Vec::new();
    for ty in types {
        let name = ty.get("name").and_then(Value::as_str);
        let kind = if name == query_type {
            Some(GraphqlOperationKind::Query)
        } else if name == mutation_type {
            Some(GraphqlOperationKind::Mutation)
        } else if name == subscription_type {
            Some(GraphqlOperationKind::Subscription)
        } else {
            None
        };
        let Some(kind) = kind else {
            continue;
        };
        for field in ty
            .get("fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(field_name) = field.get("name").and_then(Value::as_str) else {
                continue;
            };
            if field_name.starts_with("__") {
                continue;
            }
            let args = field
                .get("args")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(graphql_arg_from_introspection)
                .collect();
            let return_type = graphql_type_ref_from_introspection(field.get("type"));
            fields.push(GraphqlField {
                kind,
                name: field_name.to_string(),
                args,
                return_type,
                deprecated: field
                    .get("isDeprecated")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                deprecation_reason: field
                    .get("deprecationReason")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            });
        }
    }
    fields
}

fn graphql_schema_index_from_introspection(value: &Value) -> GraphqlSchemaIndex {
    let schema = value
        .get("data")
        .and_then(|data| data.get("__schema"))
        .unwrap_or(value);
    let mut index = GraphqlSchemaIndex::default();
    for ty in schema
        .get("types")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(name) = ty.get("name").and_then(Value::as_str) else {
            continue;
        };
        if name.starts_with("__") {
            continue;
        }
        let kind = ty
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();
        let mut fields = BTreeMap::new();
        for field in ty
            .get("fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(field_name) = field.get("name").and_then(Value::as_str) else {
                continue;
            };
            let args = field
                .get("args")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(graphql_arg_from_introspection)
                .collect();
            fields.insert(
                field_name.to_string(),
                GraphqlTypeField {
                    name: field_name.to_string(),
                    args,
                    type_ref: graphql_type_ref_from_introspection(field.get("type")),
                },
            );
        }
        let input_fields = ty
            .get("inputFields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|field| {
                Some((
                    field.get("name").and_then(Value::as_str)?.to_string(),
                    graphql_type_ref_from_introspection(field.get("type")),
                ))
            })
            .collect();
        let enum_values = ty
            .get("enumValues")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| {
                value
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        index.types.insert(
            name.to_string(),
            GraphqlTypeInfo {
                kind,
                fields,
                input_fields,
                enum_values,
            },
        );
    }
    index
}

fn graphql_arg_from_introspection(value: &Value) -> Option<GraphqlArg> {
    Some(GraphqlArg {
        name: value.get("name").and_then(Value::as_str)?.to_string(),
        type_ref: graphql_type_ref_from_introspection(value.get("type")),
    })
}

fn graphql_fields_from_sdl(text: &str) -> Vec<GraphqlField> {
    let mut fields = Vec::new();
    for (type_name, kind) in [
        ("Query", GraphqlOperationKind::Query),
        ("Mutation", GraphqlOperationKind::Mutation),
        ("Subscription", GraphqlOperationKind::Subscription),
    ] {
        if let Some(body) = extract_graphql_type_body(text, type_name) {
            for line in body.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') || line.starts_with("__") {
                    continue;
                }
                let name = line
                    .split(['(', ':', ' ', '\t'])
                    .next()
                    .unwrap_or_default()
                    .trim();
                if !name.is_empty() {
                    let args = graphql_sdl_arg_names(line);
                    fields.push(GraphqlField {
                        kind,
                        name: name.to_string(),
                        args,
                        return_type: graphql_type_ref_from_sdl(line),
                        deprecated: line.contains("@deprecated"),
                        deprecation_reason: None,
                    });
                }
            }
        }
    }
    fields
}

fn graphql_type_ref_from_introspection(value: Option<&Value>) -> GraphqlTypeRef {
    graphql_type_ref_from_introspection_inner(value, true)
}

fn graphql_type_ref_from_introspection_inner(
    value: Option<&Value>,
    top_level: bool,
) -> GraphqlTypeRef {
    let Some(value) = value else {
        return GraphqlTypeRef::unknown();
    };
    let kind = value.get("kind").and_then(Value::as_str);
    match kind {
        Some("NON_NULL") => {
            let mut inner = graphql_type_ref_from_introspection_inner(value.get("ofType"), false);
            inner.graphql_type = inner.graphql_type_name().map(|name| format!("{name}!"));
            inner.required = top_level;
            inner
        }
        Some("LIST") => {
            let mut inner = graphql_type_ref_from_introspection_inner(value.get("ofType"), false);
            inner.graphql_type = inner.graphql_type_name().map(|name| format!("[{name}]"));
            inner.required = false;
            inner.is_list = true;
            inner
        }
        Some(kind @ ("SCALAR" | "ENUM" | "OBJECT" | "INTERFACE" | "UNION" | "INPUT_OBJECT")) => {
            let named_type = value
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string);
            GraphqlTypeRef {
                graphql_type: named_type.clone(),
                named_type,
                kind: Some(kind.to_string()),
                required: false,
                is_list: false,
            }
        }
        _ => GraphqlTypeRef::unknown(),
    }
}

fn graphql_sdl_arg_names(line: &str) -> Vec<GraphqlArg> {
    let Some(open) = line.find('(') else {
        return Vec::new();
    };
    let args_start = open + '('.len_utf8();
    let Some(close) = line.get(args_start..).and_then(|rest| rest.find(')')) else {
        return vec![GraphqlArg {
            name: "__args".to_string(),
            type_ref: GraphqlTypeRef::unknown(),
        }];
    };
    let Some(args) = line.get(args_start..args_start + close) else {
        return vec![GraphqlArg {
            name: "__args".to_string(),
            type_ref: GraphqlTypeRef::unknown(),
        }];
    };
    let names = args
        .split(',')
        .filter_map(|arg| {
            let (name, raw_type) = arg.split_once(':')?;
            let name = name.trim();
            if name.is_empty() {
                return None;
            }
            Some(GraphqlArg {
                name: name.to_string(),
                type_ref: graphql_type_ref_from_sdl_type(raw_type),
            })
        })
        .collect::<Vec<_>>();
    if names.is_empty() {
        vec![GraphqlArg {
            name: "__args".to_string(),
            type_ref: GraphqlTypeRef::unknown(),
        }]
    } else {
        names
    }
}

pub(super) fn graphql_type_ref_from_sdl(line: &str) -> GraphqlTypeRef {
    let Some(return_type) = graphql_sdl_return_type(line) else {
        return GraphqlTypeRef::unknown();
    };
    graphql_type_ref_from_sdl_type(return_type)
}

fn graphql_sdl_return_type(line: &str) -> Option<&str> {
    let mut paren_depth = 0_u32;
    for (index, ch) in line.char_indices() {
        match ch {
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ':' if paren_depth == 0 => return line.get(index + ':'.len_utf8()..),
            _ => {}
        }
    }
    None
}

fn graphql_type_ref_from_sdl_type(raw_type: &str) -> GraphqlTypeRef {
    let raw_type = raw_type.trim();
    let required = raw_type.ends_with('!');
    let named_type = raw_type
        .split([' ', '\t', '@', '#'])
        .next()
        .unwrap_or_default()
        .trim_matches(['[', ']', '!']);
    let graphql_type = if named_type.is_empty() {
        None
    } else if required {
        Some(format!("{named_type}!"))
    } else {
        Some(named_type.to_string())
    };
    if matches!(named_type, "Int" | "Float" | "String" | "Boolean" | "ID") {
        GraphqlTypeRef {
            graphql_type,
            named_type: Some(named_type.to_string()),
            kind: Some("SCALAR".to_string()),
            required,
            is_list: raw_type.starts_with('['),
        }
    } else if named_type.is_empty() {
        GraphqlTypeRef::unknown()
    } else {
        GraphqlTypeRef {
            graphql_type,
            named_type: Some(named_type.to_string()),
            kind: Some("OBJECT".to_string()),
            required,
            is_list: raw_type.starts_with('['),
        }
    }
}

fn extract_graphql_type_body<'a>(text: &'a str, type_name: &str) -> Option<&'a str> {
    let start = text.find(&format!("type {type_name}"))?;
    let after_start = text.get(start..)?;
    let open = after_start.find('{')? + start + '{'.len_utf8();
    let after_open = text.get(open..)?;
    let close = after_open.find('}')? + open;
    text.get(open..close)
}

fn graphql_arg_snapshot(arg: &GraphqlArg) -> Value {
    serde_json::json!({
        "name": arg.name,
        "type": arg.type_ref.graphql_type_name(),
        "required": arg.type_ref.required,
    })
}

fn graphql_selection_set(
    return_type: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
) -> Option<String> {
    if return_type.return_kind() == GraphqlReturnKind::Scalar {
        return None;
    }
    let named_type = return_type.named_type.as_deref()?;
    let selection = graphql_selection_for_type(named_type, schema_index, 0);
    if selection.is_empty() {
        Some("__typename".to_string())
    } else {
        Some(selection)
    }
}

fn graphql_selection_for_type(
    named_type: &str,
    schema_index: &GraphqlSchemaIndex,
    depth: usize,
) -> String {
    if depth > 1 {
        return "__typename".to_string();
    }
    let Some(type_info) = schema_index.types.get(named_type) else {
        return "__typename".to_string();
    };
    if type_info.kind == "UNION" {
        return "__typename".to_string();
    }
    if let Some(connection_selection) = graphql_connection_selection(type_info, schema_index, depth)
    {
        return connection_selection;
    }
    let mut fields = preferred_graphql_scalar_fields(type_info)
        .into_iter()
        .take(12)
        .collect::<Vec<_>>();
    if depth <= 1 {
        fields.extend(preferred_graphql_relation_fields(type_info, schema_index));
    }
    if fields.is_empty() {
        "__typename".to_string()
    } else {
        fields.join(" ")
    }
}

fn graphql_connection_selection(
    type_info: &GraphqlTypeInfo,
    schema_index: &GraphqlSchemaIndex,
    depth: usize,
) -> Option<String> {
    let nodes = type_info.fields.get("nodes")?;
    let node_type = nodes.type_ref.named_type.as_deref()?;
    let node_selection = graphql_selection_for_type(node_type, schema_index, depth + 1);
    let mut parts = vec![format!("nodes {{ {node_selection} }}")];
    if type_info.fields.contains_key("pageInfo") {
        parts.push("pageInfo { hasPreviousPage hasNextPage startCursor endCursor }".to_string());
    }
    Some(parts.join(" "))
}

fn graphql_response_path(field: &GraphqlField, schema_index: &GraphqlSchemaIndex) -> Vec<String> {
    let mut path = vec![field.name.clone()];
    if graphql_is_connection_type(&field.return_type, schema_index) {
        path.push("nodes".to_string());
    }
    path
}

fn graphql_shape_hints(
    field: &GraphqlField,
    schema_index: &GraphqlSchemaIndex,
    response_path: Vec<String>,
) -> ShapeHints {
    if field.kind != GraphqlOperationKind::Query {
        return ShapeHints::unknown();
    }
    if field.return_type.is_list || graphql_is_connection_type(&field.return_type, schema_index) {
        return ShapeHints::list_at_path(response_path);
    }
    ShapeHints::singleton_at_path(response_path)
}

fn graphql_is_connection_type(
    type_ref: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
) -> bool {
    let Some(named_type) = type_ref.named_type.as_deref() else {
        return false;
    };
    schema_index.types.get(named_type).is_some_and(|type_info| {
        type_info
            .fields
            .get("nodes")
            .is_some_and(|field| field.type_ref.is_list)
    })
}

fn preferred_graphql_scalar_fields(type_info: &GraphqlTypeInfo) -> Vec<String> {
    const PREFERRED: &[&str] = &[
        "id",
        "identifier",
        "number",
        "name",
        "title",
        "key",
        "url",
        "createdAt",
        "updatedAt",
        "archivedAt",
        "state",
        "status",
    ];
    let mut selected = Vec::new();
    for preferred in PREFERRED {
        if let Some(field) = type_info.fields.get(*preferred)
            && graphql_field_is_argless_scalar(field)
        {
            selected.push(field.name.clone());
        }
    }
    for field in type_info.fields.values() {
        if selected.iter().any(|selected| selected == &field.name) {
            continue;
        }
        if graphql_field_is_argless_scalar(field) {
            selected.push(field.name.clone());
        }
    }
    selected
}

/// Relation fields preferred when generating GraphQL selection sets and the
/// matching output schema. Consumed by both `preferred_graphql_relation_fields`
/// and `insert_selected_relation_schemas` so the selection set and the declared
/// output schema never drift apart.
const PREFERRED_GRAPHQL_RELATION_FIELDS: &[&str] =
    &["state", "assignee", "team", "owner", "creator", "project"];

fn preferred_graphql_relation_fields(
    type_info: &GraphqlTypeInfo,
    schema_index: &GraphqlSchemaIndex,
) -> Vec<String> {
    let mut selected = Vec::new();
    for preferred in PREFERRED_GRAPHQL_RELATION_FIELDS {
        let Some(field) = type_info.fields.get(*preferred) else {
            continue;
        };
        if !field.args.is_empty() || field.type_ref.return_kind() != GraphqlReturnKind::Composite {
            continue;
        }
        let Some(named_type) = field.type_ref.named_type.as_deref() else {
            continue;
        };
        let Some(nested_type) = schema_index.types.get(named_type) else {
            continue;
        };
        if nested_type.kind == "UNION" {
            continue;
        }
        let nested_fields = preferred_graphql_relation_scalar_fields(nested_type);
        if nested_fields.is_empty() {
            continue;
        }
        selected.push(format!("{} {{ {} }}", field.name, nested_fields.join(" ")));
    }
    selected
}

fn preferred_graphql_relation_scalar_fields(type_info: &GraphqlTypeInfo) -> Vec<String> {
    const SAFE: &[&str] = &[
        "id",
        "identifier",
        "name",
        "displayName",
        "key",
        "type",
        "status",
    ];
    let mut selected = Vec::new();
    for field_name in SAFE {
        if let Some(field) = type_info.fields.get(*field_name)
            && graphql_field_is_argless_scalar(field)
        {
            selected.push(field.name.clone());
        }
    }
    selected
}

fn graphql_field_is_argless_scalar(field: &GraphqlTypeField) -> bool {
    field.args.is_empty() && field.type_ref.return_kind() == GraphqlReturnKind::Scalar
}

fn graphql_input_schema(
    args: &[GraphqlArg],
    schema_index: &GraphqlSchemaIndex,
) -> InvocationSchema {
    let properties = args
        .iter()
        .map(|arg| {
            (
                arg.name.clone(),
                graphql_json_schema_for_type(&arg.type_ref, schema_index),
            )
        })
        .collect::<Map<_, _>>();
    let required = args
        .iter()
        .filter(|arg| arg.type_ref.required)
        .map(|arg| arg.name.clone())
        .collect::<Vec<_>>();
    let mut schema = serde_json::json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": false
    });
    if !required.is_empty()
        && let Some(object) = schema.as_object_mut()
    {
        object.insert("required".to_string(), serde_json::json!(required));
    }
    InvocationSchema::new(schema)
}

fn graphql_output_schema(
    field_name: &str,
    return_type: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
) -> InvocationSchema {
    InvocationSchema::new(serde_json::json!({
        "type": "object",
        "properties": {
            field_name: graphql_selected_json_schema_for_type(return_type, schema_index)
        },
        "additionalProperties": false
    }))
}

fn graphql_selected_json_schema_for_type(
    type_ref: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
) -> Value {
    graphql_selected_json_schema_for_type_inner(type_ref, schema_index, 0)
}

fn graphql_selected_json_schema_for_type_inner(
    type_ref: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
    depth: usize,
) -> Value {
    if type_ref.is_list {
        let mut item_type = type_ref.clone();
        item_type.is_list = false;
        return serde_json::json!({
            "type": "array",
            "items": graphql_selected_json_schema_for_type_inner(
                &item_type,
                schema_index,
                depth
            )
        });
    }
    if type_ref.return_kind() == GraphqlReturnKind::Scalar {
        return graphql_json_schema_for_type(type_ref, schema_index);
    }
    if depth > 1 {
        return graphql_typename_schema();
    }
    let Some(named_type) = type_ref.named_type.as_deref() else {
        return graphql_typename_schema();
    };
    let Some(type_info) = schema_index.types.get(named_type) else {
        return graphql_typename_schema();
    };
    if type_info.kind == "UNION" {
        return graphql_typename_schema();
    }
    if let Some(schema) = graphql_selected_connection_schema(type_info, schema_index, depth) {
        return schema;
    }
    let mut properties = Map::new();
    for field_name in preferred_graphql_scalar_fields(type_info)
        .into_iter()
        .take(12)
    {
        if let Some(field) = type_info.fields.get(&field_name) {
            properties.insert(
                field.name.clone(),
                graphql_json_schema_for_type(&field.type_ref, schema_index),
            );
        }
    }
    if depth <= 1 {
        insert_selected_relation_schemas(&mut properties, type_info, schema_index);
    }
    if properties.is_empty() {
        return graphql_typename_schema();
    }
    serde_json::json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": true
    })
}

fn graphql_selected_connection_schema(
    type_info: &GraphqlTypeInfo,
    schema_index: &GraphqlSchemaIndex,
    depth: usize,
) -> Option<Value> {
    let nodes = type_info.fields.get("nodes")?;
    let mut node_type = nodes.type_ref.clone();
    node_type.is_list = false;
    let mut properties = Map::from_iter([(
        "nodes".to_string(),
        serde_json::json!({
            "type": "array",
            "items": graphql_selected_json_schema_for_type_inner(
                &node_type,
                schema_index,
                depth + 1
            )
        }),
    )]);
    if type_info.fields.contains_key("pageInfo") {
        properties.insert("pageInfo".to_string(), graphql_page_info_schema());
    }
    Some(serde_json::json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": true
    }))
}

fn insert_selected_relation_schemas(
    properties: &mut Map<String, Value>,
    type_info: &GraphqlTypeInfo,
    schema_index: &GraphqlSchemaIndex,
) {
    for field_name in PREFERRED_GRAPHQL_RELATION_FIELDS.iter().copied() {
        let Some(field) = type_info.fields.get(field_name) else {
            continue;
        };
        if !field.args.is_empty() || field.type_ref.return_kind() != GraphqlReturnKind::Composite {
            continue;
        }
        let Some(named_type) = field.type_ref.named_type.as_deref() else {
            continue;
        };
        let Some(nested_type) = schema_index.types.get(named_type) else {
            continue;
        };
        if nested_type.kind == "UNION" {
            continue;
        }
        let nested_properties = preferred_graphql_relation_scalar_fields(nested_type)
            .into_iter()
            .filter_map(|nested_field_name| {
                nested_type
                    .fields
                    .get(&nested_field_name)
                    .map(|nested_field| {
                        (
                            nested_field.name.clone(),
                            graphql_json_schema_for_type(&nested_field.type_ref, schema_index),
                        )
                    })
            })
            .collect::<Map<_, _>>();
        if nested_properties.is_empty() {
            continue;
        }
        properties.insert(
            field.name.clone(),
            serde_json::json!({
                "type": "object",
                "properties": nested_properties,
                "additionalProperties": true
            }),
        );
    }
}

fn graphql_page_info_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "hasPreviousPage": { "type": "boolean" },
            "hasNextPage": { "type": "boolean" },
            "startCursor": { "type": "string" },
            "endCursor": { "type": "string" }
        },
        "additionalProperties": true
    })
}

fn graphql_typename_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "__typename": { "type": "string" }
        },
        "additionalProperties": true
    })
}

fn graphql_json_schema_for_type(
    type_ref: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
) -> Value {
    graphql_json_schema_for_type_inner(type_ref, schema_index, 0, &mut BTreeSet::new())
}

fn graphql_json_schema_for_type_inner(
    type_ref: &GraphqlTypeRef,
    schema_index: &GraphqlSchemaIndex,
    depth: usize,
    seen: &mut BTreeSet<String>,
) -> Value {
    if type_ref.is_list {
        let mut item_type = type_ref.clone();
        item_type.is_list = false;
        return serde_json::json!({
            "type": "array",
            "items": graphql_json_schema_for_type_inner(
                &item_type,
                schema_index,
                depth,
                seen
            )
        });
    }
    let Some(name) = type_ref.named_type.as_deref() else {
        return graphql_any_json_schema();
    };
    if let Some(schema) = graphql_builtin_json_schema(name) {
        return schema;
    }
    if let Some(schema) = graphql_enum_json_schema(name, schema_index) {
        return schema;
    }
    match schema_index
        .types
        .get(name)
        .map(|type_info| type_info.kind.as_str())
    {
        Some("INPUT_OBJECT") => {
            if depth >= 2 || !seen.insert(name.to_string()) {
                return serde_json::json!({ "type": "object", "additionalProperties": true });
            }
            let Some(type_info) = schema_index.types.get(name) else {
                return serde_json::json!({ "type": "object", "additionalProperties": true });
            };
            let properties = type_info
                .input_fields
                .iter()
                .map(|(field_name, field_type)| {
                    (
                        field_name.clone(),
                        graphql_json_schema_for_type_inner(
                            field_type,
                            schema_index,
                            depth + 1,
                            seen,
                        ),
                    )
                })
                .collect::<Map<_, _>>();
            seen.remove(name);
            serde_json::json!({
                "type": "object",
                "properties": properties,
                "additionalProperties": true
            })
        }
        Some("OBJECT" | "INTERFACE" | "UNION") => {
            if depth >= 3 || !seen.insert(name.to_string()) {
                return serde_json::json!({ "type": "object", "additionalProperties": true });
            }
            let Some(type_info) = schema_index.types.get(name) else {
                return serde_json::json!({ "type": "object", "additionalProperties": true });
            };
            let properties = type_info
                .fields
                .iter()
                .filter(|(_, field)| field.args.is_empty())
                .map(|(field_name, field)| {
                    (
                        field_name.clone(),
                        graphql_json_schema_for_type_inner(
                            &field.type_ref,
                            schema_index,
                            depth + 1,
                            seen,
                        ),
                    )
                })
                .collect::<Map<_, _>>();
            seen.remove(name);
            serde_json::json!({
                "type": "object",
                "properties": properties,
                "additionalProperties": true
            })
        }
        _ => graphql_any_json_schema(),
    }
}

fn graphql_builtin_json_schema(name: &str) -> Option<Value> {
    match name {
        "Int" => Some(serde_json::json!({ "type": "integer" })),
        "Float" => Some(serde_json::json!({ "type": "number" })),
        "Boolean" => Some(serde_json::json!({ "type": "boolean" })),
        "ID" | "String" => Some(serde_json::json!({ "type": "string" })),
        _ => None,
    }
}

fn graphql_enum_json_schema(name: &str, schema_index: &GraphqlSchemaIndex) -> Option<Value> {
    let type_info = schema_index.types.get(name)?;
    if type_info.kind != "ENUM" {
        return None;
    }
    if type_info.enum_values.is_empty() {
        Some(serde_json::json!({ "type": "string" }))
    } else {
        Some(serde_json::json!({ "type": "string", "enum": type_info.enum_values.clone() }))
    }
}

fn graphql_any_json_schema() -> Value {
    serde_json::json!({ "type": ["object", "array", "string", "number", "boolean", "null"] })
}

fn graphql_schema_descriptor_name(schema: &GraphqlSchemaDescriptor) -> &'static str {
    match schema {
        GraphqlSchemaDescriptor::SdlUrl { .. } => "sdl_url",
        GraphqlSchemaDescriptor::SdlFile { .. } => "sdl_file",
        GraphqlSchemaDescriptor::IntrospectionJsonUrl { .. } => "introspection_json_url",
        GraphqlSchemaDescriptor::IntrospectionJsonFile { .. } => "introspection_json_file",
        GraphqlSchemaDescriptor::IntrospectionQuery { .. } => "introspection_query",
    }
}
