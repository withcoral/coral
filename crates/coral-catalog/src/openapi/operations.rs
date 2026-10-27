//! One traversal defines Tool identity, references and server declaration inheritance.
use super::schema::{ReferenceObjectKind, child_pointer, resolve_reference_object};
use crate::{CatalogDiagnostic, HttpMethod, ToolKey};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};

pub(super) struct LocatedOperation {
    pub key: ToolKey,
    pub path: String,
    /// The query a path key such as `/v1/models?beta=true` fixes.
    pub query: Option<String>,
    pub method: HttpMethod,
    pub object: Map<String, Value>,
    pub pointer: String,
    pub path_parameters: Option<String>,
    pub server_declaration: String,
}

pub(super) struct LocatedOperations {
    pub operations: Vec<LocatedOperation>,
    pub servers: BTreeMap<String, Map<String, Value>>,
}

pub(super) fn locate_operations(
    root: &Map<String, Value>,
    document: &Value,
) -> Result<LocatedOperations, CatalogDiagnostic> {
    let paths = root
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            CatalogDiagnostic::stable(
                "invalid_openapi_document",
                "the pinned OpenAPI document must contain a paths object",
            )
        })?;
    let mut servers = BTreeMap::from([("#/servers".into(), server_fields(root))]);
    let mut keys = HashSet::new();
    let mut operations = Vec::new();
    for path in paths.keys().filter(|path| !path.starts_with("x-")) {
        if !path.starts_with('/') || path.chars().any(char::is_control) {
            return Err(CatalogDiagnostic::stable(
                "invalid_tool_path",
                "an OpenAPI Operation path does not begin with '/' or is unsafe",
            ));
        }
        // OpenAPI forbids a query in a path key, but generated documents such
        // as Anthropic's still fix one there.
        let (template, query) = path
            .split_once('?')
            .map_or((path.as_str(), None), |(template, query)| {
                (template, Some(query).filter(|query| !query.is_empty()))
            });
        let path_item = resolve_reference_object(
            document,
            &child_pointer("#/paths", path),
            ReferenceObjectKind::PathItem,
        )?;
        let path_server = if path_item.object.contains_key("servers") {
            let pointer = child_pointer(&path_item.pointer, "servers");
            servers.insert(pointer.clone(), server_fields(&path_item.object));
            pointer
        } else {
            "#/servers".into()
        };
        for (name, value) in &path_item.object {
            let Some(method) = HttpMethod::parse(name) else {
                continue;
            };
            let object = value.as_object().ok_or_else(|| {
                CatalogDiagnostic::stable(
                    "invalid_openapi_operation",
                    "an identifiable OpenAPI Operation is not an object",
                )
            })?;
            let key = tool_key(object, method, path)?;
            if !keys.insert(key.clone()) {
                return Err(CatalogDiagnostic::stable(
                    "duplicate_tool_key",
                    "the pinned OpenAPI document produces duplicate Tool Keys",
                ));
            }
            let pointer = child_pointer(&path_item.pointer, name);
            let server_declaration = if object.contains_key("servers") {
                let declaration = child_pointer(&pointer, "servers");
                servers.insert(declaration.clone(), server_fields(object));
                declaration
            } else {
                path_server.clone()
            };
            operations.push(LocatedOperation {
                key,
                path: template.to_owned(),
                query: query.map(str::to_owned),
                method,
                object: object.clone(),
                pointer,
                path_parameters: path_item
                    .object
                    .contains_key("parameters")
                    .then(|| child_pointer(&path_item.pointer, "parameters")),
                server_declaration,
            });
        }
    }
    if operations.is_empty() {
        return Err(CatalogDiagnostic::stable(
            "empty_tool_catalog",
            "the pinned OpenAPI document contains no callable Tools",
        ));
    }
    operations.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(LocatedOperations {
        operations,
        servers,
    })
}

fn server_fields(object: &Map<String, Value>) -> Map<String, Value> {
    object
        .get("servers")
        .map(|servers| ("servers".into(), servers.clone()))
        .into_iter()
        .collect()
}

fn tool_key(
    object: &Map<String, Value>,
    method: HttpMethod,
    path: &str,
) -> Result<ToolKey, CatalogDiagnostic> {
    let key = match object.get("operationId") {
        None => format!("{} {path}", method.as_str()),
        Some(Value::String(value)) if !value.is_empty() && !value.chars().any(char::is_control) => {
            value.clone()
        }
        Some(_) => {
            return Err(CatalogDiagnostic::stable(
                "invalid_tool_key",
                "an OpenAPI operationId is not a nonempty safe string",
            ));
        }
    };
    ToolKey::parse(&key).map_err(|_invalid| {
        CatalogDiagnostic::stable(
            "invalid_tool_key",
            "an OpenAPI Operation has an invalid Tool Key",
        )
    })
}
