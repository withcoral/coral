//! Pure source-surface scope derivation for observed-value invalidation.

use std::collections::BTreeMap;

use coral_engine::{QuerySource, RuntimeSourceComponent};
use coral_spec::backends::file::{
    FileFormatOptions, FileMetadataColumnSpec, FileObjectStoreSpec, FileSourceManifest,
    FileTableSpec, PartitionColumnSpec, PartitionPathSpec, S3AuthSpec,
};
use coral_spec::backends::http::{AuthSpec, HttpSourceManifest, HttpTableSpec};
use coral_spec::{
    BodySpec, ColumnSpec, HeaderSpec, ManifestInputKind, ManifestInputSpec, McpEnvSpec,
    McpHttpAuthSpec, McpLimitBinding, McpServerSpec, McpSourceManifest, McpTableFilterBinding,
    McpTableFunctionSpec, McpTableSpec, ParsedTemplate, RequestSpec, ResponseSpec,
    SourceTableFunctionSpec, TableFunctionArgSpec, TemplateNamespace, ValueSourceSpec,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::hash::sha256_hex;
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SurfaceKey {
    pub(super) source_name: String,
    pub(super) surface_kind: ObservedValuesSurfaceKind,
    pub(super) surface_name: String,
}

#[derive(Debug, Clone)]
pub(super) struct ObservedSourceSurfaceScope {
    /// Canonical installed source that owns lifecycle clears and invalidation epochs.
    pub(super) owner_source_name: String,
    /// Runtime component schema used in SQL and search results.
    pub(super) source_name: String,
    surface_key: SurfaceKey,
    pub(super) source_scope_id: String,
}

impl ObservedSourceSurfaceScope {
    pub(super) fn key(&self) -> SurfaceKey {
        self.surface_key.clone()
    }
}

pub(super) fn source_surface_scopes(source: &QuerySource) -> Vec<ObservedSourceSurfaceScope> {
    let mut scopes = Vec::new();
    for component in source.components() {
        match component {
            RuntimeSourceComponent::Http(manifest) => {
                scopes.extend(http_surface_scopes(source, manifest));
            }
            RuntimeSourceComponent::File(manifest) => {
                scopes.extend(file_surface_scopes(source, manifest));
            }
            RuntimeSourceComponent::Mcp(manifest) => {
                scopes.extend(mcp_surface_scopes(source, manifest));
            }
        }
    }
    scopes
}

fn http_surface_scopes(
    source: &QuerySource,
    manifest: &HttpSourceManifest,
) -> Vec<ObservedSourceSurfaceScope> {
    let component_shape = json!({
        "backend": "http",
        "component_source_name": manifest.common.name,
        "base_url": template_shape(&manifest.base_url),
        "auth": auth_shape(&manifest.auth),
        "request_headers": header_shapes(&manifest.request_headers),
    });
    let mut scopes = Vec::new();
    for table in &manifest.tables {
        scopes.push(surface_scope(
            source,
            manifest.common.name.as_str(),
            ObservedValuesSurfaceKind::Table,
            table.name(),
            &component_shape,
            &http_table_shape(table),
        ));
    }
    for function in &manifest.functions {
        scopes.push(surface_scope(
            source,
            manifest.common.name.as_str(),
            ObservedValuesSurfaceKind::Function,
            function.name.as_str(),
            &component_shape,
            &source_function_shape(function),
        ));
    }
    scopes
}

fn file_surface_scopes(
    source: &QuerySource,
    manifest: &FileSourceManifest,
) -> Vec<ObservedSourceSurfaceScope> {
    let component_shape = json!({
        "backend": "file",
        "component_source_name": manifest.common.name,
    });
    manifest
        .tables
        .iter()
        .map(|table| {
            surface_scope(
                source,
                manifest.common.name.as_str(),
                ObservedValuesSurfaceKind::Table,
                table.name(),
                &component_shape,
                &file_table_shape(table),
            )
        })
        .collect()
}

fn mcp_surface_scopes(
    source: &QuerySource,
    manifest: &McpSourceManifest,
) -> Vec<ObservedSourceSurfaceScope> {
    let component_shape = json!({
        "backend": "mcp",
        "component_source_name": manifest.common.name,
        "server": mcp_server_shape(&manifest.server),
    });
    let mut scopes = Vec::new();
    for table in &manifest.tables {
        scopes.push(surface_scope(
            source,
            manifest.common.name.as_str(),
            ObservedValuesSurfaceKind::Table,
            table.name(),
            &component_shape,
            &mcp_table_shape(table),
        ));
    }
    for function in &manifest.functions {
        scopes.push(surface_scope(
            source,
            manifest.common.name.as_str(),
            ObservedValuesSurfaceKind::Function,
            function.name(),
            &component_shape,
            &mcp_function_shape(function),
        ));
    }
    scopes
}

fn surface_scope(
    source: &QuerySource,
    component_source_name: &str,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: &str,
    component_shape: &Value,
    surface_shape: &Value,
) -> ObservedSourceSurfaceScope {
    let scope = json!({
        "logical_source_name": source.source_name(),
        "authored_version": source.version(),
        "declared_inputs": input_shapes(source.declared_inputs()),
        "variables": source.variables(),
        "component": component_shape,
        "surface": surface_shape,
    });
    let scope_bytes =
        serde_json::to_vec(&scope).expect("observed-values source scope must serialize");
    ObservedSourceSurfaceScope {
        owner_source_name: source.source_name().to_string(),
        source_name: component_source_name.to_string(),
        surface_key: SurfaceKey {
            source_name: component_source_name.to_string(),
            surface_kind,
            surface_name: surface_name.to_string(),
        },
        source_scope_id: sha256_hex(&scope_bytes),
    }
}

fn input_shapes(inputs: &[ManifestInputSpec]) -> Vec<Value> {
    inputs
        .iter()
        .map(|input| {
            json!({
                "key": input.key,
                "kind": input_kind_label(input.kind),
                "required": input.required,
                "has_default": !input.default_value.is_empty(),
                "has_hint": input.hint.is_some(),
                "has_credential": input.credential.is_some(),
            })
        })
        .collect()
}

fn input_kind_label(kind: ManifestInputKind) -> &'static str {
    match kind {
        ManifestInputKind::Variable => "variable",
        ManifestInputKind::Secret => "secret",
    }
}

fn http_table_shape(table: &HttpTableSpec) -> Value {
    json!({
        "kind": "table",
        "name": table.name(),
        "filters": to_json(table.filters()),
        "fetch_limit_default": table.fetch_limit_default(),
        "request": request_shape(&table.request),
        "requests": table.requests.iter().map(request_route_shape).collect::<Vec<_>>(),
        "response": response_shape(&table.response),
        "pagination": to_json(&table.pagination),
        "columns": column_shapes(table.columns()),
    })
}

fn source_function_shape(function: &SourceTableFunctionSpec) -> Value {
    json!({
        "kind": "function",
        "name": function.name,
        "function_kind": function.kind.as_str(),
        "fetch_limit_default": function.fetch_limit_default,
        "search_limits": to_json(&function.search_limits),
        "args": function.args.iter().map(function_arg_shape).collect::<Vec<_>>(),
        "request": request_shape(&function.request),
        "response": response_shape(&function.response),
        "pagination": to_json(&function.pagination),
        "columns": column_shapes(&function.columns),
    })
}

fn file_table_shape(table: &FileTableSpec) -> Value {
    json!({
        "kind": "table",
        "name": table.name(),
        "format": table.format.as_str(),
        "format_options": file_format_options_shape(&table.format_options),
        "source": {
            "location": template_shape(&table.source.location),
            "glob": table.source.glob_or_default(table.format),
            "partitions": table.source.partitions.iter().map(partition_shape).collect::<Vec<_>>(),
            "metadata": table.source.metadata.iter().map(file_metadata_shape).collect::<Vec<_>>(),
            "object_store": file_object_store_shape(table.source.object_store.as_ref()),
        },
        "filters": to_json(table.filters()),
        "columns": column_shapes(table.columns()),
    })
}

fn file_format_options_shape(options: &FileFormatOptions) -> Value {
    json!({
        "has_header": options.has_header,
        "delimiter": options.delimiter,
    })
}

fn partition_shape(partition: &PartitionColumnSpec) -> Value {
    json!({
        "name": partition.name,
        "data_type": partition.data_type.as_str(),
        "path": partition_path_shape(&partition.path),
    })
}

fn partition_path_shape(path: &PartitionPathSpec) -> Value {
    match path {
        PartitionPathSpec::Hive => json!({ "kind": "hive" }),
        PartitionPathSpec::Segment { index } => json!({
            "kind": "segment",
            "index": index,
        }),
    }
}

fn file_metadata_shape(metadata: &FileMetadataColumnSpec) -> Value {
    json!({
        "name": metadata.name,
        "kind": metadata.kind.as_str(),
    })
}

fn file_object_store_shape(object_store: Option<&FileObjectStoreSpec>) -> Value {
    match object_store {
        Some(FileObjectStoreSpec::S3 { region, auth }) => json!({
            "type": "s3",
            "region": region.as_ref().map(template_shape),
            "auth": s3_auth_shape(auth),
        }),
        None => Value::Null,
    }
}

fn s3_auth_shape(auth: &S3AuthSpec) -> Value {
    match auth {
        S3AuthSpec::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => json!({
            "type": "access_key",
            "access_key_id": template_shape(access_key_id),
            "secret_access_key": template_shape(secret_access_key),
            "has_session_token": session_token.is_some(),
        }),
        S3AuthSpec::InstanceProfile => json!({ "type": "instance_profile" }),
    }
}

fn mcp_table_shape(table: &McpTableSpec) -> Value {
    json!({
        "kind": "table",
        "name": table.name(),
        "tool": table.tool,
        "tool_args": value_source_map_shape(&table.tool_args),
        "filter_bindings": table.filter_bindings.iter().map(mcp_filter_binding_shape).collect::<Vec<_>>(),
        "limit_binding": table.limit_binding.as_ref().map(mcp_limit_binding_shape),
        "pagination": to_json(&table.pagination),
        "offset_pagination": to_json(&table.offset_pagination),
        "response": response_shape(&table.response),
        "filters": to_json(table.filters()),
        "columns": column_shapes(table.columns()),
    })
}

fn mcp_function_shape(function: &McpTableFunctionSpec) -> Value {
    json!({
        "kind": "function",
        "name": function.name(),
        "tool": function.tool,
        "args": function.args().iter().map(function_arg_shape).collect::<Vec<_>>(),
        "pagination": to_json(&function.pagination),
        "offset_pagination": to_json(&function.offset_pagination),
        "response": response_shape(&function.common.response),
        "columns": column_shapes(function.columns()),
    })
}

fn auth_shape(auth: &AuthSpec) -> Value {
    match auth {
        AuthSpec::BasicAuth(basic_auth) => json!({
            "type": "BasicAuth",
            "username": template_shape(&basic_auth.username),
            "password": template_shape(&basic_auth.password),
        }),
        AuthSpec::HeaderAuth(header_auth) => json!({
            "type": "HeaderAuth",
            "headers": header_shapes(&header_auth.headers),
        }),
        AuthSpec::CustomAuth(custom_auth) => {
            let mut config_keys = custom_auth.config.keys().collect::<Vec<_>>();
            config_keys.sort();
            json!({
                "type": "CustomAuth",
                "authenticator": custom_auth.authenticator,
                "config_keys": config_keys,
            })
        }
    }
}

fn request_shape(request: &RequestSpec) -> Value {
    json!({
        "method": request.method,
        "path": template_shape(&request.path),
        "query": request.query.iter().map(|query| {
            json!({
                "name": query.name,
                "value": value_source_shape(query),
            })
        }).collect::<Vec<_>>(),
        "body": body_shape(&request.body),
        "headers": header_shapes(&request.headers),
    })
}

fn request_route_shape(route: &coral_spec::RequestRouteSpec) -> Value {
    json!({
        "when_filters": route.when_filters,
        "request": request_shape(&route.request),
    })
}

fn body_shape(body: &BodySpec) -> Value {
    match body {
        BodySpec::Json { fields } => json!({
            "format": "json",
            "fields": fields.iter().map(|field| {
                json!({
                    "path": field.path,
                    "when_arg": field.when_arg,
                    "value": value_source_shape(field),
                })
            }).collect::<Vec<_>>(),
        }),
        BodySpec::Text { content } => json!({
            "format": "text",
            "content": value_source_shape(content),
        }),
    }
}

fn response_shape(response: &ResponseSpec) -> Value {
    json!({
        "format": response.format,
        "rows_path": response.rows_path,
        "ok_path": response.ok_path,
        "error_path": response.error_path,
        "allow_404_empty": response.allow_404_empty,
        "row_strategy": response.row_strategy,
    })
}

fn header_shapes(headers: &[HeaderSpec]) -> Vec<Value> {
    headers
        .iter()
        .map(|header| {
            json!({
                "name": header.name,
                "value": value_source_shape(header),
            })
        })
        .collect()
}

fn mcp_server_shape(server: &McpServerSpec) -> Value {
    match server {
        McpServerSpec::Stdio { command, args, env } => json!({
            "transport": "stdio",
            "command": command,
            "args": args,
            "env": env.iter().map(mcp_env_shape).collect::<Vec<_>>(),
        }),
        McpServerSpec::StreamableHttp { url, auth } => json!({
            "transport": "streamable_http",
            "url": url,
            "auth": auth.as_ref().map(mcp_http_auth_shape),
        }),
    }
}

fn mcp_env_shape(env: &McpEnvSpec) -> Value {
    json!({
        "name": env.name,
        "value": value_source_shape(&env.value),
    })
}

fn mcp_http_auth_shape(auth: &McpHttpAuthSpec) -> Value {
    json!({
        "type": "bearer",
        "token": value_source_shape(auth.bearer_token()),
    })
}

fn value_source_map_shape(values: &BTreeMap<String, ValueSourceSpec>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(key, value)| (key.clone(), value_source_shape(value)))
            .collect(),
    )
}

fn mcp_filter_binding_shape(binding: &McpTableFilterBinding) -> Value {
    json!({
        "name": binding.name,
        "tool_arg": binding.tool_arg,
    })
}

fn mcp_limit_binding_shape(binding: &McpLimitBinding) -> Value {
    json!({
        "tool_arg": binding.tool_arg,
        "max": binding.max,
    })
}

fn function_arg_shape(arg: &TableFunctionArgSpec) -> Value {
    json!({
        "name": arg.name,
        "required": arg.required,
        "values": arg.values,
        "bind": {
            "arg": arg.bind.arg,
        },
    })
}

fn column_shapes(columns: &[ColumnSpec]) -> Vec<Value> {
    columns
        .iter()
        .map(|column| {
            json!({
                "name": column.name,
                "data_type": column.data_type,
                "nullable": column.nullable,
                "virtual": column.r#virtual,
                "description": column.description,
                "expr": to_json(&column.expr),
            })
        })
        .collect()
}

fn value_source_shape<T>(value: &T) -> Value
where
    T: ValueSourceLike + ?Sized,
{
    value.value_source_shape()
}

trait ValueSourceLike {
    fn value_source_shape(&self) -> Value;
}

impl ValueSourceLike for ValueSourceSpec {
    fn value_source_shape(&self) -> Value {
        match self {
            ValueSourceSpec::Template { template } => json!({
                "from": "template",
                "template": template_shape(template),
            }),
            ValueSourceSpec::OneOf { values } => json!({
                "from": "one_of",
                "values": values.iter().map(value_source_shape).collect::<Vec<_>>(),
            }),
            ValueSourceSpec::Literal { value } => json!({
                "from": "literal",
                "value_type": json_value_type(value),
            }),
            ValueSourceSpec::Filter { key, default } => {
                keyed_default_source_shape("filter", key, default.is_some())
            }
            ValueSourceSpec::FilterInt { key, default } => {
                keyed_default_source_shape("filter_int", key, default.is_some())
            }
            ValueSourceSpec::FilterBool { key, default } => {
                keyed_default_source_shape("filter_bool", key, default.is_some())
            }
            ValueSourceSpec::FilterStringArray { key, default } => {
                keyed_default_source_shape("filter_string_array", key, default.is_some())
            }
            ValueSourceSpec::FilterSplit {
                key,
                separator,
                part,
            } => json!({
                "from": "filter_split",
                "key": key,
                "separator": separator,
                "part": part,
            }),
            ValueSourceSpec::FilterSplitInt {
                key,
                separator,
                part,
            } => json!({
                "from": "filter_split_int",
                "key": key,
                "separator": separator,
                "part": part,
            }),
            ValueSourceSpec::Arg { key, default } => {
                keyed_default_source_shape("arg", key, default.is_some())
            }
            ValueSourceSpec::ArgInt { key, default } => {
                keyed_default_source_shape("arg_int", key, default.is_some())
            }
            ValueSourceSpec::ArgBool { key, default } => {
                keyed_default_source_shape("arg_bool", key, default.is_some())
            }
            ValueSourceSpec::ArgSplit {
                key,
                separator,
                part,
            } => json!({
                "from": "arg_split",
                "key": key,
                "separator": separator,
                "part": part,
            }),
            ValueSourceSpec::ArgSplitInt {
                key,
                separator,
                part,
            } => json!({
                "from": "arg_split_int",
                "key": key,
                "separator": separator,
                "part": part,
            }),
            ValueSourceSpec::Input { key } => json!({
                "from": "input",
                "key": key,
            }),
            ValueSourceSpec::Bearer { key } => json!({
                "from": "bearer",
                "key": key,
            }),
            ValueSourceSpec::State { key } => json!({
                "from": "state",
                "key": key,
            }),
            ValueSourceSpec::NowEpochMinusSeconds { seconds } => json!({
                "from": "now_epoch_minus_seconds",
                "seconds": seconds,
            }),
        }
    }
}

fn keyed_default_source_shape(from: &str, key: &str, has_default: bool) -> Value {
    json!({
        "from": from,
        "key": key,
        "has_default": has_default,
    })
}

impl ValueSourceLike for HeaderSpec {
    fn value_source_shape(&self) -> Value {
        value_source_shape(&self.value)
    }
}

impl ValueSourceLike for coral_spec::QueryParamSpec {
    fn value_source_shape(&self) -> Value {
        value_source_shape(&self.value)
    }
}

impl ValueSourceLike for coral_spec::BodyFieldSpec {
    fn value_source_shape(&self) -> Value {
        value_source_shape(&self.value)
    }
}

fn json_value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn template_shape(template: &ParsedTemplate) -> Value {
    json!({
        "raw": template.raw(),
        "tokens": template.tokens().map(|token| {
            json!({
                "namespace": template_namespace_shape(token.namespace()),
                "key": token.key(),
                "has_default": token.default_value().is_some(),
            })
        }).collect::<Vec<_>>(),
    })
}

fn template_namespace_shape(namespace: &TemplateNamespace) -> Value {
    match namespace {
        TemplateNamespace::Input => json!("input"),
        TemplateNamespace::Filter => json!("filter"),
        TemplateNamespace::Arg => json!("arg"),
        TemplateNamespace::Expr => json!("expr"),
        TemplateNamespace::State => json!("state"),
        TemplateNamespace::Other(value) => json!({ "other": value }),
    }
}

fn to_json<T>(value: &T) -> Value
where
    T: Serialize + ?Sized,
{
    serde_json::to_value(value).expect("observed-values source shape must serialize")
}
