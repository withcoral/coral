//! `SourceSpec` source materialization and artifact loading.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead as _, BufReader, Read as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use coral_capabilities::{SourceCapabilitySet, SourceId, UpstreamBinding};
use coral_exports::{BindingBuildContext, SourceKey, TypescriptBindingContributor};
use coral_importers::{ImportResult, RawInterfaceInput, import_source};
use coral_spec::{
    AuthDescriptor, FileInterface, GraphqlInterface, GraphqlSchemaDescriptor, McpInterface,
    McpTransportDescriptor, OpenApiDescriptor, OpenApiInterface, ParsedTemplate,
    SourceFileFormatDescriptor, SourceInterface, SourceSpec, TemplateNamespace, TemplatePart,
};
use coral_sql::SqlBindingContributor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::bootstrap::stdio_path_env;
use crate::graphql_documents::{
    GENERATED_GRAPHQL_OPERATIONS_DIR, operation_document_filename, render_operation_document,
};
use crate::sources::SourceName;
use crate::sources::model::InstalledSourceIdentity;
use crate::state::AppStateLayout;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

const DESCRIPTOR_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;
const DESCRIPTOR_USER_AGENT: &str = "coral-source-materializer";
const MAX_MCP_TOOLS_LIST_PAGES: usize = 16;
const MAX_FILE_SCHEMA_SAMPLE_ROWS: usize = 100;
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const MCP_PROTOCOL_VERSION_HEADER: &str = "MCP-Protocol-Version";
const MCP_SESSION_ID_HEADER: &str = "Mcp-Session-Id";
const MCP_ACCEPT: &str = "application/json, text/event-stream";
const GRAPHQL_INTROSPECTION_QUERY: &str = r"
query CoralIntrospection {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types {
      kind
      name
      fields(includeDeprecated: true) {
        name
        isDeprecated
        deprecationReason
        args {
          name
          type { ...CoralTypeRef }
        }
        type { ...CoralTypeRef }
      }
      inputFields {
        name
        type { ...CoralTypeRef }
      }
      enumValues(includeDeprecated: true) {
        name
      }
    }
  }
}

fragment CoralTypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
          }
        }
      }
    }
  }
}
";

type ProviderCredentialMaterial = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderRequestHeader {
    name: String,
    value: String,
}

#[derive(Debug)]
struct ProviderJsonResponse {
    value: Option<Value>,
    headers: BTreeMap<String, String>,
}

struct BlockingMcpStreamableHttpSession<'a> {
    endpoint: &'a str,
    request_headers: &'a [ProviderRequestHeader],
    session_id: Option<String>,
    next_id: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct SourceMaterializationBuildRequest<'a> {
    pub(crate) layout: &'a AppStateLayout,
    pub(crate) workspace_name: &'a WorkspaceName,
    pub(crate) source_name: &'a SourceName,
    pub(crate) identity: &'a InstalledSourceIdentity,
    pub(crate) manifest_yaml: &'a str,
    pub(crate) manifest: &'a SourceSpec,
    pub(crate) temp_suffix: &'a str,
    pub(crate) provider_credentials: &'a BTreeMap<String, String>,
}

#[derive(Debug)]
pub(crate) struct MaterializationBuild {
    pub(crate) temp_dir: PathBuf,
    pub(crate) kind: MaterializationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaterializationKind {
    SourceSpec,
}

#[derive(Debug)]
pub(crate) struct MaterializationSwap {
    pub(crate) kind: MaterializationKind,
    pub(crate) backup: Option<PathBuf>,
}

pub(crate) fn build_source_materialization_tmp(
    request: SourceMaterializationBuildRequest<'_>,
) -> Result<MaterializationBuild, AppError> {
    let SourceMaterializationBuildRequest {
        layout,
        workspace_name,
        source_name,
        identity,
        manifest_yaml,
        manifest,
        temp_suffix,
        provider_credentials,
    } = request;
    let temp_dir = layout.source_materialized_tmp_dir(workspace_name, source_name, temp_suffix);
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    fs::ensure_private_dir(&temp_dir)?;

    match write_source_materialization(
        &temp_dir,
        source_name,
        identity,
        manifest_yaml,
        manifest,
        provider_credentials,
    ) {
        Ok(()) => Ok(MaterializationBuild {
            temp_dir,
            kind: MaterializationKind::SourceSpec,
        }),
        Err(error) => {
            if temp_dir.exists() {
                drop(std::fs::remove_dir_all(&temp_dir));
            }
            Err(error)
        }
    }
}

pub(crate) fn replace_materialization(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    build: &MaterializationBuild,
) -> Result<MaterializationSwap, AppError> {
    let backup = match build.kind {
        MaterializationKind::SourceSpec => {
            replace_source_materialization(layout, workspace_name, source_name, &build.temp_dir)?
        }
    };
    Ok(MaterializationSwap {
        kind: build.kind,
        backup,
    })
}

pub(crate) fn replace_source_materialization(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    temp_dir: &Path,
) -> Result<Option<PathBuf>, AppError> {
    let target = layout.source_materialized_dir(workspace_name, source_name);
    let backup = layout.source_materialized_tmp_dir(
        workspace_name,
        source_name,
        &format!("rollback.{}", Uuid::new_v4()),
    );
    if let Some(parent) = target.parent() {
        fs::ensure_private_dir(parent)?;
    }
    if backup.exists() {
        std::fs::remove_dir_all(&backup)?;
    }
    let had_existing = target.exists();
    if had_existing {
        std::fs::rename(&target, &backup)?;
    }
    if let Err(error) = std::fs::rename(temp_dir, &target) {
        if had_existing
            && backup.exists()
            && let Err(rollback_error) = std::fs::rename(&backup, &target)
        {
            return Err(AppError::FailedPrecondition(format!(
                "failed to install SourceSpec materialization for source '{source_name}': {error}; failed to restore previous materialization from '{}': {rollback_error}",
                backup.display()
            )));
        }
        return Err(error.into());
    }
    Ok(had_existing.then_some(backup))
}

pub(crate) fn cleanup_materialization_backup(swap: Option<MaterializationSwap>) {
    if let Some(swap) = swap
        && let Some(backup) = swap.backup
        && backup.exists()
    {
        drop(std::fs::remove_dir_all(backup));
    }
}

pub(crate) fn cleanup_materialization_tmp(temp_dir: Option<&Path>) {
    if let Some(temp_dir) = temp_dir
        && temp_dir.exists()
    {
        drop(std::fs::remove_dir_all(temp_dir));
    }
}

pub(crate) fn restore_materialization_backup(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    swap: Option<MaterializationSwap>,
) -> Result<(), AppError> {
    if let Some(swap) = swap {
        let target = match swap.kind {
            MaterializationKind::SourceSpec => {
                layout.source_materialized_dir(workspace_name, source_name)
            }
        };
        if let Some(backup) = swap.backup {
            if target.exists() {
                std::fs::remove_dir_all(&target)?;
            }
            if backup.exists() {
                std::fs::rename(backup, target)?;
            }
        } else if target.exists() {
            std::fs::remove_dir_all(target)?;
        }
    }
    Ok(())
}

fn write_source_materialization(
    temp_dir: &Path,
    source_name: &SourceName,
    identity: &InstalledSourceIdentity,
    manifest_yaml: &str,
    manifest: &SourceSpec,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<(), AppError> {
    let source_id = SourceId(identity.source_id.clone());
    let source_key = SourceKey(identity.source_key.clone());
    let display_name = identity.display_name.clone();
    let raw_inputs = acquire_raw_interface_inputs(manifest, provider_credentials)?;
    let import = import_source(source_id.clone(), manifest, &raw_inputs)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    let binding_ctx = BindingBuildContext {
        source_id: source_id.clone(),
        display_name: display_name.clone(),
        source_key: source_key.clone(),
    };
    let ts_contributor = TypescriptBindingContributor::new();
    let sql_contributor = SqlBindingContributor::new();
    let source_exports = coral_exports::build_source_exports(
        &import.capabilities,
        &binding_ctx,
        &[&ts_contributor, &sql_contributor],
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;

    let mut diagnostics = import.diagnostics.clone();
    diagnostics.extend(source_exports.diagnostics.clone());

    let (interface_index, fingerprint_interfaces) =
        write_interface_materialization_artifacts(temp_dir, manifest, &raw_inputs, &import)?;

    write_yaml(&temp_dir.join("capabilities.yaml"), &import.capabilities)?;
    write_yaml(
        &temp_dir.join("exports/source-exports.yaml"),
        &source_exports,
    )?;
    write_yaml(&temp_dir.join("diagnostics.yaml"), &diagnostics)?;

    let fingerprint = json!({
        "artifact_schema_version": 1,
        "source_id": source_id.as_str(),
        "display_name": display_name,
        "source_key": source_key.as_str(),
        "spec_sha256": sha256_hex(manifest_yaml.as_bytes()),
        "capability_generator_version": import.capabilities.generator_version,
        "source_exports_generator_version": source_exports.generator_version,
        "interfaces": fingerprint_interfaces,
    });
    write_yaml(&temp_dir.join("fingerprint.yaml"), &fingerprint)?;

    let artifacts = json!({
        "artifact_schema_version": 1,
        "source_id": source_id.as_str(),
        "fingerprint": "fingerprint.yaml",
        "capabilities": "capabilities.yaml",
        "exports": "exports/source-exports.yaml",
        "diagnostics": "diagnostics.yaml",
        "interfaces": interface_index,
    });
    write_yaml(&temp_dir.join("artifacts.yaml"), &artifacts)?;
    validate_source_materialization_temp_dir(temp_dir, source_name)?;
    Ok(())
}

fn write_interface_materialization_artifacts(
    temp_dir: &Path,
    manifest: &SourceSpec,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
    import: &ImportResult,
) -> Result<(Map<String, Value>, Vec<Value>), AppError> {
    let mut interface_index = Map::new();
    let mut fingerprint_interfaces = Vec::new();
    for snapshot in &import.provider_snapshots {
        let interface_dir = temp_dir.join("interfaces").join(&snapshot.interface_id);
        fs::ensure_private_dir(&interface_dir)?;
        let raw_document_path = interface_dir.join("source-document.raw");
        write_raw_interface_document(&raw_document_path, &snapshot.interface_id, raw_inputs)?;
        write_yaml(&interface_dir.join("provider-snapshot.yaml"), snapshot)?;
        if let Some(file_interface) = file_interface_by_id(manifest, &snapshot.interface_id) {
            write_file_interface_artifacts(&interface_dir, file_interface)?;
        }
        let generated_operations_sha = if snapshot.interface_type == "graphql" {
            write_graphql_operation_artifacts(
                &interface_dir,
                &snapshot.interface_id,
                &import.capabilities,
            )?
        } else {
            None
        };
        let provider_snapshot_bytes = serde_yaml::to_string(snapshot)?;
        let raw_document_bytes = std::fs::read(&raw_document_path)?;
        let raw_sha = sha256_hex(&raw_document_bytes);
        let snapshot_sha = sha256_hex(provider_snapshot_bytes.as_bytes());
        interface_index.insert(
            snapshot.interface_id.clone(),
            interface_artifact_index_entry(&snapshot.interface_id, &snapshot_sha),
        );
        if generated_operations_sha.is_some()
            && let Some(entry) = interface_index
                .get_mut(&snapshot.interface_id)
                .and_then(Value::as_object_mut)
        {
            entry.insert(
                "generated_operations_dir".to_string(),
                json!(format!(
                    "interfaces/{}/{}",
                    snapshot.interface_id, GENERATED_GRAPHQL_OPERATIONS_DIR
                )),
            );
        }
        fingerprint_interfaces.push(interface_fingerprint_entry(
            &snapshot.interface_id,
            &snapshot.interface_type,
            &snapshot_sha,
            &raw_sha,
            generated_operations_sha,
        ));
    }
    Ok((interface_index, fingerprint_interfaces))
}

fn file_interface_by_id<'a>(
    manifest: &'a SourceSpec,
    interface_id: &str,
) -> Option<&'a FileInterface> {
    manifest
        .interfaces
        .iter()
        .find_map(|interface| match interface {
            SourceInterface::File(file_interface) if file_interface.id == interface_id => {
                Some(file_interface)
            }
            _ => None,
        })
}

fn interface_artifact_index_entry(interface_id: &str, snapshot_sha: &str) -> Value {
    json!({
        "raw_document": format!("interfaces/{interface_id}/source-document.raw"),
        "provider_snapshot": format!("interfaces/{interface_id}/provider-snapshot.yaml"),
        "provider_snapshot_sha256": snapshot_sha,
    })
}

fn interface_fingerprint_entry(
    interface_id: &str,
    interface_type: &str,
    snapshot_sha: &str,
    raw_sha: &str,
    generated_operations_sha: Option<String>,
) -> Value {
    let mut fingerprint = json!({
        "id": interface_id,
        "type": interface_type,
        "provider_snapshot_sha256": snapshot_sha,
    });
    if let Some(object) = fingerprint.as_object_mut() {
        object.insert(
            source_document_hash_key(interface_type).to_string(),
            json!(raw_sha),
        );
        if let Some(generated_operations_sha) = generated_operations_sha {
            object.insert(
                "generated_operations_sha256".to_string(),
                json!(generated_operations_sha),
            );
        }
    }
    fingerprint
}

fn write_file_interface_artifacts(
    interface_dir: &Path,
    interface: &FileInterface,
) -> Result<(), AppError> {
    let files_dir = interface_dir.join("files");
    fs::ensure_private_dir(&files_dir)?;
    for (index, file) in interface.files.iter().enumerate() {
        let canonical = canonicalize_file_descriptor(file)?;
        std::fs::copy(&canonical, files_dir.join(format!("file_{index}")))?;
    }
    Ok(())
}

fn write_graphql_operation_artifacts(
    interface_dir: &Path,
    interface_id: &str,
    capabilities: &SourceCapabilitySet,
) -> Result<Option<String>, AppError> {
    let mut documents = Vec::new();
    for capability in capabilities
        .capabilities
        .iter()
        .filter(|capability| capability.interface_id == interface_id)
    {
        let UpstreamBinding::Graphql(binding) = &capability.upstream_binding else {
            continue;
        };
        let filename = operation_document_filename(binding).map_err(|message| {
            AppError::FailedPrecondition(format!(
                "SourceSpec materialization for GraphQL interface '{interface_id}' has invalid operation document reference: {message}"
            ))
        })?;
        let document = render_operation_document(binding).map_err(|message| {
            AppError::FailedPrecondition(format!(
                "SourceSpec materialization for GraphQL interface '{interface_id}' could not render generated operation document '{}': {message}",
                capability.operation_id
            ))
        })?;
        documents.push((filename, document));
    }
    if documents.is_empty() {
        return Ok(None);
    }
    documents.sort_by(|left, right| left.0.cmp(&right.0));
    let operations_dir = interface_dir.join(GENERATED_GRAPHQL_OPERATIONS_DIR);
    fs::ensure_private_dir(&operations_dir)?;
    let mut hash_input = Vec::new();
    for (filename, document) in &documents {
        std::fs::write(operations_dir.join(filename), document.as_bytes())?;
        hash_input.extend_from_slice(filename.as_bytes());
        hash_input.push(0);
        hash_input.extend_from_slice(document.as_bytes());
        hash_input.push(0);
    }
    Ok(Some(sha256_hex(&hash_input)))
}

fn acquire_raw_interface_inputs(
    manifest: &SourceSpec,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<BTreeMap<String, RawInterfaceInput>, AppError> {
    let mut raw_inputs = BTreeMap::new();
    for interface in &manifest.interfaces {
        match interface {
            SourceInterface::OpenApi(openapi) => {
                raw_inputs.insert(
                    openapi.id.clone(),
                    RawInterfaceInput::OpenApiDocument {
                        bytes: read_openapi_interface_document(openapi, provider_credentials)?,
                    },
                );
            }
            SourceInterface::Mcp(mcp) => {
                raw_inputs.insert(
                    mcp.id.clone(),
                    RawInterfaceInput::McpToolsList {
                        value: read_mcp_tools_list(mcp, provider_credentials)?,
                    },
                );
            }
            SourceInterface::Graphql(graphql) => {
                raw_inputs.insert(
                    graphql.id.clone(),
                    read_graphql_schema_input(graphql, provider_credentials)?,
                );
            }
            SourceInterface::File(file) => {
                validate_file_interface(file)?;
                raw_inputs.insert(
                    file.id.clone(),
                    RawInterfaceInput::FileListing {
                        schema: infer_file_interface_schema(file)?,
                    },
                );
            }
        }
    }
    Ok(raw_inputs)
}

fn read_openapi_interface_document(
    interface: &OpenApiInterface,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<Vec<u8>, AppError> {
    match &interface.descriptor {
        OpenApiDescriptor::File { file } => read_file_descriptor(file),
        OpenApiDescriptor::Url { url } => {
            let url =
                render_provider_url_template(url, "OpenAPI descriptor url", provider_credentials)?;
            let provider_origin = interface
                .base_url
                .as_ref()
                .map(|base_url| {
                    render_provider_url_template(base_url, "OpenAPI base_url", provider_credentials)
                })
                .transpose()?;
            let request_headers = descriptor_materialization_auth_headers(
                interface.auth.as_ref(),
                "OpenAPI descriptor acquisition",
                provider_credentials,
                &url,
                provider_origin.as_deref(),
            )?;
            read_url_descriptor(&url, "OpenAPI descriptor", &request_headers)
        }
    }
}

fn read_mcp_tools_list(
    interface: &McpInterface,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<Value, AppError> {
    match &interface.server.transport {
        McpTransportDescriptor::StreamableHttp { url } => {
            let request_headers = materialization_auth_headers(
                interface.server.auth.as_ref(),
                "MCP tools/list acquisition",
                provider_credentials,
            )?;
            let endpoint =
                render_provider_url_template(url, "MCP Streamable HTTP url", provider_credentials)?;
            read_mcp_tools_list_streamable_http(&interface.id, &endpoint, &request_headers)
        }
        McpTransportDescriptor::Stdio { command, args } => {
            let env = materialization_mcp_stdio_env(interface, provider_credentials)?;
            read_mcp_tools_list_stdio(&interface.id, command, args, env)
        }
    }
}

fn materialization_mcp_stdio_env(
    interface: &McpInterface,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<Vec<(String, coral_upstream::RedactableString)>, AppError> {
    let mut env = minimal_stdio_env();
    for binding in &interface.server.env {
        env.push((
            binding.name.clone(),
            coral_upstream::RedactableString::new(provider_source_input_value(
                provider_credentials,
                &binding.key,
                "MCP stdio environment acquisition",
            )?),
        ));
    }
    Ok(env)
}

fn minimal_stdio_env() -> Vec<(String, coral_upstream::RedactableString)> {
    stdio_path_env()
        .map(|value| {
            vec![(
                "PATH".to_string(),
                coral_upstream::RedactableString::new(value.to_string_lossy().to_string()),
            )]
        })
        .unwrap_or_default()
}

fn read_mcp_tools_list_stdio(
    interface_id: &str,
    command: &str,
    args: &[String],
    env: Vec<(String, coral_upstream::RedactableString)>,
) -> Result<Value, AppError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            AppError::FailedPrecondition(format!(
                "failed to initialize MCP stdio tools/list runtime for interface '{interface_id}': {error}"
            ))
        })?;
    let server = coral_upstream::McpConnectionTarget::Stdio {
        command: command.to_string(),
        args: args.to_vec(),
        env,
    };
    runtime
        .block_on(coral_upstream::list_mcp_tools(
            &server,
            Some(DESCRIPTOR_FETCH_TIMEOUT),
        ))
        .map_err(|error| {
            AppError::FailedPrecondition(format!(
                "MCP interface '{interface_id}' stdio tools/list failed: {error}"
            ))
        })
}

fn read_graphql_schema_input(
    interface: &GraphqlInterface,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<RawInterfaceInput, AppError> {
    match &interface.schema {
        GraphqlSchemaDescriptor::SdlFile { file } => {
            let bytes = read_file_descriptor(file)?;
            let text = String::from_utf8(bytes).map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "GraphQL SDL file for interface '{}' is not UTF-8: {error}",
                    interface.id
                ))
            })?;
            Ok(RawInterfaceInput::GraphqlSchema { text })
        }
        GraphqlSchemaDescriptor::SdlUrl { url } => {
            let url = literal_string_url(url, "GraphQL SDL URL")?;
            let provider_origin = render_provider_url_template(
                &interface.endpoint,
                "GraphQL endpoint",
                provider_credentials,
            )?;
            let request_headers = descriptor_materialization_auth_headers(
                interface.auth.as_ref(),
                "GraphQL SDL URL acquisition",
                provider_credentials,
                &url,
                Some(&provider_origin),
            )?;
            let bytes = read_url_descriptor(&url, "GraphQL SDL URL", &request_headers)?;
            let text = String::from_utf8(bytes).map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "GraphQL SDL URL for interface '{}' is not UTF-8: {error}",
                    interface.id
                ))
            })?;
            Ok(RawInterfaceInput::GraphqlSchema { text })
        }
        GraphqlSchemaDescriptor::IntrospectionJsonFile { file } => {
            let bytes = read_file_descriptor(file)?;
            Ok(RawInterfaceInput::GraphqlIntrospection {
                value: serde_json::from_slice(&bytes).map_err(|error| {
                    AppError::FailedPrecondition(format!(
                        "failed to parse GraphQL introspection JSON for interface '{}': {error}",
                        interface.id
                    ))
                })?,
            })
        }
        GraphqlSchemaDescriptor::IntrospectionJsonUrl { url } => {
            let url = literal_string_url(url, "GraphQL introspection JSON URL")?;
            let provider_origin = render_provider_url_template(
                &interface.endpoint,
                "GraphQL endpoint",
                provider_credentials,
            )?;
            let request_headers = descriptor_materialization_auth_headers(
                interface.auth.as_ref(),
                "GraphQL introspection JSON URL acquisition",
                provider_credentials,
                &url,
                Some(&provider_origin),
            )?;
            let bytes =
                read_url_descriptor(&url, "GraphQL introspection JSON URL", &request_headers)?;
            Ok(RawInterfaceInput::GraphqlIntrospection {
                value: serde_json::from_slice(&bytes).map_err(|error| {
                    AppError::FailedPrecondition(format!(
                        "failed to parse GraphQL introspection JSON for interface '{}': {error}",
                        interface.id
                    ))
                })?,
            })
        }
        GraphqlSchemaDescriptor::IntrospectionQuery { endpoint } => {
            read_graphql_live_introspection(interface, endpoint.as_ref(), provider_credentials)
        }
    }
}

fn read_graphql_live_introspection(
    interface: &GraphqlInterface,
    endpoint_override: Option<&ParsedTemplate>,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<RawInterfaceInput, AppError> {
    let endpoint_template = endpoint_override.unwrap_or(&interface.endpoint);
    let endpoint = render_provider_url_template(
        endpoint_template,
        "GraphQL introspection endpoint",
        provider_credentials,
    )?;
    let provider_origin = render_provider_url_template(
        &interface.endpoint,
        "GraphQL endpoint",
        provider_credentials,
    )?;
    let request_headers = descriptor_materialization_auth_headers(
        interface.auth.as_ref(),
        "GraphQL live introspection",
        provider_credentials,
        &endpoint,
        Some(&provider_origin),
    )?;
    let value = post_json_rpc_like_document(
        endpoint,
        &json!({ "query": GRAPHQL_INTROSPECTION_QUERY, "operationName": "CoralIntrospection" }),
        &request_headers,
    )?;
    if let Some(errors) = value.get("errors").filter(|errors| !errors.is_null()) {
        return Err(AppError::FailedPrecondition(format!(
            "GraphQL live introspection for interface '{}' returned errors: {errors}",
            interface.id
        )));
    }
    Ok(RawInterfaceInput::GraphqlIntrospection { value })
}

fn validate_file_interface(interface: &FileInterface) -> Result<(), AppError> {
    for file in &interface.files {
        let canonical = canonicalize_file_descriptor(file)?;
        if !canonical.is_file() {
            return Err(AppError::FailedPrecondition(format!(
                "file interface '{}' path '{}' is not a regular file",
                interface.id,
                file.display()
            )));
        }
    }
    Ok(())
}

fn infer_file_interface_schema(interface: &FileInterface) -> Result<Value, AppError> {
    let mut properties = Map::new();
    for file in &interface.files {
        let canonical = canonicalize_file_descriptor(file)?;
        let file_schema = infer_file_schema(&canonical, interface.format)?;
        merge_schema_properties(&mut properties, &file_schema);
    }
    Ok(json!({
        "type": "object",
        "properties": properties,
    }))
}

fn infer_file_schema(path: &Path, format: SourceFileFormatDescriptor) -> Result<Value, AppError> {
    match format {
        SourceFileFormatDescriptor::Json => infer_json_file_schema(path),
        SourceFileFormatDescriptor::Jsonl => infer_jsonl_file_schema(path),
        SourceFileFormatDescriptor::Csv => infer_csv_file_schema(path),
        SourceFileFormatDescriptor::Parquet => infer_parquet_file_schema(path),
    }
}

fn infer_json_file_schema(path: &Path) -> Result<Value, AppError> {
    let bytes = std::fs::read(path)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "file '{}' is not valid JSON for schema inference: {error}",
            path.display()
        ))
    })?;
    let mut properties = Map::new();
    match value {
        Value::Object(object) => merge_json_object_properties(&mut properties, &object),
        Value::Array(values) => {
            for value in values.iter().take(MAX_FILE_SCHEMA_SAMPLE_ROWS) {
                if let Value::Object(object) = value {
                    merge_json_object_properties(&mut properties, object);
                }
            }
        }
        _ => {}
    }
    Ok(object_schema(properties))
}

fn infer_jsonl_file_schema(path: &Path) -> Result<Value, AppError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut properties = Map::new();
    let mut sampled = 0usize;
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "file '{}' is not valid JSONL for schema inference: {error}",
                path.display()
            ))
        })?;
        if let Value::Object(object) = value {
            merge_json_object_properties(&mut properties, &object);
            sampled += 1;
            if sampled >= MAX_FILE_SCHEMA_SAMPLE_ROWS {
                break;
            }
        }
    }
    Ok(object_schema(properties))
}

fn infer_csv_file_schema(path: &Path) -> Result<Value, AppError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let properties = parse_csv_header(&header)
        .into_iter()
        .filter(|name| !name.is_empty())
        .map(|name| (name, json!({ "type": "string" })))
        .collect::<Map<_, _>>();
    Ok(object_schema(properties))
}

fn infer_parquet_file_schema(path: &Path) -> Result<Value, AppError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "file '{}' is not valid Parquet for schema inference: {error}",
            path.display()
        ))
    })?;
    Ok(arrow_schema_to_json_schema(builder.schema().as_ref()))
}

fn merge_schema_properties(properties: &mut Map<String, Value>, schema: &Value) {
    let Some(schema_properties) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (name, property_schema) in schema_properties {
        merge_property_schema(properties, name, property_schema.clone());
    }
}

fn merge_json_object_properties(properties: &mut Map<String, Value>, object: &Map<String, Value>) {
    for (name, value) in object {
        merge_property_schema(properties, name, json_value_to_schema(value));
    }
}

fn merge_property_schema(properties: &mut Map<String, Value>, name: &str, incoming: Value) {
    match properties.get_mut(name) {
        Some(existing) if existing == &incoming => {}
        Some(existing) => {
            let existing_type = existing.get("type").and_then(Value::as_str);
            let incoming_type = incoming.get("type").and_then(Value::as_str);
            if existing_type != incoming_type {
                *existing = json!({ "type": "string" });
            }
        }
        None => {
            properties.insert(name.to_string(), incoming);
        }
    }
}

fn json_value_to_schema(value: &Value) -> Value {
    match value {
        Value::Bool(_) => json!({ "type": "boolean" }),
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            json!({ "type": "integer" })
        }
        Value::Number(_) => json!({ "type": "number" }),
        Value::String(_) | Value::Null => json!({ "type": "string" }),
        Value::Array(_) | Value::Object(_) => json!({ "type": "object" }),
    }
}

fn object_schema(properties: Map<String, Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert("properties".to_string(), Value::Object(properties));
    Value::Object(schema)
}

fn parse_csv_header(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.trim_end_matches(['\r', '\n']).chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => {
                in_quotes = !in_quotes;
            }
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_string());
    fields
}

fn arrow_schema_to_json_schema(schema: &Schema) -> Value {
    let properties = schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), arrow_field_to_json_schema(field)))
        .collect::<Map<_, _>>();
    object_schema(properties)
}

fn arrow_field_to_json_schema(field: &Field) -> Value {
    match field.data_type() {
        DataType::Boolean => json!({ "type": "boolean" }),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => json!({ "type": "integer" }),
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => json!({ "type": "number" }),
        DataType::Struct(fields) => {
            let properties = fields
                .iter()
                .map(|field| (field.name().clone(), arrow_field_to_json_schema(field)))
                .collect::<Map<_, _>>();
            object_schema(properties)
        }
        _ => json!({ "type": "string" }),
    }
}

fn write_raw_interface_document(
    path: &Path,
    interface_id: &str,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<(), AppError> {
    let raw_input = raw_inputs.get(interface_id);
    let bytes = match raw_input {
        Some(RawInterfaceInput::OpenApiDocument { bytes }) => bytes.clone(),
        Some(RawInterfaceInput::McpToolsList { value }) => serde_json::to_vec_pretty(value)?,
        Some(RawInterfaceInput::GraphqlSchema { text }) => text.as_bytes().to_vec(),
        Some(RawInterfaceInput::GraphqlIntrospection { value }) => {
            serde_json::to_vec_pretty(value)?
        }
        Some(RawInterfaceInput::FileListing { schema }) => serde_json::to_vec_pretty(schema)?,
        None => Vec::new(),
    };
    std::fs::write(path, bytes)?;
    Ok(())
}

fn validate_source_materialization_temp_dir(
    temp_dir: &Path,
    source_name: &SourceName,
) -> Result<(), AppError> {
    for artifact in [
        "artifacts.yaml",
        "fingerprint.yaml",
        "capabilities.yaml",
        "exports/source-exports.yaml",
        "diagnostics.yaml",
    ] {
        let path = temp_dir.join(artifact);
        if !path.exists() {
            return Err(AppError::FailedPrecondition(format!(
                "SourceSpec materialization for source '{source_name}' is missing required artifact '{artifact}'"
            )));
        }
    }
    reject_deleted_artifact(temp_dir, source_name, "projections", "projection")?;
    reject_deleted_artifact(temp_dir, source_name, "semantic-ir", "semantic IR")?;
    validate_generated_graphql_operation_documents(temp_dir, source_name)?;
    Ok(())
}

fn reject_deleted_artifact(
    dir: &Path,
    source_name: &SourceName,
    stem: &str,
    label: &str,
) -> Result<(), AppError> {
    let deleted_artifact = deleted_artifact_name(stem);
    reject_deleted_artifact_inner(dir, source_name, &deleted_artifact, label)
}

fn reject_deleted_artifact_inner(
    dir: &Path,
    source_name: &SourceName,
    deleted_artifact: &str,
    label: &str,
) -> Result<(), AppError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            reject_deleted_artifact_inner(&path, source_name, deleted_artifact, label)?;
            continue;
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == deleted_artifact)
        {
            return Err(AppError::FailedPrecondition(format!(
                "SourceSpec materialization for source '{source_name}' must not write deleted {label} artifacts"
            )));
        }
    }
    Ok(())
}

fn validate_generated_graphql_operation_documents(
    temp_dir: &Path,
    source_name: &SourceName,
) -> Result<(), AppError> {
    let capabilities_path = temp_dir.join("capabilities.yaml");
    let capabilities: SourceCapabilitySet =
        serde_yaml::from_slice(&std::fs::read(&capabilities_path)?).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "SourceSpec materialization for source '{source_name}' has invalid capabilities.yaml: {error}"
            ))
        })?;
    for capability in &capabilities.capabilities {
        let UpstreamBinding::Graphql(binding) = &capability.upstream_binding else {
            continue;
        };
        let document_path = crate::graphql_documents::operation_document_path(
            temp_dir,
            &capability.interface_id,
            binding,
        )
        .map_err(|message| {
            AppError::FailedPrecondition(format!(
                "SourceSpec materialization for source '{source_name}' has invalid GraphQL document reference for capability '{}': {message}",
                capability.capability_id
            ))
        })?;
        if !document_path.is_file() {
            return Err(AppError::FailedPrecondition(format!(
                "SourceSpec materialization for source '{source_name}' is missing generated GraphQL operation document '{}' for capability '{}'; re-add the source to regenerate artifacts",
                document_path.display(),
                capability.capability_id
            )));
        }
    }
    Ok(())
}

fn source_document_hash_key(interface_type: &str) -> &'static str {
    match interface_type {
        "mcp" => "tools_list_snapshot_sha256",
        "graphql" => "schema_document_sha256",
        _ => "source_document_sha256",
    }
}

fn deleted_artifact_name(stem: &str) -> String {
    format!("{stem}.yaml")
}

fn materialization_auth_headers(
    auth: Option<&AuthDescriptor>,
    context: &str,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<Vec<ProviderRequestHeader>, AppError> {
    match auth {
        None | Some(AuthDescriptor::None) => Ok(Vec::new()),
        Some(AuthDescriptor::BearerInput { key }) => Ok(vec![ProviderRequestHeader {
            name: AUTHORIZATION.as_str().to_string(),
            value: format!(
                "Bearer {}",
                provider_credential_value(provider_credentials, key, context)?
            ),
        }]),
        Some(AuthDescriptor::HeaderInput { name, key }) => {
            validate_provider_header_name(name, context)?;
            Ok(vec![ProviderRequestHeader {
                name: name.clone(),
                value: provider_credential_value(provider_credentials, key, context)?,
            }])
        }
        Some(AuthDescriptor::Headers { headers }) => headers
            .iter()
            .map(|header| {
                validate_provider_header_name(&header.name, context)?;
                Ok(ProviderRequestHeader {
                    name: header.name.clone(),
                    value: provider_credential_value(provider_credentials, &header.key, context)?,
                })
            })
            .collect(),
    }
}

fn descriptor_materialization_auth_headers(
    auth: Option<&AuthDescriptor>,
    context: &str,
    provider_credentials: &ProviderCredentialMaterial,
    descriptor_url: &str,
    provider_origin_url: Option<&str>,
) -> Result<Vec<ProviderRequestHeader>, AppError> {
    if !descriptor_auth_origin_matches(descriptor_url, provider_origin_url, context)? {
        return Ok(Vec::new());
    }
    materialization_auth_headers(auth, context, provider_credentials)
}

fn descriptor_auth_origin_matches(
    descriptor_url: &str,
    provider_origin_url: Option<&str>,
    context: &str,
) -> Result<bool, AppError> {
    let Some(provider_origin_url) = provider_origin_url else {
        return Ok(false);
    };
    let descriptor_url = reqwest::Url::parse(descriptor_url).map_err(|error| {
        AppError::InvalidInput(format!(
            "{context} URL '{descriptor_url}' is invalid: {error}"
        ))
    })?;
    let provider_origin_url = reqwest::Url::parse(provider_origin_url).map_err(|error| {
        AppError::InvalidInput(format!(
            "{context} provider URL '{provider_origin_url}' is invalid: {error}"
        ))
    })?;
    Ok(same_origin(&descriptor_url, &provider_origin_url))
}

fn same_origin(left: &reqwest::Url, right: &reqwest::Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn provider_credential_value(
    provider_credentials: &ProviderCredentialMaterial,
    key: &str,
    context: &str,
) -> Result<String, AppError> {
    provider_credentials.get(key).cloned().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "{context} requires source secret '{key}', but no credential material was available during source materialization"
        ))
    })
}

fn provider_source_input_value(
    provider_credentials: &ProviderCredentialMaterial,
    key: &str,
    context: &str,
) -> Result<String, AppError> {
    provider_credentials.get(key).cloned().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "{context} requires source input '{key}', but no material was available during source materialization"
        ))
    })
}

fn validate_provider_header_name(name: &str, context: &str) -> Result<(), AppError> {
    HeaderName::from_bytes(name.as_bytes())
        .map(|_| ())
        .map_err(|error| {
            AppError::InvalidInput(format!(
                "{context} auth header name '{name}' is invalid: {error}"
            ))
        })
}

fn render_provider_url_template(
    template: &ParsedTemplate,
    field: &str,
    provider_credentials: &ProviderCredentialMaterial,
) -> Result<String, AppError> {
    let mut rendered = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(literal) => rendered.push_str(literal),
            TemplatePart::Token(token) => {
                if token.namespace() != &TemplateNamespace::Input {
                    return Err(AppError::FailedPrecondition(format!(
                        "{field} contains unsupported template token '{}'",
                        token.raw()
                    )));
                }
                rendered.push_str(&provider_source_input_value(
                    provider_credentials,
                    token.key(),
                    field,
                )?);
            }
        }
    }
    let parsed = reqwest::Url::parse(&rendered).map_err(|error| {
        AppError::InvalidInput(format!("{field} URL '{rendered}' is invalid: {error}"))
    })?;
    if !descriptor_url_is_allowed(&parsed) {
        return Err(AppError::FailedPrecondition(format!(
            "{field} URL '{rendered}' must use HTTPS, except localhost development URLs"
        )));
    }
    Ok(rendered)
}

fn literal_string_url(url: &str, field: &str) -> Result<String, AppError> {
    if url.contains("{{") || url.contains("}}") {
        return Err(AppError::FailedPrecondition(format!(
            "{field} contains template tokens, but source materialization currently supports only literal provider acquisition URLs"
        )));
    }
    Ok(url.to_string())
}

impl<'a> BlockingMcpStreamableHttpSession<'a> {
    fn initialize(
        interface_id: &str,
        endpoint: &'a str,
        request_headers: &'a [ProviderRequestHeader],
    ) -> Result<Self, AppError> {
        let mut session = Self {
            endpoint,
            request_headers,
            session_id: None,
            next_id: 1,
        };
        let initialize = json!({
            "jsonrpc": "2.0",
            "id": session.next_request_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "coral",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            },
        });
        let response = session.post_json(&initialize, true)?;
        session.remember_session_id(&response);
        let value = response.value.ok_or_else(|| {
            AppError::FailedPrecondition(format!(
                "MCP interface '{interface_id}' initialize response was empty"
            ))
        })?;
        json_rpc_result(interface_id, "initialize", value)?;
        session.post_notification(interface_id, "notifications/initialized")?;
        Ok(session)
    }

    fn post_request(&mut self, method: &str, params: &Value) -> Result<Value, AppError> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": self.next_request_id(),
            "method": method,
            "params": params,
        });
        let response = self.post_json(&request, true)?;
        self.remember_session_id(&response);
        response
            .value
            .ok_or_else(|| AppError::FailedPrecondition(format!("MCP {method} response was empty")))
    }

    fn post_notification(&mut self, interface_id: &str, method: &str) -> Result<(), AppError> {
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        let response = self.post_json(&request, false)?;
        self.remember_session_id(&response);
        if let Some(value) = response.value
            && let Some(error) = value.get("error")
        {
            return Err(AppError::FailedPrecondition(format!(
                "MCP interface '{interface_id}' {method} returned JSON-RPC error: {error}"
            )));
        }
        Ok(())
    }

    fn post_json(
        &self,
        body: &Value,
        require_body: bool,
    ) -> Result<ProviderJsonResponse, AppError> {
        let request_headers = self.mcp_request_headers();
        post_json_request_on_blocking_thread(self.endpoint, body, &request_headers, require_body)
    }

    fn mcp_request_headers(&self) -> Vec<ProviderRequestHeader> {
        let mut headers = self.request_headers.to_vec();
        headers.push(ProviderRequestHeader {
            name: ACCEPT.as_str().to_string(),
            value: MCP_ACCEPT.to_string(),
        });
        headers.push(ProviderRequestHeader {
            name: MCP_PROTOCOL_VERSION_HEADER.to_string(),
            value: MCP_PROTOCOL_VERSION.to_string(),
        });
        if let Some(session_id) = &self.session_id {
            headers.push(ProviderRequestHeader {
                name: MCP_SESSION_ID_HEADER.to_string(),
                value: session_id.clone(),
            });
        }
        headers
    }

    fn remember_session_id(&mut self, response: &ProviderJsonResponse) {
        if let Some(session_id) = response
            .headers
            .get(&MCP_SESSION_ID_HEADER.to_ascii_lowercase())
        {
            self.session_id = Some(session_id.clone());
        }
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_id;
        self.next_id += 1;
        request_id
    }
}

fn read_mcp_tools_list_streamable_http(
    interface_id: &str,
    endpoint: &str,
    request_headers: &[ProviderRequestHeader],
) -> Result<Value, AppError> {
    let mut session =
        BlockingMcpStreamableHttpSession::initialize(interface_id, endpoint, request_headers)?;
    let mut all_tools = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        pages += 1;
        if pages > MAX_MCP_TOOLS_LIST_PAGES {
            return Err(AppError::FailedPrecondition(format!(
                "MCP interface '{interface_id}' tools/list exceeded {MAX_MCP_TOOLS_LIST_PAGES} pages"
            )));
        }
        let mut params = serde_json::Map::new();
        if let Some(cursor) = cursor.take() {
            params.insert("cursor".to_string(), Value::String(cursor));
        }
        let response = session.post_request("tools/list", &Value::Object(params))?;
        let result = json_rpc_result(interface_id, "tools/list", response)?;
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "MCP interface '{interface_id}' tools/list result did not contain a tools array"
                ))
            })?;
        all_tools.extend(tools);
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    Ok(json!({
        "protocol_version": null,
        "server": {
            "transport": "streamable_http",
            "url": endpoint,
        },
        "tools": all_tools,
        "tools_list_changed": false,
    }))
}

fn json_rpc_result(interface_id: &str, method: &str, response: Value) -> Result<Value, AppError> {
    let Value::Object(mut object) = response else {
        return Err(AppError::FailedPrecondition(format!(
            "MCP interface '{interface_id}' {method} response was not a JSON object"
        )));
    };
    if let Some(error) = object.remove("error") {
        return Err(AppError::FailedPrecondition(format!(
            "MCP interface '{interface_id}' {method} returned JSON-RPC error: {error}"
        )));
    }
    object.remove("result").ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "MCP interface '{interface_id}' {method} response did not include result"
        ))
    })
}

fn post_json_rpc_like_document(
    endpoint: String,
    body: &Value,
    request_headers: &[ProviderRequestHeader],
) -> Result<Value, AppError> {
    let panic_endpoint = endpoint.clone();
    let body = body.clone();
    let request_headers = request_headers.to_vec();
    std::thread::spawn(move || post_json_on_blocking_thread(&endpoint, &body, &request_headers))
        .join()
        .map_err(|_panic| {
            AppError::Unavailable(format!(
                "failed to POST provider descriptor '{panic_endpoint}': fetch thread panicked"
            ))
        })?
}

fn post_json_on_blocking_thread(
    endpoint: &str,
    body: &Value,
    request_headers: &[ProviderRequestHeader],
) -> Result<Value, AppError> {
    post_json_request_on_blocking_thread(endpoint, body, request_headers, true)?
        .value
        .ok_or_else(|| {
            AppError::FailedPrecondition(format!(
                "provider descriptor response '{endpoint}' was empty"
            ))
        })
}

fn post_json_request_on_blocking_thread(
    endpoint: &str,
    body: &Value,
    request_headers: &[ProviderRequestHeader],
    require_body: bool,
) -> Result<ProviderJsonResponse, AppError> {
    let allows_http_loopback = ensure_allowed_descriptor_url(endpoint)?;
    let redirect_policy = if request_headers.is_empty() {
        descriptor_redirect_policy()
    } else {
        reqwest::redirect::Policy::none()
    };
    let mut client_builder = reqwest::blocking::Client::builder()
        .timeout(DESCRIPTOR_FETCH_TIMEOUT)
        .redirect(redirect_policy)
        .user_agent(DESCRIPTOR_USER_AGENT);
    if !allows_http_loopback {
        client_builder = client_builder.https_only(true);
    }
    let client = client_builder.build().map_err(|error| {
        AppError::Unavailable(format!(
            "failed to build provider descriptor client for '{endpoint}': {error}"
        ))
    })?;
    let request =
        apply_provider_request_headers(client.post(endpoint).json(body), request_headers)?;
    let mut response = request
        .send()
        .map_err(|error| AppError::Unavailable(format!("failed to POST '{endpoint}': {error}")))?;
    if !descriptor_url_is_allowed(response.url()) {
        return Err(AppError::FailedPrecondition(format!(
            "provider descriptor '{endpoint}' redirected to disallowed URL '{}'",
            response.url()
        )));
    }
    let status = response.status();
    if !status.is_success() {
        return Err(AppError::Unavailable(format!(
            "failed to POST provider descriptor '{endpoint}': HTTP {status}"
        )));
    }
    let response_headers = blocking_response_headers(response.headers());
    let media_type = response_headers
        .get(CONTENT_TYPE.as_str())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let bytes =
        read_bounded_descriptor_response(&mut response, endpoint, "provider descriptor response")?;
    let value = if bytes.is_empty() {
        None
    } else {
        Some(decode_provider_json_response(
            endpoint,
            media_type.as_deref(),
            &bytes,
        )?)
    };
    if require_body && value.is_none() {
        return Err(AppError::FailedPrecondition(format!(
            "provider descriptor response '{endpoint}' was empty"
        )));
    }
    Ok(ProviderJsonResponse {
        value,
        headers: response_headers,
    })
}

fn decode_provider_json_response(
    endpoint: &str,
    media_type: Option<&str>,
    bytes: &[u8],
) -> Result<Value, AppError> {
    if media_type.is_some_and(|value| value.starts_with("text/event-stream")) {
        return decode_sse_json_response(endpoint, bytes);
    }
    serde_json::from_slice(bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "provider descriptor response '{endpoint}' was not JSON: {error}"
        ))
    })
}

fn decode_sse_json_response(endpoint: &str, bytes: &[u8]) -> Result<Value, AppError> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "provider descriptor response '{endpoint}' SSE stream was not UTF-8: {error}"
        ))
    })?;
    for event in text.split("\n\n") {
        let mut data = String::new();
        for line in event.lines() {
            if let Some(value) = line.strip_prefix("data:") {
                data.push_str(value.trim_start());
                data.push('\n');
            }
        }
        let data = data.trim();
        if !data.is_empty() {
            return serde_json::from_str(data).map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "provider descriptor response '{endpoint}' SSE data was not JSON: {error}"
                ))
            });
        }
    }
    Err(AppError::FailedPrecondition(format!(
        "provider descriptor response '{endpoint}' SSE stream did not include a JSON data event"
    )))
}

fn read_file_descriptor(file: &Path) -> Result<Vec<u8>, AppError> {
    let canonical = canonicalize_file_descriptor(file)?;
    let metadata = std::fs::metadata(&canonical)?;
    if metadata.len() > MAX_DESCRIPTOR_BYTES {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' is too large: {} bytes exceeds {MAX_DESCRIPTOR_BYTES}",
            file.display(),
            metadata.len()
        )));
    }
    std::fs::read(canonical).map_err(AppError::from)
}

pub(crate) fn canonicalize_file_descriptor(file: &Path) -> Result<PathBuf, AppError> {
    if !file.is_absolute() {
        return Err(AppError::InvalidInput(format!(
            "OpenAPI descriptor '{}' is relative, but SourceSpec manifests must use absolute file descriptors after import. Use `coral source add --file <manifest>` so Coral can resolve relative descriptors from the manifest directory.",
            file.display()
        )));
    }
    let metadata = std::fs::symlink_metadata(file)?;
    if metadata.file_type().is_symlink() {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' must not be a symlink",
            file.display()
        )));
    }
    if !metadata.file_type().is_file() {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' must be a regular file",
            file.display()
        )));
    }
    let canonical = file.canonicalize()?;
    Ok(canonical)
}

fn apply_provider_request_headers(
    mut request: reqwest::blocking::RequestBuilder,
    request_headers: &[ProviderRequestHeader],
) -> Result<reqwest::blocking::RequestBuilder, AppError> {
    for header in request_headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|error| {
            AppError::InvalidInput(format!(
                "provider descriptor header '{}' is invalid: {error}",
                header.name
            ))
        })?;
        let value = HeaderValue::from_str(&header.value).map_err(|error| {
            AppError::InvalidInput(format!(
                "provider descriptor header '{}' value is invalid: {error}",
                header.name
            ))
        })?;
        request = request.header(name, value);
    }
    Ok(request)
}

fn read_url_descriptor(
    url: &str,
    label: &'static str,
    request_headers: &[ProviderRequestHeader],
) -> Result<Vec<u8>, AppError> {
    let url = url.to_string();
    let panic_url = url.clone();
    let request_headers = request_headers.to_vec();
    std::thread::spawn(move || {
        read_url_descriptor_on_blocking_thread(&url, label, &request_headers)
    })
    .join()
    .map_err(|_panic| {
        AppError::Unavailable(format!(
            "failed to fetch {label} '{panic_url}': fetch thread panicked"
        ))
    })?
}

fn read_url_descriptor_on_blocking_thread(
    url: &str,
    label: &'static str,
    request_headers: &[ProviderRequestHeader],
) -> Result<Vec<u8>, AppError> {
    let allows_http_loopback = ensure_allowed_descriptor_url(url)?;
    let redirect_policy = if request_headers.is_empty() {
        descriptor_redirect_policy()
    } else {
        reqwest::redirect::Policy::none()
    };
    let mut client_builder = reqwest::blocking::Client::builder()
        .timeout(DESCRIPTOR_FETCH_TIMEOUT)
        .redirect(redirect_policy)
        .user_agent(DESCRIPTOR_USER_AGENT);
    if !allows_http_loopback {
        client_builder = client_builder.https_only(true);
    }
    let client = client_builder.build().map_err(|error| {
        AppError::Unavailable(format!(
            "failed to build {label} client for '{url}': {error}"
        ))
    })?;
    let request = apply_provider_request_headers(client.get(url), request_headers)?;
    let mut response = request.send().map_err(|error| {
        AppError::Unavailable(format!("failed to fetch {label} '{url}': {error}"))
    })?;
    if !descriptor_url_is_allowed(response.url()) {
        return Err(AppError::FailedPrecondition(format!(
            "{label} '{url}' redirected to disallowed URL '{}'",
            response.url()
        )));
    }
    if !response.status().is_success() {
        return Err(AppError::Unavailable(format!(
            "failed to fetch {label} '{url}': HTTP {}",
            response.status()
        )));
    }
    read_bounded_descriptor_response(&mut response, url, label)
}

fn read_bounded_descriptor_response(
    response: &mut reqwest::blocking::Response,
    url: &str,
    label: &str,
) -> Result<Vec<u8>, AppError> {
    if let Some(length) = response.content_length()
        && length > MAX_DESCRIPTOR_BYTES
    {
        return Err(AppError::FailedPrecondition(format!(
            "{label} '{url}' is too large: {length} bytes exceeds {MAX_DESCRIPTOR_BYTES}"
        )));
    }
    let mut bytes = Vec::new();
    let mut limited = response.by_ref().take(MAX_DESCRIPTOR_BYTES + 1);
    limited.read_to_end(&mut bytes).map_err(|error| {
        AppError::Unavailable(format!("failed to read {label} '{url}': {error}"))
    })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_DESCRIPTOR_BYTES {
        return Err(AppError::FailedPrecondition(format!(
            "{label} '{url}' is too large: exceeds {MAX_DESCRIPTOR_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

fn blocking_response_headers(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
        })
        .collect()
}

fn ensure_allowed_descriptor_url(url: &str) -> Result<bool, AppError> {
    let parsed = reqwest::Url::parse(url).map_err(|error| {
        AppError::InvalidInput(format!(
            "OpenAPI descriptor URL '{url}' is invalid: {error}"
        ))
    })?;
    if !descriptor_url_is_allowed(&parsed) {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor URL '{url}' must use HTTPS, except localhost development URLs"
        )));
    }
    Ok(parsed.scheme() == "http")
}

fn descriptor_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= 5 {
            return attempt.error("too many descriptor redirects");
        }
        if descriptor_url_is_allowed(attempt.url()) {
            attempt.follow()
        } else {
            let target = attempt.url().to_string();
            attempt.error(format!(
                "descriptor redirect target '{target}' is disallowed"
            ))
        }
    })
}

fn descriptor_url_is_allowed(url: &reqwest::Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        },
        _ => false,
    }
}

fn write_yaml<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::ensure_private_dir(parent)?;
    }
    let bytes = serde_yaml::to_string(value)?;
    fs::write_atomic(path, bytes.as_bytes())?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn new_materialization_suffix(prefix: &str) -> String {
    format!("{prefix}.{}", Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    use coral_spec::{AuthHeaderDescriptor, parse_source_manifest_yaml};
    use tempfile::TempDir;

    use super::*;

    fn workspace_name() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn openapi_fixture() -> &'static str {
        r"
openapi: 3.0.3
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: integer}
"
    }

    fn loopback_http_response(response: String) -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept descriptor request");
            let mut buffer = [0_u8; 2048];
            let read = stream.read(&mut buffer).expect("read descriptor request");
            stream
                .write_all(response.as_bytes())
                .expect("write descriptor response");
            let request_bytes = buffer.get(..read).expect("read length within buffer");
            String::from_utf8_lossy(request_bytes).into_owned()
        });
        (format!("http://{addr}/descriptor"), handle)
    }

    fn loopback_http_conversation(responses: Vec<String>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept descriptor request");
                let mut buffer = [0_u8; 4096];
                let read = stream.read(&mut buffer).expect("read descriptor request");
                stream
                    .write_all(response.as_bytes())
                    .expect("write descriptor response");
                let request_bytes = buffer.get(..read).expect("read length within buffer");
                requests.push(String::from_utf8_lossy(request_bytes).into_owned());
            }
            requests
        });
        (format!("http://{addr}/mcp"), handle)
    }

    fn http_response(status: &str, headers: &[(&str, &str)], body: &str) -> String {
        let mut response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
        for (name, value) in headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        response.push_str(body);
        response
    }

    fn graphql_fixture() -> &'static str {
        r"
type Query {
  repository(owner: String, name: String): Repository
}

type Repository {
  id: ID
  name: String
}
"
    }

    fn build_source_spec_materialization_fixture()
    -> (TempDir, AppStateLayout, SourceName, MaterializationBuild) {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("openapi.yaml");
        let graphql_file = descriptor_temp.path().join("schema.graphql");
        let data_file = descriptor_temp.path().join("issues.jsonl");
        std::fs::write(&openapi_file, openapi_fixture()).expect("write OpenAPI");
        std::fs::write(&graphql_file, graphql_fixture()).expect("write GraphQL");
        std::fs::write(&data_file, "{}\n").expect("write data");

        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let source_name = SourceName::parse("github_source_materialization_test").expect("source");
        let manifest_yaml = format!(
            r"
spec_version: 1
kind: source
name: github_source_materialization_test
interfaces:
  - id: rest
    type: openapi
    file: {}
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: sdl_file
      file: {}
  - id: files
    type: file
    files:
      - {}
    format:
      kind: jsonl
",
            openapi_file.display(),
            graphql_file.display(),
            data_file.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml).expect("parse SourceSpec");

        build_source_materialization_tmp(SourceMaterializationBuildRequest {
            layout: &layout,
            workspace_name: &workspace_name(),
            source_name: &source_name,
            identity: &crate::sources::model::InstalledSource::identity_for_name(&source_name),
            manifest_yaml: &manifest_yaml,
            manifest: &manifest,
            temp_suffix: "test",
            provider_credentials: &BTreeMap::new(),
        })
        .map(|build| (state_temp, layout, source_name, build))
        .expect("build SourceSpec materialization")
    }

    #[test]
    fn source_spec_materialization_writes_capabilities_and_exports() {
        let (_state_temp, layout, source_name, build) = build_source_spec_materialization_fixture();
        assert_eq!(build.kind, MaterializationKind::SourceSpec);
        assert!(build.temp_dir.join("artifacts.yaml").exists());
        assert!(build.temp_dir.join("capabilities.yaml").exists());
        assert!(build.temp_dir.join("exports/source-exports.yaml").exists());
        assert!(
            build
                .temp_dir
                .join("interfaces/rest/provider-snapshot.yaml")
                .exists()
        );
        assert!(
            build
                .temp_dir
                .join("interfaces/graph/provider-snapshot.yaml")
                .exists()
        );
        let graphql_document = build
            .temp_dir
            .join("interfaces/graph")
            .join(crate::graphql_documents::GENERATED_GRAPHQL_OPERATIONS_DIR)
            .join("query_repository.graphql");
        assert_eq!(
            std::fs::read_to_string(&graphql_document).expect("read generated GraphQL document"),
            "query QueryRepository($owner: String, $name: String) { repository(owner: $owner, name: $name) { __typename } }"
        );
        let artifacts: Value = serde_yaml::from_str(
            &std::fs::read_to_string(build.temp_dir.join("artifacts.yaml"))
                .expect("read artifacts"),
        )
        .expect("parse artifacts");
        assert_eq!(
            artifacts
                .pointer("/interfaces/graph/generated_operations_dir")
                .and_then(Value::as_str),
            Some("interfaces/graph/generated-graphql-operations")
        );
        let fingerprint: Value = serde_yaml::from_str(
            &std::fs::read_to_string(build.temp_dir.join("fingerprint.yaml"))
                .expect("read fingerprint"),
        )
        .expect("parse fingerprint");
        assert!(
            fingerprint
                .pointer("/interfaces/1/generated_operations_sha256")
                .and_then(Value::as_str)
                .is_some()
        );
        assert_eq!(
            std::fs::read_to_string(build.temp_dir.join("interfaces/files/files/file_0"))
                .expect("read installed file artifact"),
            "{}\n"
        );
        assert!(
            !build
                .temp_dir
                .join(deleted_artifact_name("projections"))
                .exists()
        );
        assert!(
            !build
                .temp_dir
                .join("interfaces/rest")
                .join(deleted_artifact_name("semantic-ir"))
                .exists()
        );

        let swap = replace_materialization(&layout, &workspace_name(), &source_name, &build)
            .expect("install SourceSpec materialization");
        assert_eq!(swap.kind, MaterializationKind::SourceSpec);
        let installed = layout.source_materialized_dir(&workspace_name(), &source_name);
        assert!(installed.join("capabilities.yaml").exists());
        assert!(installed.join("exports/source-exports.yaml").exists());
    }

    #[test]
    fn materialization_validation_requires_generated_graphql_operation_documents() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let graphql_file = descriptor_temp.path().join("schema.graphql");
        std::fs::write(&graphql_file, graphql_fixture()).expect("write GraphQL");

        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let source_name = SourceName::parse("missing_graphql_doc_test").expect("source");
        let manifest_yaml = format!(
            r"
spec_version: 1
kind: source
name: missing_graphql_doc_test
interfaces:
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: sdl_file
      file: {}
",
            graphql_file.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml).expect("parse SourceSpec");

        let build = build_source_materialization_tmp(SourceMaterializationBuildRequest {
            layout: &layout,
            workspace_name: &workspace_name(),
            source_name: &source_name,
            identity: &crate::sources::model::InstalledSource::identity_for_name(&source_name),
            manifest_yaml: &manifest_yaml,
            manifest: &manifest,
            temp_suffix: "test",
            provider_credentials: &BTreeMap::new(),
        })
        .expect("build SourceSpec materialization");
        std::fs::remove_file(
            build
                .temp_dir
                .join("interfaces/graph/generated-graphql-operations/query_repository.graphql"),
        )
        .expect("remove generated GraphQL document");

        let error = validate_source_materialization_temp_dir(&build.temp_dir, &source_name)
            .expect_err("missing generated GraphQL document should fail validation");

        assert!(
            error
                .to_string()
                .contains("missing generated GraphQL operation document"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn read_url_descriptor_rejects_non_https_urls() {
        let error = read_url_descriptor_on_blocking_thread(
            "http://example.com/openapi.yaml",
            "OpenAPI descriptor",
            &[],
        )
        .expect_err("plain HTTP descriptor should fail");

        assert!(
            error.to_string().contains("must use HTTPS"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn descriptor_url_policy_allows_loopback_http_for_development() {
        assert!(
            !ensure_allowed_descriptor_url("https://example.com/openapi.yaml")
                .expect("https descriptor")
        );
        assert!(
            ensure_allowed_descriptor_url("http://localhost:3000/openapi.yaml")
                .expect("localhost descriptor")
        );
        assert!(
            ensure_allowed_descriptor_url("http://127.0.0.1:3000/openapi.yaml")
                .expect("loopback descriptor")
        );
        assert!(
            ensure_allowed_descriptor_url("http://[::1]:3000/openapi.yaml")
                .expect("ipv6 loopback descriptor")
        );
    }

    #[test]
    fn read_url_descriptor_allows_loopback_http_for_development() {
        let body = "openapi: 3.0.3\npaths: {}\n";
        let (url, server) = loopback_http_response(http_response(
            "200 OK",
            &[("Content-Type", "application/yaml")],
            body,
        ));

        let bytes = read_url_descriptor_on_blocking_thread(&url, "OpenAPI descriptor", &[])
            .expect("read descriptor");
        server.join().expect("server thread");
        assert_eq!(bytes, body.as_bytes());
    }

    #[test]
    fn read_url_descriptor_sends_provider_auth_headers() {
        let body = "openapi: 3.0.3\npaths: {}\n";
        let (url, server) = loopback_http_response(http_response(
            "200 OK",
            &[("Content-Type", "application/yaml")],
            body,
        ));

        let bytes = read_url_descriptor_on_blocking_thread(
            &url,
            "OpenAPI descriptor",
            &[ProviderRequestHeader {
                name: "authorization".to_string(),
                value: "Bearer secret-token".to_string(),
            }],
        )
        .expect("read descriptor");
        let request = server.join().expect("server thread");
        assert_eq!(bytes, body.as_bytes());
        assert!(
            request
                .lines()
                .any(|line| line.eq_ignore_ascii_case("authorization: Bearer secret-token")),
            "descriptor request did not include auth header:\n{request}"
        );
    }

    #[test]
    fn openapi_descriptor_acquisition_omits_cross_origin_provider_auth() {
        let body = openapi_fixture();
        let (url, server) = loopback_http_response(http_response(
            "200 OK",
            &[("Content-Type", "application/yaml")],
            body,
        ));
        let interface = OpenApiInterface {
            id: "rest".to_string(),
            descriptor: OpenApiDescriptor::Url {
                url: ParsedTemplate::parse(&url).expect("descriptor URL"),
            },
            base_url: Some(ParsedTemplate::parse("https://api.example.com").expect("base URL")),
            auth: Some(AuthDescriptor::BearerInput {
                key: "token".to_string(),
            }),
            inputs: Vec::new(),
        };
        let credentials = BTreeMap::from([("token".to_string(), "secret-token".to_string())]);

        let bytes = read_openapi_interface_document(&interface, &credentials)
            .expect("read OpenAPI descriptor");
        let request = server.join().expect("server thread");

        assert_eq!(bytes, body.as_bytes());
        assert!(
            !request
                .lines()
                .any(|line| line.to_ascii_lowercase().starts_with("authorization:")),
            "cross-origin descriptor request leaked auth header:\n{request}"
        );
    }

    #[test]
    fn openapi_descriptor_acquisition_sends_same_origin_provider_auth() {
        let body = openapi_fixture();
        let (url, server) = loopback_http_response(http_response(
            "200 OK",
            &[("Content-Type", "application/yaml")],
            body,
        ));
        let mut base_url = reqwest::Url::parse(&url).expect("descriptor URL");
        base_url.set_path("/api");
        let interface = OpenApiInterface {
            id: "rest".to_string(),
            descriptor: OpenApiDescriptor::Url {
                url: ParsedTemplate::parse(&url).expect("descriptor URL"),
            },
            base_url: Some(ParsedTemplate::parse(base_url.as_str()).expect("base URL")),
            auth: Some(AuthDescriptor::BearerInput {
                key: "token".to_string(),
            }),
            inputs: Vec::new(),
        };
        let credentials = BTreeMap::from([("token".to_string(), "secret-token".to_string())]);

        let bytes = read_openapi_interface_document(&interface, &credentials)
            .expect("read OpenAPI descriptor");
        let request = server.join().expect("server thread");

        assert_eq!(bytes, body.as_bytes());
        assert!(
            request
                .lines()
                .any(|line| line.eq_ignore_ascii_case("authorization: Bearer secret-token")),
            "same-origin descriptor request did not include auth header:\n{request}"
        );
    }

    #[test]
    fn post_json_descriptor_allows_loopback_http_for_development() {
        let body = r#"{"data":{"__schema":{"queryType":{"name":"Query"}}}}"#;
        let (url, server) = loopback_http_response(http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            body,
        ));

        let value =
            post_json_on_blocking_thread(&url, &json!({ "query": "query { __schema }" }), &[])
                .expect("POST descriptor");
        server.join().expect("server thread");
        assert_eq!(
            value
                .pointer("/data/__schema/queryType/name")
                .and_then(Value::as_str),
            Some("Query")
        );
    }

    #[test]
    fn graphql_introspection_query_requests_field_deprecation_metadata() {
        assert!(GRAPHQL_INTROSPECTION_QUERY.contains("fields(includeDeprecated: true)"));
        assert!(GRAPHQL_INTROSPECTION_QUERY.contains("isDeprecated"));
        assert!(GRAPHQL_INTROSPECTION_QUERY.contains("deprecationReason"));
    }

    #[test]
    fn materialization_auth_headers_resolve_secret_material() {
        let mut credentials = BTreeMap::new();
        credentials.insert("token".to_string(), "secret-token".to_string());
        let headers = materialization_auth_headers(
            Some(&AuthDescriptor::BearerInput {
                key: "token".to_string(),
            }),
            "GraphQL live introspection",
            &credentials,
        )
        .expect("auth headers");

        assert_eq!(
            headers,
            vec![ProviderRequestHeader {
                name: "authorization".to_string(),
                value: "Bearer secret-token".to_string(),
            }]
        );
    }

    #[test]
    fn materialization_auth_headers_resolve_multiple_headers() {
        let credentials = BTreeMap::from([
            ("api_key".to_string(), "api-secret".to_string()),
            ("app_key".to_string(), "app-secret".to_string()),
        ]);
        let headers = materialization_auth_headers(
            Some(&AuthDescriptor::Headers {
                headers: vec![
                    AuthHeaderDescriptor {
                        name: "DD-API-KEY".to_string(),
                        key: "api_key".to_string(),
                    },
                    AuthHeaderDescriptor {
                        name: "DD-APPLICATION-KEY".to_string(),
                        key: "app_key".to_string(),
                    },
                ],
            }),
            "Datadog descriptor acquisition",
            &credentials,
        )
        .expect("auth headers");

        assert_eq!(
            headers,
            vec![
                ProviderRequestHeader {
                    name: "DD-API-KEY".to_string(),
                    value: "api-secret".to_string(),
                },
                ProviderRequestHeader {
                    name: "DD-APPLICATION-KEY".to_string(),
                    value: "app-secret".to_string(),
                },
            ]
        );
    }

    #[test]
    fn materialization_auth_headers_reject_missing_secret_material() {
        let error = materialization_auth_headers(
            Some(&AuthDescriptor::HeaderInput {
                name: "X-Api-Key".to_string(),
                key: "token".to_string(),
            }),
            "MCP tools/list acquisition",
            &BTreeMap::new(),
        )
        .expect_err("missing provider material should fail");

        assert!(
            error.to_string().contains("no credential material"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn descriptor_auth_headers_do_not_require_secret_for_cross_origin_descriptors() {
        let headers = descriptor_materialization_auth_headers(
            Some(&AuthDescriptor::BearerInput {
                key: "token".to_string(),
            }),
            "GraphQL SDL URL acquisition",
            &BTreeMap::new(),
            "https://schemas.example.com/schema.graphql",
            Some("https://api.example.com/graphql"),
        )
        .expect("cross-origin descriptor auth decision");

        assert!(headers.is_empty());
    }

    #[test]
    fn post_json_descriptor_sends_provider_auth_headers() {
        let body = r#"{"data":{"__schema":{"queryType":{"name":"Query"}}}}"#;
        let (url, server) = loopback_http_response(http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            body,
        ));

        let value = post_json_on_blocking_thread(
            &url,
            &json!({ "query": "query { __schema }" }),
            &[
                ProviderRequestHeader {
                    name: "Authorization".to_string(),
                    value: "Bearer secret-token".to_string(),
                },
                ProviderRequestHeader {
                    name: "X-Api-Key".to_string(),
                    value: "secret-key".to_string(),
                },
            ],
        )
        .expect("POST descriptor");
        let request = server.join().expect("server thread").to_ascii_lowercase();
        assert_eq!(
            value
                .pointer("/data/__schema/queryType/name")
                .and_then(Value::as_str),
            Some("Query")
        );
        assert!(
            request.contains("\r\nauthorization: bearer secret-token\r\n"),
            "request did not include authorization header:\n{request}"
        );
        assert!(
            request.contains("\r\nx-api-key: secret-key\r\n"),
            "request did not include custom auth header:\n{request}"
        );
    }

    #[test]
    fn mcp_tools_list_initializes_streamable_http_session() {
        let (url, server) = loopback_http_conversation(vec![
            http_response(
                "200 OK",
                &[
                    ("Content-Type", "application/json"),
                    (MCP_SESSION_ID_HEADER, "session-123"),
                ],
                r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}"#,
            ),
            http_response("202 Accepted", &[], ""),
            http_response(
                "200 OK",
                &[("Content-Type", "application/json")],
                r#"{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"list_issues","inputSchema":{"type":"object"}}]}}"#,
            ),
        ]);

        let value =
            read_mcp_tools_list_streamable_http("tools", &url, &[]).expect("read tools/list");
        let requests = server.join().expect("server thread");
        assert_eq!(
            value
                .pointer("/tools/0/name")
                .and_then(serde_json::Value::as_str),
            Some("list_issues")
        );
        let [initialize, initialized, tools_list] = requests.as_slice() else {
            panic!("expected three MCP requests, got {}", requests.len());
        };
        assert!(
            initialize.contains(r#""method":"initialize""#),
            "first request should initialize:\n{initialize}"
        );
        assert!(
            initialized.contains(r#""method":"notifications/initialized""#),
            "second request should send initialized notification:\n{initialized}"
        );
        assert!(
            tools_list.contains(r#""method":"tools/list""#),
            "third request should list tools:\n{tools_list}"
        );
        for request in [initialized, tools_list] {
            let lower = request.to_ascii_lowercase();
            assert!(
                lower.contains("\r\nmcp-session-id: session-123\r\n"),
                "request did not include MCP session id:\n{request}"
            );
            assert!(
                lower.contains("\r\nmcp-protocol-version: 2025-06-18\r\n"),
                "request did not include MCP protocol version:\n{request}"
            );
        }
    }

    #[test]
    fn mcp_tools_list_supports_stdio_transport() {
        let script = r#"
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fixture","version":"0.1.0"}}}\n' "$id"
      ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}}\n' "$id"
      ;;
  esac
done
"#;
        let value = read_mcp_tools_list_stdio(
            "tools",
            "sh",
            &["-c".to_string(), script.to_string()],
            Vec::new(),
        )
        .expect("read stdio tools/list");

        assert_eq!(
            value.pointer("/tools/0/name").and_then(Value::as_str),
            Some("echo")
        );
        assert_eq!(
            value
                .pointer("/tools/0/inputSchema/type")
                .and_then(Value::as_str),
            Some("object")
        );
    }

    #[test]
    fn post_json_descriptor_rejects_oversized_response_before_reading_body() {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{{}}",
            MAX_DESCRIPTOR_BYTES + 1
        );
        let (url, server) = loopback_http_response(response);

        let error =
            post_json_on_blocking_thread(&url, &json!({ "query": "query { __schema }" }), &[])
                .expect_err("oversized POST descriptor should fail");
        server.join().expect("server thread");
        assert!(
            error.to_string().contains("is too large"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn read_url_descriptor_rejects_disallowed_redirect_before_following() {
        let (url, server) = loopback_http_response(http_response(
            "302 Found",
            &[("Location", "http://example.com/openapi.yaml")],
            "",
        ));

        let error = read_url_descriptor_on_blocking_thread(&url, "OpenAPI descriptor", &[])
            .expect_err("remote HTTP redirect should fail");
        server.join().expect("server thread");
        let message = error.to_string();
        assert!(
            message.contains("following redirect"),
            "unexpected error: {error}"
        );
    }
}
