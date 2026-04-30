//! Backend-specific source implementations and compilation into runtime sources.

use std::collections::HashMap;
use std::sync::Arc;

use crate::{CoreError, QuerySource, RequestAuthenticator};
use coral_spec::ValidatedSourceManifest;

pub(crate) mod common;
pub(crate) use common::{
    BackendCompileRequest, BackendRegistration, CompiledBackendSource, RegisteredSource,
    RegisteredTable, build_registered_inputs, build_registered_table,
    build_registered_table_function, partition_columns_to_arrow, registered_columns_from_schema,
    registered_columns_from_specs, required_filter_names, schema_from_columns,
};

pub(crate) mod http;
pub(crate) mod jsonl;
pub(crate) mod parquet;
pub(crate) mod shared;

pub(crate) fn compile_query_source(
    source: &QuerySource,
    runtime_context: &crate::QueryRuntimeContext,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    compile_validated_manifest(
        source.source_spec(),
        &BackendCompileRequest {
            runtime_context,
            source_secrets: source.secrets().clone(),
            source_variables: source.variables().clone(),
            request_authenticators,
        },
    )
}

#[cfg(test)]
pub(crate) fn compile_source_manifest(
    manifest: &ValidatedSourceManifest,
    source_secrets: std::collections::BTreeMap<String, String>,
    source_variables: std::collections::BTreeMap<String, String>,
    runtime_context: &crate::QueryRuntimeContext,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    let request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>> = HashMap::new();
    compile_validated_manifest(
        manifest,
        &BackendCompileRequest {
            runtime_context,
            source_secrets,
            source_variables,
            request_authenticators: &request_authenticators,
        },
    )
}

pub(crate) fn compile_validated_manifest(
    manifest: &ValidatedSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    if let Some(http_manifest) = manifest.as_http() {
        return Ok(http::compile_manifest(http_manifest, request));
    }
    if let Some(parquet_manifest) = manifest.as_parquet() {
        return Ok(parquet::compile_manifest(parquet_manifest, request));
    }
    if let Some(jsonl_manifest) = manifest.as_jsonl() {
        return jsonl::compile_manifest(jsonl_manifest, request);
    }

    Err(CoreError::internal(
        "unsupported validated manifest backend",
    ))
}
