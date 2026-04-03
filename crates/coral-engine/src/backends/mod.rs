//! Backend-specific source implementations and compilation into runtime sources.

use crate::{CoreError, QueryRuntimeProvider, QuerySource};
use coral_spec::ValidatedSourceManifest;

pub(crate) mod common;
pub(crate) use common::{
    BackendCompileRequest, BackendRegistration, CompiledBackendSource, RegisteredSource,
    RegisteredTable, arrow_type_for_column, build_registered_table, partition_columns_to_arrow,
    registered_columns_from_schema, registered_columns_from_specs, required_filter_names,
    schema_from_columns,
};

pub(crate) mod http;
pub(crate) mod jsonl;
pub(crate) mod parquet;
pub(crate) mod shared;

pub(crate) fn compile_query_source(
    source: &QuerySource,
    runtime: &dyn QueryRuntimeProvider,
    runtime_context: &crate::QueryRuntimeContext,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    let source_secrets =
        runtime.resolve_source_secrets(source, &source.source_spec().required_secret_names())?;
    compile_validated_manifest(
        source.source_spec(),
        &BackendCompileRequest {
            runtime_context,
            source_secrets,
            source_variables: source.variables().clone(),
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
    compile_validated_manifest(
        manifest,
        &BackendCompileRequest {
            runtime_context,
            source_secrets,
            source_variables,
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
