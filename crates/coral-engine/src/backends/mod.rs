//! Backend-specific source implementations and compilation into runtime sources.
//!
//! # Module layout conventions for backends
//!
//! Each backend (`http`, `mcp`, `file`, ...) implements
//! [`CompiledBackendSource`] and registers tables, table functions, and
//! metadata at runtime. A new backend module should match the shape below
//! where applicable:
//!
//! | File | Purpose | When to include |
//! |---|---|---|
//! | `mod.rs` | Module entry. `CompiledBackendSource` impl, `compile_source` / `compile_manifest`, internal module declarations. | always |
//! | `provider.rs` | `DataFusion` `TableProvider` implementation. | if the backend exposes tables |
//! | `function.rs` | `DataFusion` `TableFunctionImpl` for source-scoped UDTFs. | only if the backend exposes table functions |
//! | `client.rs` | Configured stateful wrapper (the value the rest of the backend talks to) and any transport-abstracting trait. | if the backend has a per-source client; skip if config is per-table (file backend) |
//! | `transport.rs` | Per-instance transport impls (HTTP requests, stdio child spawn, object-store wiring, ...). | if there are multiple transports or transport code is non-trivial |
//! | `response.rs` | Decode one response from the backend into the JSON payload that `shared/response_rows::extract_rows` consumes. | if response decoding is non-trivial |
//! | `fetch.rs` | Multi-request orchestration: pagination loop, cursor handling, row accumulation. | if pagination is backend-driven; skip if `DataFusion` drives the scan (file) |
//! | `error.rs` | Backend-specific structured error variants (`MISSING_REQUIRED_FILTER`, `*_TOOL_RETURNED_ERROR`, ...) that surface stable codes to CLI / MCP consumers. | always — even with one or two variants today |
//! | `tests.rs` | In-module unit tests and helpers. | always |
//!
//! Format-specific modules (`json.rs`, `parquet_schema.rs`, `partitions.rs`
//! in the file backend) are allowed when one backend handles multiple wire
//! formats.
//!
//! ## Where validation lives
//!
//! - **Manifest-shape validation** (schema, well-formedness, cross-reference
//!   checks) belongs in `coral-spec/backends/<name>.rs`, not here.
//! - **Registration-time runtime validation** (input resolution,
//!   authenticator wiring, env-var resolution) lives in the engine. Keep it
//!   next to `mod.rs` for small surfaces; factor into a focused file when it
//!   grows. Name the file after what it does (`registration_checks.rs`),
//!   not `validation.rs` — the latter reads as a `coral-spec` concern.
//!
//! ## Shared, not per-backend
//!
//! Reuse rather than re-implement:
//!
//! - [`shared::response_rows::extract_rows`](shared::response_rows) — JSON
//!   payload → row list via `ResponseSpec` (`RowStrategy::{Direct,
//!   DictEntries, SeriesPointList}`).
//! - [`shared::template`] — template / value-source rendering.
//! - [`shared::json_path::get_path_value`] — nested JSON traversal.
//! - [`common`] — `registered_columns_from_specs`, `required_filter_names`,
//!   `build_registered_table`, etc.
//!
//! If a backend needs row extraction, template rendering, or column-spec
//! plumbing, prefer extending the shared module over duplicating.
//!
//! ## Naming consistency
//!
//! - `client.rs` = configured stateful wrapper + transport interface.
//! - `transport.rs` = the actual round-trip / process spawn / store handle.
//! - `response.rs` = decode one response.
//! - `fetch.rs` = multi-request orchestration / pagination loop.
//!
//! If a concern doesn't apply, omit the file — do not repurpose the name.
//!
//! ## Reference backends
//!
//! - [`http`] — fullest surface: separate `client`, `transport`, `response`,
//!   `pagination`, `request`, `rate_limit`, `validation`, `trace`.
//! - [`mcp`] — smaller: `client` + `transport` + `response` + `fetch` (no
//!   per-request body building, no rate limit yet).
//! - [`file`] — `DataFusion`-driven: `listing.rs` wraps `ObjectStore` and
//!   hands a `ListingTableConfig` to `DataFusion`. No transport / fetch /
//!   response files because `DataFusion` owns those layers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::{CoreError, QuerySource, RequestAuthenticator, SourceInputResolver};
use coral_spec::ValidatedSourceManifest;

pub(crate) mod common;
pub(crate) use common::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext, CompiledBackendSource,
    RegisteredSource, RegisteredTable, RegisteredTableFunction, SourceTableFunctions,
    build_registered_inputs, build_registered_table, build_registered_table_function,
    internal_table_function_name, registered_columns_from_schema, registered_columns_from_specs,
    required_filter_names, schema_from_columns,
};

pub(crate) mod file;
pub(crate) mod http;
pub(crate) mod mcp;
pub(crate) mod shared;

pub(crate) fn compile_query_source(
    source: &QuerySource,
    runtime_context: &crate::QueryRuntimeContext,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    compile_validated_manifest(
        source.source_spec(),
        &BackendCompileRequest {
            source,
            runtime_context,
            source_secrets: source.secrets().clone(),
            source_variables: source.variables().clone(),
            request_authenticators,
            source_input_resolver,
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
    let source = QuerySource::new(
        manifest.clone(),
        source_variables.clone(),
        source_secrets.clone(),
    );
    compile_validated_manifest(
        manifest,
        &BackendCompileRequest {
            source: &source,
            runtime_context,
            source_secrets,
            source_variables,
            request_authenticators: &request_authenticators,
            source_input_resolver: None,
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
    if let Some(file_manifest) = manifest.as_file() {
        return Ok(file::compile_manifest(file_manifest, request));
    }
    if let Some(mcp_manifest) = manifest.as_mcp() {
        return Ok(mcp::compile_manifest(mcp_manifest, request));
    }

    Err(CoreError::internal(
        "unsupported validated manifest backend",
    ))
}
