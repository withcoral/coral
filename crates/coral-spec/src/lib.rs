//! Declarative source-spec parsing, validation, and normalized source models
//! for Coral.
//!
//! `coral-spec` owns the source-spec DSL and validated source-definition model.
//! It is responsible for:
//!
//! - parsing raw `YAML` or structured source-spec values
//! - parsing raw `YAML` or structured identity-spec values
//! - validating source-spec shape and source-level invariants
//! - extracting interactive install-time inputs such as variables and secrets
//! - exposing normalized backend-specific source-spec models to sibling crates
//!
//! In Coral terminology, a source spec is usually authored as a manifest YAML
//! file on disk. Public parser APIs still use `manifest` in their names because
//! they operate on that file format, but the semantic model owned by this crate
//! is the validated source spec.
//!
//! This crate is intentionally engine-neutral. It does **not** depend on
//! `DataFusion`, `Arrow`, gRPC, or application-state concerns.
//!
//! # Primary Entry Points
//!
//! - [`parse_source_manifest_yaml`] is the full acceptance path used by
//!   lint, add, import, and discovery — it parses one manifest from `YAML`
//!   text, running the same validation as the server
//! - [`load_manifest_path`] loads a persisted manifest file from disk for the
//!   query/runtime path
//! - [`parse_source_manifest_value`] parses a pre-built structured value for
//!   engine callers that construct manifests programmatically
//! - [`ValidatedSourceManifest`] provides a backend-agnostic validated
//!   source-spec view with typed accessors for backend-specific models and
//!   for the declared interactive inputs
//! - [`ManifestInputSpec`] describes one install-time input (variable or secret)
//!   surfaced via [`ValidatedSourceManifest::declared_inputs`]
//! - [`parse_identity_manifest_yaml`] parses identity specs that describe how
//!   an app-owned identity is instantiated and injected into HTTP requests
//!
//! # Crate Relationships
//!
//! - `coral-app` and `coral-cli` use this crate for import-time source-spec
//!   inspection and interactive input discovery.
//! - `coral-engine` consumes validated backend-specific source-spec models and
//!   compiles them into executable runtime sources.
//!
//! # Example
//!
//! ```no_run
//! use coral_spec::parse_source_manifest_yaml;
//!
//! let manifest = parse_source_manifest_yaml(
//!     r#"
//! name: demo
//! version: 0.1.0
//! dsl_version: 3
//! backend: file
//! tables:
//!   - name: events
//!     description: Demo events
//!     format: jsonl
//!     source:
//!       location: file:///tmp/demo/
//!     columns:
//!       - name: kind
//!         type: Utf8
//! "#,
//! )?;
//!
//! assert_eq!(manifest.schema_name(), "demo");
//! assert!(manifest.as_file().is_some());
//! let _inputs = manifest.declared_inputs();
//! # Ok::<(), coral_spec::ManifestError>(())
//! ```

#![allow(
    clippy::missing_errors_doc,
    reason = "This internal crate exposes many validation-heavy helpers to sibling crates."
)]
#![allow(
    clippy::must_use_candidate,
    reason = "These manifest builders and accessors are internal crate APIs, not end-user APIs."
)]
pub mod backends;
mod bundle;
mod common;
mod error;
mod identities;
mod inputs;
mod loader;
mod parser;
mod schema;
mod template;
mod udf;
pub mod v4;
mod validate;

pub use backends::http::{AuthSpec, BasicAuthSpec, CustomAuthSpec, HeaderAuthSpec};
pub use backends::mcp::{
    McpEnvSpec, McpHttpAuthSpec, McpLimitBinding, McpServerSpec, McpSourceManifest,
    McpTableFilterBinding, McpTableFilterSpec, McpTableFunctionSpec, McpTableSpec,
};
pub use bundle::{IdentityManifestDocument, ManifestBundle, parse_manifest_bundle_yaml};
pub use common::{
    BodyFieldSpec, BodySpec, ColumnSpec, DO_NOT_INDEX_COLUMN_METADATA_KEY, DetailHintSpec,
    ExprSpec, FilterMode, FilterSpec, FunctionArgBinding, HeaderSpec, HttpMethod, ManifestDataType,
    PageSizeSpec, PaginationMode, PaginationSpec, QueryParamSpec, RequestRouteSpec, RequestSpec,
    ResponseBodyFormat, ResponseSpec, RowStrategy, SearchLimitsSpec, SourceBackend,
    SourceManifestCommon, SourceTableFunctionKind, SourceTableFunctionSpec, TableCommon,
    TableFunctionArgSpec, TimestampInput, ValidatedPagination, ValidatedPaginationMode,
    ValueSourceSpec,
};
pub(crate) use common::{
    validate_reserved_source_schema_name, validate_source_name, validate_test_queries,
};
pub use error::{ManifestError, Result};
pub use identities::{
    IDENTITY_SPEC_VERSION, IdentityManifest, IdentityOAuthMethodSpec, IdentityOAuthSpec,
    IdentitySpecConfig, IdentitySpecType, generated_identity_manifest_schema,
    parse_identity_manifest_value, parse_identity_manifest_yaml,
};
pub use inputs::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
    ManifestInputKind, ManifestInputSpec, ManifestOAuthClientIdSpec, ManifestOAuthClientSecretSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
    ManifestOAuthDynamicClientRegistrationAuthMethod, ManifestOAuthDynamicClientRegistrationSpec,
    ManifestOAuthEndpointUrls, ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
    ManifestOAuthRedirectBindPort, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
    ManifestOAuthScopeSpec, ManifestOAuthScopesSpec, resolve_inputs,
};
pub use loader::load_manifest_path;
pub use parser::{
    ValidatedSourceManifest, parse_source_manifest_value, parse_source_manifest_yaml,
};
pub use template::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken};
pub use udf::{
    FunctionCoralSqlImplementationSpec, FunctionImplementationSpec, FunctionSpec,
    parse_function_sql,
};
pub(crate) use validate::{
    DeclaredRelation, DetailHintDeclaringSurface, DetailHintTargetTable, HttpTableValidation,
    validate_columns, validate_declared_relation_namespace, validate_detail_hint_references,
    validate_filters_and_column_exprs, validate_http_function, validate_http_table,
    validate_identifier, validate_required_guide, validate_unique_values,
};
