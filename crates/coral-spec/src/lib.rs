//! Declarative source-spec parsing, validation, and normalized source models
//! for Coral.
//!
//! `coral-spec` owns the source-spec DSL and validated source-definition model.
//! It is responsible for:
//!
//! - parsing raw `YAML` or structured source-spec values
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
//! backend: jsonl
//! tables:
//!   - name: events
//!     description: Demo events
//!     source:
//!       location: file:///tmp/demo/
//!     columns:
//!       - name: kind
//!         type: Utf8
//! "#,
//! )?;
//!
//! assert_eq!(manifest.schema_name(), "demo");
//! assert!(manifest.as_jsonl().is_some());
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
mod common;
mod error;
mod inputs;
mod loader;
mod parser;
mod schema;
mod template;
mod validate;

pub(crate) use common::validate_test_queries;
pub use common::{
    AuthSpec, BodyFieldSpec, ColumnSpec, ExprSpec, FilterMode, FilterSpec, HeaderSpec, HttpMethod,
    ManifestDataType, PageSizeSpec, PaginationMode, PaginationSpec, QueryParamSpec,
    RequestRouteSpec, RequestSpec, ResponseSpec, RowStrategy, SourceBackend, SourceManifestCommon,
    TableCommon, TimestampInput, ValidatedPagination, ValidatedPaginationMode, ValueSourceSpec,
};
pub use error::{ManifestError, Result};
pub use inputs::{ManifestInputKind, ManifestInputSpec};
pub use loader::load_manifest_path;
pub use parser::{
    ValidatedSourceManifest, parse_source_manifest_value, parse_source_manifest_yaml,
};
pub use template::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken};
pub(crate) use validate::{
    validate_columns, validate_filters_and_column_exprs, validate_http_table,
};
