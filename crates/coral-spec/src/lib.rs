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
//! - [`parse_source_manifest_yaml`] parses and validates raw `YAML`
//! - [`parse_source_manifest_value`] parses and validates structured source-spec
//!   data
//! - [`ValidatedSourceManifest`] provides a backend-agnostic validated
//!   source-spec view with typed accessors for backend-specific models
//! - [`collect_source_inputs_yaml`] and [`collect_source_inputs_value`] extract
//!   variables and secrets that must be collected at install time
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
#[cfg(test)]
mod loader;
mod parser;
mod template;
mod validate;

pub use common::{
    AuthSpec, BodyFieldSpec, ColumnSpec, ExprSpec, FilterMode, FilterSpec, HeaderSpec, HttpMethod,
    ManifestDataType, Onboarding, PageSizeSpec, PaginationMode, PaginationSpec, QueryParamSpec,
    RequestRouteSpec, RequestSpec, ResponseSpec, RowStrategy, SourceBackend, SourceManifestCommon,
    TableCommon, ValidatedPagination, ValidatedPaginationMode, ValueSourceSpec,
};
pub use common::{collect_source_onboarding_value, collect_source_onboarding_yaml};
pub use error::{ManifestError, Result};
pub use inputs::{
    InputSpec, InputKind, collect_source_inputs_value, collect_source_inputs_yaml,
};
pub use parser::{
    ValidatedSourceManifest, parse_source_manifest_value, parse_source_manifest_yaml,
};
pub use template::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken};
pub(crate) use validate::{
    validate_columns, validate_filters_and_column_exprs, validate_http_table,
    validate_manifest_top_level,
};
