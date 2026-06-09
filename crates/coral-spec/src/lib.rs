//! Declarative source-spec parsing, validation, and normalized source models
//! for Coral.
//!
//! `coral-spec` owns the source-spec DSL and validated source-definition model.
//! It is responsible for:
//!
//! - parsing raw `YAML` or structured source-spec values
//! - validating source-spec shape and source-level invariants
//! - extracting interactive install-time inputs such as variables and secrets
//! - exposing normalized `SourceSpec` models to sibling crates
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
//! - [`parse_source_manifest_value`] parses a pre-built structured value for
//!   engine callers that construct manifests programmatically
//! - [`SourceSpec`] is the validated source contract with one or more
//!   provider interfaces
//! - [`ManifestInputSpec`] describes one install-time input (variable or secret)
//!   surfaced via [`SourceSpec::declared_inputs`]
//!
//! # Crate Relationships
//!
//! - `coral-app` and `coral-cli` use this crate for import-time source-spec
//!   inspection and interactive input discovery.
//! - Runtime code consumes capabilities and exports generated from
//!   `SourceSpec`; manifest parsing is limited to this crate.
//!
//! # Example
//!
//! ```no_run
//! use coral_spec::parse_source_manifest_yaml;
//!
//! let manifest = parse_source_manifest_yaml(
//!     r#"
//! spec_version: 1
//! kind: source
//! name: demo
//! interfaces:
//!   - id: files
//!     type: file
//!     files: [./events.jsonl]
//!     format:
//!       kind: jsonl
//! "#,
//! )?;
//!
//! assert_eq!(manifest.name, "demo");
//! let _inputs = manifest.declared_inputs;
//! # Ok::<(), coral_spec::ManifestError>(())
//! ```

#![expect(
    clippy::missing_errors_doc,
    reason = "This internal crate exposes many validation-heavy helpers to sibling crates."
)]
#![expect(
    clippy::must_use_candidate,
    reason = "These manifest builders and accessors are internal crate APIs, not end-user APIs."
)]
mod error;
mod inputs;
mod openapi;
mod parser;
mod schema;
mod source;
mod template;

pub use error::{ManifestError, Result};
pub use inputs::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
    ManifestInputKind, ManifestInputSpec, ManifestOAuthClientIdSpec, ManifestOAuthClientSecretSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
    ManifestOAuthEndpointUrls, ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
    ManifestOAuthRedirectBindPort, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
    ManifestOAuthScopeSpec, ManifestOAuthScopesSpec,
};
pub use openapi::{
    OpenApiDocumentMetadata, openapi_document_metadata, openapi_document_metadata_from_value,
};
pub use parser::{parse_source_manifest_value, parse_source_manifest_yaml};
pub use source::{
    AuthDescriptor, AuthHeaderDescriptor, FileFormatDescriptor as SourceFileFormatDescriptor,
    FileInterface, GraphqlInterface, GraphqlSchemaDescriptor, McpEnvBinding, McpInterface,
    McpServerDescriptor, McpTransportDescriptor, OpenApiDescriptor, OpenApiInterface,
    OpenApiOverlay, SourceInterface, SourceSpec, SourceSpecKind,
    filter_source_manifest_yaml_interfaces, generated_source_spec_schema,
};
pub use template::{ParsedTemplate, TemplateNamespace, TemplatePart, TemplateToken};
