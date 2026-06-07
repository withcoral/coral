//! Provider snapshot import and capability generation.
//!
//! `coral-importers` consumes app-acquired raw interface inputs plus
//! `coral-spec` descriptors. It produces provider snapshots and a
//! `SourceCapabilitySet`; it does not derive SQL projections or exports.

#![allow(
    missing_docs,
    reason = "Importer snapshot artifacts are versioned test fixtures while the provider-specific shapes evolve."
)]
#![allow(
    clippy::module_name_repetitions,
    reason = "Provider snapshot and importer names intentionally include interface domains."
)]

use std::collections::BTreeMap;

use coral_capabilities::{
    Capability, Diagnostic, SOURCE_CAPABILITY_GENERATOR_VERSION, SourceCapabilitySet, SourceId,
};
use coral_spec::{SourceInterface, SourceSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod auth;
mod file;
mod graphql;
mod hash;
mod mcp;
mod naming;
mod openapi;
mod schema_shape;

use file::import_file;
use graphql::import_graphql;
use mcp::import_mcp;
use openapi::import_openapi;

/// Importer result type.
pub type Result<T> = std::result::Result<T, ImporterError>;

/// Importer errors.
#[derive(Debug, thiserror::Error)]
pub enum ImporterError {
    /// Required raw input was missing.
    #[error("missing raw interface input for '{0}'")]
    MissingRawInput(String),
    /// Raw input could not be parsed.
    #[error("failed to parse {interface_id} input: {message}")]
    Parse {
        interface_id: String,
        message: String,
    },
    /// Source descriptor is unsupported.
    #[error("unsupported source interface: {0}")]
    Unsupported(String),
}

/// Raw app-acquired interface input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RawInterfaceInput {
    OpenApiDocument { bytes: Vec<u8> },
    McpToolsList { value: Value },
    GraphqlSchema { text: String },
    GraphqlIntrospection { value: Value },
    FileListing { schema: Value },
}

/// Full import result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportResult {
    pub provider_snapshots: Vec<ProviderSnapshotArtifact>,
    pub capabilities: SourceCapabilitySet,
    pub diagnostics: Vec<Diagnostic>,
}

/// Versioned provider snapshot artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSnapshotArtifact {
    pub artifact_schema_version: u32,
    pub source_id: SourceId,
    pub interface_id: String,
    pub interface_type: String,
    pub importer_version: String,
    pub source_document_sha256: String,
    pub snapshot: Value,
    pub diagnostics: Vec<Diagnostic>,
}

/// Import all source interfaces into provider snapshots and capabilities.
///
/// # Errors
///
/// Returns [`ImporterError`] when a required raw input is missing or malformed.
pub fn import_source(
    source_id: SourceId,
    spec: &SourceSpec,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<ImportResult> {
    let mut snapshots = Vec::new();
    let mut capabilities = Vec::new();
    let mut diagnostics = Vec::new();

    for interface in &spec.interfaces {
        let imported = import_interface(&source_id, spec, interface, raw_inputs)?;
        diagnostics.extend(imported.snapshot.diagnostics.clone());
        capabilities.extend(imported.capabilities);
        snapshots.push(imported.snapshot);
    }

    let capability_set = SourceCapabilitySet {
        artifact_schema_version: 1,
        source_id,
        generator_version: SOURCE_CAPABILITY_GENERATOR_VERSION.to_string(),
        capabilities,
        diagnostics: diagnostics.clone(),
    };
    Ok(ImportResult {
        provider_snapshots: snapshots,
        capabilities: capability_set,
        diagnostics,
    })
}

struct ImportedInterface {
    snapshot: ProviderSnapshotArtifact,
    capabilities: Vec<Capability>,
}

fn import_interface(
    source_id: &SourceId,
    spec: &SourceSpec,
    interface: &SourceInterface,
    raw_inputs: &BTreeMap<String, RawInterfaceInput>,
) -> Result<ImportedInterface> {
    match interface {
        SourceInterface::OpenApi(openapi) => import_openapi(source_id, spec, openapi, raw_inputs),
        SourceInterface::Mcp(mcp) => import_mcp(source_id, spec, mcp, raw_inputs),
        SourceInterface::Graphql(graphql) => import_graphql(source_id, spec, graphql, raw_inputs),
        SourceInterface::File(file) => import_file(source_id, spec, file, raw_inputs),
    }
}

#[cfg(test)]
mod tests;
