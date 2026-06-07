use std::collections::BTreeSet;

use coral_capabilities::SourceCapabilitySet;

use super::model::{Binding, ExportKind, SourceExports, WorkspaceExports, capability_by_id};

/// Export result type.
pub type Result<T> = std::result::Result<T, ExportError>;

/// Export validation errors.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// A semantic export invariant failed.
    #[error("{0}")]
    Validation(String),
}

impl ExportError {
    /// Build a validation error.
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

/// Validate a source export artifact against its capability set.
///
/// # Errors
///
/// Returns [`ExportError`] if refs collide or entries point at missing
/// capabilities.
pub fn validate_source_exports(
    capabilities: &SourceCapabilitySet,
    exports: &SourceExports,
) -> Result<()> {
    if exports.artifact_schema_version != 1 {
        return Err(ExportError::validation(
            "SourceExports artifact_schema_version must be 1",
        ));
    }
    if exports.source_id != capabilities.source_id {
        return Err(ExportError::validation(
            "SourceExports source_id must match SourceCapabilitySet source_id",
        ));
    }
    let known = capability_by_id(capabilities);
    let mut refs = BTreeSet::new();
    let mut ts_paths = BTreeSet::new();
    for entry in &exports.entries {
        if !known.contains(&entry.capability_id) {
            return Err(ExportError::validation(format!(
                "export entry references missing capability '{}'",
                entry.capability_id
            )));
        }
        for binding in &entry.bindings {
            let binding_ref = binding.ref_();
            if !refs.insert(binding_ref.value.clone()) {
                return Err(ExportError::validation(format!(
                    "duplicate export ref '{}'",
                    binding_ref.value
                )));
            }
            match binding {
                Binding::Typescript(binding) => {
                    if binding.ref_.kind != ExportKind::Typescript {
                        return Err(ExportError::validation(
                            "TypeScript binding must use a TypeScript export ref",
                        ));
                    }
                    if !ts_paths.insert(binding.path.join(".")) {
                        return Err(ExportError::validation(format!(
                            "duplicate TypeScript binding path '{}'",
                            binding.path.join(".")
                        )));
                    }
                }
                Binding::Sql(binding) => {
                    let expected_ref = binding.kind.export_ref(binding.sql_reference.as_str());
                    if binding.ref_ != expected_ref {
                        return Err(ExportError::validation(format!(
                            "SQL binding '{}' must use matching export ref '{}'",
                            binding.sql_reference, expected_ref.value
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Validate a workspace export view.
///
/// # Errors
///
/// Returns [`ExportError`] when cross-source typed refs collide.
pub fn validate_workspace_exports(exports: &WorkspaceExports) -> Result<()> {
    if exports.artifact_schema_version != 1 {
        return Err(ExportError::validation(
            "WorkspaceExports artifact_schema_version must be 1",
        ));
    }
    let mut refs = BTreeSet::new();
    for entry in &exports.entries {
        for binding in &entry.bindings {
            if !refs.insert(binding.ref_().value.clone()) {
                return Err(ExportError::validation(format!(
                    "duplicate workspace export ref '{}'",
                    binding.ref_().value
                )));
            }
        }
    }
    Ok(())
}
