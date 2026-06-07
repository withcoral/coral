use coral_capabilities::{Diagnostic, DiagnosticSeverity, DiagnosticStage, SourceCapabilitySet};

use super::model::{
    BindingBuildContext, BindingContributor, CapabilityExport, SourceExports,
    WorkspaceExportSource, WorkspaceExports,
};
use super::validate::{Result, validate_source_exports, validate_workspace_exports};

/// Build source exports from capabilities and binding contributors.
///
/// # Errors
///
/// Returns an export error when contributor output or artifact validation fails.
pub fn build_source_exports(
    capabilities: &SourceCapabilitySet,
    ctx: &BindingBuildContext,
    contributors: &[&dyn BindingContributor],
) -> Result<SourceExports> {
    capabilities
        .validate()
        .map_err(|error| super::validate::ExportError::validation(error.to_string()))?;

    let mut exports = SourceExports::empty(ctx);
    for capability in &capabilities.capabilities {
        let mut entry = CapabilityExport::from_capability(capability, ctx);
        for contributor in contributors {
            let contribution = contributor.contribute(capability, ctx)?;
            entry.bindings.extend(contribution.bindings);
            entry.search_text.extend(contribution.search_text);
            entry.diagnostics.extend(contribution.diagnostics);
        }
        dedup_preserve_order(&mut entry.search_text);
        if !entry.bindings.is_empty() {
            exports.entries.push(entry);
        }
    }

    validate_source_exports(capabilities, &exports)?;
    Ok(exports)
}

/// Compose workspace exports from installed source exports.
///
/// # Errors
///
/// Returns an export error when cross-source typed refs collide.
pub fn compose_workspace_exports(
    workspace_id: impl Into<String>,
    sources: &[SourceExports],
) -> Result<WorkspaceExports> {
    let mut workspace = WorkspaceExports {
        artifact_schema_version: 1,
        workspace_id: workspace_id.into(),
        sources: Vec::with_capacity(sources.len()),
        entries: Vec::new(),
        diagnostics: Vec::new(),
    };
    for source in sources {
        workspace.sources.push(WorkspaceExportSource {
            source_id: source.source_id.clone(),
            display_name: source.display_name.clone(),
            source_key: source.source_key.clone(),
            source_exports_generator_version: source.generator_version.clone(),
        });
        workspace.entries.extend(source.entries.clone());
        workspace.diagnostics.extend(source.diagnostics.clone());
    }
    if let Err(error) = validate_workspace_exports(&workspace) {
        workspace.diagnostics.push(Diagnostic::new(
            "EXPORT_REF_COLLISION",
            DiagnosticSeverity::Error,
            DiagnosticStage::ExportGeneration,
            error.to_string(),
        ));
        return Err(error);
    }
    Ok(workspace)
}

fn dedup_preserve_order(values: &mut Vec<String>) {
    let mut seen = std::collections::BTreeSet::new();
    values.retain(|value| seen.insert(value.to_ascii_lowercase()));
}
