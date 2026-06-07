use std::collections::{BTreeMap, BTreeSet};

use coral_capabilities::{Diagnostic, DiagnosticSeverity, DiagnosticStage, SourceCapabilitySet};

use super::model::{
    Binding, BindingBuildContext, BindingContributor, CapabilityExport, SourceExports,
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
    disambiguate_duplicate_sql_refs(&mut exports);

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
    let mut seen = BTreeSet::new();
    values.retain(|value| seen.insert(value.to_ascii_lowercase()));
}

fn disambiguate_duplicate_sql_refs(exports: &mut SourceExports) {
    let mut counts = BTreeMap::new();
    for entry in &exports.entries {
        for binding in &entry.bindings {
            if let Binding::Sql(binding) = binding {
                *counts.entry(binding.ref_.value.clone()).or_insert(0usize) += 1;
            }
        }
    }
    let duplicates = counts
        .iter()
        .filter_map(|(ref_, count)| (*count > 1).then_some(ref_.clone()))
        .collect::<BTreeSet<_>>();
    if duplicates.is_empty() {
        return;
    }

    let mut used_refs = BTreeSet::new();
    for entry in &exports.entries {
        for binding in &entry.bindings {
            let ref_value = binding.ref_().value.clone();
            if !duplicates.contains(&ref_value) {
                used_refs.insert(ref_value);
            }
        }
    }

    for entry in &mut exports.entries {
        for binding in &mut entry.bindings {
            let Binding::Sql(binding) = binding else {
                continue;
            };
            if !duplicates.contains(&binding.ref_.value) {
                continue;
            }

            let Some((schema, table)) = binding.sql_reference.split_once('.') else {
                continue;
            };
            let table_base = format!("{}_{}", entry.interface_id, table);
            let mut candidate = format!("{schema}.{table_base}");
            let mut suffix = 2usize;
            while used_refs.contains(&binding.kind.export_ref(candidate.clone()).value) {
                candidate = format!("{schema}.{table_base}_{suffix}");
                suffix += 1;
            }
            binding.sql_reference = candidate;
            binding.ref_ = binding.kind.export_ref(binding.sql_reference.clone());
            used_refs.insert(binding.ref_.value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use coral_capabilities::{
        Capability, FileFormatDescriptor, FileScanBinding, ProviderOrigin, ProviderOriginKind,
        SourceCapabilitySet, SourceId, UpstreamBinding,
    };

    use crate::package::SourceKey;
    use crate::{BindingContribution, SqlBinding, SqlBindingKind, SqlProjectionV1, SqlRowShape};

    use super::*;

    #[derive(Debug)]
    struct DuplicateSqlContributor {
        kind: SqlBindingKind,
    }

    impl BindingContributor for DuplicateSqlContributor {
        fn name(&self) -> &'static str {
            "duplicate_sql"
        }

        fn contribute(
            &self,
            _capability: &Capability,
            _ctx: &BindingBuildContext,
        ) -> crate::Result<BindingContribution> {
            let sql_reference = "datadog.list_apikeys";
            Ok(BindingContribution {
                bindings: vec![Binding::Sql(SqlBinding {
                    kind: self.kind,
                    ref_: self.kind.export_ref(sql_reference),
                    sql_reference: sql_reference.to_string(),
                    projection: SqlProjectionV1 {
                        row_shape: SqlRowShape::Collection,
                        columns: Vec::new(),
                        inputs: Vec::new(),
                        response_selection: None,
                        pagination: None,
                        file_scan: None,
                        diagnostics: Vec::new(),
                    },
                })],
                search_text: Vec::new(),
                diagnostics: Vec::new(),
            })
        }
    }

    #[test]
    fn build_source_exports_disambiguates_duplicate_sql_refs_by_interface() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                test_capability(source_id.clone(), "rest_v1"),
                test_capability(source_id.clone(), "rest_v2"),
            ],
        );
        let exports = build_source_exports(
            &capabilities,
            &BindingBuildContext {
                source_id,
                display_name: "Datadog".to_string(),
                source_key: SourceKey("datadog".to_string()),
            },
            &[&DuplicateSqlContributor {
                kind: SqlBindingKind::Table,
            }],
        )
        .expect("exports");
        let sql_refs = exports
            .entries
            .iter()
            .flat_map(|entry| &entry.bindings)
            .filter_map(|binding| match binding {
                Binding::Sql(binding) => Some(binding.sql_reference.as_str()),
                Binding::Typescript(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            sql_refs,
            vec![
                "datadog.rest_v1_list_apikeys",
                "datadog.rest_v2_list_apikeys"
            ]
        );
    }

    #[test]
    fn build_source_exports_preserves_duplicate_sql_function_refs() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                test_capability(source_id.clone(), "rest_v1"),
                test_capability(source_id.clone(), "rest_v2"),
            ],
        );
        let exports = build_source_exports(
            &capabilities,
            &BindingBuildContext {
                source_id,
                display_name: "Datadog".to_string(),
                source_key: SourceKey("datadog".to_string()),
            },
            &[&DuplicateSqlContributor {
                kind: SqlBindingKind::Function,
            }],
        )
        .expect("exports");
        let sql_bindings = exports
            .entries
            .iter()
            .flat_map(|entry| &entry.bindings)
            .filter_map(|binding| match binding {
                Binding::Sql(binding) => Some(binding),
                Binding::Typescript(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            sql_bindings
                .iter()
                .map(|binding| (binding.kind, binding.ref_.value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (
                    SqlBindingKind::Function,
                    "sql_function:datadog.rest_v1_list_apikeys"
                ),
                (
                    SqlBindingKind::Function,
                    "sql_function:datadog.rest_v2_list_apikeys"
                ),
            ]
        );
    }

    fn test_capability(source_id: SourceId, interface_id: &str) -> Capability {
        Capability::new(
            source_id,
            interface_id,
            "list_apikeys",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: format!(
                    "interfaces/{interface_id}/provider-snapshot.yaml#/operations/list_apikeys"
                ),
                provider_name: "listAPIKeys".to_string(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: Vec::new(),
                format: FileFormatDescriptor::Json,
                schema_ref: None,
            }),
        )
    }
}
