use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use coral_capabilities::{
    Capability, CapabilityId, Diagnostic, DiagnosticSeverity, DiagnosticStage, HttpMethod,
    SourceCapabilitySet, UpstreamBinding,
};
use sha2::{Digest as _, Sha256};

use super::model::{
    Binding, BindingBuildContext, BindingContributor, CapabilityExport, ExportRef, SourceExports,
    TypescriptBinding, WorkspaceExportSource, WorkspaceExports,
};
use super::validate::ExportError;
use super::validate::{Result, validate_source_exports, validate_workspace_exports};
use crate::contributors::typescript_type_name;
use crate::paths::{identifier_segment, pascal_segment};

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
    disambiguate_duplicate_typescript_refs(capabilities, &mut exports)?;

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
            let table_base = table.to_string();
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

fn disambiguate_duplicate_typescript_refs(
    capabilities: &SourceCapabilitySet,
    exports: &mut SourceExports,
) -> Result<()> {
    let mut counts = BTreeMap::new();
    for entry in &exports.entries {
        for binding in &entry.bindings {
            if let Binding::Typescript(binding) = binding {
                *counts.entry(binding.path.clone()).or_insert(0usize) += 1;
            }
        }
    }
    let duplicates = counts
        .iter()
        .filter_map(|(path, count)| (*count > 1).then_some(path.clone()))
        .collect::<BTreeSet<_>>();
    if duplicates.is_empty() {
        return Ok(());
    }

    let capability_by_id = capabilities
        .capabilities
        .iter()
        .map(|capability| (capability.capability_id.clone(), capability))
        .collect::<BTreeMap<CapabilityId, &Capability>>();
    let mut used_paths = BTreeSet::new();
    for entry in &exports.entries {
        for binding in &entry.bindings {
            if let Binding::Typescript(binding) = binding
                && !duplicates.contains(&binding.path)
            {
                used_paths.insert(binding.path.clone());
            }
        }
    }

    for entry in &mut exports.entries {
        for binding in &mut entry.bindings {
            let Binding::Typescript(binding) = binding else {
                continue;
            };
            if !duplicates.contains(&binding.path) {
                continue;
            }
            let capability = capability_by_id.get(&entry.capability_id).ok_or_else(|| {
                ExportError::validation(format!(
                    "export entry references missing capability '{}'",
                    entry.capability_id
                ))
            })?;
            let path = disambiguated_typescript_path(&binding.path, capability, &used_paths)?;
            update_typescript_binding(binding, path.clone());
            entry.search_text.extend(path);
            dedup_preserve_order(&mut entry.search_text);
            used_paths.insert(binding.path.clone());
        }
    }
    Ok(())
}

fn disambiguated_typescript_path(
    base_path: &[String],
    capability: &Capability,
    used_paths: &BTreeSet<Vec<String>>,
) -> Result<Vec<String>> {
    let mut candidates = Vec::new();
    if let Some(version) = rest_version_segment(capability) {
        let candidate = insert_typescript_version_segment(base_path, &version);
        candidates.push(candidate.clone());
        let candidate = append_http_method_suffix(&candidate, capability);
        candidates.push(candidate.clone());
        candidates.push(append_stable_hash_suffix(&candidate, capability));
    }

    let candidate = append_http_method_suffix(base_path, capability);
    candidates.push(candidate.clone());
    candidates.push(append_stable_hash_suffix(&candidate, capability));

    for candidate in candidates {
        if typescript_path_is_available(&candidate, used_paths) {
            return Ok(candidate);
        }
    }
    Err(typescript_collision_error(base_path))
}

fn typescript_path_is_available(candidate: &[String], used_paths: &BTreeSet<Vec<String>>) -> bool {
    used_paths
        .iter()
        .all(|used_path| !typescript_paths_conflict(candidate, used_path))
}

fn typescript_paths_conflict(left: &[String], right: &[String]) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn rest_version_segment(capability: &Capability) -> Option<String> {
    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        return None;
    };
    binding
        .path_template
        .split('/')
        .find(|segment| is_version_path_segment(segment))
        .map(identifier_segment)
}

fn is_version_path_segment(segment: &str) -> bool {
    let lower = segment.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix('v') {
        return is_prefixed_version_path_body(rest);
    }
    is_version_path_body(&lower)
}

fn is_prefixed_version_path_body(value: &str) -> bool {
    !value.is_empty()
        && value.starts_with(|ch: char| ch.is_ascii_digit())
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

fn is_version_path_body(value: &str) -> bool {
    !value.is_empty()
        && value.chars().any(|ch| ch.is_ascii_digit())
        && value
            .chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
}

fn insert_typescript_version_segment(base_path: &[String], version: &str) -> Vec<String> {
    let mut candidate = base_path.to_vec();
    let insert_at = candidate.len().saturating_sub(1);
    candidate.insert(insert_at, version.to_string());
    candidate
}

fn append_http_method_suffix(base_path: &[String], capability: &Capability) -> Vec<String> {
    append_leaf_suffix(base_path, &http_method_suffix(capability))
}

fn append_stable_hash_suffix(base_path: &[String], capability: &Capability) -> Vec<String> {
    append_leaf_suffix(base_path, &typescript_collision_hash(capability))
}

fn append_leaf_suffix(base_path: &[String], suffix: &str) -> Vec<String> {
    let mut candidate = base_path.to_vec();
    let Some(leaf) = candidate.last_mut() else {
        return candidate;
    };
    leaf.push_str(suffix);
    candidate
}

fn http_method_suffix(capability: &Capability) -> String {
    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        return "Rest".to_string();
    };
    pascal_segment(match binding.method {
        HttpMethod::Get => "get",
        HttpMethod::Head => "head",
        HttpMethod::Options => "options",
        HttpMethod::Post => "post",
        HttpMethod::Put => "put",
        HttpMethod::Patch => "patch",
        HttpMethod::Delete => "delete",
    })
}

fn typescript_collision_hash(capability: &Capability) -> String {
    let mut hasher = Sha256::new();
    if let UpstreamBinding::Rest(binding) = &capability.upstream_binding {
        hasher.update(format!("{:?}", binding.method));
        hasher.update(b"\0");
        hasher.update(binding.path_template.as_bytes());
    }
    hasher.update(b"\0");
    hasher.update(capability.provider_origin.provider_name.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::new();
    for byte in digest.iter().take(4) {
        write!(out, "{byte:02x}").expect("write to string");
    }
    out
}

fn update_typescript_binding(binding: &mut TypescriptBinding, path: Vec<String>) {
    binding.ref_ = ExportRef::typescript(&path);
    binding.args_type_name = typescript_type_name(&path, "Args");
    binding.result_type_name = typescript_type_name(&path, "Result");
    binding.path = path;
}

fn typescript_collision_error(base_path: &[String]) -> ExportError {
    ExportError::validation(format!(
        "duplicate TypeScript binding path '{}' could not be disambiguated",
        base_path.join(".")
    ))
}

#[cfg(test)]
mod tests {
    use coral_capabilities::{
        Capability, FileFormatDescriptor, FileScanBinding, HttpMethod, ProviderOrigin,
        ProviderOriginKind, RestUpstreamBinding, SourceCapabilitySet, SourceId, UpstreamBinding,
    };

    use crate::package::SourceKey;
    use crate::{
        BindingContribution, SqlBinding, SqlBindingKind, SqlProjectionV1, SqlRowShape,
        TypescriptBindingContributor,
    };

    use super::*;

    #[derive(Debug)]
    struct DuplicateSqlContributor {
        kind: SqlBindingKind,
    }

    #[derive(Debug)]
    struct PrefixTypescriptContributor;

    #[derive(Debug)]
    struct FixedTypescriptContributor {
        path: Vec<&'static str>,
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

    impl BindingContributor for PrefixTypescriptContributor {
        fn name(&self) -> &'static str {
            "prefix_typescript"
        }

        fn contribute(
            &self,
            capability: &Capability,
            ctx: &BindingBuildContext,
        ) -> crate::Result<BindingContribution> {
            let mut path = vec![
                ctx.source_key.as_str().to_string(),
                capability.interface_id.clone(),
                "validate".to_string(),
            ];
            if capability.operation_id != "validate" {
                path.push("monitor".to_string());
            }
            Ok(BindingContribution {
                bindings: vec![Binding::Typescript(TypescriptBinding {
                    ref_: ExportRef::typescript(&path),
                    args_type_name: typescript_type_name(&path, "Args"),
                    result_type_name: typescript_type_name(&path, "Result"),
                    path,
                })],
                search_text: Vec::new(),
                diagnostics: Vec::new(),
            })
        }
    }

    impl BindingContributor for FixedTypescriptContributor {
        fn name(&self) -> &'static str {
            "fixed_typescript"
        }

        fn contribute(
            &self,
            _capability: &Capability,
            _ctx: &BindingBuildContext,
        ) -> crate::Result<BindingContribution> {
            let path = self
                .path
                .iter()
                .map(|segment| (*segment).to_string())
                .collect::<Vec<_>>();
            Ok(BindingContribution {
                bindings: vec![Binding::Typescript(TypescriptBinding {
                    ref_: ExportRef::typescript(&path),
                    args_type_name: typescript_type_name(&path, "Args"),
                    result_type_name: typescript_type_name(&path, "Result"),
                    path,
                })],
                search_text: Vec::new(),
                diagnostics: Vec::new(),
            })
        }
    }

    #[test]
    fn build_source_exports_derives_rest_typescript_paths_from_tags() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                rest_capability(
                    source_id.clone(),
                    "rest_v1",
                    "list_monitors",
                    "ListMonitors",
                    &["Monitors"],
                    HttpMethod::Get,
                    "/api/v1/monitor",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest_v1",
                    "validate_monitor",
                    "ValidateMonitor",
                    &["Monitors"],
                    HttpMethod::Post,
                    "/api/v1/monitor/validate",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest_v1",
                    "validate_existing_monitor",
                    "ValidateExistingMonitor",
                    &["Monitors"],
                    HttpMethod::Post,
                    "/api/v1/monitor/{monitor_id}/validate",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest_v1",
                    "validate",
                    "Validate",
                    &["Authentication"],
                    HttpMethod::Get,
                    "/api/v1/validate",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest_v1",
                    "list_monitor_downtimes",
                    "ListMonitorDowntimes",
                    &["Downtimes"],
                    HttpMethod::Get,
                    "/api/v1/downtime",
                ),
            ],
        );

        let refs = typescript_refs(&capabilities, source_id, "Datadog", "datadog");

        assert_eq!(
            refs,
            vec![
                "typescript:datadog.restV1.monitors.listMonitors",
                "typescript:datadog.restV1.monitors.validateMonitor",
                "typescript:datadog.restV1.monitors.validateExistingMonitor",
                "typescript:datadog.restV1.authentication.validate",
                "typescript:datadog.restV1.downtimes.listMonitorDowntimes",
            ]
        );
    }

    #[test]
    fn build_source_exports_disambiguates_duplicate_typescript_paths_with_versions() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_v1",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/v1/search",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_v2",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/v2/search",
                ),
            ],
        );

        let refs = typescript_refs(&capabilities, source_id, "Datadog", "datadog");

        assert_eq!(
            refs,
            vec![
                "typescript:datadog.rest.search.v1.search",
                "typescript:datadog.rest.search.v2.search",
            ]
        );
    }

    #[test]
    fn build_source_exports_disambiguates_duplicate_typescript_paths_with_bare_versions() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_2_0",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/2.0/search",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_3_0",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/3.0/search",
                ),
            ],
        );

        let refs = typescript_refs(&capabilities, source_id, "Datadog", "datadog");

        assert_eq!(
            refs,
            vec![
                "typescript:datadog.rest.search._20.search",
                "typescript:datadog.rest.search._30.search",
            ]
        );
    }

    #[test]
    fn build_source_exports_disambiguates_duplicate_typescript_paths_with_prerelease_versions() {
        let source_id = SourceId("src_google".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_v1beta1",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/v1beta1/search",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_v2alpha1",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/v2alpha1/search",
                ),
            ],
        );

        let refs = typescript_refs(&capabilities, source_id, "Google", "google");

        assert_eq!(
            refs,
            vec![
                "typescript:google.rest.search.v1beta1.search",
                "typescript:google.rest.search.v2alpha1.search",
            ]
        );
    }

    #[test]
    fn build_source_exports_avoids_prefix_conflicts_when_disambiguating_typescript_paths() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_namespace_v1",
                    "v1",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/search-namespace",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_v1_get",
                    "Search",
                    &["Search"],
                    HttpMethod::Get,
                    "/api/v1/search/get",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "search_v1_post",
                    "Search",
                    &["Search"],
                    HttpMethod::Post,
                    "/api/v1/search/post",
                ),
            ],
        );

        let refs = typescript_refs(&capabilities, source_id, "Datadog", "datadog");

        assert_eq!(
            refs,
            vec![
                "typescript:datadog.rest.search.v1",
                "typescript:datadog.rest.search.searchGet",
                "typescript:datadog.rest.search.searchPost",
            ]
        );
    }

    #[test]
    fn build_source_exports_rejects_typescript_prefix_collisions() {
        let source_id = SourceId("src_datadog".to_string());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            vec![
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "validate",
                    "Validate",
                    &["Authentication"],
                    HttpMethod::Get,
                    "/api/v1/validate",
                ),
                rest_capability(
                    source_id.clone(),
                    "rest",
                    "validate_monitor",
                    "ValidateMonitor",
                    &["Authentication"],
                    HttpMethod::Post,
                    "/api/v1/validate/monitor",
                ),
            ],
        );

        let error = build_source_exports(
            &capabilities,
            &BindingBuildContext {
                source_id,
                display_name: "Datadog".to_string(),
                source_key: SourceKey("datadog".to_string()),
            },
            &[&PrefixTypescriptContributor],
        )
        .expect_err("prefix collision should be rejected");

        assert!(error.to_string().contains("conflicts with namespace path"));
    }

    #[test]
    fn compose_workspace_exports_rejects_typescript_prefix_collisions() {
        let left_source_id = SourceId("src_left".to_string());
        let left_capabilities = SourceCapabilitySet::new(
            left_source_id.clone(),
            vec![test_capability(left_source_id.clone(), "rest")],
        );
        let left_exports = build_source_exports(
            &left_capabilities,
            &BindingBuildContext {
                source_id: left_source_id,
                display_name: "Left".to_string(),
                source_key: SourceKey("left".to_string()),
            },
            &[&FixedTypescriptContributor {
                path: vec!["shared", "rest", "issues"],
            }],
        )
        .expect("left source exports");

        let right_source_id = SourceId("src_right".to_string());
        let right_capabilities = SourceCapabilitySet::new(
            right_source_id.clone(),
            vec![test_capability(right_source_id.clone(), "rest")],
        );
        let right_exports = build_source_exports(
            &right_capabilities,
            &BindingBuildContext {
                source_id: right_source_id,
                display_name: "Right".to_string(),
                source_key: SourceKey("right".to_string()),
            },
            &[&FixedTypescriptContributor {
                path: vec!["shared", "rest", "issues", "list"],
            }],
        )
        .expect("right source exports");

        let error = compose_workspace_exports("default", &[left_exports, right_exports])
            .expect_err("workspace prefix collision should be rejected");

        assert!(error.to_string().contains("conflicts with namespace path"));
    }

    #[test]
    fn build_source_exports_disambiguates_duplicate_sql_refs_by_suffix() {
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
            vec!["datadog.list_apikeys", "datadog.list_apikeys_2"]
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
                    "sql_function:datadog.list_apikeys"
                ),
                (
                    SqlBindingKind::Function,
                    "sql_function:datadog.list_apikeys_2"
                ),
            ]
        );
    }

    fn typescript_refs(
        capabilities: &SourceCapabilitySet,
        source_id: SourceId,
        display_name: &str,
        source_key: &str,
    ) -> Vec<String> {
        let exports = build_source_exports(
            capabilities,
            &BindingBuildContext {
                source_id,
                display_name: display_name.to_string(),
                source_key: SourceKey(source_key.to_string()),
            },
            &[&TypescriptBindingContributor::new()],
        )
        .expect("exports");
        exports
            .entries
            .iter()
            .flat_map(|entry| &entry.bindings)
            .filter_map(|binding| match binding {
                Binding::Typescript(binding) => Some(binding.ref_.value.clone()),
                Binding::Sql(_) => None,
            })
            .collect()
    }

    fn rest_capability(
        source_id: SourceId,
        interface_id: &str,
        operation_id: &str,
        provider_operation_id: &str,
        tags: &[&str],
        method: HttpMethod,
        path_template: &str,
    ) -> Capability {
        Capability::new(
            source_id,
            interface_id,
            operation_id,
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref: format!(
                    "interfaces/{interface_id}/provider-snapshot.yaml#/operations/{operation_id}"
                ),
                provider_name: provider_operation_id.to_string(),
                tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
            },
            UpstreamBinding::Rest(RestUpstreamBinding {
                operation_ref: format!(
                    "interfaces/{interface_id}/provider-snapshot.yaml#/operations/{operation_id}"
                ),
                method,
                path_template: path_template.to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        )
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
                tags: Vec::new(),
            },
            UpstreamBinding::FileRead(FileScanBinding {
                file_refs: Vec::new(),
                format: FileFormatDescriptor::Json,
                schema_ref: None,
            }),
        )
    }
}
