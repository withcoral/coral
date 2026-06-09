//! App-owned composition of generated source exports into workspace exports.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use coral_capabilities::{
    Capability, CapabilityKind, Diagnostic, DiagnosticSeverity, DiagnosticStage, EffectKind,
    SOURCE_CAPABILITY_GENERATOR_VERSION, SourceCapabilitySet, SourceId, UpstreamBinding,
};
use coral_exports::{
    Binding, CapabilityExport, DescribeResolution, ExportKind, SOURCE_EXPORTS_GENERATOR_VERSION,
    SearchFilter, SearchResult, SourceExports, WorkspaceExports, compose_workspace_exports,
    describe_export, search_exports_page,
};
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::RuntimeExposureMode;
use crate::bootstrap::AppError;
use crate::credentials::CredentialStorageKind;
use crate::graphql_documents::operation_document_path;
use crate::sources::SourceName;
use crate::sources::model::InstalledSource;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

const DEFAULT_SEARCH_LIMIT: usize = 20;
const MAX_SEARCH_LIMIT: usize = 100;

#[derive(Debug, Clone)]
pub(crate) struct DiscoveryManager {
    config_store: ConfigStore,
    layout: AppStateLayout,
    runtime_exposure: RuntimeExposureMode,
}

impl DiscoveryManager {
    pub(crate) fn new(config_store: ConfigStore, layout: AppStateLayout) -> Self {
        Self::new_with_runtime_exposure(config_store, layout, RuntimeExposureMode::Both)
    }

    pub(crate) fn new_with_runtime_exposure(
        config_store: ConfigStore,
        layout: AppStateLayout,
        runtime_exposure: RuntimeExposureMode,
    ) -> Self {
        Self {
            config_store,
            layout,
            runtime_exposure,
        }
    }

    pub(crate) fn search(
        &self,
        workspace_name: &WorkspaceName,
        query: &str,
        filter: &DiscoverySearchFilter,
        pagination: DiscoveryPagination,
    ) -> Result<DiscoverySearchPage, AppError> {
        let workspace = if filter.has_source_selector() {
            self.load_workspace_exports_for_source_filter(workspace_name, filter)?
        } else {
            self.load_workspace_exports_best_effort(workspace_name)?
        };
        let page = search_exports_page(
            &workspace.exports,
            query,
            &filter.clone().into_search_filter(),
            pagination.limit,
            pagination.offset,
        );
        let total = page.total;
        let end = pagination.offset.saturating_add(page.items.len());
        Ok(DiscoverySearchPage {
            items: page.items,
            total,
            limit: pagination.limit,
            offset: pagination.offset,
            has_more: end < total,
            next_offset: (end < total).then_some(end),
            diagnostics: workspace.exports.diagnostics,
        })
    }

    pub(crate) fn describe(
        &self,
        workspace_name: &WorkspaceName,
        raw_ref: &str,
    ) -> Result<DiscoveryDescribeResult, AppError> {
        let workspace = self.load_workspace_exports_best_effort(workspace_name)?;
        let diagnostics = workspace.exports.diagnostics.clone();
        match describe_export(&workspace.exports, raw_ref) {
            DescribeResolution::Found { entry } => {
                let workspace = self.load_workspace_exports_for_capability_id(
                    workspace_name,
                    entry.capability_id.as_str(),
                )?;
                Ok(describe_loaded_workspace(&workspace, raw_ref))
            }
            DescribeResolution::Ambiguous { candidates } => {
                Ok(DiscoveryDescribeResult::Ambiguous(candidates))
            }
            DescribeResolution::NotFound => {
                if source_id_from_capability_id(raw_ref).is_some() {
                    let workspace =
                        self.load_workspace_exports_for_capability_id(workspace_name, raw_ref)?;
                    Ok(describe_loaded_workspace(&workspace, raw_ref))
                } else {
                    Ok(DiscoveryDescribeResult::NotFound { diagnostics })
                }
            }
        }
    }

    pub(crate) fn load_workspace_exports(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<LoadedWorkspaceExports, AppError> {
        self.load_workspace_exports_inner(workspace_name, |_| true, false)
    }

    pub(crate) fn load_workspace_exports_best_effort(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<LoadedWorkspaceExports, AppError> {
        self.load_workspace_exports_inner(workspace_name, |_| true, true)
    }

    pub(crate) fn load_workspace_exports_for_capability_id(
        &self,
        workspace_name: &WorkspaceName,
        capability_id: &str,
    ) -> Result<LoadedWorkspaceExports, AppError> {
        let Some(source_id) = source_id_from_capability_id(capability_id) else {
            return self.load_workspace_exports(workspace_name);
        };
        self.load_workspace_exports_inner(
            workspace_name,
            |source| source.source_id == source_id,
            false,
        )
    }

    fn load_workspace_exports_for_source_filter(
        &self,
        workspace_name: &WorkspaceName,
        filter: &DiscoverySearchFilter,
    ) -> Result<LoadedWorkspaceExports, AppError> {
        self.load_workspace_exports_inner(
            workspace_name,
            |source| filter.matches_installed_source(source),
            false,
        )
    }

    fn load_workspace_exports_inner(
        &self,
        workspace_name: &WorkspaceName,
        include_source: impl Fn(&InstalledSource) -> bool,
        tolerate_artifact_errors: bool,
    ) -> Result<LoadedWorkspaceExports, AppError> {
        let installed_sources = self.config_store.list_workspace_sources(workspace_name)?;
        let mut source_exports = Vec::with_capacity(installed_sources.len());
        let mut workspace_diagnostics = Vec::new();
        let mut capability_by_id = BTreeMap::new();
        let mut source_materialized_dir_by_id = BTreeMap::new();
        let mut source_runtime_by_id = BTreeMap::new();
        for source in installed_sources {
            if !include_source(&source) {
                continue;
            }
            let source_id = SourceId(source.source_id.clone());
            let materialized_dir = self
                .layout
                .source_materialized_dir(workspace_name, &source.name);
            let exports_path = materialized_dir.join("exports/source-exports.yaml");
            let capabilities_path = materialized_dir.join("capabilities.yaml");
            let artifacts = read_source_artifacts(&exports_path, &capabilities_path, &source);
            let (exports, capabilities) = match artifacts {
                Ok(artifacts) => artifacts,
                Err(error) if tolerate_artifact_errors => {
                    workspace_diagnostics.push(skipped_source_diagnostic(&source, &error));
                    continue;
                }
                Err(error) => return Err(error),
            };
            source_materialized_dir_by_id.insert(source_id.clone(), materialized_dir.clone());
            source_runtime_by_id.insert(
                source_id,
                LoadedSourceRuntime {
                    name: source.name.clone(),
                    credential_storage: source.effective_credential_storage(),
                    variables: source.variables.clone(),
                },
            );
            for capability in capabilities.capabilities {
                capability_by_id.insert(capability.capability_id.clone(), capability);
            }
            source_exports.push(exports);
        }
        let mut exports = compose_workspace_exports(workspace_name.as_str(), &source_exports)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        exports.diagnostics.extend(workspace_diagnostics);
        let exports = filter_runtime_exposure(exports, self.runtime_exposure);
        Ok(LoadedWorkspaceExports {
            exports,
            capability_by_id,
            source_materialized_dir_by_id,
            source_runtime_by_id,
        })
    }

    pub(crate) fn load_source_exports(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<LoadedWorkspaceExports, AppError> {
        let mut capability_by_id = BTreeMap::new();
        let source_id = SourceId(source.source_id.clone());
        let materialized_dir = self
            .layout
            .source_materialized_dir(workspace_name, &source.name);
        let exports_path = materialized_dir.join("exports/source-exports.yaml");
        let capabilities_path = materialized_dir.join("capabilities.yaml");
        let (exports, capabilities) =
            read_source_artifacts(&exports_path, &capabilities_path, source)?;
        for capability in capabilities.capabilities {
            capability_by_id.insert(capability.capability_id.clone(), capability);
        }
        let workspace_exports = compose_workspace_exports(workspace_name.as_str(), &[exports])
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        let workspace_exports = filter_runtime_exposure(workspace_exports, self.runtime_exposure);
        Ok(LoadedWorkspaceExports {
            exports: workspace_exports,
            capability_by_id,
            source_materialized_dir_by_id: BTreeMap::from([(source_id.clone(), materialized_dir)]),
            source_runtime_by_id: BTreeMap::from([(
                source_id,
                LoadedSourceRuntime {
                    name: source.name.clone(),
                    credential_storage: source.effective_credential_storage(),
                    variables: source.variables.clone(),
                },
            )]),
        })
    }
}

fn describe_loaded_workspace(
    workspace: &LoadedWorkspaceExports,
    raw_ref: &str,
) -> DiscoveryDescribeResult {
    match describe_export(&workspace.exports, raw_ref) {
        DescribeResolution::Found { entry } => {
            let capability = workspace
                .capability_by_id
                .get(&entry.capability_id)
                .cloned();
            DiscoveryDescribeResult::Found(Box::new(DiscoveryDescription { entry, capability }))
        }
        DescribeResolution::Ambiguous { candidates } => {
            DiscoveryDescribeResult::Ambiguous(candidates)
        }
        DescribeResolution::NotFound => DiscoveryDescribeResult::NotFound {
            diagnostics: workspace.exports.diagnostics.clone(),
        },
    }
}

fn source_id_from_capability_id(capability_id: &str) -> Option<&str> {
    capability_id
        .strip_prefix("source/")?
        .split_once("/interface/")
        .map(|(source_id, _)| source_id)
        .filter(|source_id| !source_id.is_empty())
}

fn read_source_artifacts(
    exports_path: &Path,
    capabilities_path: &Path,
    source: &InstalledSource,
) -> Result<(SourceExports, SourceCapabilitySet), AppError> {
    let exports: SourceExports =
        read_yaml_artifact(exports_path, source.name.as_str(), "source exports")?;
    let capabilities: SourceCapabilitySet = read_yaml_artifact(
        capabilities_path,
        source.name.as_str(),
        "source capabilities",
    )?;
    validate_source_artifact_version(
        source.name.as_str(),
        "source exports",
        &exports.generator_version,
        SOURCE_EXPORTS_GENERATOR_VERSION,
    )?;
    validate_source_artifact_version(
        source.name.as_str(),
        "source capabilities",
        &capabilities.generator_version,
        SOURCE_CAPABILITY_GENERATOR_VERSION,
    )?;
    let materialized_dir = capabilities_path.parent().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "source '{}' has generated source capabilities at an invalid path '{}'. Re-add the source to regenerate capability artifacts.",
            source.name,
            capabilities_path.display()
        ))
    })?;
    validate_graphql_operation_documents(materialized_dir, &capabilities, source)?;
    Ok((exports, capabilities))
}

fn validate_graphql_operation_documents(
    materialized_dir: &Path,
    capabilities: &SourceCapabilitySet,
    source: &InstalledSource,
) -> Result<(), AppError> {
    let mut missing = Vec::new();
    for capability in &capabilities.capabilities {
        let UpstreamBinding::Graphql(binding) = &capability.upstream_binding else {
            continue;
        };
        let document_path = operation_document_path(materialized_dir, &capability.interface_id, binding)
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "source '{}' (source_key '{}') has an invalid generated GraphQL operation document reference for '{}': {error}. Re-add the source to regenerate capability artifacts.",
                    source.name,
                    source.source_key,
                    capability.operation_id
                ))
            })?;
        if !document_path.is_file() {
            missing.push(format!(
                "{} -> {} ({})",
                capability.operation_id,
                binding.document_ref,
                document_path.display()
            ));
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let shown = missing
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    let extra = missing.len().saturating_sub(3);
    let suffix = if extra == 0 {
        String::new()
    } else {
        format!("; and {extra} more")
    };
    Err(AppError::FailedPrecondition(format!(
        "source '{}' (source_key '{}') is missing {} generated GraphQL operation document(s): {shown}{suffix}. Re-add the source to regenerate capability artifacts.",
        source.name,
        source.source_key,
        missing.len()
    )))
}

fn validate_source_artifact_version(
    source_name: &str,
    artifact_label: &str,
    actual: &str,
    expected: &str,
) -> Result<(), AppError> {
    if actual == expected {
        return Ok(());
    }
    Err(AppError::FailedPrecondition(format!(
        "source '{source_name}' has generated {artifact_label} from generator '{actual}', but this binary requires '{expected}'. {}",
        artifact_recovery_guidance(source_name)
    )))
}

fn artifact_recovery_guidance(source_name: &str) -> String {
    let binary_path = std::env::current_exe().map_or_else(
        |error| format!("unavailable ({error})"),
        |path| path.display().to_string(),
    );
    format!(
        "Running Coral binary: '{binary_path}' (version {}). Restart any MCP/client session using an older Coral process, then re-add source '{source_name}' to regenerate capability artifacts.",
        env!("CARGO_PKG_VERSION")
    )
}

fn skipped_source_diagnostic(source: &InstalledSource, error: &AppError) -> Diagnostic {
    let mut diagnostic = Diagnostic::new(
        "SOURCE_ARTIFACTS_UNAVAILABLE",
        DiagnosticSeverity::Warning,
        DiagnosticStage::Materialization,
        format!(
            "Skipping source '{}' during workspace discovery because generated artifacts are unavailable: {error}",
            source.name
        ),
    );
    diagnostic.source_id = Some(SourceId(source.source_id.clone()));
    diagnostic.source_ref = Some(source.name.as_str().to_string());
    diagnostic.details = json!({
        "source_name": source.name.as_str(),
        "source_key": source.source_key,
        "error": error.to_string(),
    });
    diagnostic
}

fn filter_runtime_exposure(
    mut exports: WorkspaceExports,
    runtime_exposure: RuntimeExposureMode,
) -> WorkspaceExports {
    exports.entries = exports
        .entries
        .into_iter()
        .filter_map(|mut entry| {
            entry
                .bindings
                .retain(|binding| runtime_exposure.exposes_binding(binding));
            expose_binding_diagnostics(&mut entry, runtime_exposure);
            (!entry.bindings.is_empty()).then_some(entry)
        })
        .collect();
    exports
}

fn expose_binding_diagnostics(entry: &mut CapabilityExport, runtime_exposure: RuntimeExposureMode) {
    let binding_diagnostics = std::mem::take(&mut entry.binding_diagnostics);
    entry
        .diagnostics
        .extend(binding_diagnostics.into_iter().filter_map(|diagnostic| {
            diagnostic
                .applies_to
                .iter()
                .any(|kind| runtime_exposure.exposes_kind(*kind))
                .then_some(diagnostic.diagnostic)
        }));
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoverySearchFilter {
    pub(crate) source_key: Option<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) kind: Option<ExportKind>,
    pub(crate) allowed_kinds: Vec<ExportKind>,
    pub(crate) capability_kind: Option<CapabilityKind>,
    pub(crate) effect: Option<EffectKind>,
}

impl DiscoverySearchFilter {
    fn has_source_selector(&self) -> bool {
        self.source_key.is_some() || self.display_name.is_some()
    }

    fn matches_installed_source(&self, source: &InstalledSource) -> bool {
        self.source_key
            .as_ref()
            .is_none_or(|source_key| source_key == &source.source_key)
            && self
                .display_name
                .as_ref()
                .is_none_or(|display_name| display_name == &source.display_name)
    }

    fn into_search_filter(self) -> SearchFilter {
        SearchFilter {
            source_key: self.source_key,
            display_name: self.display_name,
            kind: self.kind,
            allowed_kinds: self.allowed_kinds,
            capability_kind: self.capability_kind,
            effect: self.effect,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DiscoveryPagination {
    pub(crate) limit: usize,
    pub(crate) offset: usize,
}

impl DiscoveryPagination {
    pub(crate) fn new(limit: u32, offset: u32) -> Self {
        let limit = if limit == 0 {
            DEFAULT_SEARCH_LIMIT
        } else {
            usize::try_from(limit).unwrap_or(MAX_SEARCH_LIMIT)
        }
        .clamp(1, MAX_SEARCH_LIMIT);
        let offset = usize::try_from(offset).unwrap_or(usize::MAX);
        Self { limit, offset }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DiscoverySearchPage {
    pub(crate) items: Vec<SearchResult>,
    pub(crate) total: usize,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) has_more: bool,
    pub(crate) next_offset: Option<usize>,
    pub(crate) diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedWorkspaceExports {
    pub(crate) exports: WorkspaceExports,
    pub(crate) capability_by_id: BTreeMap<coral_capabilities::CapabilityId, Capability>,
    pub(crate) source_materialized_dir_by_id: BTreeMap<SourceId, PathBuf>,
    pub(crate) source_runtime_by_id: BTreeMap<SourceId, LoadedSourceRuntime>,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedSourceRuntime {
    pub(crate) name: SourceName,
    pub(crate) credential_storage: CredentialStorageKind,
    pub(crate) variables: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub(crate) enum DiscoveryDescribeResult {
    Found(Box<DiscoveryDescription>),
    Ambiguous(Vec<CapabilityExport>),
    NotFound { diagnostics: Vec<Diagnostic> },
}

#[derive(Debug, Clone)]
pub(crate) struct DiscoveryDescription {
    pub(crate) entry: CapabilityExport,
    pub(crate) capability: Option<Capability>,
}

pub(crate) fn binding_refs(entry: &CapabilityExport) -> Vec<String> {
    entry
        .bindings
        .iter()
        .map(|binding| binding.ref_().value.clone())
        .collect()
}

pub(crate) fn binding_kinds(entry: &CapabilityExport) -> Vec<ExportKind> {
    entry
        .bindings
        .iter()
        .map(|binding| binding.ref_().kind)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(crate) fn binding_alias(binding: &Binding) -> String {
    binding.alias()
}

fn read_yaml_artifact<T: DeserializeOwned>(
    path: &Path,
    source_name: &str,
    artifact_label: &str,
) -> Result<T, AppError> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            AppError::FailedPrecondition(format!(
                "source '{source_name}' is missing generated {artifact_label} at '{}'. Re-add the source to regenerate capability artifacts.",
                path.display()
            ))
        } else {
            AppError::Io(error)
        }
    })?;
    serde_yaml::from_str(&raw).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use coral_capabilities::{
        Capability, CapabilityId, CapabilityKind, Diagnostic, DiagnosticSeverity, DiagnosticStage,
        EffectKind, EffectProfile, GraphqlOperationBinding, GraphqlOperationKind, HttpMethod,
        IdempotencyKind, InvocationSchema, OutputContract, ProviderOrigin, ProviderOriginKind,
        RestUpstreamBinding, SOURCE_CAPABILITY_GENERATOR_VERSION, ShapeHints, SourceCapabilitySet,
        SourceId, SupportStatus, UpstreamBinding,
    };
    use coral_exports::{
        Binding, BindingBuildContext, BindingDiagnostic, CapabilityExport, EffectProfileSnapshot,
        ExportKind, ExportRef, SOURCE_EXPORTS_GENERATOR_VERSION, SourceExports, SourceKey,
        SqlBinding, SqlBindingKind, SqlProjectionV1, SqlRowShape, TypescriptBinding,
        TypescriptBindingContributor, WorkspaceExports, build_source_exports,
    };
    use serde_json::json;

    use super::{DiscoveryDescribeResult, DiscoveryManager, filter_runtime_exposure};
    use crate::RuntimeExposureMode;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    fn workspace_with_dual_binding_entry() -> WorkspaceExports {
        WorkspaceExports {
            artifact_schema_version: 1,
            workspace_id: "default".to_string(),
            sources: Vec::new(),
            entries: vec![CapabilityExport {
                capability_id: CapabilityId(
                    "source/src_demo/interface/rest/operation/list".to_string(),
                ),
                source_id: SourceId("src_demo".to_string()),
                display_name: "Demo".to_string(),
                source_key: SourceKey("demo".to_string()),
                interface_id: "rest".to_string(),
                operation_id: "list".to_string(),
                title: "List demo rows".to_string(),
                description: "List demo rows".to_string(),
                deprecated: false,
                support_status: SupportStatus::Generated,
                bindings: vec![
                    Binding::Typescript(TypescriptBinding {
                        ref_: ExportRef::typescript(&[
                            "demo".to_string(),
                            "rest".to_string(),
                            "list".to_string(),
                        ]),
                        path: vec!["demo".to_string(), "rest".to_string(), "list".to_string()],
                        args_type_name: "ListArgs".to_string(),
                        result_type_name: "ListResult".to_string(),
                    }),
                    Binding::Sql(SqlBinding {
                        kind: SqlBindingKind::Table,
                        ref_: ExportRef::sql_table("demo.rows"),
                        sql_reference: "demo.rows".to_string(),
                        projection: SqlProjectionV1 {
                            row_shape: SqlRowShape::Collection,
                            columns: Vec::new(),
                            inputs: Vec::new(),
                            response_selection: None,
                            pagination: None,
                            file_scan: None,
                            diagnostics: Vec::new(),
                        },
                    }),
                ],
                search_text: Vec::new(),
                effect_profile: EffectProfileSnapshot {
                    capability_kind: CapabilityKind::Query,
                    effects: vec![EffectKind::Read],
                    idempotency: IdempotencyKind::Idempotent,
                },
                diagnostics: Vec::new(),
                binding_diagnostics: Vec::new(),
            }],
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn runtime_exposure_filters_hidden_bindings_before_discovery() {
        let workspace = workspace_with_dual_binding_entry();

        let typescript_only =
            filter_runtime_exposure(workspace.clone(), RuntimeExposureMode::TypeScript);
        assert_eq!(typescript_only.entries.len(), 1);
        let entry = typescript_only.entries.first().expect("typescript entry");
        assert!(matches!(
            entry.bindings.as_slice(),
            [Binding::Typescript(_)]
        ));

        let sql_only = filter_runtime_exposure(workspace, RuntimeExposureMode::Sql);
        assert_eq!(sql_only.entries.len(), 1);
        let entry = sql_only.entries.first().expect("sql entry");
        assert!(matches!(entry.bindings.as_slice(), [Binding::Sql(_)]));
    }

    #[test]
    fn runtime_exposure_filters_binding_diagnostics_before_discovery() {
        let mut workspace = workspace_with_dual_binding_entry();
        let entry = workspace.entries.first_mut().expect("entry");
        entry.diagnostics.push(test_diagnostic("GLOBAL_SOURCE"));
        entry.binding_diagnostics = vec![
            BindingDiagnostic::new(
                vec![ExportKind::SqlTable, ExportKind::SqlFunction],
                test_diagnostic("SQL_ONLY"),
            ),
            BindingDiagnostic::new(
                vec![ExportKind::Typescript],
                test_diagnostic("TYPESCRIPT_ONLY"),
            ),
        ];

        let typescript_only =
            filter_runtime_exposure(workspace.clone(), RuntimeExposureMode::TypeScript);
        let typescript_codes = diagnostic_codes(&typescript_only);
        assert_eq!(typescript_codes, vec!["GLOBAL_SOURCE", "TYPESCRIPT_ONLY"]);
        assert!(
            typescript_only
                .entries
                .first()
                .expect("entry")
                .binding_diagnostics
                .is_empty()
        );

        let sql_only = filter_runtime_exposure(workspace.clone(), RuntimeExposureMode::Sql);
        let sql_codes = diagnostic_codes(&sql_only);
        assert_eq!(sql_codes, vec!["GLOBAL_SOURCE", "SQL_ONLY"]);

        let both = filter_runtime_exposure(workspace, RuntimeExposureMode::Both);
        let both_codes = diagnostic_codes(&both);
        assert_eq!(
            both_codes,
            vec!["GLOBAL_SOURCE", "SQL_ONLY", "TYPESCRIPT_ONLY"]
        );
    }

    fn test_diagnostic(code: &str) -> Diagnostic {
        Diagnostic::new(
            code,
            DiagnosticSeverity::Info,
            DiagnosticStage::ExportGeneration,
            code,
        )
    }

    fn diagnostic_codes(exports: &WorkspaceExports) -> Vec<&str> {
        exports
            .entries
            .first()
            .expect("entry")
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect()
    }

    #[test]
    fn load_workspace_exports_reports_stale_installed_sources() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let stale_source = installed_source("codex");
        config_store
            .upsert_source(&workspace_name, stale_source.clone())
            .expect("stale source config");

        let manager = DiscoveryManager::new(config_store, layout);
        let error = manager
            .load_workspace_exports(&workspace_name)
            .expect_err("strict workspace load should report stale artifacts");

        assert!(
            error.to_string().contains("source 'codex' is missing"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unfiltered_search_skips_stale_unrelated_sources() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let valid_source = installed_source("github");
        let stale_source = installed_source("codex");
        config_store
            .upsert_source(&workspace_name, valid_source.clone())
            .expect("valid source config");
        config_store
            .upsert_source(&workspace_name, stale_source.clone())
            .expect("stale source config");
        write_source_artifacts(
            &layout.source_materialized_dir(&workspace_name, &valid_source.name),
            &valid_source,
        );

        let manager = DiscoveryManager::new(config_store, layout);
        let loaded = manager
            .load_workspace_exports_best_effort(&workspace_name)
            .expect("best-effort workspace load should skip stale source");
        assert!(
            loaded
                .exports
                .diagnostics
                .iter()
                .any(
                    |diagnostic| diagnostic.code == "SOURCE_ARTIFACTS_UNAVAILABLE"
                        && diagnostic.source_ref.as_deref() == Some("codex")
                ),
            "expected stale source diagnostic: {:#?}",
            loaded.exports.diagnostics
        );

        let page = manager
            .search(
                &workspace_name,
                "pull request review",
                &super::DiscoverySearchFilter::default(),
                super::DiscoveryPagination::new(10, 0),
            )
            .expect("unfiltered search should skip stale unrelated source");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items.first().expect("one item").source_key, "github");
        assert!(
            page.diagnostics.iter().any(|diagnostic| diagnostic.code
                == "SOURCE_ARTIFACTS_UNAVAILABLE"
                && diagnostic.source_ref.as_deref() == Some("codex")),
            "expected stale source diagnostic: {:#?}",
            page.diagnostics
        );
    }

    #[test]
    fn describe_skips_stale_unrelated_sources() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let valid_source = installed_source("github");
        let stale_source = installed_source("codex");
        config_store
            .upsert_source(&workspace_name, valid_source.clone())
            .expect("valid source config");
        config_store
            .upsert_source(&workspace_name, stale_source)
            .expect("stale source config");
        write_source_artifacts(
            &layout.source_materialized_dir(&workspace_name, &valid_source.name),
            &valid_source,
        );

        let manager = DiscoveryManager::new(config_store, layout);
        let description = manager
            .describe(
                &workspace_name,
                "tools.github.rest.search.issuesAndPullRequests",
            )
            .expect("describe should skip stale unrelated source");

        let DiscoveryDescribeResult::Found(description) = description else {
            panic!("expected found description");
        };
        assert_eq!(description.entry.source_key.as_str(), "github");
    }

    #[test]
    fn describe_reports_stale_target_source() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let stale_source = installed_source("codex");
        config_store
            .upsert_source(&workspace_name, stale_source)
            .expect("stale source config");

        let manager = DiscoveryManager::new(config_store, layout);
        let error = manager
            .describe(
                &workspace_name,
                "source/src_codex/interface/rest/operation/searchIssuesAndPullRequests",
            )
            .expect_err("target stale source should be explicit");

        assert!(
            error.to_string().contains("source 'codex' is missing"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn describe_not_found_reports_stale_workspace_sources() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let stale_source = installed_source("github");
        config_store
            .upsert_source(&workspace_name, stale_source)
            .expect("stale source config");

        let manager = DiscoveryManager::new(config_store, layout);
        let result = manager
            .describe(
                &workspace_name,
                "tools.github.rest.search.issuesAndPullRequests",
            )
            .expect("untyped stale describe should return not found diagnostics");

        let DiscoveryDescribeResult::NotFound { diagnostics } = result else {
            panic!("expected not found with diagnostics");
        };
        assert!(
            diagnostics.iter().any(
                |diagnostic| diagnostic.code == "SOURCE_ARTIFACTS_UNAVAILABLE"
                    && diagnostic.source_ref.as_deref() == Some("github")
            ),
            "expected stale source diagnostic: {diagnostics:#?}"
        );
    }

    #[test]
    fn source_filtered_search_reports_matching_stale_source() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let valid_source = installed_source("github");
        let stale_source = installed_source("codex");
        config_store
            .upsert_source(&workspace_name, valid_source.clone())
            .expect("valid source config");
        config_store
            .upsert_source(&workspace_name, stale_source)
            .expect("stale source config");
        write_source_artifacts(
            &layout.source_materialized_dir(&workspace_name, &valid_source.name),
            &valid_source,
        );

        let manager = DiscoveryManager::new(config_store, layout);
        let error = manager
            .search(
                &workspace_name,
                "pull request review",
                &super::DiscoverySearchFilter {
                    source_key: Some("codex".to_string()),
                    ..Default::default()
                },
                super::DiscoveryPagination::new(10, 0),
            )
            .expect_err("matching stale source should be explicit");

        assert!(
            error.to_string().contains("source 'codex' is missing"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn source_filtered_search_reports_missing_graphql_operation_documents() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source = installed_source("linear_graphql");
        config_store
            .upsert_source(&workspace_name, source.clone())
            .expect("source config");
        write_graphql_source_artifacts_without_documents(
            &layout.source_materialized_dir(&workspace_name, &source.name),
            &source,
        );

        let manager = DiscoveryManager::new(config_store, layout);
        let error = manager
            .search(
                &workspace_name,
                "issues",
                &super::DiscoverySearchFilter {
                    source_key: Some("linear_graphql".to_string()),
                    ..Default::default()
                },
                super::DiscoveryPagination::new(10, 0),
            )
            .expect_err("missing GraphQL operation documents should be explicit");
        let error = error.to_string();

        assert!(
            error.contains("missing 1 generated GraphQL operation document"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("source_key 'linear_graphql'"),
            "expected source key in error: {error}"
        );
        assert!(
            error.contains("query_issues"),
            "expected operation id in error: {error}"
        );
    }

    #[test]
    fn load_workspace_exports_reports_stale_source_export_generator_version() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source = installed_source("github");
        config_store
            .upsert_source(&workspace_name, source.clone())
            .expect("source config");
        let materialized_dir = layout.source_materialized_dir(&workspace_name, &source.name);
        write_source_artifacts(&materialized_dir, &source);
        rewrite_source_exports(&materialized_dir, |exports| {
            exports.generator_version = "stale-source-exports-generator".to_string();
        });
        remove_source_export_entry_fields(&materialized_dir, &["deprecated", "support_status"]);

        let manager = DiscoveryManager::new(config_store, layout);
        let error = manager
            .load_workspace_exports(&workspace_name)
            .expect_err("stale export generator should fail");

        let error = error.to_string();
        assert!(
            error.contains("requires 'source-exports-v12'"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("Running Coral binary:"),
            "expected binary path in error: {error}"
        );
        assert!(
            error.contains("Restart any MCP/client session"),
            "expected restart guidance in error: {error}"
        );
        assert_eq!(SOURCE_EXPORTS_GENERATOR_VERSION, "source-exports-v12");
    }

    #[test]
    fn load_workspace_exports_reports_stale_capability_generator_version() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source = installed_source("github");
        config_store
            .upsert_source(&workspace_name, source.clone())
            .expect("source config");
        let materialized_dir = layout.source_materialized_dir(&workspace_name, &source.name);
        write_source_artifacts(&materialized_dir, &source);
        rewrite_capabilities(&materialized_dir, |capabilities| {
            capabilities.generator_version = "stale-capability-generator".to_string();
        });
        remove_capability_display_fields(&materialized_dir, &["deprecated", "support_status"]);

        let manager = DiscoveryManager::new(config_store, layout);
        let error = manager
            .load_workspace_exports(&workspace_name)
            .expect_err("stale capability generator should fail");

        let error = error.to_string();
        assert!(
            error.contains("requires 'derive-capabilities-v13'"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("Running Coral binary:"),
            "expected binary path in error: {error}"
        );
        assert!(
            error.contains("Restart any MCP/client session"),
            "expected restart guidance in error: {error}"
        );
        assert_eq!(
            SOURCE_CAPABILITY_GENERATOR_VERSION,
            "derive-capabilities-v13"
        );
    }

    fn installed_source(name: &str) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            source_id: format!("src_{name}"),
            display_name: name.to_string(),
            source_key: name.to_string(),
            version: None,
            interface_ids: Vec::new(),
            variables: std::collections::BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        }
    }

    fn write_source_artifacts(materialized_dir: &std::path::Path, source: &InstalledSource) {
        let source_id = SourceId(source.source_id.clone());
        let capability = Capability::new(
            source_id.clone(),
            "rest",
            "searchIssuesAndPullRequests",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/searchIssuesAndPullRequests"
                        .to_string(),
                provider_name: "search/issues-and-pull-requests".to_string(),
                tags: vec!["Search".to_string()],
            },
            UpstreamBinding::Rest(RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/searchIssuesAndPullRequests"
                        .to_string(),
                method: HttpMethod::Get,
                path_template: "/search/issues".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        );
        let mut capability = capability;
        capability.effect_profile = EffectProfile::read();
        capability.display.title = "Search issues and pull requests".to_string();
        let capabilities = SourceCapabilitySet::new(source_id.clone(), vec![capability]);
        let exports = build_source_exports(
            &capabilities,
            &BindingBuildContext {
                source_id,
                display_name: source.display_name.clone(),
                source_key: SourceKey(source.source_key.clone()),
            },
            &[&TypescriptBindingContributor::new()],
        )
        .expect("source exports");
        std::fs::create_dir_all(materialized_dir.join("exports")).expect("exports dir");
        std::fs::write(
            materialized_dir.join("exports/source-exports.yaml"),
            serde_yaml::to_string(&exports).expect("exports yaml"),
        )
        .expect("write exports");
        std::fs::write(
            materialized_dir.join("capabilities.yaml"),
            serde_yaml::to_string(&capabilities).expect("capabilities yaml"),
        )
        .expect("write capabilities");
    }

    fn write_graphql_source_artifacts_without_documents(
        materialized_dir: &std::path::Path,
        source: &InstalledSource,
    ) {
        let source_id = SourceId(source.source_id.clone());
        let mut capability = Capability::new(
            source_id.clone(),
            "graph",
            "query_issues",
            ProviderOrigin {
                kind: ProviderOriginKind::GraphqlRootField,
                snapshot_ref: "interfaces/graph/provider-snapshot.yaml#/root_fields/query_issues"
                    .to_string(),
                provider_name: "issues".to_string(),
                tags: Vec::new(),
            },
            UpstreamBinding::Graphql(GraphqlOperationBinding {
                endpoint_ref: "source/src_linear_graphql/interface/graph/endpoint/default"
                    .to_string(),
                operation_name: "QueryIssues".to_string(),
                graphql_operation_kind: GraphqlOperationKind::Query,
                document_ref:
                    "source/src_linear_graphql/interface/graph/generated/query_issues.graphql"
                        .to_string(),
                selection_set: Some("nodes { id identifier title }".to_string()),
                variable_bindings: Vec::new(),
                response_path: vec!["issues".to_string()],
            }),
        );
        capability.effect_profile = EffectProfile::read();
        capability.display.title = "Query issues".to_string();
        capability.shape_hints = ShapeHints::root_list();
        capability.shape_hints.row_path_candidates = vec![vec!["issues".to_string()]];
        capability.output_contract = OutputContract::GraphqlData {
            schema: InvocationSchema::new(json!({
                "type": "object",
                "properties": {
                    "issues": {
                        "type": "object",
                        "properties": {
                            "nodes": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": { "type": "string" },
                                        "identifier": { "type": "string" },
                                        "title": { "type": "string" }
                                    }
                                }
                            }
                        }
                    }
                }
            })),
        };
        let capabilities = SourceCapabilitySet::new(source_id.clone(), vec![capability]);
        let exports = build_source_exports(
            &capabilities,
            &BindingBuildContext {
                source_id,
                display_name: source.display_name.clone(),
                source_key: SourceKey(source.source_key.clone()),
            },
            &[&TypescriptBindingContributor::new()],
        )
        .expect("source exports");
        std::fs::create_dir_all(materialized_dir.join("exports")).expect("exports dir");
        std::fs::write(
            materialized_dir.join("exports/source-exports.yaml"),
            serde_yaml::to_string(&exports).expect("exports yaml"),
        )
        .expect("write exports");
        std::fs::write(
            materialized_dir.join("capabilities.yaml"),
            serde_yaml::to_string(&capabilities).expect("capabilities yaml"),
        )
        .expect("write capabilities");
    }

    fn rewrite_source_exports(
        materialized_dir: &std::path::Path,
        rewrite: impl FnOnce(&mut SourceExports),
    ) {
        let path = materialized_dir.join("exports/source-exports.yaml");
        let mut exports: SourceExports =
            serde_yaml::from_str(&std::fs::read_to_string(&path).expect("read exports"))
                .expect("exports yaml");
        rewrite(&mut exports);
        std::fs::write(
            &path,
            serde_yaml::to_string(&exports).expect("exports yaml"),
        )
        .expect("write exports");
    }

    fn remove_source_export_entry_fields(materialized_dir: &std::path::Path, fields: &[&str]) {
        let path = materialized_dir.join("exports/source-exports.yaml");
        let mut exports: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(&path).expect("read exports"))
                .expect("exports yaml");
        let entries = exports
            .get_mut("entries")
            .and_then(serde_yaml::Value::as_sequence_mut)
            .expect("entries");
        for entry in entries {
            let entry = entry.as_mapping_mut().expect("entry mapping");
            for field in fields {
                let key = serde_yaml::Value::String((*field).to_string());
                entry.remove(&key);
            }
        }
        std::fs::write(
            &path,
            serde_yaml::to_string(&exports).expect("exports yaml"),
        )
        .expect("write exports");
    }

    fn remove_capability_display_fields(materialized_dir: &std::path::Path, fields: &[&str]) {
        let path = materialized_dir.join("capabilities.yaml");
        let mut capabilities: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(&path).expect("read capabilities"))
                .expect("capabilities yaml");
        let capabilities_sequence = capabilities
            .get_mut("capabilities")
            .and_then(serde_yaml::Value::as_sequence_mut)
            .expect("capabilities");
        for capability in capabilities_sequence {
            let display = capability
                .get_mut("display")
                .and_then(serde_yaml::Value::as_mapping_mut)
                .expect("display mapping");
            for field in fields {
                let key = serde_yaml::Value::String((*field).to_string());
                display.remove(&key);
            }
        }
        std::fs::write(
            &path,
            serde_yaml::to_string(&capabilities).expect("capabilities yaml"),
        )
        .expect("write capabilities");
    }

    fn rewrite_capabilities(
        materialized_dir: &std::path::Path,
        rewrite: impl FnOnce(&mut SourceCapabilitySet),
    ) {
        let path = materialized_dir.join("capabilities.yaml");
        let mut capabilities: SourceCapabilitySet =
            serde_yaml::from_str(&std::fs::read_to_string(&path).expect("read capabilities"))
                .expect("capabilities yaml");
        rewrite(&mut capabilities);
        std::fs::write(
            &path,
            serde_yaml::to_string(&capabilities).expect("capabilities yaml"),
        )
        .expect("write capabilities");
    }
}
