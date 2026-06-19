//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;

use coral_engine::{
    BoundRequestIdentityHttpAuthenticator, CatalogInfo, CoralQuery, CoreError, DescribeTableInfo,
    QueryExecution, QueryPlan, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    RequestIdentityHttpAuthenticatorError, RequestIdentityHttpAuthenticatorFactory,
    RequestIdentitySelectionContext, RequestIdentitySelectionError, RequestIdentitySelector,
    RuntimeIdentityRequirements, RuntimeSourceComponent, RuntimeSourcePackage,
    SelectedRequestIdentity, SourceValidationReport, StatusCode, TableInfo,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec, ValidatedSourceManifest};
use opentelemetry::trace::Status as OtelStatus;
use sqlparser::ast::{
    ObjectName as SqlObjectName, ObjectNamePart as SqlObjectNamePart, visit_relations_mut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId, CredentialsError};
use crate::episode::EpisodeId;
use crate::features::Features;
use crate::identity::{
    IdentityManager, IdentityOwnerKind, SourceIdentityBinding, SourceIdentityProvider,
    SourceIdentityResolutionRequest, SourceIdentitySelection, SourceIdentitySelectionRequest,
    UserPrincipal,
};
use crate::query::QueryContext;
use crate::query::extensions::{
    CredentialRefreshingInputResolver, EngineExtensionsProvider, engine_extensions_for_providers,
};
#[cfg(test)]
use crate::source_artifacts::LocalSourceArtifactStore;
use crate::source_artifacts::SourceArtifactStore;
use crate::source_registry::{SourceRegistry, installed_source_from_record};
use crate::sources::SourceName;
use crate::sources::catalog::resolve_installed_manifest;
use crate::sources::materialization::incompatible_materialization_error;
use crate::sources::model::InstalledSource;
use crate::sources::runtime_package::{
    runtime_components_for_v4_source, runtime_relation_namespace,
};
use crate::state::{AppConfig, AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[derive(Debug)]
pub(crate) enum QueryManagerError {
    App(AppError),
    Core(CoreError),
}

pub(crate) struct ValidatedSource {
    pub(crate) source: InstalledSource,
    pub(crate) report: SourceValidationReport,
}

type SourceIdentityBindingsSnapshot = BTreeMap<String, BTreeMap<String, SourceIdentityBinding>>;

#[derive(Debug)]
struct LoadedQuerySource {
    query_source: QuerySource,
    version: Option<String>,
    identity_bindings: BTreeMap<String, SourceIdentityBinding>,
}

#[derive(Default)]
pub(crate) struct QueryManagerOptions {
    pub(crate) features: Features,
    pub(crate) source_identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
}

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    source_registry: Arc<dyn SourceRegistry>,
    credential_manager: CredentialManager,
    runtime_context: QueryRuntimeContext,
    #[cfg(test)]
    layout: AppStateLayout,
    artifact_store: Arc<dyn SourceArtifactStore>,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    features: Features,
    identity_manager: IdentityManager,
}

impl QueryManager {
    #[expect(
        clippy::too_many_arguments,
        reason = "one-time wiring constructor; every argument is a distinct runtime dependency"
    )]
    pub(crate) fn new(
        config_store: ConfigStore,
        source_registry: Arc<dyn SourceRegistry>,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        #[cfg(test)] layout: AppStateLayout,
        #[cfg(not(test))] _layout: AppStateLayout,
        artifact_store: Arc<dyn SourceArtifactStore>,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        options: QueryManagerOptions,
    ) -> Self {
        Self {
            config_store,
            source_registry,
            credential_manager,
            runtime_context,
            #[cfg(test)]
            layout,
            artifact_store,
            engine_extensions_providers,
            features: options.features,
            identity_manager: IdentityManager::new(options.source_identity_providers),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self::new_for_tests_with_options(
            config_store,
            credential_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
            QueryManagerOptions::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests_with_options(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        options: QueryManagerOptions,
    ) -> Self {
        let source_registry = Arc::new(config_store.clone());
        let artifact_store = Arc::new(LocalSourceArtifactStore::new(layout.clone()));
        Self::new(
            config_store,
            source_registry,
            credential_manager,
            runtime_context,
            layout,
            artifact_store,
            engine_extensions_providers,
            options,
        )
    }

    fn list_registry_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        self.source_registry
            .list_workspace_sources(workspace_name.as_str())?
            .into_iter()
            .map(|record| installed_source_from_record(workspace_name, record))
            .collect()
    }

    fn require_registry_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        self.source_registry
            .get_source(workspace_name.as_str(), source_name.as_str())?
            .map(|record| installed_source_from_record(workspace_name, record))
            .transpose()?
            .ok_or_else(|| AppError::SourceNotFound(format!("{workspace_name}:{source_name}")))
    }

    fn load_query_runtime(
        &self,
        context: &QueryContext,
    ) -> Result<(Vec<QuerySource>, QueryRuntimeConfig), QueryManagerError> {
        let workspace_name = context.workspace_name();
        let config = self
            .config_store
            .load_config()
            .map_err(QueryManagerError::App)?;
        let loaded_sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let identity_bindings = identity_binding_snapshot_for_sources(&loaded_sources);
        let sources = query_sources_from_loaded(loaded_sources);
        let runtime = self
            .runtime_config(
                workspace_name,
                context.principal(),
                &sources,
                identity_bindings,
                &config,
            )
            .map_err(QueryManagerError::App)?;
        Ok((sources, runtime))
    }

    pub(crate) async fn list_tables(
        &self,
        context: &QueryContext,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let workspace_name = context.workspace_name();
        let trace_sql = list_tables_trace_sql(schema_filter, table_filter);
        run_query_operation(
            QueryOperation::ListTables,
            workspace_name,
            &trace_sql,
            context.episode_id(),
            async {
                let (sources, runtime) = self.load_query_runtime(context)?;
                CoralQuery::list_tables(&sources, runtime, schema_filter, table_filter)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |tables| Some(u64::try_from(tables.len()).unwrap_or(u64::MAX)),
        )
        .await
    }

    pub(crate) async fn list_catalog(
        &self,
        context: &QueryContext,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let workspace_name = context.workspace_name();
        let trace_sql = list_catalog_trace_sql(schema_filter);
        run_query_operation(
            QueryOperation::ListCatalog,
            workspace_name,
            &trace_sql,
            context.episode_id(),
            async {
                let (sources, runtime) = self.load_query_runtime(context)?;
                CoralQuery::list_catalog(&sources, runtime, schema_filter)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |catalog| {
                Some(
                    u64::try_from(
                        catalog
                            .tables
                            .len()
                            .saturating_add(catalog.table_functions.len()),
                    )
                    .unwrap_or(u64::MAX),
                )
            },
        )
        .await
    }

    pub(crate) async fn describe_table(
        &self,
        context: &QueryContext,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableInfo, QueryManagerError> {
        let workspace_name = context.workspace_name();
        let trace_sql = describe_table_trace_sql(schema_name, table_name);
        run_query_operation(
            QueryOperation::DescribeTable,
            workspace_name,
            &trace_sql,
            context.episode_id(),
            async {
                let (sources, runtime) = self.load_query_runtime(context)?;
                CoralQuery::describe_table(&sources, runtime, schema_name, table_name)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |_| None,
        )
        .await
    }

    pub(crate) async fn execute_sql(
        &self,
        context: &QueryContext,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        let workspace_name = context.workspace_name();
        run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            context.episode_id(),
            async {
                let (sources, runtime) = self.load_query_runtime(context)?;
                CoralQuery::execute_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |execution| Some(u64::try_from(execution.row_count()).unwrap_or(u64::MAX)),
        )
        .await
    }

    pub(crate) async fn explain_sql(
        &self,
        context: &QueryContext,
        sql: &str,
    ) -> Result<QueryPlan, QueryManagerError> {
        let workspace_name = context.workspace_name();
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            context.episode_id(),
            async {
                let (sources, runtime) = self.load_query_runtime(context)?;
                CoralQuery::explain_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |_| None,
        )
        .await
    }

    pub(crate) async fn validate_source(
        &self,
        context: &QueryContext,
        source_name: &SourceName,
    ) -> Result<ValidatedSource, QueryManagerError> {
        let workspace_name = context.workspace_name();
        let config = self
            .config_store
            .load_config()
            .map_err(QueryManagerError::App)?;
        let source = self
            .require_registry_source(workspace_name, source_name)
            .map_err(QueryManagerError::App)?;
        let loaded_source = self
            .load_query_source(workspace_name, &source)
            .map_err(QueryManagerError::App)?;
        let validation_test_queries = self
            .source_validation_test_queries(workspace_name, &source)
            .map_err(QueryManagerError::App)?;
        self.validate_source_identity_bindings(workspace_name, context.principal(), &loaded_source)
            .await
            .map_err(QueryManagerError::App)?;
        let identity_bindings =
            identity_binding_snapshot_for_sources(std::slice::from_ref(&loaded_source));
        let runtime = self
            .runtime_config(
                workspace_name,
                context.principal(),
                std::slice::from_ref(&loaded_source.query_source),
                identity_bindings,
                &config,
            )
            .map_err(QueryManagerError::App)?;
        let report = CoralQuery::validate_source(
            &loaded_source.query_source,
            runtime,
            &validation_test_queries,
        )
        .await
        .map_err(QueryManagerError::Core)?;
        let mut source = source;
        source.version = loaded_source.version;

        Ok(ValidatedSource { source, report })
    }

    fn load_query_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<LoadedQuerySource>, AppError> {
        let span = tracing::info_span!(
            "coral.app.query_sources.load",
            workspace = %workspace_name,
            source.count = tracing::field::Empty,
        );
        let _guard = span.enter();
        let mut query_sources = Vec::new();
        for source in self.list_registry_sources(workspace_name)? {
            match self.load_query_source(workspace_name, &source) {
                Ok(query_source) => query_sources.push(query_source),
                Err(
                    error @ (AppError::Credentials(CredentialsError::Unavailable(_))
                    | AppError::MissingOrIncompatibleV4Materialization { .. }
                    | AppError::SourceUnservable(_)),
                ) => {
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(
                        source = %source.name,
                        detail = %error,
                        "skipping source during query-source load"
                    );
                }
            }
        }
        span.record("source.count", query_sources.len());
        Ok(query_sources)
    }

    fn load_query_source(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<LoadedQuerySource, AppError> {
        let installed =
            resolve_installed_manifest(workspace_name, source, self.artifact_store.as_ref())?;
        let source_spec = installed.source_spec;
        let v4_runtime_components = if let Some(v4) = source_spec.as_v4() {
            self.features.ensure_dsl_v4_enabled()?;
            let materialized = self.artifact_store.load_v4_materialization(
                workspace_name.as_str(),
                source.name.as_str(),
                &installed.manifest_yaml,
                v4,
            )?;
            Some(
                runtime_components_for_v4_source(
                    v4,
                    &materialized,
                    source.source_spec_id(),
                    source.name.as_str(),
                )
                .map_err(|error| {
                    incompatible_materialization_error(
                        &source.name,
                        format!("failed to assemble runtime package: {error}"),
                    )
                })?,
            )
        } else {
            if source.name.as_str() != source_spec.schema_name() {
                return Err(AppError::FailedPrecondition(format!(
                    "installed source alias '{}' for source spec '{}' requires DSL v4",
                    source.name,
                    source_spec.schema_name()
                )));
            }
            None
        };
        validate_required_variables(source, source_spec.declared_inputs())?;
        let stored_secrets =
            if let Some(credential_storage) = source.credential_storage_for_material() {
                let credential_set_id = CredentialSetId::for_source(&source.name);
                self.credential_manager.read_material(
                    workspace_name,
                    &credential_set_id,
                    credential_storage,
                )?
            } else {
                BTreeMap::new()
            };
        let mut resolved_secrets = BTreeMap::new();
        let missing_secrets: Vec<String> = source_spec
            .required_secret_names()
            .into_iter()
            .filter(|name| !stored_secrets.contains_key(name))
            .collect();
        if let Some((first, rest)) = missing_secrets.split_first() {
            let detail = if rest.is_empty() {
                format!("secret '{first}'")
            } else {
                format!("secret '{first}' and {} other(s)", rest.len())
            };
            return Err(AppError::FailedPrecondition(format!(
                "source '{}' is missing {detail}",
                source.name
            )));
        }
        for secret_name in source_spec.declared_secret_names() {
            if let Some(value) = stored_secrets.get(&secret_name) {
                resolved_secrets.insert(secret_name, value.clone());
            }
        }
        let query_source = if let Some(components) = v4_runtime_components {
            QuerySource::from_runtime_components(
                RuntimeSourcePackage {
                    source_name: source.name.as_str().to_string(),
                    authored_version: source_spec.source_version().map(ToString::to_string),
                    description: source_spec.description().to_string(),
                    declared_inputs: source_spec.declared_inputs().to_vec(),
                    test_queries: source_spec.test_queries().to_vec(),
                    components,
                },
                source.variables.clone(),
                resolved_secrets,
            )
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?
        } else {
            QuerySource::from_manifest(&source_spec, source.variables.clone(), resolved_secrets)
        };
        Ok(LoadedQuerySource {
            query_source,
            version: installed.candidate.version,
            identity_bindings: source.identity_bindings.clone(),
        })
    }

    fn source_validation_test_queries(
        &self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<Vec<String>, AppError> {
        let installed =
            resolve_installed_manifest(workspace_name, source, self.artifact_store.as_ref())?;
        validation_test_queries(&installed.source_spec, source)
    }

    fn request_identity_selector_and_factory(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        selected_sources: &[QuerySource],
        identity_bindings: SourceIdentityBindingsSnapshot,
    ) -> (
        Option<Arc<dyn RequestIdentitySelector>>,
        Option<RequestIdentityHttpAuthenticatorFactory>,
    ) {
        if !selected_sources
            .iter()
            .any(|source| identity_requirements_for_source(source).next().is_some())
        {
            return (None, None);
        }
        let selector = Arc::new(LazyRuntimeIdentitySelector {
            workspace_name: workspace_name.clone(),
            request_principal: request_principal.clone(),
            source_identity_bindings: Arc::new(identity_bindings),
            identity_manager: self.identity_manager.clone(),
            selected_identities: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        });
        let authenticator_selector = Arc::clone(&selector);
        let factory: RequestIdentityHttpAuthenticatorFactory =
            Arc::new(move |selected| authenticator_selector.bound_authenticator_for(selected));
        (Some(selector), Some(factory))
    }

    async fn validate_source_identity_bindings(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        loaded_source: &LoadedQuerySource,
    ) -> Result<(), AppError> {
        let mut source_identity_bindings = BTreeMap::new();
        source_identity_bindings.insert(
            loaded_source.query_source.source_name().to_string(),
            loaded_source.identity_bindings.clone(),
        );
        let resolver = LazyRuntimeIdentitySelector {
            workspace_name: workspace_name.clone(),
            request_principal: request_principal.clone(),
            source_identity_bindings: Arc::new(source_identity_bindings),
            identity_manager: self.identity_manager.clone(),
            selected_identities: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        };
        for requirements in identity_requirements_for_source(&loaded_source.query_source) {
            let context = RequestIdentitySelectionContext::new(
                loaded_source.query_source.source_name().to_string(),
                requirements.surface_id,
                requirements.requirements,
            );
            resolver
                .select_identity(&context)
                .await
                .map_err(identity_selection_error_to_app_error)?;
        }
        Ok(())
    }

    fn runtime_config(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        selected_sources: &[QuerySource],
        identity_bindings: SourceIdentityBindingsSnapshot,
        config: &AppConfig,
    ) -> Result<QueryRuntimeConfig, AppError> {
        let mut extensions =
            engine_extensions_for_providers(&self.engine_extensions_providers, selected_sources);
        let provider_input_resolver = extensions.source_input_resolver.take();
        extensions.source_input_resolver = Some(Arc::new(CredentialRefreshingInputResolver::new(
            workspace_name.clone(),
            Arc::clone(&self.source_registry),
            self.credential_manager.clone(),
            provider_input_resolver,
        )));
        let (request_identity_selector, request_identity_http_authenticator_factory) = self
            .request_identity_selector_and_factory(
                workspace_name,
                request_principal,
                selected_sources,
                identity_bindings,
            );
        let mut runtime_context = self.runtime_context.clone();
        runtime_context.trace_context = Some(tracing::Span::current().context());
        let mut runtime = QueryRuntimeConfig::new(runtime_context, extensions)
            .with_request_identity_selector(request_identity_selector)
            .with_request_identity_http_authenticator_factory(
                request_identity_http_authenticator_factory,
            );
        let selected_source_names = selected_sources
            .iter()
            .map(|source| source.source_name().to_string())
            .collect::<Vec<_>>();
        runtime.memory = config.memory_config()?;
        runtime.dependent_join = config.dependent_join_config(&selected_source_names)?;
        Ok(runtime)
    }
}

fn query_sources_from_loaded(loaded_sources: Vec<LoadedQuerySource>) -> Vec<QuerySource> {
    loaded_sources
        .into_iter()
        .map(|loaded| loaded.query_source)
        .collect()
}

fn identity_binding_snapshot_for_sources(
    loaded_sources: &[LoadedQuerySource],
) -> SourceIdentityBindingsSnapshot {
    loaded_sources
        .iter()
        .map(|loaded| {
            (
                loaded.query_source.source_name().to_string(),
                loaded.identity_bindings.clone(),
            )
        })
        .collect()
}

fn validation_test_queries(
    source_spec: &ValidatedSourceManifest,
    source: &InstalledSource,
) -> Result<Vec<String>, AppError> {
    let Some(v4) = source_spec.as_v4() else {
        return Ok(source_spec.test_queries().to_vec());
    };
    if source.source_spec_id() == source.name.as_str() {
        return Ok(source_spec.test_queries().to_vec());
    }
    let namespace_map = v4
        .surfaces
        .iter()
        .map(|surface| {
            (
                surface.relation_namespace.clone(),
                runtime_relation_namespace(
                    &surface.relation_namespace,
                    source.source_spec_id(),
                    source.name.as_str(),
                ),
            )
        })
        .filter(|(authored, runtime)| authored != runtime)
        .collect::<BTreeMap<_, _>>();
    source_spec
        .test_queries()
        .iter()
        .map(|sql| rewrite_query_namespaces(sql, &namespace_map))
        .collect()
}

fn rewrite_query_namespaces(
    sql: &str,
    namespace_map: &BTreeMap<String, String>,
) -> Result<String, AppError> {
    if namespace_map.is_empty() {
        return Ok(sql.to_string());
    }
    let dialect = GenericDialect;
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|error| {
        AppError::InvalidInput(format!("source test query is invalid: {error}"))
    })?;
    match visit_relations_mut(&mut statements, |relation| {
        rewrite_relation_namespace(relation, namespace_map);
        ControlFlow::<()>::Continue(())
    }) {
        ControlFlow::Continue(()) => {}
        ControlFlow::Break(()) => unreachable!("query namespace rewrite never breaks traversal"),
    }
    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

fn rewrite_relation_namespace(
    relation: &mut SqlObjectName,
    namespace_map: &BTreeMap<String, String>,
) {
    if relation.0.len() < 2 {
        return;
    }
    let namespace_index = relation.0.len() - 2;
    let Some(SqlObjectNamePart::Identifier(namespace)) = relation.0.get_mut(namespace_index) else {
        return;
    };
    let Some(runtime_namespace) = namespace_map.get(&namespace.value) else {
        return;
    };
    namespace.value.clone_from(runtime_namespace);
}

#[derive(Clone)]
struct LazyRuntimeIdentitySelector {
    workspace_name: WorkspaceName,
    request_principal: UserPrincipal,
    source_identity_bindings: Arc<SourceIdentityBindingsSnapshot>,
    identity_manager: IdentityManager,
    selected_identities:
        Arc<std::sync::Mutex<BTreeMap<String, Arc<dyn crate::identity::RuntimeSourceIdentity>>>>,
}

impl fmt::Debug for LazyRuntimeIdentitySelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyRuntimeIdentitySelector")
            .field("workspace_name", &self.workspace_name)
            .field("source_identity_bindings", &self.source_identity_bindings)
            .field("identity_manager", &self.identity_manager)
            .finish_non_exhaustive()
    }
}

impl LazyRuntimeIdentitySelector {
    async fn resolve_runtime_identity(
        &self,
        identity: &RequestIdentitySelectionContext,
    ) -> Result<
        (
            SourceIdentitySelection,
            Arc<dyn crate::identity::RuntimeSourceIdentity>,
        ),
        AppError,
    > {
        SourceName::parse(identity.source_name())
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let binding = identity_binding_for_surface(
            &self.source_identity_bindings,
            identity.source_name(),
            identity.surface_id(),
        )?;
        let selection = self
            .identity_manager
            .resolve_source_identity_selection(SourceIdentitySelectionRequest {
                workspace_name: self.workspace_name.as_str().to_string(),
                user_id: self.request_principal.user_id().to_string(),
                source_name: identity.source_name().to_string(),
                surface_id: identity.surface_id().to_string(),
                binding: binding.clone(),
            })
            .await?;
        let user_id = (binding.owner == IdentityOwnerKind::User)
            .then(|| self.request_principal.user_id().to_string());
        let runtime_identity = self
            .identity_manager
            .resolve_source_identity(SourceIdentityResolutionRequest {
                workspace_name: self.workspace_name.as_str().to_string(),
                user_id,
                source_name: identity.source_name().to_string(),
                surface_id: identity.surface_id().to_string(),
                binding,
                selection: selection.clone(),
                identity_requirements: identity.identity_requirements().clone(),
            })
            .await?;
        if !identity.accepts_identity(
            runtime_identity.identity_spec_id(),
            runtime_identity.audience(),
        ) {
            return Err(AppError::FailedPrecondition(format!(
                "resolved identity does not satisfy identity_requirements for source '{}' surface '{}'",
                identity.source_name(),
                identity.surface_id()
            )));
        }
        Ok((selection, runtime_identity))
    }

    fn bound_authenticator_for(
        &self,
        selected: SelectedRequestIdentity,
    ) -> Result<BoundRequestIdentityHttpAuthenticator, RequestIdentityHttpAuthenticatorError> {
        let runtime_identity = self
            .selected_identities
            .lock()
            .map_err(|_error| {
                RequestIdentityHttpAuthenticatorError::failed_precondition(
                    "selected identity cache lock was poisoned",
                )
            })?
            .get(selected.identity_id())
            .cloned()
            .ok_or_else(|| {
                RequestIdentityHttpAuthenticatorError::failed_precondition(format!(
                    "selected identity '{}' was not resolved during identity selection",
                    selected.identity_id()
                ))
            })?;
        let bound: BoundRequestIdentityHttpAuthenticator = Arc::new(
            move |request: &reqwest::Request, resolved_inputs: &BTreeMap<String, String>| {
                let runtime_identity = Arc::clone(&runtime_identity);
                let selected = selected.clone();
                Box::pin(async move {
                    runtime_identity
                        .resolve_headers(&selected, request, resolved_inputs)
                        .await
                })
            },
        );
        Ok(bound)
    }
}

#[tonic::async_trait]
impl RequestIdentitySelector for LazyRuntimeIdentitySelector {
    async fn select_identity(
        &self,
        identity: &RequestIdentitySelectionContext,
    ) -> Result<SelectedRequestIdentity, RequestIdentitySelectionError> {
        let (selection, runtime_identity) = self
            .resolve_runtime_identity(identity)
            .await
            .map_err(|error| app_error_to_identity_selection_error(&error))?;
        let selected = SelectedRequestIdentity::new(
            selection.identity,
            runtime_identity.identity_spec_id().to_string(),
            runtime_identity.audience().clone(),
        );
        self.selected_identities
            .lock()
            .map_err(|_error| {
                RequestIdentitySelectionError::failed_precondition(
                    "selected identity cache lock was poisoned",
                )
            })?
            .insert(
                selected.identity_id().to_string(),
                Arc::clone(&runtime_identity),
            );
        Ok(selected)
    }
}

fn identity_requirements_for_source(
    source: &QuerySource,
) -> impl Iterator<Item = RuntimeIdentityRequirements> + '_ {
    source
        .components()
        .iter()
        .filter_map(|component| match component {
            RuntimeSourceComponent::Http(component) => component.identity_requirements.clone(),
            RuntimeSourceComponent::File(_) | RuntimeSourceComponent::Mcp(_) => None,
        })
}

fn identity_binding_for_surface(
    source_identity_bindings: &SourceIdentityBindingsSnapshot,
    source_name: &str,
    surface_id: &str,
) -> Result<SourceIdentityBinding, AppError> {
    source_identity_bindings
        .get(source_name)
        .and_then(|bindings| bindings.get(surface_id))
        .cloned()
        .ok_or_else(|| {
            AppError::FailedPrecondition(format!(
                "source '{source_name}' surface '{surface_id}' declares identity_requirements but has no workspace identity binding"
            ))
        })
}

fn app_error_to_identity_selection_error(error: &AppError) -> RequestIdentitySelectionError {
    let detail = error.to_string();
    match error {
        AppError::InvalidInput(_) => RequestIdentitySelectionError::invalid_input(detail),
        _ => RequestIdentitySelectionError::failed_precondition(detail),
    }
}

fn identity_selection_error_to_app_error(error: RequestIdentitySelectionError) -> AppError {
    match error {
        RequestIdentitySelectionError::InvalidInput(detail) => AppError::InvalidInput(detail),
        RequestIdentitySelectionError::FailedPrecondition(detail) => {
            AppError::FailedPrecondition(detail)
        }
    }
}

#[derive(Clone, Copy)]
enum QueryOperation {
    ExecuteSql,
    ExplainSql,
    ListTables,
    ListCatalog,
    DescribeTable,
}

impl QueryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ExplainSql => "explain_sql",
            Self::ListTables => "list_tables",
            Self::ListCatalog => "list_catalog",
            Self::DescribeTable => "describe_table",
        }
    }
}

fn list_tables_trace_sql(schema_filter: Option<&str>, table_filter: Option<&str>) -> String {
    match (schema_filter, table_filter) {
        (Some(schema), Some(table)) => format!("LIST TABLES {schema}.{table}"),
        (Some(schema), None) => format!("LIST TABLES {schema}.*"),
        (None, Some(table)) => format!("LIST TABLES *.{table}"),
        (None, None) => "LIST TABLES *.*".to_string(),
    }
}

fn list_catalog_trace_sql(schema_filter: Option<&str>) -> String {
    match schema_filter {
        Some(schema) => format!("LIST CATALOG {schema}"),
        None => "LIST CATALOG".to_string(),
    }
}

fn describe_table_trace_sql(schema_name: &str, table_name: &str) -> String {
    format!("DESCRIBE TABLE {schema_name}.{table_name}")
}

async fn run_query_operation<T, Fut, RowCount>(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    episode_id: Option<&EpisodeId>,
    query: Fut,
    row_count: RowCount,
) -> Result<T, QueryManagerError>
where
    Fut: Future<Output = Result<T, QueryManagerError>>,
    RowCount: FnOnce(&T) -> Option<u64>,
{
    let started_at = Instant::now();
    let query_span = create_query_span(operation, workspace_name, sql, episode_id);
    let result = query.instrument(query_span.clone()).await;

    let row_count = result.as_ref().ok().and_then(row_count);
    crate::telemetry::metrics::metrics().record_query(
        operation.as_str(),
        started_at.elapsed(),
        row_count,
        result.is_ok(),
    );

    if result.is_ok() {
        query_span.record("status", "ok");
        query_span.set_status(OtelStatus::Ok);
        if let Some(row_count) = row_count {
            query_span.record("row_count", row_count);
        }
    } else if let Err(error) = &result {
        let error_kind = query_error_kind(error);
        let error_type = query_error_type(error);
        let error_message = query_error_message(error);
        query_span.record("status", "error");
        query_span.record("error.kind", error_kind);
        query_span.record("error.type", error_type.as_str());
        query_span.record("exception.message", error_message.as_str());
        query_span.set_status(OtelStatus::error(error_message));
    }

    result
}

fn create_query_span(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    episode_id: Option<&EpisodeId>,
) -> tracing::Span {
    let operation = operation.as_str();
    let span = tracing::info_span!(
        "coral.query",
        otel.name = "coral.query",
        operation = operation,
        workspace = %workspace_name.as_str(),
        sql = %sql,
        // Trajectory-memory attribution: present only when the caller tagged the
        // call with a valid `coral-episode-id`. Joins to the intent registered by
        // `OpenEpisode`; never carries the intent text itself.
        episode.id = tracing::field::Empty,
        row_count = tracing::field::Empty,
        status = tracing::field::Empty,
        error.kind = tracing::field::Empty,
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    );
    if let Some(episode_id) = episode_id {
        span.record("episode.id", episode_id.as_str());
    }
    span
}

fn query_error_kind(error: &QueryManagerError) -> &'static str {
    match error {
        QueryManagerError::App(_) => "app",
        QueryManagerError::Core(_) => "core",
    }
}

fn query_error_type(error: &QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => app_error_type(error).to_string(),
        QueryManagerError::Core(error) => core_error_type(error),
    }
}

fn query_error_message(error: &QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => error.to_string(),
        QueryManagerError::Core(CoreError::QueryFailure(error)) => error.summary().to_string(),
        QueryManagerError::Core(error) => error.to_string(),
    }
}

fn app_error_type(error: &AppError) -> &'static str {
    match error {
        AppError::Unauthenticated(_) => "UNAUTHENTICATED",
        AppError::SourceNotFound(_) => "SOURCE_NOT_FOUND",
        AppError::IdentitySpecNotFound(_) => "IDENTITY_SPEC_NOT_FOUND",
        AppError::IdentityNotFound(_) => "IDENTITY_NOT_FOUND",
        AppError::InvalidInput(_) => "INVALID_INPUT",
        AppError::FailedPrecondition(_) => "FAILED_PRECONDITION",
        AppError::MissingOrIncompatibleV4Materialization { .. } => {
            "MISSING_OR_INCOMPATIBLE_V4_MATERIALIZATION"
        }
        AppError::SourceUnservable(_) => "SOURCE_UNSERVABLE",
        AppError::CredentialRefresh(_) => "CREDENTIAL_REFRESH",
        AppError::Unavailable(_) => "UNAVAILABLE",
        AppError::Io(_) => "IO",
        AppError::Yaml(_) => "YAML",
        AppError::TomlDecode(_) | AppError::TomlEditDecode(_) => "TOML_DECODE",
        AppError::TomlEncode(_) => "TOML_ENCODE",
        AppError::Json(_) => "JSON",
        AppError::Transport(_) => "TRANSPORT",
        AppError::TaskJoin(_) => "TASK_JOIN",
        AppError::Credentials(_) => "CREDENTIALS",
        AppError::MissingConfigDir => "MISSING_CONFIG_DIR",
    }
}

fn core_error_type(error: &CoreError) -> String {
    match error {
        CoreError::QueryFailure(error) => error.reason().to_string(),
        error => status_code_error_type(error.status_code()).to_string(),
    }
}

fn status_code_error_type(status: StatusCode) -> &'static str {
    match status {
        StatusCode::InvalidArgument => "INVALID_ARGUMENT",
        StatusCode::NotFound => "NOT_FOUND",
        StatusCode::FailedPrecondition => "FAILED_PRECONDITION",
        StatusCode::Unavailable => "UNAVAILABLE",
        StatusCode::Unimplemented => "UNIMPLEMENTED",
        StatusCode::Internal => "INTERNAL",
    }
}

fn validate_required_variables(
    source: &InstalledSource,
    inputs: &[ManifestInputSpec],
) -> Result<(), AppError> {
    let missing: Vec<_> = inputs
        .iter()
        .filter(|input| {
            input.kind == ManifestInputKind::Variable
                && input.required
                && !source.variables.contains_key(&input.key)
        })
        .collect();
    if let Some((first, rest)) = missing.split_first() {
        let detail = if rest.is_empty() {
            format!("variable '{}'", first.key)
        } else {
            format!("variable '{}' and {} other(s)", first.key, rest.len())
        };
        return Err(AppError::FailedPrecondition(format!(
            "source '{}' is missing {detail}",
            source.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use coral_engine::{
        EngineExtensions, QueryExecution, RuntimeHttpSourceComponent, SourceInputResolutionContext,
        SourceInputResolver, SourceInputResolverError,
    };
    use coral_spec::parse_source_manifest_yaml;
    use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::authorization::AllowAllWorkspaceReadAuthorizer;
    use crate::credentials::{CredentialStorageKind, CredentialStoragePreference, CredentialStore};
    use crate::features::{Features, dsl_v4_features};
    use crate::identity::UserPrincipal;
    use crate::query::QueryAttribution;
    use crate::request_context::RequestContext;
    use crate::sources::manager::{ImportSourceCommand, SourceBindings, SourceManager};
    use crate::sources::model::SourceOrigin;

    struct QueryManagerFixture {
        _temp: TempDir,
        manager: QueryManager,
    }

    fn query_manager_with(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> QueryManagerFixture {
        query_manager_with_options(runtime_context, providers, Features::default(), Vec::new())
    }

    fn query_manager_with_features(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        features: Features,
    ) -> QueryManagerFixture {
        query_manager_with_options(runtime_context, providers, features, Vec::new())
    }

    fn query_manager_with_identity_providers(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
    ) -> QueryManagerFixture {
        query_manager_with_options(
            runtime_context,
            providers,
            Features::default(),
            identity_providers,
        )
    }

    fn query_manager_with_options(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        features: Features,
        identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
    ) -> QueryManagerFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let manager = QueryManager::new_for_tests_with_options(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            runtime_context,
            layout,
            providers,
            QueryManagerOptions {
                features,
                source_identity_providers: identity_providers,
            },
        );
        QueryManagerFixture {
            _temp: temp,
            manager,
        }
    }

    fn test_installed_source(
        name: &SourceName,
        secrets: Vec<&str>,
        credential_storage: Option<CredentialStorageKind>,
    ) -> InstalledSource {
        InstalledSource {
            name: name.clone(),
            source_spec_id: None,
            version: None,
            variables: BTreeMap::new(),
            secrets: secrets.into_iter().map(ToString::to_string).collect(),
            credential_storage,
            identity_bindings: BTreeMap::new(),
            origin: SourceOrigin::Bundled,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_sql_stamps_episode_id_on_query_span() {
        use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
        use coral_api::v1::{ExecuteSqlRequest, Workspace};
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tonic::Request;
        use tracing_subscriber::layer::SubscriberExt as _;

        use crate::query::service::QueryService;

        // Capture finished spans into memory via a scoped subscriber so the
        // assertion exercises the real metadata -> manager -> span path end to end.
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("episode-attribution-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new());
        let service = QueryService::new(
            fixture.manager.clone(),
            Arc::new(AllowAllWorkspaceReadAuthorizer),
        );

        let mut request = Request::new(ExecuteSqlRequest {
            workspace: Some(Workspace {
                name: WorkspaceName::default().as_str().to_string(),
            }),
            sql: "SELECT 1".to_string(),
        });
        let episode_id = EpisodeId::parse("ep_trace_1").expect("episode id");
        request
            .extensions_mut()
            .insert(RequestContext::with_attribution(
                UserPrincipal::local(),
                QueryAttribution::new(Some(episode_id)),
            ));

        // The query may fail (the fixture has no installed sources); the
        // `coral.query` span is created and stamped before execution regardless.
        let _result = service.execute_sql(request).await;

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let query_span = spans
            .iter()
            .find(|span| span.name == "coral.query")
            .expect("coral.query span recorded");
        let episode_attr = query_span
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "episode.id")
            .expect("episode.id attribute present");
        assert_eq!(episode_attr.value.as_str(), "ep_trace_1");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn catalog_service_stamps_episode_id_on_query_spans() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing_subscriber::layer::SubscriberExt as _;

        use crate::catalog::service::CatalogService;

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("catalog-episode-attribution-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new());
        let service = CatalogService::new(
            fixture.manager.clone(),
            Arc::new(AllowAllWorkspaceReadAuthorizer),
        );

        call_catalog_tools_with_episode(&service).await;

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        assert_catalog_episode_spans(&spans);
    }

    async fn call_catalog_tools_with_episode(service: &crate::catalog::service::CatalogService) {
        use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
        use coral_api::v1::{
            DescribeTableRequest, ListCatalogRequest, ListColumnsRequest, PaginationRequest,
            SearchCatalogRequest,
        };

        let _list_catalog_result = service
            .list_catalog(tagged_catalog_request(ListCatalogRequest {
                workspace: Some(default_workspace_proto()),
                schema_name: String::new(),
                kind: 0,
                pagination: Some(PaginationRequest {
                    limit: 10,
                    offset: 0,
                }),
            }))
            .await;
        let _search_catalog_result = service
            .search_catalog(tagged_catalog_request(SearchCatalogRequest {
                workspace: Some(default_workspace_proto()),
                pattern: "tables".to_string(),
                ignore_case: true,
                schema_name: String::new(),
                kind: 0,
                pagination: Some(PaginationRequest {
                    limit: 10,
                    offset: 0,
                }),
            }))
            .await;
        let _describe_table_result = service
            .describe_table(tagged_catalog_request(DescribeTableRequest {
                workspace: Some(default_workspace_proto()),
                schema_name: "coral".to_string(),
                table_name: "tables".to_string(),
            }))
            .await;
        let _list_columns_result = service
            .list_columns(tagged_catalog_request(ListColumnsRequest {
                workspace: Some(default_workspace_proto()),
                schema_name: "coral".to_string(),
                table_name: "tables".to_string(),
                pattern: None,
                ignore_case: true,
                required_only: false,
                pagination: Some(PaginationRequest {
                    limit: 10,
                    offset: 0,
                }),
            }))
            .await;
    }

    fn default_workspace_proto() -> coral_api::v1::Workspace {
        coral_api::v1::Workspace {
            name: WorkspaceName::default().as_str().to_string(),
        }
    }

    fn tagged_catalog_request<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        let episode_id = crate::episode::EpisodeId::parse("ep_catalog_trace").expect("episode id");
        request
            .extensions_mut()
            .insert(RequestContext::with_attribution(
                UserPrincipal::local(),
                QueryAttribution::new(Some(episode_id)),
            ));
        request
    }

    fn assert_catalog_episode_spans(spans: &[opentelemetry_sdk::trace::SpanData]) {
        let attributed_query_spans = spans
            .iter()
            .filter(|span| {
                span.name == "coral.query"
                    && span.attributes.iter().any(|attribute| {
                        attribute.key.as_str() == "episode.id"
                            && attribute.value.as_str() == "ep_catalog_trace"
                    })
            })
            .collect::<Vec<_>>();
        assert!(
            attributed_query_spans.len() >= 4,
            "each catalog service call should stamp a backend query span: {spans:?}"
        );
        let operations = attributed_query_spans
            .iter()
            .flat_map(|span| {
                span.attributes
                    .iter()
                    .filter(|attribute| attribute.key.as_str() == "operation")
                    .map(|attribute| attribute.value.as_str().to_string())
            })
            .collect::<Vec<_>>();
        assert!(
            operations
                .iter()
                .any(|operation| operation == "list_catalog")
        );
        assert!(
            operations
                .iter()
                .any(|operation| operation == "describe_table")
        );
        assert!(
            operations
                .iter()
                .any(|operation| operation == "list_tables")
        );
    }

    fn execution_to_rows(execution: &QueryExecution) -> Vec<Value> {
        let mut bytes = Vec::new();
        {
            let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
            for batch in execution.batches() {
                writer.write(batch).expect("batch should encode to json");
            }
            writer.finish().expect("json writer should finish");
        }
        serde_json::from_slice(&bytes).expect("json rows should decode")
    }

    fn issues_openapi_fixture(server_uri: impl std::fmt::Display) -> String {
        format!(
            r"
openapi: 3.0.3
info:
  title: GitHub
servers:
  - url: {server_uri}
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {{type: integer}}
                    title: {{type: string}}
"
        )
    }

    #[test]
    fn runtime_config_preserves_app_owned_body_capture_max_bytes() {
        let fixture = query_manager_with(
            QueryRuntimeContext::default().with_body_capture_max_bytes(Some(42)),
            Vec::new(),
        );

        let runtime = fixture
            .manager
            .runtime_config(
                &WorkspaceName::default(),
                &UserPrincipal::local(),
                &[],
                BTreeMap::new(),
                &AppConfig::default(),
            )
            .expect("runtime config");

        let config = runtime
            .context
            .body_capture_max_bytes
            .expect("body capture config");
        assert_eq!(config, 42);
    }

    #[derive(Debug)]
    struct TestRuntimeIdentity {
        identity_spec_id: String,
        audience: BTreeMap<String, Value>,
    }

    #[tonic::async_trait]
    impl crate::identity::RuntimeSourceIdentity for TestRuntimeIdentity {
        fn identity_spec_id(&self) -> &str {
            &self.identity_spec_id
        }

        fn audience(&self) -> &BTreeMap<String, Value> {
            &self.audience
        }

        async fn resolve_headers(
            &self,
            _identity: &SelectedRequestIdentity,
            _request: &reqwest::Request,
            _resolved_inputs: &BTreeMap<String, String>,
        ) -> Result<
            Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
            RequestIdentityHttpAuthenticatorError,
        > {
            Ok(vec![(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_static("Bearer selected-token"),
            )])
        }
    }

    #[derive(Debug)]
    struct TestSourceIdentityProvider {
        selection_user_ids: Arc<Mutex<Vec<String>>>,
        resolution_user_ids: Arc<Mutex<Vec<Option<String>>>>,
    }

    #[tonic::async_trait]
    impl SourceIdentityProvider for TestSourceIdentityProvider {
        async fn resolve_source_identity_selection(
            &self,
            request: &SourceIdentitySelectionRequest,
        ) -> Result<Option<SourceIdentitySelection>, AppError> {
            self.selection_user_ids
                .lock()
                .expect("selection lock")
                .push(request.user_id.clone());
            Ok(Some(
                SourceIdentitySelection::new("github_saul").expect("selection"),
            ))
        }

        async fn resolve_source_identity(
            &self,
            request: &SourceIdentityResolutionRequest,
        ) -> Result<Option<Arc<dyn crate::identity::RuntimeSourceIdentity>>, AppError> {
            self.resolution_user_ids
                .lock()
                .expect("resolution lock")
                .push(request.user_id.clone());
            let selected_requirement = request
                .identity_requirements
                .accepts
                .first()
                .expect("selected identity requirement");
            assert_eq!(request.selection.identity, "github_saul");
            assert_eq!(request.identity_requirements.accepts.len(), 1);
            assert_eq!(selected_requirement.id, "github-rest-read");
            Ok(Some(Arc::new(TestRuntimeIdentity {
                identity_spec_id: "github_pat".to_string(),
                audience: BTreeMap::from([("host".to_string(), json!("api.example.test"))]),
            })))
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This integration-style unit test sets up source, binding, provider, and resolver state."
    )]
    #[tokio::test]
    async fn runtime_config_resolves_user_source_identity_for_request_principal() {
        let selection_user_ids = Arc::new(Mutex::new(Vec::new()));
        let resolution_user_ids = Arc::new(Mutex::new(Vec::new()));
        let fixture = query_manager_with_identity_providers(
            QueryRuntimeContext::default(),
            Vec::new(),
            vec![Arc::new(TestSourceIdentityProvider {
                selection_user_ids: Arc::clone(&selection_user_ids),
                resolution_user_ids: Arc::clone(&resolution_user_ids),
            })],
        );
        let requirements = IdentityRequirements {
            accepts: vec![AcceptedIdentityRequirement {
                id: "github-rest-read".to_string(),
                identity_specs: vec!["github_pat".to_string()],
                audience: BTreeMap::from([("host".to_string(), json!("api.example.test"))]),
            }],
        };
        let mut http_manifest = parse_source_manifest_yaml(
            r"
name: github_v4_query
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://api.example.test
tables:
  - name: issues
    description: Issues
    request:
      method: GET
      path: /issues
    response: {}
    columns:
      - name: id
        type: Int64
",
        )
        .expect("parse manifest")
        .as_http()
        .expect("http manifest")
        .clone();
        http_manifest.common.dsl_version = 4;
        let source = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "github_v4_query".to_string(),
                authored_version: None,
                description: "GitHub".to_string(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                components: vec![RuntimeSourceComponent::Http(
                    RuntimeHttpSourceComponent::with_identity_requirements(
                        http_manifest,
                        "rest",
                        requirements.clone(),
                    ),
                )],
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("runtime source");
        let runtime = fixture
            .manager
            .runtime_config(
                &WorkspaceName::default(),
                &UserPrincipal::for_user("saul").expect("request principal"),
                std::slice::from_ref(&source),
                BTreeMap::from([(
                    "github_v4_query".to_string(),
                    BTreeMap::from([("rest".to_string(), SourceIdentityBinding::user_owned())]),
                )]),
                &AppConfig::default(),
            )
            .expect("runtime config");
        let selector = runtime
            .request_identity_selector
            .expect("identity selector installed");
        let factory = runtime
            .request_identity_http_authenticator_factory
            .expect("identity authenticator factory installed");
        let selected = selector
            .select_identity(&RequestIdentitySelectionContext::new(
                "github_v4_query".to_string(),
                "rest".to_string(),
                requirements,
            ))
            .await
            .expect("identity selected");
        assert_eq!(selected.identity_id(), "github_saul");
        assert_eq!(selected.identity_spec_id(), "github_pat");
        let authenticator = factory(selected).expect("identity authenticator");
        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.example.test/issues".parse().expect("url"),
        );
        let headers = authenticator.as_ref()(&request, &BTreeMap::new())
            .await
            .expect("identity headers");

        assert_eq!(
            selection_user_ids
                .lock()
                .expect("selection lock")
                .as_slice(),
            &["saul".to_string()]
        );
        assert_eq!(
            resolution_user_ids
                .lock()
                .expect("resolution lock")
                .as_slice(),
            &[Some("saul".to_string())]
        );
        let authorization = headers.first().expect("authorization header");
        assert_eq!(authorization.0, reqwest::header::AUTHORIZATION);
        assert_eq!(authorization.1, "Bearer selected-token");
    }

    #[test]
    fn load_query_source_passes_present_optional_secrets_to_runtime() {
        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("optional_auth").expect("source name");
        let manifest_path = fixture
            .manager
            .layout
            .manifest_file(&workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: optional_auth
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://api.example.com
inputs:
  API_KEY:
    kind: secret
    required: false
  OAUTH_TOKEN:
    kind: secret
    required: false
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: one_of
      values:
        - from: input
          key: API_KEY
        - from: bearer
          key: OAUTH_TOKEN
tables:
  - name: items
    description: Items
    request:
      path: /items
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("write manifest");
        let source = InstalledSource {
            name: source_name.clone(),
            source_spec_id: None,
            version: Some("0.1.0".to_string()),
            variables: BTreeMap::new(),
            secrets: vec!["API_KEY".to_string(), "OAUTH_TOKEN".to_string()],
            credential_storage: Some(CredentialStorageKind::File),
            identity_bindings: BTreeMap::new(),
            origin: SourceOrigin::Imported,
        };
        fixture
            .manager
            .config_store
            .upsert_source(&workspace_name, source.clone())
            .expect("persist source");
        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &CredentialSetId::for_source(&source_name),
                CredentialStorageKind::File,
                &BTreeMap::from([("OAUTH_TOKEN".to_string(), "oauth-token".to_string())]),
            )
            .expect("persist secret material");

        let loaded_source = fixture
            .manager
            .load_query_source(&workspace_name, &source)
            .expect("optional secret should load when present");

        assert_eq!(
            loaded_source.query_source.secrets(),
            &BTreeMap::from([("OAUTH_TOKEN".to_string(), "oauth-token".to_string())])
        );
    }

    #[tokio::test]
    async fn installed_v4_source_queries_through_app_assembled_runtime_component() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/issues"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"id": 1, "title": "Generated runtime package"}
            ])))
            .mount(&server)
            .await;

        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        fixture.manager.layout.ensure().expect("ensure layout");
        let source_manager = SourceManager::new_with_features(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.manager.layout.clone(),
            dsl_v4_features(),
        );
        let workspace_name = WorkspaceName::default();
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(
            &openapi_file,
            format!(
                r"
openapi: 3.0.3
info:
  title: GitHub
servers:
  - url: {}
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {{type: integer}}
                    title: {{type: string}}
",
                server.uri()
            ),
        )
        .expect("write OpenAPI fixture");
        source_manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: github_v4_query
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                    identity_bindings: BTreeMap::new(),
                    replace_identity_bindings: false,
                },
            )
            .expect("import v4 source");
        std::fs::remove_file(&openapi_file).expect("remove authored descriptor after import");
        let query_context = QueryContext::new(
            workspace_name.clone(),
            RequestContext::with_attribution(UserPrincipal::local(), QueryAttribution::default()),
        );

        let execution = fixture
            .manager
            .execute_sql(
                &query_context,
                "SELECT id, title FROM github_v4_query.issues",
            )
            .await
            .expect("query executes");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 1, "title": "Generated runtime package"})]
        );
    }

    #[tokio::test]
    async fn aliased_v4_source_queries_and_validates_through_installed_source_name() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/issues"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"id": 7, "title": "Aliased runtime package"}
            ])))
            .mount(&server)
            .await;

        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        fixture.manager.layout.ensure().expect("ensure layout");
        let source_manager = SourceManager::new_with_features(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.manager.layout.clone(),
            dsl_v4_features(),
        );
        let workspace_name = WorkspaceName::default();
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(
            &openapi_file,
            format!(
                r"
openapi: 3.0.3
info:
  title: GitHub
servers:
  - url: {}
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {{type: integer}}
                    title: {{type: string}}
",
                server.uri()
            ),
        )
        .expect("write OpenAPI fixture");
        let source_name = SourceName::parse("github_alias").expect("source alias");
        let source_spec_id = SourceName::parse("github_v4_query").expect("source spec");
        source_manager
            .import_source_as(
                &workspace_name,
                &source_name,
                &source_spec_id,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: github_v4_query
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
test_queries:
  - SELECT id, title FROM github_v4_query.issues
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                    identity_bindings: BTreeMap::new(),
                    replace_identity_bindings: false,
                },
            )
            .expect("import v4 source alias");
        let query_context = QueryContext::new(
            workspace_name.clone(),
            RequestContext::with_attribution(UserPrincipal::local(), QueryAttribution::default()),
        );

        fixture
            .manager
            .validate_source(&query_context, &source_name)
            .await
            .expect("authored validation query is rewritten to installed alias");
        let execution = fixture
            .manager
            .execute_sql(&query_context, "SELECT id, title FROM github_alias.issues")
            .await
            .expect("query executes through installed alias");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 7, "title": "Aliased runtime package"})]
        );
    }

    #[tokio::test]
    async fn aliased_v4_source_runtime_load_keeps_authored_test_queries_unparsed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/issues"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"id": 9, "title": "Runtime ignores invalid validation SQL"}
            ])))
            .mount(&server)
            .await;

        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        fixture.manager.layout.ensure().expect("ensure layout");
        let source_manager = SourceManager::new_with_features(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.manager.layout.clone(),
            dsl_v4_features(),
        );
        let workspace_name = WorkspaceName::default();
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(&openapi_file, issues_openapi_fixture(server.uri()))
            .expect("write OpenAPI fixture");
        let source_name = SourceName::parse("github_alias").expect("source alias");
        let source_spec_id = SourceName::parse("github_v4_query").expect("source spec");
        source_manager
            .import_source_as(
                &workspace_name,
                &source_name,
                &source_spec_id,
                &ImportSourceCommand {
                    manifest_yaml: format!(
                        r"
name: github_v4_query
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
test_queries:
  - SELECT * FROM github_v4_query.issues WHERE
",
                        openapi_file.display()
                    ),
                    bindings: SourceBindings::default(),
                    identity_bindings: BTreeMap::new(),
                    replace_identity_bindings: false,
                },
            )
            .expect("import v4 source alias");
        let query_context = QueryContext::new(
            workspace_name.clone(),
            RequestContext::with_attribution(UserPrincipal::local(), QueryAttribution::default()),
        );

        let execution = fixture
            .manager
            .execute_sql(&query_context, "SELECT id, title FROM github_alias.issues")
            .await
            .expect("query executes without parsing validation SQL during runtime load");
        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 9, "title": "Runtime ignores invalid validation SQL"})]
        );

        let validation_result = fixture
            .manager
            .validate_source(&query_context, &source_name)
            .await;
        assert!(
            validation_result.is_err(),
            "validation should parse and reject invalid validation SQL"
        );
    }

    #[test]
    fn rewrite_query_namespaces_updates_relations_without_touching_literals() {
        let namespace_map = BTreeMap::from([
            ("github_v4".to_string(), "github".to_string()),
            ("github_v4_mcp".to_string(), "github_mcp".to_string()),
        ]);

        let rewritten_table = rewrite_query_namespaces(
            "SELECT id FROM github_v4.issues WHERE note = 'github_v4.issues'",
            &namespace_map,
        )
        .expect("rewrite table query");
        assert!(rewritten_table.contains("FROM github.issues"));
        assert!(rewritten_table.contains("'github_v4.issues'"));

        let rewritten_function = rewrite_query_namespaces(
            "SELECT result FROM github_v4_mcp.list_pull_requests('withcoral')",
            &namespace_map,
        )
        .expect("rewrite table function query");
        assert!(rewritten_function.contains("github_mcp.list_pull_requests"));
    }

    #[test]
    fn load_query_source_rejects_v4_when_dsl_v4_feature_is_disabled() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new());
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github_v4_disabled").expect("source name");
        let manifest_path = fixture
            .manager
            .layout
            .manifest_file(&workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: github_v4_disabled
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
",
        )
        .expect("write manifest");
        let source = InstalledSource {
            name: source_name,
            source_spec_id: None,
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
            identity_bindings: BTreeMap::new(),
        };

        let error = fixture
            .manager
            .load_query_source(&workspace_name, &source)
            .expect_err("v4 source should require feature opt-in");

        assert!(
            matches!(error, AppError::SourceUnservable(ref message) if message.contains("dsl_v4")),
            "unexpected error: {error:#}"
        );

        fixture
            .manager
            .config_store
            .upsert_source(&workspace_name, source)
            .expect("persist v4 source");
        let error = fixture
            .manager
            .load_query_sources(&workspace_name)
            .expect_err("normal query loading should fail on disabled v4 source");

        assert!(
            matches!(error, AppError::SourceUnservable(ref message) if message.contains("dsl_v4")),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_query_sources_fails_closed_for_missing_v4_materialization() {
        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github_v4_missing_artifacts").expect("source name");
        let manifest_path = fixture
            .manager
            .layout
            .manifest_file(&workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: github_v4_missing_artifacts
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
",
        )
        .expect("write manifest");
        fixture
            .manager
            .config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name.clone(),
                    source_spec_id: None,
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    identity_bindings: BTreeMap::new(),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("persist source");

        let error = fixture
            .manager
            .load_query_sources(&workspace_name)
            .expect_err("missing materialization should fail closed");

        assert!(
            matches!(
                error,
                AppError::MissingOrIncompatibleV4Materialization { .. }
            ),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_query_sources_fails_closed_for_v4_when_feature_is_disabled() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new());
        fixture.manager.layout.ensure().expect("ensure layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github_v4_disabled").expect("source name");
        let manifest_path = fixture
            .manager
            .layout
            .manifest_file(&workspace_name, &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(
            &manifest_path,
            r"
name: github_v4_disabled
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
",
        )
        .expect("write manifest");
        fixture
            .manager
            .config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name,
                    source_spec_id: None,
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    identity_bindings: BTreeMap::new(),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("persist source");

        let error = fixture
            .manager
            .load_query_sources(&workspace_name)
            .expect_err("disabled v4 feature should fail closed");

        assert!(
            matches!(error, AppError::SourceUnservable(ref message) if message.contains("dsl_v4")),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_query_sources_fails_closed_for_unavailable_keychain_source() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("github").expect("source name");
        config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name,
                    source_spec_id: None,
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: vec!["GITHUB_TOKEN".to_string()],
                    credential_storage: Some(CredentialStorageKind::Keychain),
                    identity_bindings: BTreeMap::new(),
                    origin: SourceOrigin::Bundled,
                },
            )
            .expect("persist source");
        let credential_store = CredentialStore::with_unavailable_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Keychain,
        );
        let manager = QueryManager::new_for_tests(
            config_store,
            CredentialManager::new(credential_store),
            QueryRuntimeContext::default(),
            layout,
            Vec::new(),
        );
        let error = manager
            .load_query_sources(&workspace_name)
            .expect_err("unavailable keychain should fail closed");

        assert!(
            matches!(
                error,
                AppError::Credentials(CredentialsError::Unavailable(_))
            ),
            "unexpected error: {error:#}"
        );
        assert!(
            error
                .to_string()
                .contains("configured for keychain storage"),
            "keychain-routed query failure should name the routed backend: {error}"
        );
    }

    #[derive(Debug)]
    struct DelegatingInputResolver {
        calls: Arc<AtomicUsize>,
        observed_token: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl SourceInputResolver for DelegatingInputResolver {
        async fn resolve_inputs(
            &self,
            source: &SourceInputResolutionContext,
        ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.observed_token.lock().expect("observed token lock") =
                source.secrets().get("API_TOKEN").cloned();
            Ok(BTreeMap::from([
                ("API_TOKEN".to_string(), "delegated-token".to_string()),
                ("DELEGATED_ONLY".to_string(), "provider-token".to_string()),
            ]))
        }
    }

    struct DelegatingInputResolverProvider {
        calls: Arc<AtomicUsize>,
        observed_token: Arc<Mutex<Option<String>>>,
    }

    impl EngineExtensionsProvider for DelegatingInputResolverProvider {
        fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
            EngineExtensions {
                source_input_resolver: Some(Arc::new(DelegatingInputResolver {
                    calls: Arc::clone(&self.calls),
                    observed_token: Arc::clone(&self.observed_token),
                })),
                ..Default::default()
            }
        }
    }

    fn installed_api_token_source(source_name: SourceName) -> InstalledSource {
        InstalledSource {
            name: source_name,
            source_spec_id: None,
            version: None,
            variables: BTreeMap::new(),
            secrets: vec!["API_TOKEN".to_string()],
            credential_storage: Some(CredentialStorageKind::File),
            identity_bindings: BTreeMap::new(),
            origin: SourceOrigin::Bundled,
        }
    }

    fn secured_messages_query_source() -> QuerySource {
        let source_spec = parse_source_manifest_yaml(
            r#"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://example.com
  API_TOKEN:
    kind: secret
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("parse source manifest");
        QuerySource::new(source_spec, BTreeMap::new(), BTreeMap::new())
    }

    fn query_manager_with_explicit_source_registry(
        config_store: ConfigStore,
        source_registry: Arc<dyn SourceRegistry>,
        layout: AppStateLayout,
    ) -> QueryManager {
        QueryManager::new(
            config_store,
            source_registry,
            CredentialManager::new(CredentialStore::new(layout.clone())),
            QueryRuntimeContext::default(),
            layout.clone(),
            Arc::new(LocalSourceArtifactStore::new(layout)),
            Vec::new(),
            QueryManagerOptions::default(),
        )
    }

    async fn resolve_runtime_inputs(
        manager: &QueryManager,
        workspace_name: &WorkspaceName,
        source: &QuerySource,
    ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
        let runtime = manager
            .runtime_config(
                workspace_name,
                &UserPrincipal::local(),
                std::slice::from_ref(source),
                BTreeMap::new(),
                &AppConfig::default(),
            )
            .expect("runtime config");
        let input_resolver = runtime
            .extensions
            .source_input_resolver
            .expect("runtime installs input resolver");
        input_resolver
            .resolve_inputs(&SourceInputResolutionContext::from_query_source(source))
            .await
    }

    #[tokio::test]
    async fn runtime_config_composes_provider_input_resolver_with_refreshed_inputs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_token = Arc::new(Mutex::new(None));
        let fixture = query_manager_with(
            QueryRuntimeContext::default(),
            vec![Arc::new(DelegatingInputResolverProvider {
                calls: Arc::clone(&calls),
                observed_token: Arc::clone(&observed_token),
            })],
        );
        let source_name = SourceName::parse("secured_messages").expect("source name");
        let workspace_name = WorkspaceName::default();
        let credential_set_id = CredentialSetId::for_source(&source_name);
        fixture
            .manager
            .config_store
            .upsert_source(
                &workspace_name,
                installed_api_token_source(source_name.clone()),
            )
            .expect("persist source");
        fixture
            .manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "stored-token".to_string())]),
            )
            .expect("write credential material");
        let source = secured_messages_query_source();

        let resolved_inputs = resolve_runtime_inputs(&fixture.manager, &workspace_name, &source)
            .await
            .expect("resolve source inputs");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            resolved_inputs.get("API_TOKEN").map(String::as_str),
            Some("stored-token")
        );
        assert_eq!(
            resolved_inputs.get("API_BASE").map(String::as_str),
            Some("https://example.com")
        );
        assert_eq!(
            resolved_inputs.get("DELEGATED_ONLY").map(String::as_str),
            Some("provider-token")
        );
        assert_eq!(
            observed_token
                .lock()
                .expect("observed token lock")
                .as_deref(),
            Some("stored-token")
        );
    }

    #[tokio::test]
    async fn runtime_config_refreshes_credentials_from_source_registry_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let registry_layout =
            AppStateLayout::discover(Some(temp.path().join("registry-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        registry_layout.ensure().expect("ensure registry layout");
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source name");
        let config_store = ConfigStore::new(layout.clone());
        let registry_store = ConfigStore::new(registry_layout);
        config_store
            .upsert_source(
                &workspace_name,
                test_installed_source(&source_name, Vec::new(), None),
            )
            .expect("persist stale config source");
        registry_store
            .upsert_source(
                &workspace_name,
                test_installed_source(
                    &source_name,
                    vec!["API_TOKEN"],
                    Some(CredentialStorageKind::File),
                ),
            )
            .expect("persist registry source");
        let source_registry: Arc<dyn SourceRegistry> = Arc::new(registry_store);
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        credential_manager
            .replace_material(
                &workspace_name,
                &CredentialSetId::for_source(&source_name),
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "registry-token".to_string())]),
            )
            .expect("write credential material");
        let manager = QueryManager::new(
            config_store,
            source_registry,
            credential_manager,
            QueryRuntimeContext::default(),
            layout.clone(),
            Arc::new(LocalSourceArtifactStore::new(layout)),
            Vec::new(),
            QueryManagerOptions::default(),
        );
        let source_spec = parse_source_manifest_yaml(
            r"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
inputs:
  API_TOKEN:
    kind: secret
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("parse source manifest");
        let source = QuerySource::new(source_spec, BTreeMap::new(), BTreeMap::new());
        let runtime = manager
            .runtime_config(
                &workspace_name,
                &UserPrincipal::local(),
                std::slice::from_ref(&source),
                BTreeMap::new(),
                &AppConfig::default(),
            )
            .expect("runtime config");
        let input_resolver = runtime
            .extensions
            .source_input_resolver
            .expect("runtime installs input resolver");

        let resolved_inputs = input_resolver
            .resolve_inputs(&SourceInputResolutionContext::from_query_source(&source))
            .await
            .expect("resolve source inputs from registry metadata");

        assert_eq!(
            resolved_inputs.get("API_TOKEN").map(String::as_str),
            Some("registry-token")
        );
    }

    #[tokio::test]
    async fn runtime_config_refreshes_inputs_from_injected_source_registry() {
        let temp = TempDir::new().expect("temp dir");
        let local_layout =
            AppStateLayout::discover(Some(temp.path().join("local-config"))).expect("layout");
        let registry_layout =
            AppStateLayout::discover(Some(temp.path().join("registry-config"))).expect("layout");
        let local_config_store = ConfigStore::new(local_layout.clone());
        let registry_store = ConfigStore::new(registry_layout);
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source name");
        registry_store
            .upsert_source(
                &workspace_name,
                installed_api_token_source(source_name.clone()),
            )
            .expect("persist registry source");
        let source_registry: Arc<dyn SourceRegistry> = Arc::new(registry_store);
        let manager = query_manager_with_explicit_source_registry(
            local_config_store.clone(),
            source_registry,
            local_layout,
        );
        manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &CredentialSetId::for_source(&source_name),
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "stored-token".to_string())]),
            )
            .expect("write credential material");
        let source = secured_messages_query_source();

        let resolved_inputs = resolve_runtime_inputs(&manager, &workspace_name, &source)
            .await
            .expect("resolve source inputs");

        assert_eq!(
            resolved_inputs.get("API_TOKEN").map(String::as_str),
            Some("stored-token")
        );
        assert_eq!(
            resolved_inputs.get("API_BASE").map(String::as_str),
            Some("https://example.com")
        );
        assert!(
            matches!(
                local_config_store.get_source(&workspace_name, &source_name),
                Err(AppError::SourceNotFound(_))
            ),
            "source should not need to exist in the local config store"
        );
    }

    #[tokio::test]
    async fn runtime_config_refresh_does_not_fallback_to_local_config_store() {
        let temp = TempDir::new().expect("temp dir");
        let local_layout =
            AppStateLayout::discover(Some(temp.path().join("local-config"))).expect("layout");
        let registry_layout =
            AppStateLayout::discover(Some(temp.path().join("registry-config"))).expect("layout");
        let local_config_store = ConfigStore::new(local_layout.clone());
        let registry_store = ConfigStore::new(registry_layout);
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source name");
        local_config_store
            .upsert_source(
                &workspace_name,
                installed_api_token_source(source_name.clone()),
            )
            .expect("persist local source");
        let source_registry: Arc<dyn SourceRegistry> = Arc::new(registry_store);
        let manager = query_manager_with_explicit_source_registry(
            local_config_store,
            source_registry,
            local_layout,
        );
        manager
            .credential_manager
            .replace_material(
                &workspace_name,
                &CredentialSetId::for_source(&source_name),
                CredentialStorageKind::File,
                &BTreeMap::from([("API_TOKEN".to_string(), "stored-token".to_string())]),
            )
            .expect("write credential material");
        let source = secured_messages_query_source();

        let error = resolve_runtime_inputs(&manager, &workspace_name, &source)
            .await
            .expect_err("missing registry source should fail");

        assert!(
            matches!(
                error,
                SourceInputResolverError::FailedPrecondition(ref detail)
                    if detail.contains("source 'default:secured_messages' not found")
            ),
            "unexpected resolver error: {error}"
        );
    }
}
