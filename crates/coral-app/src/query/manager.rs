//! Query-time loading, validation, and execution over installed sources.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;

use coral_engine::{
    CatalogInfo, CoralQuery, CoreError, DescribeTableInfo, QueryExecution, QueryPlan,
    QueryRuntimeConfig, QueryRuntimeContext, QuerySource, RequestIdentityResolutionContext,
    RequestIdentityResolver, RequestIdentityResolverError, RuntimeIdentityRequirements,
    RuntimeSourceComponent, RuntimeSourcePackage, SourceValidationReport, StatusCode, TableInfo,
};
use coral_spec::v4::IdentityRequirements;
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use opentelemetry::{KeyValue, trace::Status as OtelStatus};
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, SelectItem, SelectItemQualifiedWildcardKind,
    SetExpr, VisitMut, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId, CredentialsError};
use crate::features::Features;
use crate::identity::{
    IdentityManager, SourceIdentityBinding, SourceIdentityProvider,
    SourceIdentityResolutionRequest, SourceIdentitySelection, SourceIdentitySelectionRequest,
    SourceIdentitySubject, UserPrincipal,
};
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
use crate::sources::runtime_package::runtime_components_for_v4_source;
#[cfg(test)]
use crate::state::AppStateLayout;
use crate::state::{AppConfig, ConfigStore};
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

#[derive(Clone)]
pub(crate) struct QueryManager {
    config_store: ConfigStore,
    source_registry: Arc<dyn SourceRegistry>,
    artifact_store: Arc<dyn SourceArtifactStore>,
    credential_manager: CredentialManager,
    runtime_context: QueryRuntimeContext,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    identity_manager: IdentityManager,
    features: Features,
}

impl QueryManager {
    #[cfg(test)]
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> Self {
        Self::new_with_features(
            config_store,
            credential_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
            Vec::new(),
            Features::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_features(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
        features: Features,
    ) -> Self {
        Self::new_with_features_and_source_registry(
            config_store.clone(),
            Arc::new(config_store),
            credential_manager,
            runtime_context,
            layout,
            engine_extensions_providers,
            identity_providers,
            features,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one-time wiring constructor; every argument is a distinct runtime dependency"
    )]
    #[cfg(test)]
    pub(crate) fn new_with_features_and_source_registry(
        config_store: ConfigStore,
        source_registry: Arc<dyn SourceRegistry>,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        layout: AppStateLayout,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
        features: Features,
    ) -> Self {
        Self::new_with_features_source_registry_and_artifact_store(
            config_store,
            source_registry,
            Arc::new(LocalSourceArtifactStore::new(layout)),
            credential_manager,
            runtime_context,
            engine_extensions_providers,
            identity_providers,
            features,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one-time wiring constructor; every argument is a distinct runtime dependency"
    )]
    pub(crate) fn new_with_features_source_registry_and_artifact_store(
        config_store: ConfigStore,
        source_registry: Arc<dyn SourceRegistry>,
        artifact_store: Arc<dyn SourceArtifactStore>,
        credential_manager: CredentialManager,
        runtime_context: QueryRuntimeContext,
        engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
        features: Features,
    ) -> Self {
        Self {
            config_store,
            source_registry,
            artifact_store,
            credential_manager,
            runtime_context,
            engine_extensions_providers,
            identity_manager: IdentityManager::new(identity_providers),
            features,
        }
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

    /// Loads the workspace's query sources and the runtime config used to
    /// execute one query-time operation against them.
    fn load_query_runtime(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
    ) -> Result<(Vec<QuerySource>, QueryRuntimeConfig), QueryManagerError> {
        let config = self
            .config_store
            .load_config()
            .map_err(QueryManagerError::App)?;
        let sources = self
            .load_query_sources(workspace_name)
            .map_err(QueryManagerError::App)?;
        let identity_bindings = identity_binding_snapshot_for_sources(&sources);
        let sources = query_sources_from_loaded(sources);
        let runtime = self
            .runtime_config(
                workspace_name,
                request_principal,
                &sources,
                identity_bindings,
                &config,
            )
            .map_err(QueryManagerError::App)?;
        Ok((sources, runtime))
    }

    pub(crate) async fn list_tables(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
    ) -> Result<Vec<TableInfo>, QueryManagerError> {
        let (sources, runtime) = self.load_query_runtime(workspace_name, request_principal)?;
        CoralQuery::list_tables(&sources, runtime, schema_filter, table_filter)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        schema_filter: Option<&str>,
    ) -> Result<CatalogInfo, QueryManagerError> {
        let (sources, runtime) = self.load_query_runtime(workspace_name, request_principal)?;
        CoralQuery::list_catalog(&sources, runtime, schema_filter)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn describe_table(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableInfo, QueryManagerError> {
        let (sources, runtime) = self.load_query_runtime(workspace_name, request_principal)?;
        CoralQuery::describe_table(&sources, runtime, schema_name, table_name)
            .await
            .map_err(QueryManagerError::Core)
    }

    pub(crate) async fn execute_sql_with_context(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        run_query_operation(
            QueryOperation::ExecuteSql,
            workspace_name,
            sql,
            async {
                let (sources, runtime) =
                    self.load_query_runtime(workspace_name, request_principal)?;
                CoralQuery::execute_sql(&sources, runtime, sql)
                    .await
                    .map_err(QueryManagerError::Core)
            },
            |execution| Some(u64::try_from(execution.row_count()).unwrap_or(u64::MAX)),
        )
        .await
    }

    pub(crate) async fn explain_sql_with_context(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        sql: &str,
    ) -> Result<QueryPlan, QueryManagerError> {
        run_query_operation(
            QueryOperation::ExplainSql,
            workspace_name,
            sql,
            async {
                let (sources, runtime) =
                    self.load_query_runtime(workspace_name, request_principal)?;
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
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        source_name: &SourceName,
    ) -> Result<ValidatedSource, QueryManagerError> {
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
        self.validate_source_identity_bindings(workspace_name, request_principal, &loaded_source)
            .await
            .map_err(QueryManagerError::App)?;
        let identity_bindings =
            identity_binding_snapshot_for_sources(std::slice::from_ref(&loaded_source));
        let runtime = self
            .runtime_config(
                workspace_name,
                request_principal,
                std::slice::from_ref(&loaded_source.query_source),
                identity_bindings,
                &config,
            )
            .map_err(QueryManagerError::App)?;
        let report = CoralQuery::validate_source(
            &loaded_source.query_source,
            runtime,
            loaded_source.query_source.test_queries(),
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
                // A known source that cannot be served right now (unavailable
                // credentials, disabled feature, incompatible materialization)
                // must surface loudly rather than be silently dropped from the
                // catalog.
                Err(
                    error @ (AppError::Credentials(CredentialsError::Unavailable(_))
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
                runtime_components_for_v4_source(v4, &materialized, source.name.as_str()).map_err(
                    |error| {
                        incompatible_materialization_error(
                            &source.name,
                            format!("failed to assemble runtime package: {error}"),
                        )
                    },
                )?,
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
                    test_queries: runtime_test_queries_for_source(
                        source_spec.schema_name(),
                        source.name.as_str(),
                        source_spec.test_queries(),
                    ),
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

    fn request_identity_resolver(
        &self,
        workspace_name: &WorkspaceName,
        request_principal: &UserPrincipal,
        selected_sources: &[QuerySource],
        identity_bindings: SourceIdentityBindingsSnapshot,
    ) -> Option<Arc<dyn RequestIdentityResolver>> {
        if selected_sources
            .iter()
            .any(|source| identity_requirements_for_source(source).next().is_some())
        {
            Some(Arc::new(LazyRuntimeIdentityResolver {
                workspace_name: workspace_name.clone(),
                request_principal: request_principal.clone(),
                source_identity_bindings: Arc::new(identity_bindings),
                identity_manager: self.identity_manager.clone(),
            }))
        } else {
            None
        }
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
        let request_identity_resolver = self.request_identity_resolver(
            workspace_name,
            request_principal,
            selected_sources,
            identity_bindings,
        );
        let mut runtime_context = self.runtime_context.clone();
        runtime_context.trace_context = Some(tracing::Span::current().context());
        let mut runtime = QueryRuntimeConfig::new(runtime_context, extensions)
            .with_request_identity_resolver(request_identity_resolver);
        let selected_source_names = selected_sources
            .iter()
            .map(|source| source.source_name().to_string())
            .collect::<Vec<_>>();
        runtime.dependent_join = config.dependent_join_config(&selected_source_names)?;
        Ok(runtime)
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
        let resolver = LazyRuntimeIdentityResolver {
            workspace_name: workspace_name.clone(),
            request_principal: request_principal.clone(),
            source_identity_bindings: Arc::new(source_identity_bindings),
            identity_manager: self.identity_manager.clone(),
        };
        for requirements in identity_requirements_for_source(&loaded_source.query_source) {
            let context = RequestIdentityResolutionContext::new(
                loaded_source.query_source.source_name().to_string(),
                requirements.surface_id,
                requirements.requirements,
            );
            resolver.resolve_runtime_identity(&context).await?;
        }
        Ok(())
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

#[derive(Clone)]
struct LazyRuntimeIdentityResolver {
    workspace_name: WorkspaceName,
    request_principal: UserPrincipal,
    source_identity_bindings: Arc<SourceIdentityBindingsSnapshot>,
    identity_manager: IdentityManager,
}

impl fmt::Debug for LazyRuntimeIdentityResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyRuntimeIdentityResolver")
            .field("workspace_name", &self.workspace_name)
            .field("source_identity_bindings", &self.source_identity_bindings)
            .field("identity_manager", &self.identity_manager)
            .finish_non_exhaustive()
    }
}

impl LazyRuntimeIdentityResolver {
    async fn resolve_runtime_identity(
        &self,
        identity: &RequestIdentityResolutionContext,
    ) -> Result<Arc<dyn crate::identity::RuntimeSourceIdentity>, AppError> {
        SourceName::parse(identity.source_name())
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let binding = identity_binding_for_surface(
            &self.source_identity_bindings,
            identity.source_name(),
            identity.surface_id(),
        )?;
        let subject =
            SourceIdentitySubject::for_binding_owner(binding.owner, &self.request_principal);
        let selection = self
            .identity_manager
            .resolve_source_identity_selection(SourceIdentitySelectionRequest {
                workspace_name: self.workspace_name.as_str().to_string(),
                request_principal: self.request_principal.clone(),
                subject: subject.clone(),
                source_name: identity.source_name().to_string(),
                surface_id: identity.surface_id().to_string(),
                binding: binding.clone(),
            })
            .await?;
        let selected_requirements = select_identity_requirements_for_selection(
            identity.source_name(),
            identity.surface_id(),
            identity.identity_requirements(),
            &selection,
        )?;
        let runtime_identity = self
            .identity_manager
            .resolve_source_identity(SourceIdentityResolutionRequest {
                workspace_name: self.workspace_name.as_str().to_string(),
                request_principal: self.request_principal.clone(),
                subject,
                source_name: identity.source_name().to_string(),
                surface_id: identity.surface_id().to_string(),
                binding,
                selection,
                identity_requirements: selected_requirements.clone(),
            })
            .await?;
        let selected_context = RequestIdentityResolutionContext::new(
            identity.source_name().to_string(),
            identity.surface_id().to_string(),
            selected_requirements,
        );
        if !selected_context.accepts_identity(
            runtime_identity.identity_spec_id(),
            runtime_identity.audience(),
        ) {
            return Err(AppError::FailedPrecondition(format!(
                "resolved identity does not satisfy selected identity requirements for source '{}' surface '{}'",
                identity.source_name(),
                identity.surface_id()
            )));
        }
        Ok(runtime_identity)
    }
}

#[tonic::async_trait]
impl RequestIdentityResolver for LazyRuntimeIdentityResolver {
    async fn resolve_identity_headers(
        &self,
        identity: &RequestIdentityResolutionContext,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<
        Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
        RequestIdentityResolverError,
    > {
        let runtime_identity = self
            .resolve_runtime_identity(identity)
            .await
            .map_err(|error| app_error_to_identity_resolver_error(&error))?;
        runtime_identity
            .resolve_headers(identity, request, resolved_inputs)
            .await
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

fn runtime_test_queries_for_source(
    authored_source_name: &str,
    runtime_source_name: &str,
    test_queries: &[String],
) -> Vec<String> {
    if authored_source_name == runtime_source_name {
        return test_queries.to_vec();
    }

    test_queries
        .iter()
        .map(|sql| {
            rewrite_test_query_source_name(sql, authored_source_name, runtime_source_name)
                .unwrap_or_else(|| sql.clone())
        })
        .collect()
}

fn rewrite_test_query_source_name(
    sql: &str,
    authored_source_name: &str,
    runtime_source_name: &str,
) -> Option<String> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    let mut rewriter = SourceNameTestQueryRewriter {
        authored_source_name,
        runtime_source_name,
        rewritten: false,
    };
    if statements.visit(&mut rewriter).is_break() {
        return None;
    }
    if rewriter.rewritten {
        Some(
            statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        )
    } else {
        None
    }
}

struct SourceNameTestQueryRewriter<'a> {
    authored_source_name: &'a str,
    runtime_source_name: &'a str,
    rewritten: bool,
}

impl SourceNameTestQueryRewriter<'_> {
    fn runtime_source_ident(&self) -> Ident {
        Ident::with_quote('"', self.runtime_source_name)
    }

    fn rewrite_ident(&mut self, ident: &mut Ident) {
        if ident.value == self.authored_source_name {
            *ident = self.runtime_source_ident();
            self.rewritten = true;
        }
    }

    fn rewrite_object_name_source_prefix(
        &mut self,
        object_name: &mut ObjectName,
        minimum_parts: usize,
    ) {
        if object_name.0.len() < minimum_parts {
            return;
        }
        let Some(ObjectNamePart::Identifier(source)) = object_name.0.first_mut() else {
            return;
        };
        self.rewrite_ident(source);
    }

    fn rewrite_select_item_qualified_wildcards(&mut self, set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                for item in &mut select.projection {
                    let SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(object_name),
                        _,
                    ) = item
                    else {
                        continue;
                    };
                    self.rewrite_object_name_source_prefix(object_name, 2);
                }
            }
            SetExpr::Query(query) => self.rewrite_select_item_qualified_wildcards(&mut query.body),
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_select_item_qualified_wildcards(left);
                self.rewrite_select_item_qualified_wildcards(right);
            }
            SetExpr::Values(_)
            | SetExpr::Insert(_)
            | SetExpr::Update(_)
            | SetExpr::Delete(_)
            | SetExpr::Merge(_)
            | SetExpr::Table(_) => {}
        }
    }
}

impl VisitorMut for SourceNameTestQueryRewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.rewrite_select_item_qualified_wildcards(&mut query.body);
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        self.rewrite_object_name_source_prefix(relation, 2);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::CompoundIdentifier(idents) if idents.len() >= 3 => {
                if let Some(source) = idents.first_mut() {
                    self.rewrite_ident(source);
                }
            }
            Expr::QualifiedWildcard(object_name, _) => {
                self.rewrite_object_name_source_prefix(object_name, 2);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
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

fn select_identity_requirements_for_selection(
    source_name: &str,
    surface_id: &str,
    requirements: &IdentityRequirements,
    selection: &SourceIdentitySelection,
) -> Result<coral_spec::v4::IdentityRequirements, AppError> {
    if let Some(accepted_identity) = selection.accepted_identity.as_deref() {
        if let Some(accepted) = requirements
            .accepts
            .iter()
            .find(|accepted| accepted.id == accepted_identity)
        {
            return Ok(coral_spec::v4::IdentityRequirements {
                accepts: vec![accepted.clone()],
            });
        }
        return Err(AppError::FailedPrecondition(format!(
            "source '{source_name}' surface '{}' binds identity '{}' to unknown accepted_identity '{}'",
            surface_id, selection.identity, accepted_identity
        )));
    }

    if requirements.accepts.len() == 1 {
        return Ok(requirements.clone());
    }
    Err(AppError::FailedPrecondition(format!(
        "source '{source_name}' surface '{}' has multiple accepted identities; configure accepted_identity for binding '{}'",
        surface_id, selection.identity
    )))
}

fn app_error_to_identity_resolver_error(error: &AppError) -> RequestIdentityResolverError {
    let detail = error.to_string();
    match error {
        AppError::InvalidInput(_) => RequestIdentityResolverError::invalid_input(detail),
        _ => RequestIdentityResolverError::failed_precondition(detail),
    }
}

#[derive(Clone, Copy)]
enum QueryOperation {
    ExecuteSql,
    ExplainSql,
}

impl QueryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ExplainSql => "explain_sql",
        }
    }
}

async fn run_query_operation<T, Fut, RowCount>(
    operation: QueryOperation,
    workspace_name: &WorkspaceName,
    sql: &str,
    query: Fut,
    row_count: RowCount,
) -> Result<T, QueryManagerError>
where
    Fut: Future<Output = Result<T, QueryManagerError>>,
    RowCount: FnOnce(&T) -> Option<u64>,
{
    let started_at = Instant::now();
    let query_span = create_query_span(operation, workspace_name, sql);
    let result = query.instrument(query_span.clone()).await;

    let metrics = crate::telemetry::metrics::metrics();
    let status = crate::telemetry::metrics::status_attr(result.is_ok());
    let attributes = [status, KeyValue::new("operation", operation.as_str())];
    metrics.count.add(1, &attributes);
    metrics
        .duration
        .record(started_at.elapsed().as_secs_f64(), &attributes);

    if let Ok(value) = &result {
        query_span.record("status", "ok");
        query_span.set_status(OtelStatus::Ok);
        if let Some(row_count) = row_count(value) {
            query_span.record("row_count", row_count);
            metrics.rows.record(row_count, &attributes);
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
) -> tracing::Span {
    let operation = operation.as_str();
    tracing::info_span!(
        "coral.query",
        otel.name = "coral.query",
        operation = operation,
        workspace = %workspace_name.as_str(),
        sql = %sql,
        row_count = tracing::field::Empty,
        status = tracing::field::Empty,
        error.kind = tracing::field::Empty,
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
    )
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
        AppError::SourceNotFound(_) => "SOURCE_NOT_FOUND",
        AppError::IdentitySpecNotFound(_) => "IDENTITY_SPEC_NOT_FOUND",
        AppError::IdentityNotFound(_) => "IDENTITY_NOT_FOUND",
        AppError::InvalidInput(_) => "INVALID_INPUT",
        AppError::FailedPrecondition(_) | AppError::SourceUnservable(_) => "FAILED_PRECONDITION",
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
        EngineExtensions, QueryExecution, RequestIdentityResolutionContext,
        RequestIdentityResolverError, SourceInputResolutionContext, SourceInputResolver,
        SourceInputResolverError,
    };
    use coral_spec::parse_source_manifest_yaml;
    use reqwest::header::{HeaderName, HeaderValue};
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::credentials::{CredentialStorageKind, CredentialStoragePreference, CredentialStore};
    use crate::features::{Features, dsl_v4_features};
    use crate::identity::{
        RuntimeSourceIdentity, SourceIdentityBinding, SourceIdentityOwner, SourceIdentityProvider,
        SourceIdentityResolutionRequest, SourceIdentitySelection, SourceIdentitySelectionRequest,
        SourceIdentitySubject, UserPrincipal,
    };
    use crate::sources::manager::{ImportSourceCommand, SourceBindings, SourceManager};
    use crate::sources::materialization::sha256_hex;
    use crate::sources::model::SourceOrigin;

    struct QueryManagerFixture {
        _temp: TempDir,
        layout: AppStateLayout,
        manager: QueryManager,
    }

    fn query_manager_with(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
    ) -> QueryManagerFixture {
        query_manager_with_features(runtime_context, providers, Features::default())
    }

    fn query_manager_with_features(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        features: Features,
    ) -> QueryManagerFixture {
        query_manager_with_features_and_identities(runtime_context, providers, Vec::new(), features)
    }

    fn query_manager_with_features_and_identities(
        runtime_context: QueryRuntimeContext,
        providers: Vec<Arc<dyn EngineExtensionsProvider>>,
        identity_providers: Vec<Arc<dyn SourceIdentityProvider>>,
        features: Features,
    ) -> QueryManagerFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let manager = QueryManager::new_with_features(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            runtime_context,
            layout.clone(),
            providers,
            identity_providers,
            features,
        );
        QueryManagerFixture {
            _temp: temp,
            layout,
            manager,
        }
    }

    /// Builds a v4-enabled query manager backed by a [`TestIdentityProvider`]
    /// that selects `selection_identity` for user-owned bindings.
    fn identity_fixture(
        selection_identity: &str,
    ) -> (QueryManagerFixture, ObservedIdentityRequestCell) {
        let observed = Arc::new(Mutex::new(None));
        let fixture = query_manager_with_features_and_identities(
            QueryRuntimeContext::default(),
            Vec::new(),
            vec![Arc::new(TestIdentityProvider::selecting(
                &observed,
                selection_identity,
            ))],
            dsl_v4_features(),
        );
        (fixture, observed)
    }

    fn local_request_principal() -> UserPrincipal {
        UserPrincipal::local()
    }

    fn user_request_principal(user_id: &str) -> UserPrincipal {
        UserPrincipal::for_user(user_id).expect("user")
    }

    async fn run_sql(
        fixture: &QueryManagerFixture,
        request_principal: &UserPrincipal,
        sql: &str,
    ) -> Result<QueryExecution, QueryManagerError> {
        fixture
            .manager
            .execute_sql_with_context(&WorkspaceName::default(), request_principal, sql)
            .await
    }

    /// Runs the canonical `github_v4_identity` query as request user `saul`.
    async fn query_identity_issues(
        fixture: &QueryManagerFixture,
    ) -> Result<QueryExecution, QueryManagerError> {
        run_sql(
            fixture,
            &user_request_principal("saul"),
            "SELECT id, title FROM github_v4_identity.issues",
        )
        .await
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

    type ObservedIdentityRequestCell = Arc<Mutex<Option<ObservedIdentityRequest>>>;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ObservedIdentityRequest {
        workspace_name: String,
        subject: SourceIdentitySubject,
        source_name: String,
        surface_id: String,
        identity: String,
        owner: SourceIdentityOwner,
    }

    /// Asserts the provider observed one identity resolution request for the
    /// `github_v4_identity` source's `rest` surface in the default workspace.
    #[track_caller]
    fn assert_observed_identity(
        observed: &ObservedIdentityRequestCell,
        subject: SourceIdentitySubject,
        identity: &str,
        owner: SourceIdentityOwner,
    ) {
        assert_eq!(
            *observed.lock().expect("observed identity request"),
            Some(ObservedIdentityRequest {
                workspace_name: "default".to_string(),
                subject,
                source_name: "github_v4_identity".to_string(),
                surface_id: "rest".to_string(),
                identity: identity.to_string(),
                owner,
            })
        );
    }

    #[derive(Debug)]
    struct TestIdentityProvider {
        observed: ObservedIdentityRequestCell,
        selection: SourceIdentitySelection,
    }

    impl TestIdentityProvider {
        fn selecting(observed: &ObservedIdentityRequestCell, identity: &str) -> Self {
            Self {
                observed: Arc::clone(observed),
                selection: SourceIdentitySelection::new(
                    identity,
                    Some("github-rest-read".to_string()),
                )
                .expect("selection"),
            }
        }
    }

    #[tonic::async_trait]
    impl SourceIdentityProvider for TestIdentityProvider {
        async fn resolve_source_identity_selection(
            &self,
            request: &SourceIdentitySelectionRequest,
        ) -> Result<Option<SourceIdentitySelection>, AppError> {
            if request.subject.user_id().is_some() {
                Ok(Some(self.selection.clone()))
            } else {
                Ok(None)
            }
        }

        async fn resolve_source_identity(
            &self,
            request: &SourceIdentityResolutionRequest,
        ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
            *self.observed.lock().expect("observed identity request") =
                Some(ObservedIdentityRequest {
                    workspace_name: request.workspace_name.clone(),
                    subject: request.subject.clone(),
                    source_name: request.source_name.clone(),
                    surface_id: request.surface_id.clone(),
                    identity: request.selection.identity.clone(),
                    owner: request.binding.owner,
                });
            let identity: Arc<dyn RuntimeSourceIdentity> = match request.selection.identity.as_str()
            {
                "github_local" | "github_workspace" => {
                    Arc::new(TestRuntimeIdentity::new("github_oauth", "github.com"))
                }
                "gitlab_wrong" => Arc::new(TestRuntimeIdentity::new("gitlab_oauth", "gitlab.com")),
                _ => return Ok(None),
            };
            Ok(Some(identity))
        }
    }

    #[derive(Debug)]
    struct TestRuntimeIdentity {
        identity_spec_id: &'static str,
        audience: BTreeMap<String, Value>,
    }

    impl TestRuntimeIdentity {
        fn new(identity_spec_id: &'static str, host: &str) -> Self {
            Self {
                identity_spec_id,
                audience: BTreeMap::from([("host".to_string(), json!(host))]),
            }
        }
    }

    #[tonic::async_trait]
    impl RuntimeSourceIdentity for TestRuntimeIdentity {
        fn identity_spec_id(&self) -> &str {
            self.identity_spec_id
        }

        fn audience(&self) -> &BTreeMap<String, Value> {
            &self.audience
        }

        async fn resolve_headers(
            &self,
            _identity: &RequestIdentityResolutionContext,
            _request: &reqwest::Request,
            _resolved_inputs: &BTreeMap<String, String>,
        ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
            Ok(vec![(
                HeaderName::from_static("x-coral-identity"),
                HeaderValue::from_static("member-token"),
            )])
        }
    }

    /// Ensures the fixture layout exists and returns a v4-enabled source
    /// manager that shares the fixture's stores.
    fn v4_source_manager(fixture: &QueryManagerFixture) -> SourceManager {
        fixture.layout.ensure().expect("ensure layout");
        SourceManager::new_with_features(
            fixture.manager.config_store.clone(),
            fixture.manager.credential_manager.clone(),
            fixture.layout.clone(),
            dsl_v4_features(),
        )
    }

    async fn mount_issues_endpoint(server: &MockServer, require_member_token: bool, body: Value) {
        let mut mock = Mock::given(method("GET")).and(path("/issues"));
        if require_member_token {
            mock = mock.and(header("x-coral-identity", "member-token"));
        }
        mock.respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    fn issues_openapi_yaml(server: &MockServer) -> String {
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
        )
    }

    const GITHUB_REST_ACCEPTS_YAML: &str = r"        - id: github-rest-read
          identity_specs:
            - github_oauth
            - github_pat
          audience:
            host: github.com
";

    struct ImportV4SourceOptions<'a> {
        identity_accepts_yaml: Option<&'a str>,
        identity_bindings: BTreeMap<String, SourceIdentityBinding>,
        test_queries_yaml: Option<&'a str>,
    }

    /// Imports a DSL v4 source whose `rest` surface targets the mock server,
    /// optionally declaring identity requirements, then removes the authored
    /// descriptor so queries must run from materialized artifacts.
    fn import_v4_source(
        source_manager: &SourceManager,
        workspace_name: &WorkspaceName,
        source_name: &str,
        server: &MockServer,
        identity_accepts_yaml: Option<&str>,
        identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    ) {
        import_v4_source_as(
            source_manager,
            workspace_name,
            source_name,
            source_name,
            server,
            identity_accepts_yaml,
            identity_bindings,
        );
    }

    fn import_v4_source_as(
        source_manager: &SourceManager,
        workspace_name: &WorkspaceName,
        source_spec_id: &str,
        source_name: &str,
        server: &MockServer,
        identity_accepts_yaml: Option<&str>,
        identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    ) {
        import_v4_source_with_options(
            source_manager,
            workspace_name,
            source_spec_id,
            source_name,
            server,
            ImportV4SourceOptions {
                identity_accepts_yaml,
                identity_bindings,
                test_queries_yaml: None,
            },
        );
    }

    fn import_v4_source_with_options(
        source_manager: &SourceManager,
        workspace_name: &WorkspaceName,
        source_spec_id: &str,
        source_name: &str,
        server: &MockServer,
        options: ImportV4SourceOptions<'_>,
    ) {
        let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        let openapi_yaml = issues_openapi_yaml(server);
        let openapi_sha256 = sha256_hex(openapi_yaml.as_bytes());
        std::fs::write(&openapi_file, openapi_yaml).expect("write OpenAPI fixture");
        let identity_requirements_yaml = options
            .identity_accepts_yaml
            .map(|accepts| format!("    identity_requirements:\n      accepts:\n{accepts}"))
            .unwrap_or_default();
        let test_queries_yaml = options.test_queries_yaml.unwrap_or_default();
        source_manager
            .import_source(
                workspace_name,
                &ImportSourceCommand {
                    source_name: (source_name != source_spec_id)
                        .then(|| SourceName::parse(source_name).expect("source alias")),
                    source_spec_id: Some(SourceName::parse(source_spec_id).expect("source spec")),
                    manifest_yaml: format!(
                        r"
name: {source_spec_id}
dsl_version: 4
{test_queries_yaml}surfaces:
  - id: rest
    type: openapi
    file: {}
    sha256: {}
{identity_requirements_yaml}",
                        openapi_file.display(),
                        openapi_sha256
                    ),
                    bindings: SourceBindings::default(),
                    identity_bindings: options.identity_bindings,
                    replace_identity_bindings: false,
                },
            )
            .expect("import v4 source");
        std::fs::remove_file(&openapi_file).expect("remove authored descriptor after import");
    }

    /// Imports the `github_v4_identity` source with the standard GitHub accept
    /// branch and the given identity binding into the default workspace.
    fn import_identity_v4_source(
        fixture: &QueryManagerFixture,
        server: &MockServer,
        identity: &str,
        owner: SourceIdentityOwner,
    ) {
        import_v4_source(
            &v4_source_manager(fixture),
            &WorkspaceName::default(),
            "github_v4_identity",
            server,
            Some(GITHUB_REST_ACCEPTS_YAML),
            source_identity_bindings(identity, owner, "github-rest-read"),
        );
    }

    fn source_identity_bindings(
        identity: &str,
        owner: SourceIdentityOwner,
        accepted_identity: &str,
    ) -> BTreeMap<String, SourceIdentityBinding> {
        let binding = match owner {
            SourceIdentityOwner::User => SourceIdentityBinding::user_owned(),
            SourceIdentityOwner::Workspace => SourceIdentityBinding::workspace_owned(
                identity,
                Some(accepted_identity.to_string()),
            )
            .expect("workspace identity binding"),
        };
        BTreeMap::from([("rest".to_string(), binding)])
    }

    fn clear_identity_bindings(fixture: &QueryManagerFixture, source_name: &SourceName) {
        let workspace_name = WorkspaceName::default();
        let mut installed = fixture
            .manager
            .config_store
            .get_source(&workspace_name, source_name)
            .expect("installed source");
        installed.identity_bindings.clear();
        fixture
            .manager
            .config_store
            .upsert_source(&workspace_name, installed)
            .expect("clear identity binding");
    }

    fn imported_source(source_name: &SourceName) -> InstalledSource {
        InstalledSource {
            name: source_name.clone(),
            source_spec_id: None,
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            identity_bindings: BTreeMap::new(),
            origin: SourceOrigin::Imported,
        }
    }

    /// Ensures the fixture layout, writes `manifest_yaml` as the installed
    /// manifest for `source_name` in the default workspace, and returns the
    /// parsed source name.
    fn write_manifest(
        fixture: &QueryManagerFixture,
        source_name: &str,
        manifest_yaml: &str,
    ) -> SourceName {
        fixture.layout.ensure().expect("ensure layout");
        let source_name = SourceName::parse(source_name).expect("source name");
        let manifest_path = fixture
            .layout
            .manifest_file(&WorkspaceName::default(), &source_name);
        std::fs::create_dir_all(manifest_path.parent().expect("manifest parent"))
            .expect("create source dir");
        std::fs::write(&manifest_path, manifest_yaml).expect("write manifest");
        source_name
    }

    fn persist_source(fixture: &QueryManagerFixture, source: InstalledSource) {
        fixture
            .manager
            .config_store
            .upsert_source(&WorkspaceName::default(), source)
            .expect("persist source");
    }

    /// Stores `material` as the source's file-backed credential set in the
    /// default workspace.
    fn persist_file_secrets(
        fixture: &QueryManagerFixture,
        source_name: &SourceName,
        material: &[(&str, &str)],
    ) {
        let material = material
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        fixture
            .manager
            .credential_manager
            .replace_material(
                &WorkspaceName::default(),
                &CredentialSetId::for_source(source_name),
                CredentialStorageKind::File,
                &material,
            )
            .expect("persist secret material");
    }

    #[tokio::test]
    async fn lazy_identity_resolver_uses_loaded_source_binding_snapshot() {
        let observed = Arc::new(Mutex::new(None));
        let resolver = LazyRuntimeIdentityResolver {
            workspace_name: WorkspaceName::default(),
            request_principal: user_request_principal("saul"),
            source_identity_bindings: Arc::new(BTreeMap::from([(
                "github_v4_identity".to_string(),
                source_identity_bindings(
                    "github_workspace",
                    SourceIdentityOwner::Workspace,
                    "github-rest-read",
                ),
            )])),
            identity_manager: IdentityManager::new(vec![Arc::new(
                TestIdentityProvider::selecting(&observed, "github_local"),
            )]),
        };
        let context = RequestIdentityResolutionContext::new(
            "github_v4_identity",
            "rest",
            coral_spec::v4::IdentityRequirements {
                accepts: vec![coral_spec::v4::AcceptedIdentityRequirement {
                    id: "github-rest-read".to_string(),
                    identity_specs: vec!["github_oauth".to_string()],
                    audience: BTreeMap::from([("host".to_string(), json!("github.com"))]),
                }],
            },
        );

        let identity = resolver
            .resolve_runtime_identity(&context)
            .await
            .expect("resolve identity from snapshot");

        assert_eq!(identity.identity_spec_id(), "github_oauth");
        assert_observed_identity(
            &observed,
            SourceIdentitySubject::Workspace,
            "github_workspace",
            SourceIdentityOwner::Workspace,
        );
    }

    #[tokio::test]
    async fn runtime_config_preserves_app_owned_body_capture_max_bytes() {
        let fixture = query_manager_with(
            QueryRuntimeContext::default().with_body_capture_max_bytes(Some(42)),
            Vec::new(),
        );

        let runtime = fixture
            .manager
            .runtime_config(
                &WorkspaceName::default(),
                &local_request_principal(),
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

    #[test]
    fn load_query_source_passes_present_optional_secrets_to_runtime() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new());
        let workspace_name = WorkspaceName::default();
        let source_name = write_manifest(
            &fixture,
            "optional_auth",
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
        );
        let mut source = imported_source(&source_name);
        source.version = Some("0.1.0".to_string());
        source.secrets = vec!["API_KEY".to_string(), "OAUTH_TOKEN".to_string()];
        source.credential_storage = Some(CredentialStorageKind::File);
        persist_source(&fixture, source.clone());
        persist_file_secrets(&fixture, &source_name, &[("OAUTH_TOKEN", "oauth-token")]);

        let loaded = fixture
            .manager
            .load_query_source(&workspace_name, &source)
            .expect("optional secret should load when present");

        assert_eq!(
            loaded.query_source.secrets(),
            &BTreeMap::from([("OAUTH_TOKEN".to_string(), "oauth-token".to_string())])
        );
    }

    #[tokio::test]
    async fn installed_v4_source_queries_through_app_assembled_runtime_component() {
        let server = MockServer::start().await;
        mount_issues_endpoint(
            &server,
            false,
            json!([{"id": 1, "title": "Generated runtime package"}]),
        )
        .await;

        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        import_v4_source(
            &v4_source_manager(&fixture),
            &WorkspaceName::default(),
            "github_v4_query",
            &server,
            None,
            BTreeMap::new(),
        );

        let execution = run_sql(
            &fixture,
            &local_request_principal(),
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
    async fn aliased_v4_source_queries_through_installed_source_name() {
        let server = MockServer::start().await;
        mount_issues_endpoint(
            &server,
            false,
            json!([{"id": 7, "title": "Aliased runtime package"}]),
        )
        .await;

        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        import_v4_source_as(
            &v4_source_manager(&fixture),
            &WorkspaceName::default(),
            "github_v4_query",
            "github_alias",
            &server,
            None,
            BTreeMap::new(),
        );

        let execution = run_sql(
            &fixture,
            &local_request_principal(),
            "SELECT id, title FROM github_alias.issues",
        )
        .await
        .expect("query executes through installed alias");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 7, "title": "Aliased runtime package"})]
        );
    }

    #[tokio::test]
    async fn aliased_v4_source_validates_authored_test_queries_through_installed_source_name() {
        let server = MockServer::start().await;
        mount_issues_endpoint(
            &server,
            false,
            json!([{"id": 8, "title": "Aliased validation"}]),
        )
        .await;

        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        import_v4_source_with_options(
            &v4_source_manager(&fixture),
            &WorkspaceName::default(),
            "github_v4_query",
            "github-alias",
            &server,
            ImportV4SourceOptions {
                identity_accepts_yaml: None,
                identity_bindings: BTreeMap::new(),
                test_queries_yaml: Some(
                    r"test_queries:
  - SELECT github_v4_query.issues.id, github_v4_query.issues.title FROM github_v4_query.issues
  - SELECT github_v4_query.issues.* FROM github_v4_query.issues
",
                ),
            },
        );

        let source_name = SourceName::parse("github-alias").expect("source alias");
        let validation = fixture
            .manager
            .validate_source(
                &WorkspaceName::default(),
                &local_request_principal(),
                &source_name,
            )
            .await
            .expect("aliased source should validate authored test query");

        assert_eq!(validation.report.query_tests.len(), 2);
        let qualified_column_query_test = validation
            .report
            .query_tests
            .first()
            .expect("query test should be present");
        assert!(
            qualified_column_query_test.passed(),
            "query test failed: {qualified_column_query_test:?}"
        );
        assert_eq!(
            qualified_column_query_test.sql(),
            r#"SELECT "github-alias".issues.id, "github-alias".issues.title FROM "github-alias".issues"#
        );
        assert_eq!(qualified_column_query_test.row_count(), Some(1));

        let qualified_wildcard_query_test = validation
            .report
            .query_tests
            .get(1)
            .expect("qualified wildcard query test should be present");
        assert!(
            qualified_wildcard_query_test.passed(),
            "query test failed: {qualified_wildcard_query_test:?}"
        );
        assert_eq!(
            qualified_wildcard_query_test.sql(),
            r#"SELECT "github-alias".issues.* FROM "github-alias".issues"#
        );
        assert_eq!(qualified_wildcard_query_test.row_count(), Some(1));
    }

    #[tokio::test]
    async fn identity_backed_v4_source_uses_workspace_binding_for_request_user() {
        let server = MockServer::start().await;
        mount_issues_endpoint(&server, true, json!([{"id": 9, "title": "Bound identity"}])).await;

        let (fixture, observed) = identity_fixture("github_local");
        import_identity_v4_source(&fixture, &server, "github_local", SourceIdentityOwner::User);

        let execution = query_identity_issues(&fixture)
            .await
            .expect("query executes");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 9, "title": "Bound identity"})]
        );
        assert_observed_identity(
            &observed,
            SourceIdentitySubject::User("saul".to_string()),
            "github_local",
            SourceIdentityOwner::User,
        );
    }

    #[tokio::test]
    async fn identity_backed_v4_source_uses_workspace_owned_binding_without_request_user() {
        let server = MockServer::start().await;
        mount_issues_endpoint(
            &server,
            true,
            json!([{"id": 10, "title": "Workspace identity"}]),
        )
        .await;

        let (fixture, observed) = identity_fixture("github_local");
        import_identity_v4_source(
            &fixture,
            &server,
            "github_workspace",
            SourceIdentityOwner::Workspace,
        );

        let execution = query_identity_issues(&fixture)
            .await
            .expect("query executes");

        assert_eq!(
            execution_to_rows(&execution),
            vec![json!({"id": 10, "title": "Workspace identity"})]
        );
        assert_observed_identity(
            &observed,
            SourceIdentitySubject::Workspace,
            "github_workspace",
            SourceIdentityOwner::Workspace,
        );
    }

    #[tokio::test]
    async fn identity_backed_v4_source_rejects_identity_matching_unselected_accept_branch() {
        let server = MockServer::start().await;
        mount_issues_endpoint(
            &server,
            true,
            json!([{"id": 11, "title": "Wrong identity branch"}]),
        )
        .await;

        let (fixture, observed) = identity_fixture("gitlab_wrong");
        import_v4_source(
            &v4_source_manager(&fixture),
            &WorkspaceName::default(),
            "github_v4_identity",
            &server,
            Some(
                r"        - id: github-rest-read
          identity_specs:
            - github_oauth
            - github_pat
          audience:
            host: github.com
        - id: gitlab-project-read
          identity_specs:
            - gitlab_oauth
          audience:
            host: gitlab.com
",
            ),
            source_identity_bindings(
                "gitlab_wrong",
                SourceIdentityOwner::User,
                "github-rest-read",
            ),
        );

        let error = query_identity_issues(&fixture)
            .await
            .expect_err("identity matching only the unselected accepted branch should fail");

        let message = query_error_message(&error);
        assert!(
            message.contains("selected identity requirements"),
            "unexpected error: {message}"
        );
        assert_observed_identity(
            &observed,
            SourceIdentitySubject::User("saul".to_string()),
            "gitlab_wrong",
            SourceIdentityOwner::User,
        );
    }

    #[tokio::test]
    async fn identity_backed_v4_source_fails_without_workspace_binding() {
        let server = MockServer::start().await;
        let (fixture, _observed) = identity_fixture("github_local");
        import_identity_v4_source(&fixture, &server, "github_local", SourceIdentityOwner::User);
        let source_name = SourceName::parse("github_v4_identity").expect("source name");
        clear_identity_bindings(&fixture, &source_name);

        let error = query_identity_issues(&fixture)
            .await
            .expect_err("missing identity binding should fail when the source is used");

        let message = query_error_message(&error);
        assert!(
            message.contains("has no workspace identity binding"),
            "unexpected error: {message}"
        );
    }

    #[tokio::test]
    async fn unrelated_query_ignores_broken_identity_backed_source() {
        let server = MockServer::start().await;
        let (fixture, observed) = identity_fixture("github_local");
        import_identity_v4_source(&fixture, &server, "github_local", SourceIdentityOwner::User);
        let source_name = SourceName::parse("github_v4_identity").expect("source name");
        clear_identity_bindings(&fixture, &source_name);

        let execution = run_sql(
            &fixture,
            &user_request_principal("saul"),
            "SELECT 1 AS value",
        )
        .await
        .expect("unrelated query should not resolve broken source identity");

        assert_eq!(execution_to_rows(&execution), vec![json!({"value": 1})]);
        assert_eq!(
            *observed.lock().expect("observed identity request"),
            None,
            "unrelated query should not resolve source identities"
        );
    }

    #[test]
    fn load_query_source_rejects_v4_when_dsl_v4_feature_is_disabled() {
        let fixture = query_manager_with(QueryRuntimeContext::default(), Vec::new());
        let workspace_name = WorkspaceName::default();
        let source_name = write_manifest(
            &fixture,
            "github_v4_disabled",
            r"
name: github_v4_disabled
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
",
        );
        let source = imported_source(&source_name);

        let error = fixture
            .manager
            .load_query_source(&workspace_name, &source)
            .expect_err("disabled v4 feature should reject query loading");

        assert!(
            error.to_string().contains("dsl_v4"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn load_query_sources_fails_closed_for_missing_v4_materialization() {
        let fixture = query_manager_with_features(
            QueryRuntimeContext::default(),
            Vec::new(),
            dsl_v4_features(),
        );
        let workspace_name = WorkspaceName::default();
        let source_name = write_manifest(
            &fixture,
            "github_v4_missing_artifacts",
            r"
name: github_v4_missing_artifacts
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
",
        );
        persist_source(&fixture, imported_source(&source_name));

        let error = fixture
            .manager
            .load_query_sources(&workspace_name)
            .expect_err("missing materialization should fail closed");

        assert!(
            matches!(error, AppError::SourceUnservable(_)),
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
        let mut source = imported_source(&SourceName::parse("github").expect("source name"));
        source.secrets = vec!["GITHUB_TOKEN".to_string()];
        source.credential_storage = Some(CredentialStorageKind::Keychain);
        source.origin = SourceOrigin::Bundled;
        config_store
            .upsert_source(&workspace_name, source)
            .expect("persist source");
        let credential_store = CredentialStore::with_unavailable_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Keychain,
        );
        let manager = QueryManager::new(
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
        let mut installed = imported_source(&source_name);
        installed.secrets = vec!["API_TOKEN".to_string()];
        installed.credential_storage = Some(CredentialStorageKind::File);
        installed.origin = SourceOrigin::Bundled;
        persist_source(&fixture, installed);
        persist_file_secrets(&fixture, &source_name, &[("API_TOKEN", "stored-token")]);
        let source = secured_messages_query_source();
        let runtime = fixture
            .manager
            .runtime_config(
                &workspace_name,
                &local_request_principal(),
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
}
