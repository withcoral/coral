//! App-owned selection of optional engine extensions for query runtime builds.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_auth_aws::AwsSigV4Authenticator;
use coral_engine::{
    EngineExtensions, QuerySource, RequestAuthenticator, SourceInputResolutionContext,
    SourceInputResolver, SourceInputResolverError,
};
use coral_spec::ManifestInputKind;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId, CredentialsError};
use crate::sources::SourceName;
use crate::state::ConfigStore;
use crate::workspaces::WorkspaceName;

/// App-layer provider that selects engine extensions for one runtime build.
pub trait EngineExtensionsProvider: Send + Sync {
    /// Returns the extensions to install for a runtime built from exactly
    /// `selected_sources`.
    ///
    /// Returned extensions may act on only a subset of those sources, but they
    /// must be valid for the full selected-source set of this runtime build.
    fn extensions_for(&self, selected_sources: &[QuerySource]) -> EngineExtensions;
}

/// Default OSS provider that installs no engine extensions.
#[derive(Debug, Default)]
pub struct NoopEngineExtensionsProvider;

impl EngineExtensionsProvider for NoopEngineExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        EngineExtensions::default()
    }
}

/// Provider that installs Coral's AWS engine extensions.
#[derive(Debug, Default)]
pub struct AwsEngineExtensionsProvider;

impl EngineExtensionsProvider for AwsEngineExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        let mut extensions = EngineExtensions::default();
        let authenticator = Arc::new(AwsSigV4Authenticator);
        extensions
            .request_authenticators
            .insert(authenticator.name().to_string(), authenticator);
        extensions
    }
}

#[derive(Clone)]
pub(crate) struct CredentialRefreshingInputResolver {
    workspace_name: WorkspaceName,
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    delegate: Option<Arc<dyn SourceInputResolver>>,
}

impl CredentialRefreshingInputResolver {
    pub(crate) fn new(
        workspace_name: WorkspaceName,
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        delegate: Option<Arc<dyn SourceInputResolver>>,
    ) -> Self {
        Self {
            workspace_name,
            config_store,
            credential_manager,
            delegate,
        }
    }
}

impl fmt::Debug for CredentialRefreshingInputResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialRefreshingInputResolver")
            .field("workspace_name", &self.workspace_name)
            .field("has_delegate", &self.delegate.is_some())
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl SourceInputResolver for CredentialRefreshingInputResolver {
    async fn resolve_inputs(
        &self,
        source: &SourceInputResolutionContext,
    ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
        let source_name = SourceName::parse(source.source_name())
            .map_err(|error| SourceInputResolverError::invalid_input(error.to_string()))?;
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let installed_source = self
            .config_store
            .get_source(&self.workspace_name, &source_name)
            .map_err(source_input_error)?;
        let material =
            if let Some(credential_storage) = installed_source.credential_storage_for_material() {
                self.credential_manager
                    .read_material_for_inputs(
                        &self.workspace_name,
                        &credential_set_id,
                        credential_storage,
                        source.declared_inputs(),
                    )
                    .await
                    .map_err(source_input_error)?
            } else {
                BTreeMap::new()
            };
        let mut resolved = resolve_from_material(source, &material);
        if let Some(delegate) = &self.delegate {
            let delegated_source = source_with_refreshed_secrets(source, &material);
            for (key, value) in delegate.resolve_inputs(&delegated_source).await? {
                resolved.entry(key).or_insert(value);
            }
        }
        let missing_secrets: Vec<String> = source
            .required_secret_names()
            .into_iter()
            .filter(|name| !resolved.contains_key(name))
            .collect();
        if let Some((first, rest)) = missing_secrets.split_first() {
            let detail = if rest.is_empty() {
                format!("secret '{first}'")
            } else {
                format!("secret '{first}' and {} other(s)", rest.len())
            };
            return Err(SourceInputResolverError::failed_precondition(format!(
                "source '{}' is missing {detail}",
                source.source_name()
            )));
        }
        Ok(resolved)
    }
}

fn resolve_from_material(
    source: &SourceInputResolutionContext,
    material: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    coral_spec::resolve_inputs(source.declared_inputs(), material, source.variables())
}

fn source_with_refreshed_secrets(
    source: &SourceInputResolutionContext,
    material: &BTreeMap<String, String>,
) -> SourceInputResolutionContext {
    let refreshed_secrets = source
        .declared_inputs()
        .iter()
        .filter(|input| input.kind == ManifestInputKind::Secret)
        .filter_map(|input| {
            material
                .get(&input.key)
                .cloned()
                .map(|value| (input.key.clone(), value))
        })
        .collect();
    source.with_secrets(refreshed_secrets)
}

pub(crate) fn engine_extensions_for_providers(
    providers: &[Arc<dyn EngineExtensionsProvider>],
    selected_sources: &[QuerySource],
) -> EngineExtensions {
    let mut merged = EngineExtensions::default();
    for provider in providers {
        let extra = provider.extensions_for(selected_sources);
        let EngineExtensions {
            source_decorators,
            query_result_observers,
            request_authenticators,
            source_input_resolver,
        } = extra;
        merged.source_decorators.extend(source_decorators);
        merged.query_result_observers.extend(query_result_observers);
        merged.request_authenticators.extend(request_authenticators);
        if source_input_resolver.is_some() {
            merged.source_input_resolver = source_input_resolver;
        }
    }
    merged
}

fn source_input_error(error: AppError) -> SourceInputResolverError {
    match error {
        AppError::InvalidInput(detail) => SourceInputResolverError::invalid_input(detail),
        AppError::FailedPrecondition(detail) | AppError::CredentialRefresh(detail) => {
            SourceInputResolverError::failed_precondition(detail)
        }
        AppError::Credentials(CredentialsError::Parse(detail)) => {
            SourceInputResolverError::failed_precondition(format!(
                "credential material could not be parsed: {detail}"
            ))
        }
        other => SourceInputResolverError::failed_precondition(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use coral_engine::{
        QueryResultObserver, QueryResultObserverError, RequestAuthenticator,
        RequestAuthenticatorError,
    };
    use reqwest::header::{HeaderName, HeaderValue};

    use super::*;

    #[derive(Debug)]
    struct TestAuthenticator {
        name: &'static str,
    }

    impl RequestAuthenticator for TestAuthenticator {
        fn name(&self) -> &str {
            self.name
        }

        fn authenticate(
            &self,
            _auth: &coral_spec::CustomAuthSpec,
            _request: &reqwest::Request,
            _resolved_inputs: &BTreeMap<String, String>,
        ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestAuthenticatorError> {
            Ok(Vec::new())
        }
    }

    struct TestObserver {
        name: &'static str,
    }

    impl QueryResultObserver for TestObserver {
        fn name(&self) -> &'static str {
            self.name
        }

        fn observe_result(
            &self,
            _sql: &str,
            _schema: &Schema,
            _batches: &[RecordBatch],
        ) -> Result<(), QueryResultObserverError> {
            Ok(())
        }
    }

    struct TestEngineExtensionsProvider {
        key: &'static str,
        name: &'static str,
    }

    impl EngineExtensionsProvider for TestEngineExtensionsProvider {
        fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
            let mut extensions = EngineExtensions::default();
            extensions.request_authenticators.insert(
                self.key.to_string(),
                Arc::new(TestAuthenticator { name: self.name }),
            );
            extensions
        }
    }

    struct TestObserverProvider {
        name: &'static str,
    }

    impl EngineExtensionsProvider for TestObserverProvider {
        fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
            let mut extensions = EngineExtensions::default();
            extensions
                .query_result_observers
                .push(Arc::new(TestObserver { name: self.name }));
            extensions
        }
    }

    #[test]
    fn noop_provider_installs_no_extensions() {
        let extensions = NoopEngineExtensionsProvider.extensions_for(&[]);

        assert!(extensions.source_decorators.is_empty());
        assert!(extensions.query_result_observers.is_empty());
        assert!(extensions.request_authenticators.is_empty());
    }

    #[test]
    fn aws_provider_registers_aws_sigv4() {
        let extensions = AwsEngineExtensionsProvider.extensions_for(&[]);
        let authenticator = extensions
            .request_authenticators
            .get("aws_sigv4")
            .expect("AWS provider should register aws authenticator");

        assert_eq!(authenticator.name(), "aws_sigv4");
    }

    #[test]
    fn provider_lists_merge_authenticators_in_call_order() {
        let providers = vec![
            Arc::new(TestEngineExtensionsProvider {
                key: "base",
                name: "base",
            }) as Arc<dyn EngineExtensionsProvider>,
            Arc::new(TestEngineExtensionsProvider {
                key: "extra",
                name: "extra",
            }),
        ];

        let extensions = engine_extensions_for_providers(&providers, &[]);

        let base_authenticator = extensions
            .request_authenticators
            .get("base")
            .expect("base provider should populate base key");
        let extra_authenticator = extensions
            .request_authenticators
            .get("extra")
            .expect("extra provider should populate extra key");

        assert_eq!(base_authenticator.name(), "base");
        assert_eq!(extra_authenticator.name(), "extra");
    }

    #[test]
    fn provider_lists_merge_query_result_observers_in_call_order() {
        let providers = vec![
            Arc::new(TestObserverProvider { name: "base" }) as Arc<dyn EngineExtensionsProvider>,
            Arc::new(TestObserverProvider { name: "extra" }),
        ];

        let extensions = engine_extensions_for_providers(&providers, &[]);
        let observer_names = extensions
            .query_result_observers
            .iter()
            .map(|observer| observer.name())
            .collect::<Vec<_>>();

        assert_eq!(observer_names, ["base", "extra"]);
    }
}
