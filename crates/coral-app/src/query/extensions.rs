//! App-owned selection of optional engine extensions for query runtime builds.

use std::sync::Arc;

use coral_auth_aws::AwsSigV4Authenticator;
use coral_engine::{EngineExtensions, QuerySource, RequestAuthenticator};

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

pub(crate) fn engine_extensions_for_providers(
    providers: &[Arc<dyn EngineExtensionsProvider>],
    selected_sources: &[QuerySource],
) -> EngineExtensions {
    let mut merged = EngineExtensions::default();
    for provider in providers {
        let extra = provider.extensions_for(selected_sources);
        merged.source_decorators.extend(extra.source_decorators);
        merged
            .query_result_observers
            .extend(extra.query_result_observers);
        merged
            .request_authenticators
            .extend(extra.request_authenticators);
    }
    merged
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
