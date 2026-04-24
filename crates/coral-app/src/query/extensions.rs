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

/// Provider that installs the built-in engine extensions shipped with
/// the open-source Coral distribution.
#[derive(Debug, Default)]
pub(crate) struct BuiltinEngineExtensionsProvider;

impl EngineExtensionsProvider for BuiltinEngineExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        let mut extensions = EngineExtensions::default();
        let authenticator = Arc::new(AwsSigV4Authenticator);
        extensions
            .request_authenticators
            .insert(authenticator.name().to_string(), authenticator);
        extensions
    }
}

pub(crate) fn compose_engine_extensions_providers(
    base: Arc<dyn EngineExtensionsProvider>,
    extra: Arc<dyn EngineExtensionsProvider>,
) -> Arc<dyn EngineExtensionsProvider> {
    Arc::new(CompositeEngineExtensionsProvider { base, extra })
}

struct CompositeEngineExtensionsProvider {
    base: Arc<dyn EngineExtensionsProvider>,
    extra: Arc<dyn EngineExtensionsProvider>,
}

impl EngineExtensionsProvider for CompositeEngineExtensionsProvider {
    fn extensions_for(&self, selected_sources: &[QuerySource]) -> EngineExtensions {
        let mut merged = self.base.extensions_for(selected_sources);
        let extra = self.extra.extensions_for(selected_sources);
        merged.source_decorators.extend(extra.source_decorators);
        merged
            .request_authenticators
            .extend(extra.request_authenticators);
        merged
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_engine::{RequestAuthenticator, RequestAuthenticatorError};
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

    #[test]
    fn noop_provider_installs_no_extensions() {
        let extensions = NoopEngineExtensionsProvider.extensions_for(&[]);

        assert!(extensions.source_decorators.is_empty());
        assert!(extensions.request_authenticators.is_empty());
    }

    #[test]
    fn builtin_provider_registers_aws_sigv4() {
        let extensions = BuiltinEngineExtensionsProvider.extensions_for(&[]);
        let authenticator = extensions
            .request_authenticators
            .get("aws_sigv4")
            .expect("builtin provider should register aws authenticator");

        assert_eq!(authenticator.name(), "aws_sigv4");
    }

    #[test]
    fn composed_provider_merges_base_and_extra_authenticators() {
        let provider = CompositeEngineExtensionsProvider {
            base: Arc::new(TestEngineExtensionsProvider {
                key: "base",
                name: "base",
            }),
            extra: Arc::new(TestEngineExtensionsProvider {
                key: "extra",
                name: "extra",
            }),
        };

        let extensions = provider.extensions_for(&[]);

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
}
