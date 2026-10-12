//! Provider consent navigation is screened at issuance, not at browser connection time.

use crate::Error;
use coral_outbound::{AddressClass, classify_address, classify_host};
use std::{net::IpAddr, sync::Arc, time::Duration};

/// Evidence that the Provider authorization Module screened this initial navigation.
/// It is not persistent authority or a guarantee about browser DNS or later redirects.
/// Adapters serialize it immediately; reuse must read authorization through the Module again.
/// There is no raw-URL constructor or deserializer.
///
/// ```compile_fail
/// use coral_provider::account::ConsentNavigation;
/// let navigation: ConsentNavigation = "http://public.example/authorize".parse().unwrap();
/// ```
///
/// ```compile_fail
/// use coral_provider::account::ConsentNavigation;
/// let restored: ConsentNavigation = serde_json::from_str(r#""http://public.example/authorize""#).unwrap();
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct ConsentNavigation(String);

impl ConsentNavigation {
    /// Consumes fresh issuance evidence for immediate transport projection.
    #[must_use]
    pub fn into_url(self) -> String {
        self.0
    }
}

impl std::ops::Deref for ConsentNavigation {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for ConsentNavigation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConsentNavigation(<redacted>)")
    }
}

#[async_trait::async_trait]
pub(crate) trait Resolver: Send + Sync {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, ()>;
}
struct SystemResolver;
#[async_trait::async_trait]
impl Resolver for SystemResolver {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, ()> {
        tokio::net::lookup_host((host, 0))
            .await
            .map(|answers| answers.take(1025).map(|answer| answer.ip()).collect())
            .map_err(|_error| ())
    }
}

#[derive(Clone)]
pub(crate) struct Issuer {
    resolver: Arc<dyn Resolver>,
}
impl Default for Issuer {
    fn default() -> Self {
        Self {
            resolver: Arc::new(SystemResolver),
        }
    }
}
impl Issuer {
    #[cfg(test)]
    pub(crate) fn with_resolver(resolver: Arc<dyn Resolver>) -> Self {
        Self { resolver }
    }

    /// The deployment's normal resolver, shared with other issuance-time
    /// screening so a test can script every lookup through one boundary.
    pub(super) fn resolver(&self) -> &dyn Resolver {
        self.resolver.as_ref()
    }

    pub(super) async fn issue(&self, address: &str) -> Result<ConsentNavigation, Error> {
        self.issue_with_timeout(address, Duration::from_secs(3))
            .await
    }

    async fn issue_with_timeout(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<ConsentNavigation, Error> {
        let rejected = || Error::ConsentDestinationRejected;
        let url = url::Url::parse(address).map_err(|_error| rejected())?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(rejected());
        }
        let host = url.host().ok_or_else(rejected)?;
        let class = classify_host(&host);
        if matches!(class, AddressClass::LinkLocal | AddressClass::Special) {
            return Err(rejected()); // Blocked literals are forbidden under BOTH schemes.
        }
        if url.scheme() == "http" {
            match host {
                url::Host::Domain(name) => {
                    let localhost = name.trim_end_matches('.').eq_ignore_ascii_case("localhost");
                    if class == AddressClass::Loopback && !localhost {
                        return Err(rejected());
                    }
                    let answers = tokio::time::timeout(timeout, self.resolver.resolve(name))
                        .await
                        .map_err(|_elapsed| rejected())?
                        .map_err(|()| rejected())?;
                    let required = if localhost {
                        AddressClass::Loopback
                    } else {
                        AddressClass::Private
                    };
                    if answers.is_empty()
                        || answers.len() > 1024
                        || answers
                            .into_iter()
                            .any(|ip| classify_address(ip) != required)
                    {
                        return Err(rejected());
                    }
                }
                _ if !matches!(class, AddressClass::Private | AddressClass::Loopback) => {
                    return Err(rejected());
                }
                _ => {}
            }
        }
        // HTTPS names need no server-side DNS prerequisite: the browser verifies TLS.
        // HTTP checks are fresh at issuance, not a preflight for a Coral fetch.
        Ok(ConsentNavigation(url.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Answers(Vec<std::net::IpAddr>);
    #[async_trait::async_trait]
    impl Resolver for Answers {
        async fn resolve(&self, _host: &str) -> Result<Vec<std::net::IpAddr>, ()> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn private_http_names_need_only_private_answers_not_server_egress_permission() {
        for (answers, accepted) in [
            (vec!["10.0.0.8", "fd00::8"], true),
            (vec!["8.8.8.8"], false),
            (vec!["10.0.0.8", "8.8.8.8"], false),
            (vec!["127.0.0.1"], false),
            (vec!["10.0.0.8", "169.254.169.254"], false),
            (vec!["10.0.0.8"; 1024], true),
            (vec!["10.0.0.8"; 1025], false),
            (vec![], false),
        ] {
            let issuer = Issuer::with_resolver(std::sync::Arc::new(Answers(
                answers.into_iter().map(|ip| ip.parse().unwrap()).collect(),
            )));
            assert_eq!(
                issuer
                    .issue("http://sidecar:5000/authorize?state=opaque")
                    .await
                    .is_ok(),
                accepted
            );
        }
    }
    #[tokio::test]
    async fn literal_transport_rules_block_special_addresses_under_both_schemes() {
        let issuer = Issuer::default();
        for address in [
            "https://public.example/authorize",
            "https://consent.internal/authorize",
            "http://127.0.0.1/authorize",
            "http://[::1]/authorize",
            "http://[::ffff:127.0.0.1]/authorize",
            "http://10.0.0.8/authorize",
            "http://172.16.0.8/authorize",
            "http://192.168.0.8/authorize",
            "http://[fd00::8]/authorize",
        ] {
            issuer.issue(address).await.unwrap();
        }
        for host in [
            "169.254.169.254",
            "[fe80::1]",
            "100.64.0.1",
            "224.0.0.1",
            "[::ffff:10.0.0.8]",
            "[64:ff9b::a00:8]",
            "0.0.0.0",
        ] {
            for scheme in ["http", "https"] {
                assert!(matches!(
                    issuer.issue(&format!("{scheme}://{host}/authorize")).await,
                    Err(Error::ConsentDestinationRejected)
                ));
            }
        }
        for address in [
            "http://8.8.8.8/authorize",
            "https://user:secret@consent.internal/",
            "https://consent.internal/#fragment",
            "javascript:alert(1)",
        ] {
            assert!(matches!(
                issuer.issue(address).await,
                Err(Error::ConsentDestinationRejected)
            ));
        }
    }

    #[tokio::test]
    async fn localhost_never_accepts_private_or_public_dns_and_other_names_cannot_gain_loopback() {
        for (answers, accepted) in [
            (vec!["127.0.0.1", "::1"], true),
            (vec!["127.0.0.1", "10.0.0.8"], false),
            (vec!["8.8.8.8"], false),
        ] {
            let issuer = Issuer::with_resolver(Arc::new(Answers(
                answers.into_iter().map(|ip| ip.parse().unwrap()).collect(),
            )));
            assert_eq!(
                issuer.issue("http://localhost./authorize").await.is_ok(),
                accepted
            );
            assert!(matches!(
                issuer.issue("http://alias.localhost/authorize").await,
                Err(Error::ConsentDestinationRejected)
            ));
        }
    }

    #[tokio::test]
    async fn lookup_errors_and_deadlines_fail_closed_without_exposing_the_url() {
        struct Failure(bool);
        #[async_trait::async_trait]
        impl Resolver for Failure {
            async fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, ()> {
                if self.0 {
                    std::future::pending().await
                } else {
                    Err(())
                }
            }
        }
        for pending in [false, true] {
            let issuer = Issuer::with_resolver(Arc::new(Failure(pending)));
            let error = issuer
                .issue_with_timeout(
                    "http://consent.internal/authorize?state=secret-canary",
                    Duration::from_millis(1),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, Error::ConsentDestinationRejected));
            assert!(!format!("{error:?}").contains("canary"));
        }
        let navigation = Issuer::default()
            .issue("https://consent.example/authorize?state=secret-canary")
            .await
            .unwrap();
        assert!(!format!("{navigation:?}").contains("canary"));
    }
}
