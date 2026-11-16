//! URL checks and DNS validation belong to the same HTTP client.

use std::sync::Arc;
use std::time::Duration;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::Url;

use crate::{ClientMetadata, DestinationPolicy, DestinationRejected, EndpointUrl, NetworkAccess};

/// HTTP client that checks every request and every DNS answer before connecting.
/// Redirects, retries, and proxies are disabled. Callers explicitly handle continuation.
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    https: reqwest::Client,
    policy: DestinationPolicy,
}

impl Client {
    /// Builds a guarded client for an explicitly selected deployment policy.
    /// # Errors
    /// Returns a client initialization error.
    pub fn new(policy: DestinationPolicy, timeout: Duration) -> Result<Self, reqwest::Error> {
        Self::with_resolver(
            policy,
            timeout,
            Arc::new(SystemResolver),
            IdleConnections::Keep,
        )
    }

    fn with_resolver(
        policy: DestinationPolicy,
        timeout: Duration,
        resolver: Arc<dyn Resolve>,
        idle_connections: IdleConnections,
    ) -> Result<Self, reqwest::Error> {
        // Resolve has no scheme argument. Separate connector pools bind the
        // scheme to every lookup, including reconnects and mixed DNS answers.
        let build = |https, resolver| {
            let builder = reqwest::Client::builder()
                .user_agent("Coral")
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .connect_timeout(timeout)
                .timeout(timeout)
                .https_only(https)
                .dns_resolver(GuardedResolver {
                    policy,
                    https,
                    resolver,
                });
            match idle_connections {
                IdleConnections::Keep => builder,
                IdleConnections::Close => builder.pool_max_idle_per_host(0),
            }
            .build()
        };
        Ok(Self {
            http: build(false, Arc::clone(&resolver))?,
            https: build(true, resolver)?,
            policy,
        })
    }

    /// Starts a request after checking its URL, including literal IP addresses.
    /// # Errors
    /// Rejects invalid URLs, unsafe transports, and hosts outside the deployment policy.
    pub fn request(
        &self,
        method: reqwest::Method,
        address: impl AsRef<str>,
    ) -> Result<reqwest::RequestBuilder, DestinationRejected> {
        let url = Url::parse(address.as_ref()).map_err(|_error| DestinationRejected)?;
        self.policy.validate(&url)?;
        let client = if url.scheme() == "https" {
            &self.https
        } else {
            &self.http
        };
        Ok(client.request(method, url))
    }

    /// Starts a guarded GET request.
    /// # Errors
    /// Rejects a destination outside the deployment policy.
    pub fn get(
        &self,
        address: impl AsRef<str>,
    ) -> Result<reqwest::RequestBuilder, DestinationRejected> {
        self.request(reqwest::Method::GET, address)
    }

    /// Starts a guarded POST request.
    /// # Errors
    /// Rejects a destination outside the deployment policy.
    pub fn post(
        &self,
        address: impl AsRef<str>,
    ) -> Result<reqwest::RequestBuilder, DestinationRejected> {
        self.request(reqwest::Method::POST, address)
    }

    /// Starts a guarded DELETE request.
    /// # Errors
    /// Rejects a destination outside the deployment policy.
    pub fn delete(
        &self,
        address: impl AsRef<str>,
    ) -> Result<reqwest::RequestBuilder, DestinationRejected> {
        self.request(reqwest::Method::DELETE, address)
    }
}

/// HTTP client for OAuth client metadata documents.
///
/// The deployment's private-network setting is for Provider destinations
/// (ADR 0008) and does not apply here: every DNS answer for a metadata host
/// must be public.
///
/// The loopback setting stays as the operator set it. It serves the exact
/// operator-derived client ID that `EndpointUrl::<ClientMetadata>::parse`
/// admits, and [`get`](Self::get) takes no other kind of URL.
///
/// Every request opens its own connection, because a sign-in fails when this
/// fetch does. A server closes an idle connection on its own schedule, and a
/// request sent on one at that moment gets no answer.
#[derive(Clone)]
pub struct MetadataClient(Client);

impl MetadataClient {
    /// Builds the client from the deployment policy, keeping its loopback
    /// setting alone.
    /// # Errors
    /// Returns a client initialization error.
    pub fn new(policy: DestinationPolicy, timeout: Duration) -> Result<Self, reqwest::Error> {
        Self::with_resolver(policy, timeout, Arc::new(SystemResolver))
    }

    fn with_resolver(
        policy: DestinationPolicy,
        timeout: Duration,
        resolver: Arc<dyn Resolve>,
    ) -> Result<Self, reqwest::Error> {
        Client::with_resolver(
            policy.with_private_networks(NetworkAccess::Deny),
            timeout,
            resolver,
            IdleConnections::Close,
        )
        .map(Self)
    }

    /// Starts a guarded GET of a client's metadata document.
    /// # Errors
    /// Rejects a loopback client ID when the deployment denies loopback HTTP.
    pub fn get(
        &self,
        url: &EndpointUrl<ClientMetadata>,
    ) -> Result<reqwest::RequestBuilder, DestinationRejected> {
        self.0.get(url.as_url())
    }
}

/// What a client does with a connection once its response is complete.
#[derive(Clone, Copy)]
enum IdleConnections {
    /// Keep it for the next request to the same host.
    Keep,
    /// Close it, so that no request is sent on a connection the server may
    /// already be closing.
    Close,
}

/// Safe discrimination between blocked DNS destinations and ordinary transport errors.
#[derive(Debug)]
pub enum FetchError {
    /// The destination was rejected before connection.
    DestinationRejected,
    /// An ordinary HTTP failure.
    Http(reqwest::Error),
}

impl From<reqwest::Error> for FetchError {
    fn from(error: reqwest::Error) -> Self {
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(cause) = source {
            if cause.is::<DestinationRejected>() {
                return Self::DestinationRejected;
            }
            source = cause.source();
        }
        Self::Http(error)
    }
}

struct SystemResolver;
impl Resolve for SystemResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((name.as_str(), 0))
                .await?
                .collect::<Vec<_>>();
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

struct GuardedResolver {
    policy: DestinationPolicy,
    https: bool,
    resolver: Arc<dyn Resolve>,
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = self.policy;
        let https = self.https;
        let local = name
            .as_str()
            .trim_end_matches('.')
            .eq_ignore_ascii_case("localhost");
        let resolution = self.resolver.resolve(name);
        Box::pin(async move {
            let addresses = resolution.await?.collect::<Vec<_>>();
            if addresses.is_empty()
                || addresses.iter().any(|address| {
                    if local {
                        !policy.permits_loopback(https) || !crate::is_loopback_ip(address.ip())
                    } else {
                        !policy.permits_remote_address(crate::classify_address(address.ip()), https)
                    }
                })
            {
                return Err(
                    Box::new(DestinationRejected) as Box<dyn std::error::Error + Send + Sync>
                );
            }
            // These exact validated answers are passed to the connector; no second lookup.
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct Answers(Vec<SocketAddr>);
    impl Resolve for Answers {
        fn resolve(&self, _name: Name) -> Resolving {
            let answers = self.0.clone();
            Box::pin(async move { Ok(Box::new(answers.into_iter()) as Addrs) })
        }
    }

    #[tokio::test]
    async fn http_private_names_reject_public_mixed_special_and_rebound_answers() {
        use crate::NetworkAccess::{Deny, HttpAndHttps, Https};
        for access in [Deny, Https, HttpAndHttps] {
            for https in [false, true] {
                for (answers, allowed) in [
                    (
                        vec!["10.0.0.1:5000", "[fd00::1]:5000"],
                        access == HttpAndHttps || (https && access == Https),
                    ),
                    (vec!["8.8.8.8:5000"], https),
                    (
                        vec!["10.0.0.1:5000", "8.8.8.8:5000"],
                        https && access != Deny,
                    ),
                    (vec!["10.0.0.1:5000", "127.0.0.1:5000"], false),
                    (vec!["169.254.169.254:5000"], false),
                    // Special-purpose answers, including IPv6 spellings of a private IPv4.
                    (vec!["0.0.0.0:5000"], false),
                    (vec!["[::]:5000"], false),
                    (vec!["[::ffff:10.0.0.1]:5000"], false),
                    (vec!["[64:ff9b::a00:1]:5000"], false),
                    (vec![], false),
                ] {
                    let resolver = GuardedResolver {
                        policy: DestinationPolicy::new(HttpAndHttps, access),
                        https,
                        resolver: Arc::new(Answers(
                            answers.iter().map(|v| v.parse().unwrap()).collect(),
                        )),
                    };
                    assert_eq!(
                        resolver.resolve("sidecar".parse().unwrap()).await.is_ok(),
                        allowed,
                        "{access:?} {https} {answers:?}"
                    );
                }
            }
        }
        let resolver = GuardedResolver {
            policy: DestinationPolicy::new(HttpAndHttps, HttpAndHttps),
            https: false,
            resolver: Arc::new(super::rebinding_tests::ChangingAnswers(
                std::sync::Mutex::new(
                    [
                        vec!["10.0.0.1:5000".parse().unwrap()],
                        vec!["8.8.8.8:5000".parse().unwrap()],
                    ]
                    .into(),
                ),
            )),
        };
        let _answers = resolver.resolve("sidecar".parse().unwrap()).await.unwrap();
        assert!(resolver.resolve("sidecar".parse().unwrap()).await.is_err());
    }

    #[tokio::test]
    async fn private_and_mixed_dns_answers_cannot_reach_a_local_sink() {
        let sink = MockServer::start().await;
        for answers in [
            vec![*sink.address()],
            vec!["8.8.8.8:443".parse().unwrap(), *sink.address()],
        ] {
            let client = Client::with_resolver(
                DestinationPolicy::default(),
                Duration::from_secs(2),
                Arc::new(Answers(answers)),
                IdleConnections::Keep,
            )
            .unwrap();
            let error = client
                .get(format!(
                    "https://provider.test:{}/secret",
                    sink.address().port()
                ))
                .unwrap()
                .header("authorization", "Bearer canary")
                .send()
                .await
                .unwrap_err();
            assert!(matches!(
                FetchError::from(error),
                FetchError::DestinationRejected
            ));
        }
        assert!(sink.received_requests().await.unwrap().is_empty());
    }

    fn client_metadata_url(value: &str, issuer: &str) -> EndpointUrl<ClientMetadata> {
        let issuer = EndpointUrl::<crate::Configured>::parse(issuer).unwrap();
        EndpointUrl::<ClientMetadata>::parse(value, &issuer, &[value.to_owned()].into()).unwrap()
    }

    /// The private-network setting is for Provider destinations. A client
    /// metadata host resolves the same way under every value of it.
    #[tokio::test]
    async fn client_metadata_resolves_to_public_addresses_under_every_operator_policy() {
        use crate::NetworkAccess::{Deny, HttpAndHttps, Https};
        let sink = MockServer::start().await;
        let port = sink.address().port();
        let url = client_metadata_url(
            &format!("https://client.example.test:{port}/oauth/client.json"),
            "https://auth.example.test",
        );
        for loopback in [Deny, Https, HttpAndHttps] {
            for private_networks in [Deny, Https, HttpAndHttps] {
                for answers in [
                    vec![format!("10.0.0.1:{port}")],
                    vec![format!("[fd00::1]:{port}")],
                    vec![format!("8.8.8.8:{port}"), format!("192.168.1.1:{port}")],
                    vec![format!("169.254.169.254:{port}")],
                    vec![sink.address().to_string()],
                ] {
                    let client = MetadataClient::with_resolver(
                        DestinationPolicy::new(loopback, private_networks),
                        Duration::from_secs(2),
                        Arc::new(Answers(
                            answers.iter().map(|v| v.parse().unwrap()).collect(),
                        )),
                    )
                    .unwrap();
                    let error = client.get(&url).unwrap().send().await.unwrap_err();
                    // The resolver refuses before any connection is attempted.
                    assert!(
                        matches!(FetchError::from(error), FetchError::DestinationRejected),
                        "{loopback:?} {private_networks:?} {answers:?}"
                    );
                }
            }
        }
        assert!(sink.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn client_metadata_reaches_an_exact_loopback_client_id_when_the_operator_allows_loopback()
    {
        use crate::NetworkAccess::{Deny, HttpAndHttps, Https};
        let sink = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&sink)
            .await;
        let url = client_metadata_url(
            &format!("{}/.well-known/oauth-client", sink.uri()),
            "http://localhost:9080",
        );
        for private_networks in [Deny, Https, HttpAndHttps] {
            let client = |loopback| {
                MetadataClient::new(
                    DestinationPolicy::new(loopback, private_networks),
                    Duration::from_secs(2),
                )
                .unwrap()
            };
            let response = client(HttpAndHttps)
                .get(&url)
                .unwrap()
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200, "{private_networks:?}");
            // The operator's loopback setting still decides this one case.
            for loopback in [Deny, Https] {
                assert_eq!(
                    client(loopback).get(&url).map(drop),
                    Err(DestinationRejected),
                    "{loopback:?} {private_networks:?}"
                );
            }
        }
    }

    /// Sends two requests to a local server that answers every request, and
    /// returns how many of them arrived on a connection that had already
    /// carried one.
    async fn requests_on_a_used_connection(get: impl Fn(&str) -> reqwest::RequestBuilder) -> usize {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/.well-known/oauth-client",
            listener.local_addr().unwrap()
        );
        let reused = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn({
            let reused = Arc::clone(&reused);
            async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    let (mut stream, _peer) = listener.accept().await.unwrap();
                    let reused = Arc::clone(&reused);
                    connections.spawn(async move {
                        let mut used = false;
                        loop {
                            let mut request = Vec::new();
                            while !request.ends_with(b"\r\n\r\n") {
                                // The client closed the connection.
                                let Ok(byte) = stream.read_u8().await else {
                                    return;
                                };
                                request.push(byte);
                            }
                            if used {
                                reused.fetch_add(1, Ordering::Relaxed);
                            }
                            used = true;
                            stream
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                                .await
                                .unwrap();
                        }
                    });
                }
            }
        });
        for _ in 0..2 {
            let response = get(&url).send().await.unwrap();
            assert_eq!(response.status(), 200);
            assert!(response.bytes().await.unwrap().is_empty());
        }
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        reused.load(Ordering::Relaxed)
    }

    /// A server closes an idle connection on its own schedule, and a request
    /// that arrives on one as it closes gets no answer. A sign-in fails when
    /// its metadata fetch does, so that client never sends on a used
    /// connection. Provider calls keep their connection.
    #[tokio::test]
    async fn only_the_metadata_client_opens_a_connection_for_every_request() {
        let policy = DestinationPolicy::default().with_loopback(crate::NetworkAccess::HttpAndHttps);
        let timeout = Duration::from_secs(2);

        let metadata = MetadataClient::new(policy, timeout).unwrap();
        let metadata_reuses = requests_on_a_used_connection(|url| {
            metadata
                .get(&client_metadata_url(url, "http://localhost:9080"))
                .unwrap()
        })
        .await;
        let provider = Client::new(policy, timeout).unwrap();
        let provider_reuses = requests_on_a_used_connection(|url| provider.get(url).unwrap()).await;

        assert_eq!((metadata_reuses, provider_reuses), (0, 1));
    }

    #[tokio::test]
    async fn literal_loopback_requires_permission_and_redirects_are_not_automatic() {
        let sink = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", "http://169.254.169.254/latest/meta-data"),
            )
            .expect(1)
            .mount(&sink)
            .await;
        let public = Client::new(DestinationPolicy::default(), Duration::from_secs(2)).unwrap();
        public
            .get(sink.uri())
            .expect_err("public policy rejects loopback");
        let local = Client::new(
            DestinationPolicy::default().with_loopback(crate::NetworkAccess::HttpAndHttps),
            Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(
            local
                .get(sink.uri())
                .unwrap()
                .send()
                .await
                .unwrap()
                .status(),
            302
        );
        sink.verify().await;
    }

    #[tokio::test]
    async fn private_network_dns_permission_preserves_other_rejections() {
        for enabled in [false, true] {
            for (policy, loopback) in [
                (DestinationPolicy::default(), false),
                (
                    DestinationPolicy::default().with_loopback(crate::NetworkAccess::HttpAndHttps),
                    true,
                ),
            ] {
                let policy = policy.with_private_networks(if enabled {
                    crate::NetworkAccess::Https
                } else {
                    crate::NetworkAccess::Deny
                });
                for (name, addresses, permitted) in [
                    (
                        "provider.test",
                        vec!["10.0.0.1:443", "[fd00::1]:443"],
                        enabled,
                    ),
                    (
                        "provider.test",
                        vec!["8.8.8.8:443", "192.168.1.1:443"],
                        enabled,
                    ),
                    (
                        "provider.test",
                        vec!["10.0.0.1:443", "169.254.169.254:443"],
                        false,
                    ),
                    (
                        "provider.test",
                        vec!["10.0.0.1:443", "127.0.0.1:443"],
                        false,
                    ),
                    (
                        "provider.test",
                        vec!["8.8.8.8:443", "[2606:4700:4700::1111]:443"],
                        true,
                    ),
                    ("provider.test", vec!["100.64.0.1:443"], false),
                    ("provider.test", vec!["[fe80::1]:443"], false),
                    ("provider.test", vec![], false),
                    ("localhost", vec!["10.0.0.1:443"], false),
                    ("localhost", vec!["127.0.0.1:443"], loopback),
                ] {
                    let answers: Vec<SocketAddr> = addresses
                        .into_iter()
                        .map(|value| value.parse().unwrap())
                        .collect();
                    let resolver = GuardedResolver {
                        https: true,
                        policy,
                        resolver: Arc::new(Answers(answers.clone())),
                    };
                    let result = resolver.resolve(name.parse().unwrap()).await;
                    if permitted {
                        assert_eq!(result.unwrap().collect::<Vec<_>>(), answers);
                    } else {
                        assert!(matches!(result, Err(error) if error.is::<DestinationRejected>()));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod rebinding_tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Mutex;

    pub(super) struct ChangingAnswers(
        pub(super) Mutex<std::collections::VecDeque<Vec<SocketAddr>>>,
    );
    impl Resolve for ChangingAnswers {
        fn resolve(&self, _name: Name) -> Resolving {
            let answers = self.0.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { Ok(Box::new(answers.into_iter()) as Addrs) })
        }
    }

    #[tokio::test]
    async fn every_new_connection_revalidates_dns_answers() {
        let resolver = GuardedResolver {
            https: true,
            policy: DestinationPolicy::default(),
            resolver: Arc::new(ChangingAnswers(Mutex::new(
                [
                    vec!["8.8.8.8:443".parse().unwrap()],
                    vec!["127.0.0.1:443".parse().unwrap()],
                    vec![
                        "8.8.8.8:443".parse().unwrap(),
                        "10.0.0.1:443".parse().unwrap(),
                    ],
                    vec![],
                ]
                .into(),
            ))),
        };
        assert_eq!(
            resolver
                .resolve("provider.test".parse().unwrap())
                .await
                .unwrap()
                .count(),
            1
        );
        for _ in 0..3 {
            let result = resolver.resolve("provider.test".parse().unwrap()).await;
            assert!(matches!(result, Err(error) if error.is::<DestinationRejected>()));
        }
    }

    #[tokio::test]
    async fn localhost_must_resolve_only_to_loopback() {
        let resolver = GuardedResolver {
            https: true,
            policy: DestinationPolicy::default().with_loopback(crate::NetworkAccess::HttpAndHttps),
            resolver: Arc::new(ChangingAnswers(Mutex::new(
                [
                    vec![
                        "127.0.0.1:443".parse().unwrap(),
                        "[::1]:443".parse().unwrap(),
                    ],
                    vec!["8.8.8.8:443".parse().unwrap()],
                ]
                .into(),
            ))),
        };
        assert_eq!(
            resolver
                .resolve("localhost".parse().unwrap())
                .await
                .unwrap()
                .count(),
            2
        );
        assert!(
            matches!(resolver.resolve("localhost".parse().unwrap()).await, Err(error) if error.is::<DestinationRejected>())
        );
    }
}
