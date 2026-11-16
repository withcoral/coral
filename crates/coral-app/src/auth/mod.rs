mod authorization_server;
mod config;
mod error;
mod id_token;
mod provider_client;
mod request;
mod sealed_handshake;
pub use request::WorkspaceAuthentication;
#[cfg(test)]
mod request_tests;

pub use authorization_server::{CoralAuthorizationServer, RunningCoralAuthorizationServer};
pub use config::AuthSettings;
/// Visible to the crate so the bootstrap composition root can build the services
/// these settings configure; resolution still happens only in this module.
pub(crate) use config::{AuthListenerSettings, ResolvedAuthSettings, WebSettings};
pub use error::AuthServerError;

/// Path the authorization server answers the upstream provider's callback on.
///
/// Configuration derives the provider redirect URI from the public issuer and
/// this same path, so the advertised callback is always one the router serves.
pub(in crate::auth) const OIDC_CALLBACK_PATH: &str = "/auth/oidc/callback";

/// A server for tests of what a client does with a connection it has used.
#[cfg(test)]
pub(super) mod test_server {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;

    /// Answers the first request on each connection with the JSON that
    /// `respond` returns for the request's path, and keeps the connection
    /// open. It closes the connection without an answer when a second request
    /// arrives on it, as a server does when its idle timeout ends while that
    /// request is on its way.
    pub(crate) async fn serve_one_request_per_connection(
        listener: TcpListener,
        respond: impl Fn(&str) -> String + Clone + Send + 'static,
    ) {
        let mut connections = JoinSet::new();
        loop {
            let (mut stream, _peer) = listener.accept().await.expect("connection");
            let respond = respond.clone();
            connections.spawn(async move {
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    head.push(stream.read_u8().await.expect("request"));
                }
                let head = String::from_utf8(head).expect("request head");
                // Read the whole request. Closing a connection with unread
                // bytes resets it, and the client can lose the response.
                let content_length = head.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().expect("content length"))
                });
                let mut request_body = vec![0; content_length.unwrap_or(0)];
                stream
                    .read_exact(&mut request_body)
                    .await
                    .expect("request body");
                let path = head.split(' ').nth(1).expect("request path");
                let body = respond(path);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                // Returns on the first byte of a second request or when the
                // client closes; dropping the stream then closes it.
                let _second_request_or_close = stream.read_u8().await;
            });
        }
    }
}

/// Minimal `[auth]` TOML sections shared by this module's tests.
///
/// `config` and `authorization_server` both build configs from the same
/// required sections, so one copy here keeps a change to the required shape
/// from being applied to one test module and missed in the other.
#[cfg(test)]
pub(super) mod test_config {
    use std::path::Path;
    use std::sync::LazyLock;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use coral_auth::SessionTokenIssuer;

    use super::config::{AuthSettings, ResolvedAuthSettings};

    pub(crate) const SESSION: &str = "[auth.session]\nsigning_key_file = 'session.key'\n";
    pub(crate) const AUTHORIZATION_SERVER: &str =
        "[listeners.auth]\npublic_url = 'http://localhost:9080'\n";
    pub(crate) const PROVIDER: &str = "[auth.provider]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret = 'provider-secret'\n";

    /// The authorization server's public issuer in [`resolved_settings`].
    pub(crate) const AUTH_ISSUER: &str = "https://auth.example.test";
    /// The upstream client secret in [`resolved_settings`], which must never leak.
    pub(crate) const PROVIDER_SECRET: &str = "provider-secret-must-not-leak";

    /// Lets tests reach mock providers and metadata servers on loopback.
    pub(crate) fn loopback_policy() -> coral_outbound::DestinationPolicy {
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
    }

    /// One session signing key for every test server, as every process of
    /// one deployment shares its key.
    static SIGNING_KEY: LazyLock<Vec<u8>> = LazyLock::new(coral_test_support::p256_pkcs8);

    /// Resolves settings for an authorization server at [`AUTH_ISSUER`] whose
    /// upstream provider is `provider_issuer`, with the shared test signing key.
    ///
    /// `extra` is appended as whole TOML tables. It may define `[auth]`
    /// fields, because TOML allows a table after its sub-tables.
    pub(crate) fn resolved_settings(
        provider_issuer: &str,
        extra: &str,
    ) -> (ResolvedAuthSettings, SessionTokenIssuer) {
        let settings = AuthSettings::from_toml(&format!(
            "[auth.session]\n[listeners.auth]\npublic_url = '{AUTH_ISSUER}'\n\
             [auth.provider]\nissuer = '{provider_issuer}'\n\
             client_id = 'provider-client'\nclient_secret = '{PROVIDER_SECRET}'\n{extra}"
        ))
        .expect("valid auth config")
        .expect("auth settings");
        let signing_key = STANDARD.encode(&*SIGNING_KEY);
        settings
            .resolve_runtime_dependencies(Path::new("config.toml"), &|name| {
                Ok((name == "CORAL_SESSION_SIGNING_KEY").then(|| signing_key.clone()))
            })
            .expect("resolved runtime dependencies")
    }
}
