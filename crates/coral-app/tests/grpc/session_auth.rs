//! Session-authentication fixture shared by the gRPC integration tests.
//!
//! These tests authenticate through the production path rather than an injected
//! provider: the server verifies tokens the real issuer minted, so an issuer
//! that drifts from its verifier fails a test here instead of passing against a
//! stub that agrees with neither.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use coral_app::{AppError, PrincipalKind};
use coral_client::local::{RunningServer, ServerBuilder};
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};

const ISSUER: &str = "https://auth.example.test";
/// The public base the per-workspace MCP resources hang under. Not an audience
/// of its own: tokens are minted for [`MINTED_AUDIENCE`], one member of the
/// family, which the private gRPC API accepts like every other member.
const PUBLIC_BASE: &str = "https://mcp.example.test";
const MINTED_AUDIENCE: &str = "https://mcp.example.test/workspace/transport-tests";
const CLIENT_ID: &str = "https://client.example/client.json";
const ACCESS_TOKEN_TTL: Duration = Duration::from_mins(5);

/// A config directory an authenticated server resolves completely, plus the key
/// its tokens are signed with.
pub(crate) struct SessionAuthFixture {
    config_dir: PathBuf,
    signing_key: Vec<u8>,
}

impl SessionAuthFixture {
    /// Writes the signing key and the config an authenticated server needs.
    ///
    /// A directory that already holds a key keeps it. A deployment restarted
    /// over its own state is the same deployment, and re-minting its key would
    /// silently invalidate every token the previous server handed out.
    pub(crate) fn write(config_dir: &Path) -> Self {
        let fixture = Self::key_in(config_dir);
        fs::write(config_dir.join("config.toml"), Self::config_toml())
            .expect("write the session auth config");
        fixture
    }

    /// Resolves the signing key in `config_dir` without touching its config.
    ///
    /// A caller that owns the rest of `config.toml` — an install whose contents
    /// are the subject of the test — composes it from [`Self::config_toml`]
    /// instead, so the configuration it started from survives the write.
    pub(crate) fn key_in(config_dir: &Path) -> Self {
        fs::create_dir_all(config_dir).expect("create config dir");
        let key_file = config_dir.join("session.key");
        let signing_key = match fs::read(&key_file) {
            Ok(existing) => existing,
            // Only an absent key may be minted. Any other fault would replace a
            // key that is still in use, invalidating the tokens minted from it
            // and surfacing far away as an authentication failure.
            Err(fault) if fault.kind() != io::ErrorKind::NotFound => {
                panic!("read the session signing key: {fault}")
            }
            Err(_absent) => {
                let generated = EcdsaKeyPair::generate_pkcs8(
                    &ECDSA_P256_SHA256_FIXED_SIGNING,
                    &SystemRandom::new(),
                )
                .expect("generate a session signing key");
                fs::write(&key_file, generated.as_ref()).expect("write the session signing key");
                generated.as_ref().to_vec()
            }
        };
        Self {
            config_dir: config_dir.to_path_buf(),
            signing_key,
        }
    }

    /// The configuration that turns a deployment into an authenticated one.
    ///
    /// The MCP HTTP surface is what gives the deployment a public audience; the
    /// private gRPC API admits every audience that fronts it, so tokens minted
    /// for this one reach the services under test.
    pub(crate) fn config_toml() -> String {
        format!(
            "[credentials]
storage = \"file\"

[server.mcp_http]
enabled = true
bind = '127.0.0.1:0'
public_url = '{PUBLIC_BASE}'

[auth.authorization_server]
issuer = '{ISSUER}'

[auth.session]
signing_key_file = 'session.key'

[auth.provider]
issuer = 'https://accounts.example.test'
client_id = 'upstream-client'
client_secret = 'test-secret'
redirect_uri = '{ISSUER}/auth/oidc/callback'
"
        )
    }

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// Mints the access token a completed login would have handed `user_id`.
    ///
    /// The login flow itself is upstream of these transport tests, so this
    /// mints through the real issuer rather than replaying the OAuth round
    /// trip: the token's wire format stays the server's own.
    pub(crate) fn access_token(&self, user_id: &str) -> String {
        self.access_token_for(user_id, PrincipalKind::User)
    }

    /// Mints the token an actor of `principal_kind` carries for `user_id`.
    ///
    /// Two callers differing only in kind is the whole point: the audience is
    /// the same either way, so a test that separates an agent from a person is
    /// separating them by what the issuer declared and by nothing else.
    pub(crate) fn access_token_for(&self, user_id: &str, principal_kind: PrincipalKind) -> String {
        coral_app::test_session_tokens::issue_access_token(
            ISSUER,
            &self.signing_key,
            ACCESS_TOKEN_TTL,
            user_id,
            CLIENT_ID,
            MINTED_AUDIENCE,
            principal_kind,
        )
        .expect("issue a session access token")
    }
}

/// Starts a server that authenticates every gRPC caller with session tokens.
///
/// # Errors
///
/// Returns [`AppError`] when the configuration cannot be resolved or the server
/// cannot be started.
pub(crate) async fn session_authenticated_server(
    fixture: &SessionAuthFixture,
) -> Result<RunningServer, AppError> {
    let builder = ServerBuilder::new().with_config_dir(fixture.config_dir());
    let session_auth = builder
        .serve_settings()?
        .take_session_auth()
        .expect("the fixture configures session authentication");
    builder.with_session_auth(session_auth).start().await
}
