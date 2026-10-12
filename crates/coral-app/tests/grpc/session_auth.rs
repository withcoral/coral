//! Session-authentication fixture shared by the gRPC integration tests.
//!
//! These tests authenticate through the production path rather than an injected
//! provider: the server verifies tokens the real issuer minted, so an issuer
//! that drifts from its verifier fails a test here instead of passing against a
//! stub that agrees with neither.

use coral_app::{AppError, RunningServer, ServerBuilder};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const ISSUER: &str = "https://auth.example.test";
/// Public Workspace URL family. These public audiences do not authenticate
/// private gRPC, which requires explicit mcp/reef audience selection.
const PUBLIC_BASE: &str = "https://mcp.example.test";
/// A config directory an authenticated server resolves completely.
pub(crate) struct SessionAuthFixture {
    config_dir: PathBuf,
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
        match fs::read(&key_file) {
            Ok(_existing) => {}
            // Only an absent key may be minted. Any other fault would replace a
            // key that is still in use, invalidating the tokens minted from it
            // and surfacing far away as an authentication failure.
            Err(fault) if fault.kind() != io::ErrorKind::NotFound => {
                panic!("read the session signing key: {fault}")
            }
            Err(_absent) => {
                let generated = coral_test_support::p256_pkcs8();
                fs::write(&key_file, generated).expect("write the session signing key");
            }
        }
        Self {
            config_dir: config_dir.to_path_buf(),
        }
    }

    /// The configuration that turns a deployment into an authenticated one.
    ///
    /// Public MCP and private gRPC share verification material, not audience
    /// admission. Private request tests exercise their exact reachability rules.
    pub(crate) fn config_toml() -> String {
        format!(
            "[listeners.mcp]
bind = '127.0.0.1:0'
public_url = '{PUBLIC_BASE}'

[listeners.auth]
public_url = '{ISSUER}'

[listeners.web]
public_url = 'http://127.0.0.1:5173'

[auth.session]
signing_key_file = 'session.key'

[auth.provider]
issuer = 'https://accounts.example.test'
client_id = 'upstream-client'
client_secret = 'test-secret'
"
        )
    }

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
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
    let builder = ServerBuilder::new()
        .with_config_dir(fixture.config_dir())
        .prepare()
        .await?;
    let session_auth = builder
        .serve_settings()?
        .take_session_auth()
        .expect("the fixture configures session authentication");
    builder.with_session_auth(session_auth).start().await
}
