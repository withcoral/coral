//! CLI composition for endpoint-bound OAuth login.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use coral_app::{
    CanonicalRemoteEndpoint, OAuthLoginError, OAuthLoginStoreError, RemoteEndpointError,
    run_oauth_login, save_oauth_login,
};

use crate::{browser, env};

#[derive(Debug, Args)]
/// Authenticate the CLI to a remote Coral server
pub(crate) struct AuthArgs {
    #[command(subcommand)]
    command: AuthCommand,
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Sign in through an OAuth authorization server
    Login(LoginArgs),
}

#[derive(Debug, Args)]
struct LoginArgs {
    /// Optional identity-provider selector forwarded to the authorization server
    #[arg(value_name = "PROVIDER")]
    provider: Option<String>,
    /// OAuth authorization server. Overrides `CORAL_AUTH_ENDPOINT`.
    #[arg(long, value_name = "URL")]
    authorization_server: Option<String>,
    /// Print the authorization URL without opening a browser
    #[arg(long)]
    no_open: bool,
}

#[derive(Debug)]
struct ResolvedLogin {
    endpoint: CanonicalRemoteEndpoint,
    authorization_server: String,
    provider: Option<String>,
    no_open: bool,
}

#[derive(Debug, thiserror::Error)]
enum AuthLoginError {
    #[error("OAuth login requires a remote endpoint; pass --endpoint or set CORAL_ENDPOINT")]
    MissingEndpoint,
    #[error(
        "OAuth login requires an authorization server; pass --authorization-server or set CORAL_AUTH_ENDPOINT"
    )]
    MissingAuthorizationServer,
    #[error(transparent)]
    Environment(#[from] env::ConnectionEnvError),
    #[error(transparent)]
    Endpoint(#[from] RemoteEndpointError),
    #[error(transparent)]
    Login(#[from] OAuthLoginError),
    #[error(transparent)]
    Store(#[from] OAuthLoginStoreError),
}

pub(crate) async fn run(
    endpoint_override: Option<String>,
    args: AuthArgs,
) -> Result<(), anyhow::Error> {
    match args.command {
        AuthCommand::Login(args) => run_login(endpoint_override, args).await.map_err(Into::into),
    }
}

async fn run_login(
    endpoint_override: Option<String>,
    args: LoginArgs,
) -> Result<(), AuthLoginError> {
    let resolved = resolve_login(endpoint_override, args, env::endpoint, env::auth_endpoint)?;
    let no_open = resolved.no_open;
    let path = execute_login(&resolved, None, move |authorization_url| {
        println!("Open this URL to authorize Coral:\n{authorization_url}");
        if !no_open && let Err(error) = browser::open_url(authorization_url) {
            eprintln!("Could not open a browser: {error}");
            eprintln!("Open the URL above manually.");
        }
    })
    .await?;
    println!(
        "Stored OAuth login for {} in {}.",
        resolved.endpoint.as_uri(),
        path.display()
    );
    Ok(())
}

async fn execute_login(
    resolved: &ResolvedLogin,
    config_dir_override: Option<PathBuf>,
    present_authorization_url: impl FnOnce(&str),
) -> Result<PathBuf, AuthLoginError> {
    let login = run_oauth_login(
        &resolved.authorization_server,
        resolved.provider.as_deref(),
        present_authorization_url,
    )
    .await?;
    save_oauth_login(config_dir_override, &resolved.endpoint, login).map_err(Into::into)
}

fn resolve_login(
    endpoint_override: Option<String>,
    args: LoginArgs,
    read_endpoint: impl FnOnce() -> Result<Option<String>, env::ConnectionEnvError>,
    read_authorization_server: impl FnOnce() -> Result<Option<String>, env::ConnectionEnvError>,
) -> Result<ResolvedLogin, AuthLoginError> {
    let endpoint = endpoint_override
        .map_or_else(read_endpoint, |value| Ok(Some(value)))?
        .filter(|value| !value.is_empty())
        .ok_or(AuthLoginError::MissingEndpoint)?;
    let endpoint = CanonicalRemoteEndpoint::parse(&endpoint)?;

    let authorization_server = args
        .authorization_server
        .map_or_else(read_authorization_server, |value| Ok(Some(value)))?
        .filter(|value| !value.is_empty())
        .ok_or(AuthLoginError::MissingAuthorizationServer)?;

    Ok(ResolvedLogin {
        endpoint,
        authorization_server,
        provider: args.provider,
        no_open: args.no_open,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt as _;
    use tokio::net::TcpStream;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn args(authorization_server: Option<&str>) -> LoginArgs {
        LoginArgs {
            provider: Some("github".into()),
            authorization_server: authorization_server.map(str::to_string),
            no_open: true,
        }
    }

    async fn send_loopback_callback(callback: Url) {
        let target = callback.query().map_or_else(
            || callback.path().to_string(),
            |query| format!("{}?{query}", callback.path()),
        );
        let mut stream = TcpStream::connect(("127.0.0.1", 14554))
            .await
            .expect("callback connection");
        stream
            .write_all(
                format!(
                    "GET {target} HTTP/1.1\r\nHost: 127.0.0.1:14554\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .expect("callback request");
    }

    #[tokio::test]
    async fn real_login_saves_distinct_metadata_result_without_dialing_coral_endpoint() {
        let server = MockServer::start().await;
        let issuer = server.uri();
        let resource = "https://protected.example.test/mcp";
        Mock::given(method("GET"))
            .and(path("/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "issuer": issuer,
                "authorization_endpoint": format!("{issuer}/oauth/authorize"),
                "token_endpoint": format!("{issuer}/oauth/token"),
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported": ["none"],
                "scopes_supported": ["coral:mcp"],
                "resource": resource,
                "client_id_metadata_document_supported": true
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token-secret",
                "token_type": "bearer",
                "scope": "coral:mcp"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let config = temp.path().join("coral");
        let record = config.join("auth/login.json");
        fs::create_dir_all(record.parent().expect("record parent")).expect("record directory");
        fs::write(&record, "corrupt prior record").expect("prior record");
        let resolved = ResolvedLogin {
            endpoint: CanonicalRemoteEndpoint::parse("https://unreachable-coral.example.test")
                .expect("endpoint"),
            authorization_server: issuer.clone(),
            provider: Some("github".into()),
            no_open: true,
        };

        let path = tokio::time::timeout(
            Duration::from_secs(3),
            execute_login(&resolved, Some(config.clone()), |authorization| {
                let url = Url::parse(authorization).expect("authorization URL");
                let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
                assert_eq!(query.get("provider").map(String::as_str), Some("github"));
                assert_eq!(query.get("resource").map(String::as_str), Some(resource));
                assert!(!authorization.contains(resolved.endpoint.as_uri()));
                let mut callback = Url::parse(
                    query
                        .get("redirect_uri")
                        .expect("authorization redirect URI"),
                )
                .expect("callback URL");
                callback
                    .query_pairs_mut()
                    .append_pair("state", query.get("state").expect("authorization state"))
                    .append_pair("code", "authorization-code");
                tokio::spawn(send_loopback_callback(callback));
            }),
        )
        .await
        .expect("login timeout")
        .expect("login");

        assert_eq!(path, record);
        let stored = coral_app::load_oauth_login(Some(config), &resolved.endpoint)
            .expect("load stored login")
            .expect("stored login");
        assert_eq!(stored.access_token(), "token-secret");
        assert_eq!(stored.issuer(), issuer);
        assert_eq!(stored.resource(), resource);
    }

    #[test]
    fn explicit_values_win_without_reading_environment_and_keep_origins_independent() {
        let resolved = resolve_login(
            Some("https://CORAL.example:443/".into()),
            args(Some("https://login.example")),
            || panic!("endpoint environment must not be read"),
            || panic!("authorization-server environment must not be read"),
        )
        .expect("resolved login");

        assert_eq!(resolved.endpoint.as_uri(), "https://coral.example");
        assert_eq!(resolved.authorization_server, "https://login.example");
        assert_eq!(resolved.provider.as_deref(), Some("github"));
        assert!(resolved.no_open);
    }

    #[test]
    fn endpoint_failure_happens_before_authorization_server_resolution() {
        let auth_read = Cell::new(false);
        let missing = resolve_login(
            None,
            args(None),
            || Ok(None),
            || {
                auth_read.set(true);
                Ok(Some("https://ignored.example".into()))
            },
        );
        assert!(matches!(missing, Err(AuthLoginError::MissingEndpoint)));
        assert!(!auth_read.get());

        let empty = resolve_login(
            Some(String::new()),
            args(None),
            || panic!("explicit empty endpoint must suppress environment fallback"),
            || panic!("authorization server must not be resolved"),
        );
        assert!(matches!(empty, Err(AuthLoginError::MissingEndpoint)));

        assert!(matches!(
            resolve_login(
                Some(" https://coral.example".into()),
                args(Some("https://login.example")),
                || unreachable!(),
                || unreachable!(),
            ),
            Err(AuthLoginError::Endpoint(_))
        ));
    }

    #[test]
    fn explicit_empty_authorization_server_suppresses_environment_fallback() {
        let result = resolve_login(
            Some("https://coral.example".into()),
            args(Some("")),
            || unreachable!(),
            || panic!("authorization-server environment must not be read"),
        );

        assert!(matches!(
            result,
            Err(AuthLoginError::MissingAuthorizationServer)
        ));
    }

    #[test]
    fn environment_values_are_used_only_when_flags_are_absent() {
        let resolved = resolve_login(
            None,
            args(None),
            || Ok(Some("https://coral.example".into())),
            || Ok(Some("http://127.0.0.1:9182".into())),
        )
        .expect("resolved login");

        assert_eq!(resolved.endpoint.as_uri(), "https://coral.example");
        assert_eq!(resolved.authorization_server, "http://127.0.0.1:9182");
    }
}
