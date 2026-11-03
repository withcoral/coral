//! OAuth Account fixtures: a gated token endpoint and a User's connected
//! caller Account on the fixture Provider, shared by the refresh and
//! enforcement tests.
use crate::tests::*;
use axum::http::StatusCode;
use coral_credentials::Secret;
use coral_provider::account::{AccountModule, OAuthClient, SetupOAuthClient};
use coral_workspace::WorkspaceId;

/// A gated OAuth token endpoint: the initial exchange answers at once with a
/// token that expires in `initial_expires_in` seconds (none by default, so
/// the first dispatch must refresh); a refresh exchange is counted, parks at
/// `entered` until `release`, and answers `refresh_status`: a renewed token
/// good for an hour when that is a success, an `invalid_grant` error body
/// otherwise. The gate takes one refresh at a time: each `entered` wakes one
/// waiter and each `release` lets one exchange through, so a test drives one
/// expired token at a time.
pub(crate) struct TokenEndpoint {
    pub(crate) url: String,
    pub(crate) entered: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
    pub(crate) exchanges: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for TokenEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl TokenEndpoint {
    pub(crate) async fn new(refresh_status: StatusCode) -> Self {
        Self::with_initial_expiry(refresh_status, 0).await
    }

    pub(crate) async fn with_initial_expiry(
        refresh_status: StatusCode,
        initial_expires_in: u64,
    ) -> Self {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let exchanges = Arc::new(AtomicUsize::new(0));
        let signals = (entered.clone(), release.clone(), exchanges.clone());
        let router = axum::Router::new().route(
            "/token",
            axum::routing::post(move |body: String| {
                let (entered, release, exchanges) = signals.clone();
                async move {
                    let refresh = url::form_urlencoded::parse(body.as_bytes())
                        .any(|(key, value)| key == "grant_type" && value == "refresh_token");
                    if refresh {
                        exchanges.fetch_add(1, Ordering::SeqCst);
                        entered.notify_one();
                        release.notified().await;
                    }
                    let status = if refresh { refresh_status } else { StatusCode::OK };
                    let response = if status.is_success() { serde_json::json!({
                        "access_token": if refresh { "renewed-canary" } else { "initial-canary" },
                        "refresh_token": "refresh-canary", "token_type": "Bearer", "scope": "read",
                        "expires_in": if refresh { 3600 } else { initial_expires_in }
                    }) } else { serde_json::json!({"error":"invalid_grant", "error_description":"private-canary"}) };
                    (status, [("content-type", "application/json")], response.to_string())
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/token", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            url,
            entered,
            release,
            exchanges,
            task,
        }
    }
}

pub(crate) async fn oauth_account(
    fixture: &TestModules,
    tool: &ToolFixture,
    server: &MockServer,
    token_url: &str,
) -> (tempfile::TempDir, AccountModule) {
    let (directory, accounts) = configure_user_caller(fixture, tool, &server.uri()).await;
    let context = change(tool.user.clone(), 40, '4');
    let workspace = &tool.workspace_id;
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Renewal","version":"1"},
        "servers":[{"url":server.uri()}], "security":[{"oauth":["read"]}],
        "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
            "authorizationUrl":format!("{}/authorize",server.uri()), "tokenUrl":token_url, "scopes":{"read":"Forecast"}
        }}}}},
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    });
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &context,
            workspace,
            OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    let review = fixture
        .providers
        .review_setup_configuration(&tool.user, workspace, setup.id())
        .await
        .unwrap();
    let review = accounts
        .confirm_setup_configuration(
            &context,
            workspace,
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
            Some(
                SetupOAuthClient::new(
                    "https://reef.example/callback".into(),
                    Some("public-client".into()),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    fixture
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                workspace.clone(),
                tool.provider_id.clone(),
                setup.id().clone(),
                review.revision,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    let authorization = accounts
        .start_user_authorization(
            &context,
            workspace,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            Some(
                OAuthClient::new(
                    "public-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    complete_oauth(&accounts, &context, workspace, &authorization).await;
    (directory, accounts)
}

pub(crate) async fn complete_oauth(
    accounts: &AccountModule,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    authorization: &coral_provider::account::Authorization,
) {
    let state = url::Url::parse(authorization.authorization_url.as_deref().unwrap())
        .unwrap()
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned();
    assert_eq!(
        accounts
            .complete_oauth(
                context,
                workspace,
                authorization.id,
                &state,
                Secret::new("code-canary".into()).unwrap()
            )
            .await
            .unwrap()
            .state,
        coral_provider::account::AuthorizationState::Succeeded
    );
}
