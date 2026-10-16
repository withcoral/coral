//! Local loopback adapter. Durable domain state identifies and fences the initiating actor.
use axum::{
    Router,
    extract::{RawQuery, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use coral_provider::account::AccountModule;

pub(super) fn router(accounts: AccountModule) -> Router {
    Router::new()
        .route("/provider-oauth/callback", get(callback))
        .with_state(accounts)
}

async fn callback(State(accounts): State<AccountModule>, RawQuery(query): RawQuery) -> Response {
    let result = complete(&accounts, query.as_deref().unwrap_or_default()).await;
    let (status, text) = if result.is_ok() {
        (
            StatusCode::OK,
            "Return to Coral to check your connection. You can close this browser tab.",
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            "Coral could not complete sign-in. Return to Coral to check your connection.",
        )
    };
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
        ],
        text,
    )
        .into_response()
}

async fn complete(accounts: &AccountModule, query: &str) -> Result<(), coral_provider::Error> {
    use coral_provider::Error;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_nanos()).ok())
        .ok_or(Error::AccountInput)?;
    let context = coral_workspace::ChangeContext::new(
        coral_workspace::local_organization_owner(),
        now,
        coral_telemetry::TraceId::current_or_new(),
    );
    accounts.complete_callback(&context, query).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::test_config::loopback_policy;
    use coral_catalog::OpenApiDocument;
    use coral_credentials::Secret;
    use coral_provider::{
        OAuthClientAuthentication, OAuthClientConfiguration, ProviderModule, SetupOAuthClient,
    };
    use coral_workspace::{
        ChangeContext, TraceId, WorkspaceDisplayName, WorkspaceModule, local_organization_owner,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_string_contains, method, path},
    };

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the adapter journey verifies durable cookie-free correlation across listener restart and duplicate callbacks"
    )]
    async fn loopback_callback_completes_saved_consent_without_a_reef_cookie() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("config.toml"),
            "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
        )
        .unwrap();
        // This callback fixture explicitly permits its loopback token server.
        // Server bootstrap opens its filesystem credential vault.
        let policy = loopback_policy();
        let host = crate::ServerBuilder::new()
            .with_outbound_defaults(policy)
            .with_config_dir(directory.path())
            .start()
            .await
            .unwrap();
        let accounts = host.provider_accounts().unwrap();
        let workspaces = WorkspaceModule::new(host.database());
        let providers = ProviderModule::new(
            policy,
            host.database(),
            workspaces.clone(),
            coral_catalog::BuiltinCatalog::running().expect("the built-in catalog prepares"),
        );
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap();
        let context =
            ChangeContext::new(local_organization_owner(), now, TraceId::current_or_new());
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Callback").unwrap())
            .await
            .unwrap();
        let owner = workspace.id().clone();
        let upstream = MockServer::start().await;
        Mock::given(method("POST")).and(path("/token")).and(body_string_contains("client_secret=application-canary"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token":"account-canary", "token_type":"Bearer", "expires_in":3600}))).expect(1).mount(&upstream).await;
        let document = OpenApiDocument::parse(&serde_json::json!({"openapi":"3.1.0", "info":{"title":"Callback", "version":"1"},
            "servers":[{"url":upstream.uri()}], "security":[{"oauth":["read"]}],
            "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{"authorizationUrl":format!("{}/authorize",upstream.uri()),"tokenUrl":format!("{}/token",upstream.uri()),"scopes":{"read":"Read"}}}}}},
            "paths":{"/files":{"get":{"operationId":"listFiles","responses":{"200":{"description":"Files"}}}}}}).to_string()).unwrap();
        let setup = providers
            .create_openapi_setup(&context, workspace.id(), document)
            .await
            .unwrap();
        let callback =
            super::super::start_listener("127.0.0.1:0".parse().unwrap(), router(accounts.clone()))
                .await
                .unwrap();
        let address = callback.local_addr();
        assert!(
            matches!(
                super::super::start_listener(address, router(accounts.clone())).await,
                Err(crate::auth::AuthServerError::Bind { address: attempted, source })
                    if attempted == address && source.kind() == std::io::ErrorKind::AddrInUse
            ),
            "a callback port conflict fails without selecting another address"
        );
        let redirect = format!("{}/provider-oauth/callback", callback.endpoint_uri());
        let configuration = OAuthClientConfiguration::new(
            "registered-client".into(),
            OAuthClientAuthentication::ClientSecretPost {
                secret: Some(Secret::new("application-canary".into()).unwrap()),
            },
        );
        let review = providers
            .review_setup_configuration(context.actor(), &owner, setup.id())
            .await
            .unwrap();
        let confirmed = accounts
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                review.revision,
                review.document.unwrap().suggested,
                Some(SetupOAuthClient::application(redirect.clone(), configuration).unwrap()),
            )
            .await
            .unwrap();
        let started = accounts
            .start_setup_authorization(
                &context,
                &owner,
                setup.id(),
                confirmed.revision,
                SetupOAuthClient::new(redirect.clone(), None).unwrap(),
            )
            .await
            .unwrap();
        let navigation = url::Url::parse(
            started
                .authorization
                .unwrap()
                .authorization_url
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            navigation
                .query_pairs()
                .find(|(key, _)| key == "redirect_uri")
                .unwrap()
                .1,
            redirect
        );
        let state = navigation
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned();
        callback.shutdown().await.unwrap();
        let callback = super::super::start_listener(address, router(accounts))
            .await
            .unwrap();
        let client = reqwest::Client::new();
        let forged = client
            .get(&redirect)
            .query(&[("state", "forged"), ("code", "code-canary")])
            .send()
            .await
            .unwrap();
        assert_eq!(forged.status(), 400);
        let duplicate = client
            .get(&redirect)
            .query(&[
                ("state", state.as_str()),
                ("state", state.as_str()),
                ("code", "code-canary"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(duplicate.status(), 400);
        for _ in 0..2 {
            let response = client
                .get(&redirect)
                .query(&[("state", state.as_str()), ("code", "code-canary")])
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
            assert!(!response.text().await.unwrap().contains("canary"));
        }
        let progress = providers
            .setup_authentication(context.actor(), &owner, setup.id(), now)
            .await
            .unwrap();
        assert!(progress.authorized);
        assert_eq!(progress.automatic_refresh, Some(false));
        callback.shutdown().await.unwrap();
        host.shutdown().await.unwrap();
    }
}
