//! Completion compares every credential method's accepted authority with the current contract.
use crate::account::{AccountScope, AuthorizationState};
use crate::account_tests::{authorization_request, publish_provider};
use crate::tests::{change_context, organization_user, test_modules};
use crate::{CredentialRoute, OpenApiDocument, ProviderRegenerationRequest};
use coral_credentials::Secret;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};
use serde_json::{Value, json};
coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
async fn secret_completion_rejects_a_changed_authentication_method() {
    rejects_changed_contract(
        json!({"type":"http", "scheme":"bearer"}),
        json!({"type":"apiKey", "in":"header", "name":"x-api-key"}),
        false,
    )
    .await;
}

#[tokio::test]
async fn secret_completion_rejects_a_changed_header() {
    rejects_changed_contract(
        json!({"type":"apiKey", "in":"header", "name":"x-first-key"}),
        json!({"type":"apiKey", "in":"header", "name":"x-second-key"}),
        false,
    )
    .await;
}

#[tokio::test]
async fn bearer_completion_rejects_a_changed_destination() {
    let bearer = json!({"type":"http", "scheme":"bearer"});
    rejects_changed_contract(bearer.clone(), bearer, true).await;
}

#[tokio::test]
async fn header_completion_rejects_a_changed_destination() {
    let header = json!({"type":"apiKey", "in":"header", "name":"x-api-key"});
    rejects_changed_contract(header.clone(), header, true).await;
}

fn document(method: &Value, destination: &str) -> String {
    json!({
        "openapi":"3.1.0", "info":{"title":"Credentials", "version":"1"},
        "servers":[{"url":destination}], "security":[{"credential":[]}],
        "components":{"securitySchemes":{"credential":method}},
        "paths":{"/data":{"get":{"operationId":"read", "responses":{"200":{"description":"Data"}}}}}
    })
    .to_string()
}

#[expect(
    clippy::too_many_lines,
    reason = "one domain journey proves changed-contract rejection and terminal replay without credential creation"
)]
async fn rejects_changed_contract(before: Value, after: Value, destination_changed: bool) {
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(db.database(), "user:contract", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Contracts").unwrap())
        .await
        .unwrap();
    let scope = workspace.id();
    let provider = publish_provider(
        &providers,
        &context,
        scope,
        &document(&before, "https://api.example.test/original"),
        "contracts",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let authorization = accounts
        .start_user_authorization(
            &context,
            scope,
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let next = document(
        &after,
        if destination_changed {
            "https://api.example.test/replacement"
        } else {
            "https://api.example.test/original"
        },
    );
    let setup = providers
        .prepare_openapi_fixture(&context, scope, OpenApiDocument::parse(&next).unwrap())
        .await
        .unwrap();
    providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                authorization_request(),
                scope.clone(),
                provider.id().clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    let before_callback = accounts
        .get_authorization(&context, scope, authorization.id)
        .await
        .unwrap();
    assert_eq!(before_callback.state, AuthorizationState::Canceled);
    let result = accounts
        .submit_secret(
            &context,
            scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("obsolete-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.state, AuthorizationState::Canceled);
    assert_eq!(result.failure_code(), Some("configuration_changed"));
    assert_eq!(result.account_id, None);
    let account = accounts
        .get_account(&context, scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .expect("initial Account remains unconnected");
    assert_eq!(account.state, crate::account::AccountState::Disconnected);
    assert_eq!(account.connected_at_unix_nanos, None);
    assert_eq!(account.configured_by_user_id, None);
    let events = accounts
        .list_user_lifecycle_events(&context, scope)
        .await
        .unwrap();
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                scope,
                authorization.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("replay-secret-canary".into()).unwrap()
                )
            )
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        accounts
            .list_user_lifecycle_events(&context, scope)
            .await
            .unwrap(),
        events
    );
    assert!(!format!("{result:?}{events:?}").contains("secret-canary"));
}
