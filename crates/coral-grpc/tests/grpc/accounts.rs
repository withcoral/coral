//! Scoped Account contracts use the real authenticated transport, not inventory adapters.
use super::*;
use management::start_provider_account_authorization_request::Target;

async fn bearer_setup(f: &Fixture) -> management::ProviderSetup {
    let mut setups = f.setups();
    setups
        .create_configured_setup(request(
            setup_request(
                format!("workspaces/{}", f.workspace.id()),
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
}

async fn caller_provider(f: &Fixture) -> String {
    let setup = bearer_setup(f).await;
    let mut providers = f.providers();
    let published = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                setup: setup.name,
                configuration_revision: 1,
                namespace: "accounts".into(),
                request_id: uuid::Uuid::new_v4().to_string(),
                credential_route: Some(route(management::credential_route::Kind::Caller)),
                ..Default::default()
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        published.outcome(),
        management::ProviderPublicationOutcome::Succeeded
    );
    published.provider
}

fn inspection(f: &Fixture, provider: &str) -> management::GetProviderAccountRequest {
    management::GetProviderAccountRequest {
        parent: format!("workspaces/{}", f.workspace.id()),
        provider: provider.into(),
        scope: management::ProviderAccountScope::User.into(),
        agent: String::new(),
    }
}

/// coral-provider owns replay, private ownership, and disconnection
/// (`account_inspection_disconnect_and_reconnect_preserve_private_ownership`,
/// `user_initial_completion_cannot_resurrect_after_disconnect_and_restart`).
/// This checks the authorization and Account projections and one denial.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Account follows authorization, submission, and disconnection RPCs"
)]
async fn account_authorization_and_disconnection_project_the_members_private_account() {
    let f = Fixture::new().await;
    let provider = caller_provider(&f).await;
    let member = f.token(&f.member, "reef");
    let mut accounts = f.accounts();
    let inspect = inspection(&f, &provider);
    let start = management::StartProviderAccountAuthorizationRequest {
        parent: inspect.parent.clone(),
        request_id: uuid::Uuid::new_v4().to_string(),
        target: Some(Target::Provider(provider.clone())),
        redirect_uri: "https://reef.example/account/callback".into(),
    };
    let pending = accounts
        .start_provider_account_authorization(request(start, &member, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert!(
        pending
            .name
            .starts_with(&format!("{}/accountAuthorizations/", inspect.parent))
    );
    assert!(pending.account.is_empty());
    let unconnected = accounts
        .get_provider_account(request(inspect.clone(), &member, "reef"))
        .await
        .unwrap()
        .into_inner()
        .account
        .unwrap();
    assert_eq!(
        unconnected.state(),
        management::ProviderAccountState::Disconnected
    );
    assert!(unconnected.connected_time.is_none());
    assert!(unconnected.configured_by_user.is_empty());
    // Workspace ownership does not reach a member's authorization.
    let hidden = accounts
        .get_provider_account_authorization(request(
            management::GetProviderAccountAuthorizationRequest {
                name: pending.name.clone(),
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (hidden.code(), error_reason(&hidden).as_str()),
        (tonic::Code::NotFound, "NOT_FOUND")
    );
    let completed = accounts
        .submit_provider_account_secret(request(
            management::SubmitProviderAccountSecretRequest {
                name: pending.name,
                credential_values: token_values(TOKEN),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        completed.state(),
        management::SetupAuthorizationState::Succeeded
    );
    assert_eq!(completed.account, unconnected.id);
    assert!(!format!("{completed:?}").contains(TOKEN));
    let connected = accounts
        .get_provider_account(request(inspect.clone(), &member, "reef"))
        .await
        .unwrap()
        .into_inner()
        .account
        .unwrap();
    assert_eq!(connected.state(), management::ProviderAccountState::Ready);
    assert!(connected.connected_time.is_some());
    assert_eq!(connected.configured_by_user, f.member.user_id().to_string());
    accounts
        .disconnect_provider_account(request(
            management::DisconnectProviderAccountRequest {
                parent: inspect.parent.clone(),
                provider,
                account: connected.id.clone(),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap();
    let disconnected = accounts
        .get_provider_account(request(inspect, &member, "reef"))
        .await
        .unwrap()
        .into_inner()
        .account
        .unwrap();
    assert_eq!(disconnected.id, connected.id);
    assert_eq!(
        disconnected.state(),
        management::ProviderAccountState::Disconnected
    );
    assert_eq!(disconnected.connected_time, connected.connected_time);
}

#[tokio::test]
async fn account_contract_rejects_invalid_scopes_and_shared_intent_on_a_caller_provider() {
    let f = Fixture::new().await;
    let provider = caller_provider(&f).await;
    let token = f.token(&f.owner, "reef");
    let mut accounts = f.accounts();
    let valid = inspection(&f, &provider);
    for (scope, agent) in [
        (0, String::new()),
        (999, String::new()),
        (
            management::ProviderAccountScope::Agent as i32,
            String::new(),
        ),
        (
            management::ProviderAccountScope::User as i32,
            "users/member".into(),
        ),
        (
            management::ProviderAccountScope::Shared as i32,
            "users/member".into(),
        ),
    ] {
        let error = accounts
            .get_provider_account(request(
                management::GetProviderAccountRequest {
                    scope,
                    agent,
                    ..valid.clone()
                },
                &token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    for target in [None, Some(Target::SharedProvider(provider))] {
        let expected = if target.is_none() {
            tonic::Code::InvalidArgument
        } else {
            tonic::Code::FailedPrecondition
        };
        assert_eq!(
            accounts
                .start_provider_account_authorization(request(
                    management::StartProviderAccountAuthorizationRequest {
                        parent: valid.parent.clone(),
                        request_id: uuid::Uuid::new_v4().to_string(),
                        target,
                        redirect_uri: "https://reef.example/account/callback".into(),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    assert!(
        accounts
            .get_provider_account(request(valid, &token, "reef"))
            .await
            .unwrap()
            .into_inner()
            .account
            .is_none()
    );
}

/// coral-provider owns which grants a shared route may retain
/// (`shared_publication_requires_both_compatible_credentials_and_explicit_retention`).
/// This checks the failed Publication's projection.
#[tokio::test]
async fn shared_publication_with_an_unreviewed_grant_fails_without_a_provider() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let setup = bearer_setup(&f).await;
    let authorized = f
        .setups()
        .authorize_provider_setup(request(
            management::AuthorizeProviderSetupRequest {
                name: setup.name.clone(),
                configuration_revision: 1,
                credential_values: token_values(TOKEN),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(authorized.authorized);
    let failed = f
        .providers()
        .publish_provider(request(
            management::PublishProviderRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                setup: setup.name,
                configuration_revision: 1,
                request_id: uuid::Uuid::new_v4().to_string(),
                namespace: "invalid_grant".into(),
                credential_route: Some(route(management::credential_route::Kind::Fixed)),
                account_grant: uuid::Uuid::new_v4().to_string(),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        failed.outcome(),
        management::ProviderPublicationOutcome::Failed
    );
    assert_eq!(failed.diagnostic.as_ref().unwrap().code, "account_required");
    assert!(failed.provider.is_empty());
    assert!(!format!("{failed:?}").contains(TOKEN));
}

/// coral-run owns when a Run waits on a connection
/// (`user_accounts_show_a_waiting_run_until_the_user_connects`). This checks one
/// row per caller-route Provider, its pending connection, and the Reef-only audience.
#[tokio::test]
async fn list_user_accounts_projects_each_caller_provider_and_its_pending_connection() {
    let f = Fixture::new().await;
    let provider = caller_provider(&f).await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let list = async |token: &str, audience: &str| {
        f.accounts()
            .list_user_accounts(request(
                management::ListUserAccountsRequest {
                    parent: parent.clone(),
                },
                token,
                audience,
            ))
            .await
    };
    let member = f.token(&f.member, "reef");
    let before = list(&member, "reef").await.unwrap().into_inner();
    let [row] = before.accounts.as_slice() else {
        panic!("one row per separate-account Provider: {before:?}")
    };
    assert_eq!(row.provider, provider);
    assert_eq!(row.provider_display_name, "Weather");
    let account = row.account.as_ref().unwrap();
    assert!(account.account.is_none(), "listing allocates no Account");
    assert!(account.can_connect);
    assert!(row.pending_connection.is_none());

    let actor = coral_workspace::AuthenticatedPrincipal::user(f.member.clone());
    let accepted = f
        .runs
        .start(
            &actor,
            f.workspace.id(),
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse("await coral.providers.accounts.getForecast({});")
                    .unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    // The initial observation returns once the Run asks for this User's connection.
    let waiting = f
        .runs
        .observe(
            &actor,
            f.workspace.id(),
            accepted.run_id(),
            &coral_run::RunObservation::initial(std::time::Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    let [action] = waiting.required_actions() else {
        panic!("one connection request: {waiting:?}")
    };
    let coral_run::RequiredActionRequest::AccountConnection(connection) = &action.request else {
        panic!("a connection request: {action:?}")
    };
    let after = list(&member, "reef").await.unwrap().into_inner();
    let pending = after.accounts.first().unwrap().pending_connection.as_ref();
    assert_eq!(
        pending.map(|request| (
            request.name.clone(),
            request.provider.clone(),
            request.state()
        )),
        Some((
            format!("{parent}/accountConnections/{connection}"),
            provider,
            management::AccountConnectionState::Pending
        ))
    );
    let denied = list(&f.token(&f.member, "mcp"), "mcp").await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}
