//! Personal Accounts and the connection requests that waiting Invocations open.
use std::time::Duration;

use coral_credentials::{CredentialValues, CredentialVault, Secret};
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceId};

use super::{
    AccountModule, AccountScope, AccountState, AuthorizationState, ConnectionRequestId,
    ConnectionResolution, ConnectionState, resolve_connection_in_transaction,
};
use crate::access::CredentialRoute;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use crate::tests::{
    CREDENTIAL_FREE_OPENAPI, change_context, organization_user, publish_provider, test_modules,
};
use crate::{Error, ProviderModule};

/// Opens `user`'s connection request the way a waiting Invocation does.
async fn open_connection_request(
    providers: &ProviderModule,
    workspace: &WorkspaceId,
    user: &coral_workspace::AuthenticatedUser,
    now: i64,
) -> ConnectionRequestId {
    let mut tx = providers.database.begin().await.unwrap();
    let target = crate::resolve_owner_tool_in_transaction(
        &mut tx,
        workspace,
        "coral.providers.private.getPrivateForecast",
    )
    .await
    .unwrap()
    .expect("callable Tool");
    let principal = coral_workspace::AuthenticatedPrincipal::user(user.clone());
    let ConnectionResolution::Waiting { request: id, .. } =
        resolve_connection_in_transaction(&mut tx, &target, principal.principal(), None, now)
            .await
            .unwrap()
    else {
        panic!("a User without an Account waits on a connection request")
    };
    tx.commit().await.unwrap();
    id
}

async fn connect(
    accounts: &AccountModule,
    context: &coral_workspace::ChangeContext,
    workspace: &WorkspaceId,
    provider: &crate::ProviderId,
    secret: &str,
) -> Result<super::Authorization, Error> {
    let authorization = accounts
        .start_user_authorization(context, workspace, provider, authorization_request(), None)
        .await
        .unwrap();
    accounts
        .submit_secret(
            context,
            workspace,
            authorization.id,
            CredentialValues::token(Secret::new(secret.into()).unwrap()),
        )
        .await
}

/// Two waiting Invocations of one User that find no Account at once open
/// one connection request. On Postgres their shared holds let both reach the
/// insert, and the second joins the first's pending row instead of failing
/// the chunk that carries it on the unique pending index; on SQLite the write
/// transaction serializes them at `BEGIN`, and the second's lookup finds the
/// first's request. Either way the User has one pending request.
#[tokio::test]
async fn concurrent_creators_of_one_users_connection_request_share_its_row() {
    const TOOL: &str = "coral.providers.private.getPrivateForecast";
    let (db, workspaces, providers) = test_modules().await;
    let user = organization_user(
        db.database(),
        "user:concurrent-connection",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(user.clone(), 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Concurrent").unwrap())
        .await
        .unwrap();
    let scope = workspace.id().clone();
    publish_provider(
        &providers,
        &context,
        &scope,
        CREDENTIAL_REQUIRED_OPENAPI,
        "private",
        CredentialRoute::Caller,
    )
    .await;
    let principal = coral_workspace::AuthenticatedPrincipal::user(user.clone());

    // The first creator keeps its request uncommitted while the second resolves.
    let mut first = providers.database.begin().await.unwrap();
    let target = crate::resolve_owner_tool_in_transaction(&mut first, &scope, TOOL)
        .await
        .unwrap()
        .expect("callable Tool");
    let ConnectionResolution::Waiting {
        request: created, ..
    } = resolve_connection_in_transaction(&mut first, &target, principal.principal(), None, 20)
        .await
        .unwrap()
    else {
        panic!("a User without an Account waits on a connection request")
    };
    let mut second = tokio::spawn({
        let database = providers.database.clone();
        let scope = scope.clone();
        let principal = principal.clone();
        async move {
            let mut tx = database.begin().await.unwrap();
            let target = crate::resolve_owner_tool_in_transaction(&mut tx, &scope, TOOL)
                .await
                .unwrap()
                .expect("callable Tool");
            let resolution = resolve_connection_in_transaction(
                &mut tx,
                &target,
                principal.principal(),
                None,
                21,
            )
            .await;
            tx.commit().await.unwrap();
            resolution
        }
    });
    // Neither backend lets the second creator finish first: SQLite's write
    // transaction waits at BEGIN, Postgres at the unique index entry.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut second)
            .await
            .is_err(),
        "the second creator waits for the first"
    );
    first.commit().await.unwrap();
    let ConnectionResolution::Waiting {
        request: joined, ..
    } = second.await.unwrap().unwrap()
    else {
        panic!("the second creator waits on a connection request")
    };
    assert_eq!(
        joined, created,
        "the second creator joins the first's request"
    );
    let user_id = coral_workspace::UserId::parse(principal.principal().id().as_str()).unwrap();
    let mut tx = providers.database.begin_read_snapshot().await.unwrap();
    assert_eq!(
        super::repository::pending_connections(&mut tx, &scope, &user_id)
            .await
            .unwrap(),
        vec![created],
        "one pending request for the User"
    );
    tx.commit().await.unwrap();
}

/// The Accounts page lists one row per caller Provider for the reading User,
/// allocates nothing to read, and shows a pending request until it connects.
#[tokio::test]
async fn user_accounts_list_caller_providers_with_only_the_readers_pending_request() {
    let (db, workspaces, providers) = test_modules().await;
    let user =
        organization_user(db.database(), "user:account-list", OrganizationRole::Member).await;
    let context = change_context(user.clone(), 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Accounts").unwrap())
        .await
        .unwrap();
    let scope = workspace.id().clone();
    let caller = publish_provider(
        &providers,
        &context,
        &scope,
        CREDENTIAL_REQUIRED_OPENAPI,
        "private",
        CredentialRoute::Caller,
    )
    .await;
    publish_provider(
        &providers,
        &context,
        &scope,
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = AccountModule::new(
        providers.clone(),
        CredentialVault::open(&directory.path().join("key")).unwrap(),
    );

    for _ in 0..2 {
        let listed = accounts.list_user_accounts(&context, &scope).await.unwrap();
        let [row] = listed.as_slice() else {
            panic!("one row per caller Provider: {}", listed.len())
        };
        assert_eq!(row.provider.id(), caller.id());
        assert!(row.account.account.is_none(), "reading allocates nothing");
        assert!(row.account.can_connect);
        assert_eq!(row.pending_connection, None);
    }

    let request = open_connection_request(&providers, &scope, &user, 20).await;
    let listed = accounts.list_user_accounts(&context, &scope).await.unwrap();
    let [row] = listed.as_slice() else {
        panic!("one row per caller Provider")
    };
    assert_eq!(
        row.account.account.as_ref().map(|account| account.state),
        Some(AccountState::Disconnected)
    );
    assert_eq!(
        row.pending_connection.as_ref().map(|pending| pending.id),
        Some(request)
    );

    let owner = change_context(
        organization_user(
            db.database(),
            "user:account-list-owner",
            OrganizationRole::Owner,
        )
        .await,
        30,
        '2',
    );
    crate::tests::join_as_owner(&workspaces, owner.actor(), &scope).await;
    assert!(
        accounts
            .list_user_accounts(&owner, &scope)
            .await
            .unwrap()
            .iter()
            .all(|row| row.account.account.is_none() && row.pending_connection.is_none()),
        "an Owner never sees another User's Account or request"
    );

    connect(&accounts, &context, &scope, caller.id(), "listed-canary")
        .await
        .unwrap();
    let listed = accounts.list_user_accounts(&context, &scope).await.unwrap();
    let [row] = listed.as_slice() else {
        panic!("one row per caller Provider")
    };
    assert_eq!(
        row.account.account.as_ref().map(|account| account.state),
        Some(AccountState::Ready)
    );
    assert!(row.account.can_reconnect);
    assert_eq!(
        row.pending_connection, None,
        "connecting clears the request"
    );
}

/// Every path that ends a connection request writes its lifecycle event in the
/// same transaction. When the event write fails, the request, the Account and
/// the authorization all keep their earlier state.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fault covers each path that ends a connection request"
)]
async fn a_failed_connection_event_rolls_back_the_request_transition() {
    let (db, workspaces, providers) = test_modules().await;
    let user = organization_user(
        db.database(),
        "user:connection-event-fault",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(user.clone(), 10, '1');
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Atomic events").unwrap(),
        )
        .await
        .unwrap();
    let scope = workspace.id().clone();
    let provider = publish_provider(
        &providers,
        &context,
        &scope,
        CREDENTIAL_REQUIRED_OPENAPI,
        "private",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = AccountModule::new(
        providers.clone(),
        CredentialVault::open(&directory.path().join("key")).unwrap(),
    );
    let first = open_connection_request(&providers, &scope, &user, 20).await;
    accounts
        .decline_connection(&context, &scope, first)
        .await
        .unwrap();

    // Reject the second completion event in this Workspace.
    db.execute_fixture_sql(
        "CREATE UNIQUE INDEX reject_second_connection_event ON provider_lifecycle_events(workspace_id) WHERE action = 'account_connection.completed'",
    )
    .await
    .unwrap();
    let request = open_connection_request(&providers, &scope, &user, 30).await;
    assert_ne!(request, first);
    let pending = accounts
        .get_connection_request(&context, &scope, request)
        .await
        .unwrap();
    assert!(matches!(
        accounts.decline_connection(&context, &scope, request).await,
        Err(Error::Database(_))
    ));
    assert!(matches!(
        accounts
            .get_connection_request(
                &change_context(user.clone(), pending.expires_at_unix_nanos, '5'),
                &scope,
                request
            )
            .await,
        Err(Error::Database(_))
    ));
    let authorization = accounts
        .start_user_authorization(
            &context,
            &scope,
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        accounts
            .submit_secret(
                &context,
                &scope,
                authorization.id,
                CredentialValues::token(Secret::new("rolled-back-credential".into()).unwrap()),
            )
            .await,
        Err(Error::Database(_))
    ));
    let administrator = change_context(
        organization_user(db.database(), "user:administrator", OrganizationRole::Owner).await,
        40,
        '4',
    );
    crate::tests::join_as_owner(&workspaces, administrator.actor(), &scope).await;
    // Offboarding ends the request too. coral-administration links its own copy of
    // this crate, so its Provider error is matched by variant name.
    let offboarding =
        coral_administration::AccessLifecycleModule::new(db.database(), workspaces.clone())
            .remove_workspace_member(&administrator, &scope, user.user_id())
            .await;
    assert!(
        matches!(&offboarding, Err(coral_administration::Error::Provider(error)) if format!("{error:?}").starts_with("Database(")),
        "{offboarding:?}"
    );

    let progress = accounts
        .get_authorization(&context, &scope, authorization.id)
        .await
        .unwrap();
    assert_eq!(progress.state, AuthorizationState::Pending);
    assert_eq!(progress.account_id, None);
    let account = accounts
        .get_account(&context, &scope, provider.id(), &AccountScope::User)
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.state, AccountState::Disconnected);
    assert_eq!(account.connected_at_unix_nanos, None);
    assert_eq!(account.configured_by_user_id, None);
    assert_eq!(
        accounts
            .get_connection_request(&context, &scope, request)
            .await
            .unwrap(),
        pending
    );
    assert!(
        !accounts
            .list_user_lifecycle_events(&context, &scope)
            .await
            .unwrap()
            .iter()
            .any(|event| event.target_id() == request.to_string())
    );
    assert_eq!(pending.state, ConnectionState::Pending);
}
