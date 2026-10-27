//! Provider authority and scoped credential routes through the domain interface.
use super::*;
use crate::LifecycleAction;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use crate::tests::{
    CREDENTIAL_FREE_OPENAPI, change_context, organization_user, publish_provider, test_modules,
};
use coral_credentials::Secret;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceRole};

#[tokio::test]
async fn exact_inspection_uses_workspace_scope_and_requires_a_membership() {
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:exact-owner", OrganizationRole::Member).await;
    let administrator =
        organization_user(db.database(), "user:administrator", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Exact").unwrap())
        .await
        .unwrap();
    let other = workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "exact",
        CredentialRoute::None,
    )
    .await;
    // An Organization Owner without a Membership gets the non-Member denial.
    assert!(matches!(
        providers
            .inspect_provider(&administrator, workspace.id(), provider.id())
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert!(matches!(
        providers
            .delete_provider(
                &change_context(administrator.clone(), 11, 'b'),
                workspace.id(),
                provider.id(),
            )
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert_eq!(
        providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        provider
    );
    assert!(matches!(
        providers
            .inspect_provider(&owner, other.id(), provider.id())
            .await,
        Err(Error::ProviderNotFound)
    ));
    // Assigned as a designated Owner, they act like any other Owner.
    crate::tests::join_as_owner(&workspaces, &administrator, workspace.id()).await;
    providers
        .delete_provider(
            &change_context(administrator.clone(), 11, 'b'),
            workspace.id(),
            provider.id(),
        )
        .await
        .unwrap();
    assert!(matches!(
        providers
            .inspect_provider(&administrator, workspace.id(), provider.id())
            .await,
        Err(Error::ProviderNotFound)
    ));
}

#[tokio::test]
async fn route_etag_race_has_one_winner_and_no_op_preserves_version_and_evidence() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:route-race", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Route").unwrap())
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = providers
        .publish_shared_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "race",
            &accounts,
            coral_credentials::CredentialValues::token(Secret::new("race-canary".into()).unwrap()),
        )
        .await
        .unwrap();
    let stale = context.clone().with_etag(&provider.etag()).unwrap();
    let (left, right) = tokio::join!(
        providers.change_account_setting_fixture(
            &stale,
            workspace.id(),
            provider.id(),
            CredentialRoute::Caller
        ),
        providers.change_account_setting_fixture(
            &stale,
            workspace.id(),
            provider.id(),
            CredentialRoute::Caller
        ),
    );
    let updated = match (left, right) {
        (Ok(updated), Err(Error::EtagMismatch | Error::ProviderChangedDuringPublication))
        | (Err(Error::EtagMismatch | Error::ProviderChangedDuringPublication), Ok(updated)) => {
            updated
        }
        other => panic!("exactly one route change must commit: {other:?}"),
    };
    assert_eq!(updated.version(), 2);
    assert_eq!(updated.call_root(), provider.call_root());
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::ProviderCredentialRouteChanged)
            .count(),
        1
    );
    let current = providers
        .inspect_provider(&owner, workspace.id(), provider.id())
        .await
        .unwrap();
    let current_context = context.with_etag(&current.etag()).unwrap();
    assert_eq!(
        providers
            .change_account_setting_fixture(
                &current_context,
                workspace.id(),
                provider.id(),
                CredentialRoute::Caller
            )
            .await
            .unwrap(),
        updated
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .iter()
            .filter(|event| event.action() == LifecycleAction::ProviderCredentialRouteChanged)
            .count(),
        1
    );
    assert!(matches!(
        providers
            .delete_provider(&stale, workspace.id(), provider.id())
            .await,
        Err(Error::EtagMismatch)
    ));
    assert_eq!(
        providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        current
    );
}

#[test]
fn call_root_parsing_accepts_owner_namespaces_and_only_the_builtin_coral_root() {
    enum Parsed<'a> {
        Owner(&'a str),
        Builtin,
        Rejected,
    }
    let segment = "a".repeat(128);
    let long = format!("coral.providers.{segment}");
    for (value, expected) in [
        ("coral.providers.github", Parsed::Owner("github")),
        ("coral.providers.$_g1", Parsed::Owner("$_g1")),
        ("coral.providers.workspace", Parsed::Owner("workspace")),
        (long.as_str(), Parsed::Owner(segment.as_str())),
        ("coral.workspace", Parsed::Builtin),
        // Coral defines no other root, so none parses.
        ("coral.billing", Parsed::Rejected),
        ("coral.functions", Parsed::Rejected),
        ("coral.emit", Parsed::Rejected),
        ("coral.ToolError", Parsed::Rejected),
        (&format!("{long}a"), Parsed::Rejected),
        ("coral.workspace.", Parsed::Rejected),
        ("coral.workspace.tools", Parsed::Rejected),
        ("github", Parsed::Rejected),
        ("coral", Parsed::Rejected),
        ("providers.github", Parsed::Rejected),
        ("coral.providers", Parsed::Rejected),
        ("coral.providers.", Parsed::Rejected),
        ("coral..github", Parsed::Rejected),
        ("coral.providers.1github", Parsed::Rejected),
        ("coral.providers.git-hub", Parsed::Rejected),
        ("coral.providers.gitHüb", Parsed::Rejected),
        ("coral.providers.then", Parsed::Rejected),
        ("coral.providers.__proto__", Parsed::Rejected),
        ("coral.providers.constructor", Parsed::Rejected),
        ("coral.prototype.github", Parsed::Rejected),
        ("coral.providers.weather.v2", Parsed::Rejected),
        ("coral.functions.github", Parsed::Rejected),
    ] {
        let parsed = ProviderCallRoot::parse(value);
        match expected {
            Parsed::Owner(expected) => {
                let Ok(ProviderCallRoot::Owner(namespace)) = &parsed else {
                    panic!("{value}: {parsed:?}");
                };
                assert_eq!(namespace.as_str(), expected, "{value}");
            }
            Parsed::Builtin => assert!(
                matches!(parsed, Ok(ProviderCallRoot::Builtin)),
                "{value}: {parsed:?}"
            ),
            Parsed::Rejected => assert!(
                matches!(parsed, Err(Error::InvalidInput(_))),
                "{value} must be rejected: {parsed:?}"
            ),
        }
        if let Ok(root) = parsed {
            assert_eq!(root.as_str(), value, "a parsed root keeps its exact text");
        }
    }
}

#[tokio::test]
async fn workspace_uniqueness_and_invocation_follow_the_stored_call_root() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:call-roots", OrganizationRole::Owner).await;
    for (case, (first, second, conflicts)) in [
        ("workspace", "workspace", true),
        ("workspace", "weather", false),
    ]
    .into_iter()
    .enumerate()
    {
        let tick = 100 * (i64::try_from(case).unwrap() + 1);
        let context = change_context(owner.clone(), tick, 'a');
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Roots").unwrap())
            .await
            .unwrap();
        let mut published = Vec::new();
        for (offset, namespace) in [(1, first), (2, second)] {
            let context = change_context(owner.clone(), tick + offset, 'b');
            let setup = providers
                .prepare_openapi_fixture(
                    &context,
                    workspace.id(),
                    crate::OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).unwrap(),
                )
                .await
                .unwrap();
            let publication = providers
                .publish_workspace_provider(
                    &context,
                    crate::WorkspaceProviderPublicationRequest::new(
                        authorization_request(),
                        workspace.id().clone(),
                        setup.id().clone(),
                        ProviderNamespace::parse(namespace).unwrap(),
                        CredentialRoute::None,
                    ),
                )
                .await
                .unwrap();
            published.push(publication);
        }
        let [earlier, later] = <[_; 2]>::try_from(published).unwrap();
        assert_eq!(
            later.diagnostic().map(crate::PublicationDiagnostic::code),
            conflicts.then_some("provider_namespace_conflict"),
            "{second} after {first}"
        );
        let database = db.database();
        let mut transaction = database.begin().await.unwrap();
        for (namespace, publication) in [(first, &earlier), (second, &later)] {
            let call_path = format!("coral.providers.{namespace}.getForecast");
            let resolved = crate::resolve_owner_tool_in_transaction(
                &mut transaction,
                workspace.id(),
                &call_path,
            )
            .await
            .unwrap()
            .map(|target| target.provider_access().id().clone());
            let owner = publication
                .published_provider()
                .or(earlier.published_provider())
                .map(|provider| provider.id().clone());
            assert_eq!(resolved, owner, "{call_path} after {first}");
        }
        transaction.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn exact_provider_reads_report_an_unparseable_stored_call_root_as_corrupt() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:stored-root", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Stored").unwrap())
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "legacy",
        CredentialRoute::None,
    )
    .await;
    // A bare namespace, as stored before Call Roots, is not a Call Root.
    db.execute_fixture_sql(&format!(
        "UPDATE provider_records SET namespace = 'legacy' WHERE id = '{}'",
        provider.id()
    ))
    .await
    .unwrap();
    let inspected = providers
        .inspect_provider(&owner, workspace.id(), provider.id())
        .await;
    assert!(
        matches!(inspected, Err(Error::CorruptData(_))),
        "{inspected:?}"
    );
    let database = db.database();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let access =
        crate::access::exact_in_transaction(&mut transaction, workspace.id(), provider.id()).await;
    assert!(matches!(access, Err(Error::CorruptData(_))), "{access:?}");
    transaction.rollback().await.unwrap();
}

/// A database from before one upgrade, holding a Provider an Owner added:
/// the upgrade keeps the Provider and its catalog, so it loads and its
/// Tools keep their Call Paths. `0028` prefixes each stored namespace.
/// `0026`, which rebuilds the catalog tables on SQLite, names the tables
/// that `0045` renamed, so it cannot run after the current schema.
#[tokio::test]
async fn upgrade_keeps_each_provider_an_owner_added() {
    for (version, before) in [(
        28,
        Some("UPDATE provider_records SET namespace = 'weather' WHERE id = '{provider}'"),
    )] {
        // Every migration but the one under test: the code arranging the rows
        // runs against the current schema, less that upgrade.
        let before_upgrade = coral_test_support::edited_migration_plan(|migrations| {
            migrations
                .into_iter()
                .filter(|migration| migration.version != version)
                .collect()
        });
        let db = coral_test_support::TestDatabase::new(&before_upgrade)
            .await
            .expect("create database from before the upgrade");
        let database = db.database();
        let workspaces = coral_workspace::WorkspaceModule::new(database.clone());
        let providers = crate::ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        );
        let owner =
            organization_user(database.clone(), "user:upgrade", OrganizationRole::Owner).await;
        let context = change_context(owner.clone(), 10, 'a');
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Upgrade").unwrap())
            .await
            .unwrap();
        let provider = publish_provider(
            &providers,
            &context,
            workspace.id(),
            CREDENTIAL_FREE_OPENAPI,
            "weather",
            CredentialRoute::None,
        )
        .await;
        if let Some(before) = before {
            db.execute_fixture_sql(&before.replace("{provider}", &provider.id().to_string()))
                .await
                .unwrap();
        }

        database
            .migrate(coral_db::migration_plan())
            .await
            .unwrap_or_else(|error| panic!("apply upgrade {version}: {error}"));

        let inspected = providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap();
        assert_eq!(
            inspected.call_root().as_str(),
            "coral.providers.weather",
            "upgrade {version}"
        );
        let mut transaction = database.begin().await.unwrap();
        let resolved = crate::resolve_owner_tool_in_transaction(
            &mut transaction,
            workspace.id(),
            "coral.providers.weather.getForecast",
        )
        .await
        .unwrap()
        .map(|target| target.provider_access().id().clone());
        assert_eq!(resolved.as_ref(), Some(provider.id()), "upgrade {version}");
        transaction.rollback().await.unwrap();
    }
}

#[test]
fn only_one_call_root_owns_a_call_path_and_splits_it() {
    for (call_path, split) in [
        (
            "coral.providers.github.repos.get",
            Some(("coral.providers.github", "repos.get")),
        ),
        (
            "coral.workspace.addProvider",
            Some(("coral.workspace", "addProvider")),
        ),
        (
            "coral.workspace.providers.create",
            Some(("coral.workspace", "providers.create")),
        ),
        (
            "coral.providers.workspace.create",
            Some(("coral.providers.workspace", "create")),
        ),
        ("coral.providers.github", None),
        ("coral.providers.github.", None),
        ("coral.providers.github..get", None),
        ("coral.providers.github.get\n", None),
        ("coral.functions.github.get", None),
        ("coral.workspace", None),
        ("github.repos.get", None),
    ] {
        assert_eq!(
            ProviderCallRoot::split_call_path(call_path)
                .as_ref()
                .map(|(root, relative)| (root.as_str(), *relative)),
            split,
            "{call_path:?}"
        );
    }
}

#[tokio::test]
async fn workspace_member_cannot_change_provider_configuration() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        db.database(),
        "user:configuration-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        db.database(),
        "user:configuration-member",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(owner, 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Authority").unwrap())
        .await
        .unwrap();
    workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    let before = providers
        .list_lifecycle_events(context.actor(), workspace.id())
        .await
        .unwrap();
    let unauthorized = change_context(member, 11, 'b');
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &unauthorized,
                workspace.id(),
                provider.id(),
                CredentialRoute::Caller
            )
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert!(matches!(
        providers
            .delete_provider(&unauthorized, workspace.id(), provider.id())
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert_eq!(
        providers
            .list_lifecycle_events(context.actor(), workspace.id())
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scoped credential journey checks eligible owners and every rejected target"
)]
async fn fixed_routes_require_exact_provider_and_eligible_administrative_owner() {
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:fixed-owner", OrganizationRole::Member).await;
    let second =
        organization_user(db.database(), "user:fixed-second", OrganizationRole::Owner).await;
    let context = change_context(owner, 10, 'a');
    let admin = change_context(second.clone(), 10, 'b');
    let left = workspaces
        .create(&context, WorkspaceDisplayName::parse("Left").unwrap())
        .await
        .unwrap();
    let right = workspaces
        .create(&context, WorkspaceDisplayName::parse("Right").unwrap())
        .await
        .unwrap();
    for workspace in [&left, &right] {
        crate::tests::join_as_owner(&workspaces, &second, workspace.id()).await;
    }
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = providers
        .publish_shared_fixture(
            &admin,
            left.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "fixed",
            &accounts,
            coral_credentials::CredentialValues::token(Secret::new("fixed-secret".into()).unwrap()),
        )
        .await
        .unwrap();
    let other = publish_provider(
        &providers,
        &admin,
        left.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "other",
        CredentialRoute::Caller,
    )
    .await;
    let foreign = publish_provider(
        &providers,
        &admin,
        right.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "fixed",
        CredentialRoute::Caller,
    )
    .await;
    let account_id = accounts
        .get_account(
            &admin,
            left.id(),
            provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    for (workspace, target) in [(&left, &other), (&right, &foreign)] {
        assert!(matches!(
            providers
                .change_account_setting_fixture(
                    &context,
                    workspace.id(),
                    target.id(),
                    CredentialRoute::Fixed
                )
                .await,
            Err(Error::AccountRequired)
        ));
    }
    assert!(
        !providers
            .list_lifecycle_events(context.actor(), right.id())
            .await
            .unwrap()
            .iter()
            .any(|event| event.action() == LifecycleAction::ProviderCredentialRouteChanged)
    );
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                right.id(),
                provider.id(),
                account_id,
                authorization_request(),
                None
            )
            .await,
        Err(Error::AccountNotFound)
    ));
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &context,
                left.id(),
                provider.id(),
                CredentialRoute::None
            )
            .await,
        Err(Error::AccountRequired)
    ));
    for route in [CredentialRoute::None, CredentialRoute::Fixed] {
        let setup = providers
            .prepare_openapi_fixture(
                &context,
                left.id(),
                crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            )
            .await
            .unwrap();
        let publication = providers
            .publish_workspace_provider(
                &context,
                crate::WorkspaceProviderPublicationRequest::new(
                    authorization_request(),
                    left.id().clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse("invalid").unwrap(),
                    route,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            publication.outcome(),
            crate::ProviderPublicationOutcome::Failed
        );
        assert_eq!(publication.diagnostic().unwrap().code(), "account_required");
    }
}

coral_test_support::provider_setup_fixtures!(crate);

/// Each Provider's account use for an Agent and its blockers, by namespace.
type AgentView = std::collections::BTreeMap<
    String,
    (
        (&'static str, Option<crate::account::ProviderAccountId>),
        Vec<ProviderBlocker>,
    ),
>;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one inspection compares every account setting before and after the Agent connects"
)]
async fn agent_inspection_shows_each_settings_account_for_that_agent_only() {
    use coral_workspace::{AgentDisplayName, WorkspaceAction};
    let h = crate::tests::Harness::new().await;
    let owner = organization_user(
        h.database.clone(),
        "user:agent-inspection",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = h
        .workspaces
        .create(&context, WorkspaceDisplayName::parse("Agents").unwrap())
        .await
        .unwrap();
    let other_workspace = h
        .workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    let create_agent = async |workspace: &crate::WorkspaceId, name: &str| {
        h.workspaces
            .create_agent(
                &context,
                workspace,
                AgentDisplayName::parse(name).unwrap(),
                None,
            )
            .await
            .unwrap()
            .agent
    };
    let agent = create_agent(workspace.id(), "Worker").await;
    let foreign_agent = create_agent(other_workspace.id(), "Elsewhere").await;
    publish_provider(
        &h.providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "open",
        CredentialRoute::None,
    )
    .await;
    let shared = h
        .providers
        .publish_shared_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "shared",
            &h.accounts,
            coral_credentials::CredentialValues::token(
                Secret::new("shared-inspection-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let shared_account = h
        .accounts
        .get_account(
            &context,
            workspace.id(),
            shared.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    let separate = publish_provider(
        &h.providers,
        &context,
        workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "separate",
        CredentialRoute::Caller,
    )
    .await;
    let connect = async |started: crate::account::Authorization| {
        h.accounts
            .submit_secret(
                &context,
                workspace.id(),
                started.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("inspection-canary".into()).unwrap(),
                ),
            )
            .await
            .unwrap()
            .account_id
            .unwrap()
    };
    // The inspecting Owner's own Account never stands in for the Agent's.
    connect(
        h.accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                separate.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap(),
    )
    .await;

    let inspect = async |actor: &coral_workspace::AuthenticatedUser,
                         agent: &coral_workspace::Agent|
           -> Result<AgentView, Error> {
        let mut tx = h.database.begin().await.unwrap();
        let evidence = h
            .workspaces
            .check_in_transaction(&mut tx, actor, workspace.id(), WorkspaceAction::Inspect)
            .await
            .unwrap();
        let inspected = inspect_agent_providers_in_transaction(&mut tx, &evidence, agent).await;
        tx.rollback().await.unwrap();
        Ok(inspected?
            .into_iter()
            .map(|inspection| {
                let account = match inspection.account {
                    AgentAccountUse::None => ("none", None),
                    AgentAccountUse::Shared(view) => {
                        ("shared", view.account.map(|account| account.id))
                    }
                    AgentAccountUse::Separate(view) => {
                        ("separate", view.account.map(|account| account.id))
                    }
                };
                (
                    inspection.readiness.provider.call_root().name().to_owned(),
                    (account, inspection.readiness.blockers),
                )
            })
            .collect())
    };
    let mut expected = AgentView::from([
        ("open".to_owned(), (("none", None), vec![])),
        (
            "separate".to_owned(),
            (
                ("separate", None),
                vec![ProviderBlocker::CallerAccountRequired],
            ),
        ),
        (
            "shared".to_owned(),
            (("shared", Some(shared_account)), vec![]),
        ),
    ]);
    assert_eq!(inspect(&owner, &agent).await.unwrap(), expected);

    let agent_account = connect(
        h.accounts
            .start_agent_authorization(
                &context,
                workspace.id(),
                agent.principal().id(),
                separate.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap(),
    )
    .await;
    expected.insert(
        "separate".to_owned(),
        (("separate", Some(agent_account)), vec![]),
    );
    assert_eq!(inspect(&owner, &agent).await.unwrap(), expected);

    assert!(
        matches!(
            inspect(&owner, &foreign_agent).await,
            Err(Error::Workspace(coral_workspace::Error::AgentNotFound))
        ),
        "an Agent from another Workspace is not found"
    );
    let member = organization_user(
        h.database.clone(),
        "user:agent-inspection-member",
        OrganizationRole::Member,
    )
    .await;
    h.workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    assert!(
        matches!(
            inspect(&member, &agent).await,
            Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                _
            )))
        ),
        "only an Owner inspects Agent Providers"
    );
}

#[test]
fn a_route_fits_a_source_only_when_it_sends_what_the_source_needs() {
    use coral_catalog::{AuthenticationConfiguration, ProviderCredentialContract};
    let free = ProviderCredentialContract::NONE;
    let bearer = ProviderCredentialContract::from(AuthenticationConfiguration::Bearer)
        .at_destinations(vec!["https://weather.example".into()]);
    for (route, contract, expected) in [
        (CredentialRoute::None, &free, RouteFit::Fits),
        (
            CredentialRoute::Fixed,
            &free,
            RouteFit::SourceNeedsNoCredential,
        ),
        (
            CredentialRoute::Caller,
            &free,
            RouteFit::SourceNeedsNoCredential,
        ),
        (
            CredentialRoute::None,
            &bearer,
            RouteFit::SourceNeedsCredential,
        ),
        (CredentialRoute::Fixed, &bearer, RouteFit::Fits),
        (CredentialRoute::Caller, &bearer, RouteFit::Fits),
    ] {
        assert_eq!(route.fits(contract), expected, "{route:?} for {contract:?}");
    }
    assert_eq!(CredentialRoute::fitting(&free), vec![CredentialRoute::None]);
    assert_eq!(
        CredentialRoute::fitting(&bearer),
        vec![CredentialRoute::Caller, CredentialRoute::Fixed]
    );
}
