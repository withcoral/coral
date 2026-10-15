use std::sync::Arc;

use coral_test_support::{TestDatabase, change, provision_user, trace};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};

use crate::{
    Error, LifecycleAction, MiddlewareChangeContext, MiddlewareDescription, MiddlewareModule,
    MiddlewareName, MiddlewareSave, MiddlewareScopes, MiddlewareSource, MoveDirection,
};

const PASS_THROUGH: &str =
    "export default async (ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) => next();";

struct Fixture {
    _database: TestDatabase,
    middleware: MiddlewareModule,
    database: Arc<coral_db::Database>,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    workspace: WorkspaceId,
}

impl Fixture {
    async fn new() -> Self {
        let migrator = coral_test_support::combined_migrator(&[
            coral_workspace::migrator(),
            crate::migrator(),
        ]);
        let fixture = TestDatabase::new(&migrator).await.expect("test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let member =
            provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(owner.clone(), 10, 'a'),
                WorkspaceDisplayName::parse("Middleware").expect("display name"),
            )
            .await
            .expect("create Workspace");
        workspaces
            .set_membership(
                &change(owner.clone(), 11, 'b'),
                workspace.id(),
                &member,
                WorkspaceRole::Member,
            )
            .await
            .expect("add Member");
        let middleware = MiddlewareModule::new(
            database.clone(),
            workspaces,
            coral_v8::IsolateLimits::default(),
        )
        .expect("start Middleware compiler");
        Self {
            _database: fixture,
            middleware,
            database,
            owner,
            member,
            workspace: workspace.id().clone(),
        }
    }

    fn context(user: &AuthenticatedUser, at: i64) -> MiddlewareChangeContext {
        MiddlewareChangeContext::new(AuthenticatedPrincipal::user(user.clone()), at, trace('c'))
    }

    async fn save(&self, name: &str, source: &str, at: i64) -> Result<crate::Middleware, Error> {
        self.middleware
            .save(
                &Self::context(&self.owner, at),
                &self.workspace,
                save(name, source),
            )
            .await
    }

    async fn chain(&self) -> Vec<(String, u32, bool)> {
        self.middleware
            .list(&self.owner, &self.workspace)
            .await
            .expect("list chain")
            .into_iter()
            .map(|item| {
                (
                    item.name().as_str().to_string(),
                    item.position(),
                    item.enabled(),
                )
            })
            .collect()
    }
}

fn save(name: &str, source: &str) -> MiddlewareSave {
    MiddlewareSave::new(
        MiddlewareName::parse(name).expect("name"),
        MiddlewareDescription::parse("Observe every Operation").expect("description"),
        MiddlewareSource::parse(source).expect("source"),
    )
}

const ALL_SCOPES: MiddlewareScopes = MiddlewareScopes {
    run: true,
    function: true,
    operation: true,
};

#[tokio::test]
async fn saves_create_disabled_innermost_middleware_and_updates_disable_it() {
    let f = Fixture::new().await;
    let created = f.save("audit", PASS_THROUGH, 20).await.expect("create");
    assert!(!created.enabled());
    assert_eq!(created.position(), 0);
    assert_eq!(created.scopes(), MiddlewareScopes::default());

    f.middleware
        .set_scopes(
            &Fixture::context(&f.owner, 21),
            &f.workspace,
            created.id(),
            ALL_SCOPES,
        )
        .await
        .expect("set scopes");
    f.middleware
        .set_enabled(
            &Fixture::context(&f.owner, 22),
            &f.workspace,
            created.id(),
            true,
        )
        .await
        .expect("enable");

    let updated = f
        .save(
            "audit",
            "export default async function audit(ctx: any, next: any) { return next(); }",
            30,
        )
        .await
        .expect("update");
    assert_eq!(updated.id(), created.id());
    assert!(!updated.enabled(), "every update disables");
    assert_eq!(updated.scopes(), ALL_SCOPES, "a save keeps scopes");
    assert!(updated.source().as_str().contains("function audit"));

    let actions: Vec<_> = f
        .middleware
        .list_lifecycle_events(&f.owner, &f.workspace)
        .await
        .expect("events")
        .into_iter()
        .map(|event| event.action)
        .collect();
    assert_eq!(
        actions,
        [
            LifecycleAction::Created,
            LifecycleAction::ScopesUpdated,
            LifecycleAction::Enabled,
            LifecycleAction::Updated,
        ]
    );
}

#[tokio::test]
async fn a_failed_save_keeps_the_accepted_definition() {
    let f = Fixture::new().await;
    let accepted = f.save("audit", PASS_THROUGH, 20).await.expect("create");
    for source in [
        "const started = Date.now();\nexport default async (ctx: any, next: any) => next();",
        "export default async (ctx: any, next: any) => coral.functions.helper();",
        "export default async (ctx: any, next: any) => {",
    ] {
        let error = f.save("audit", source, 30).await.expect_err(source);
        assert!(matches!(error, Error::Authoring(_)), "{source}: {error:?}");
    }
    let current = f
        .middleware
        .get(&f.owner, &f.workspace, accepted.id())
        .await
        .expect("get");
    assert_eq!(current, accepted);
    assert_eq!(
        f.middleware
            .list_lifecycle_events(&f.owner, &f.workspace)
            .await
            .expect("events")
            .len(),
        1
    );
}

#[tokio::test]
async fn only_workspace_owner_users_can_read_or_change_middleware() {
    let f = Fixture::new().await;
    let accepted = f.save("audit", PASS_THROUGH, 20).await.expect("create");
    let member = Fixture::context(&f.member, 30);

    let denied = f
        .middleware
        .save(&member, &f.workspace, save("other", PASS_THROUGH))
        .await
        .expect_err("Member save");
    assert!(matches!(denied, Error::PermissionDenied(_)), "{denied:?}");
    let denied = f
        .middleware
        .set_enabled(&member, &f.workspace, accepted.id(), true)
        .await
        .expect_err("Member enable");
    assert!(matches!(denied, Error::MiddlewareNotFound), "{denied:?}");
    let denied = f
        .middleware
        .list(&f.member, &f.workspace)
        .await
        .expect_err("Member list");
    assert!(matches!(denied, Error::PermissionDenied(_)), "{denied:?}");

    assert_eq!(f.chain().await, [("audit".to_string(), 0, false)]);
}

#[tokio::test]
async fn moving_disables_only_the_moved_middleware_and_delete_closes_the_gap() {
    let f = Fixture::new().await;
    let first = f.save("first", PASS_THROUGH, 20).await.expect("first");
    let second = f.save("second", PASS_THROUGH, 21).await.expect("second");
    let third = f.save("third", PASS_THROUGH, 22).await.expect("third");
    let owner = Fixture::context(&f.owner, 30);
    for item in [&first, &second, &third] {
        f.middleware
            .set_enabled(&owner, &f.workspace, item.id(), true)
            .await
            .expect("enable");
    }

    f.middleware
        .move_middleware(&owner, &f.workspace, third.id(), MoveDirection::Up)
        .await
        .expect("move up");
    assert_eq!(
        f.chain().await,
        [
            ("first".to_string(), 0, true),
            ("third".to_string(), 1, false),
            ("second".to_string(), 2, true),
        ]
    );
    let edge = f
        .middleware
        .move_middleware(&owner, &f.workspace, first.id(), MoveDirection::Up)
        .await
        .expect_err("move past the outermost end");
    assert!(matches!(edge, Error::InvalidInput(_)), "{edge:?}");

    f.middleware
        .delete(&owner, &f.workspace, first.id())
        .await
        .expect("delete");
    assert_eq!(
        f.chain().await,
        [
            ("third".to_string(), 0, false),
            ("second".to_string(), 1, true),
        ]
    );
}

#[tokio::test]
async fn admission_selects_the_enabled_chain_and_detects_any_later_change() {
    let f = Fixture::new().await;
    let outer = f.save("outer", PASS_THROUGH, 20).await.expect("outer");
    let _disabled = f
        .save("disabled", PASS_THROUGH, 21)
        .await
        .expect("disabled");
    let inner = f.save("inner", PASS_THROUGH, 22).await.expect("inner");
    let owner = Fixture::context(&f.owner, 30);
    for item in [&outer, &inner] {
        f.middleware
            .set_enabled(&owner, &f.workspace, item.id(), true)
            .await
            .expect("enable");
    }

    let mut transaction = f.database.begin_read_snapshot().await.expect("snapshot");
    let selection = crate::select_enabled_chain_in_transaction(&mut transaction, &f.workspace)
        .await
        .expect("select");
    transaction.commit().await.expect("commit");
    assert_eq!(
        selection
            .iter()
            .map(|item| item.name().as_str())
            .collect::<Vec<_>>(),
        ["outer", "inner"]
    );

    let mut transaction = f.database.begin().await.expect("transaction");
    assert!(
        crate::hold_chain_in_transaction(&mut transaction, &f.workspace, &selection)
            .await
            .expect("hold")
    );
    transaction.rollback().await.expect("rollback");

    f.middleware
        .move_middleware(&owner, &f.workspace, inner.id(), MoveDirection::Up)
        .await
        .expect("move");
    let mut transaction = f.database.begin().await.expect("transaction");
    assert!(
        !crate::hold_chain_in_transaction(&mut transaction, &f.workspace, &selection)
            .await
            .expect("hold")
    );
    transaction.rollback().await.expect("rollback");
}

#[test]
fn names_are_workspace_slugs() {
    for valid in ["a", "audit-log", "jev_2", &"a".repeat(64)] {
        MiddlewareName::parse(valid).expect(valid);
    }
    for invalid in [
        "",
        "Audit",
        "1audit",
        "audit log",
        "audit.log",
        &"a".repeat(65),
    ] {
        assert!(
            matches!(MiddlewareName::parse(invalid), Err(Error::InvalidInput(_))),
            "{invalid}"
        );
    }
}

#[tokio::test]
async fn a_workspace_holds_a_bounded_number_of_middleware() {
    let f = Fixture::new().await;
    for index in 0..crate::MIDDLEWARE_MAX_COUNT {
        f.save(&format!("m{index}"), PASS_THROUGH, 20)
            .await
            .expect("save within the limit");
    }
    let error = f
        .save("overflow", PASS_THROUGH, 30)
        .await
        .expect_err("save past the limit");
    assert!(matches!(error, Error::LimitExceeded(_)), "{error:?}");
    assert_eq!(f.chain().await.len(), crate::MIDDLEWARE_MAX_COUNT);
}
