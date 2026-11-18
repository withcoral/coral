use coral_catalog::AuthoredToolKind;
use coral_db::{Page, PageKey};
use coral_test_support::{change, page_request, read_pages};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::Notify;

use coral_catalog::ToolAccessState;
use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_v8::CompileFailure;
use coral_workspace::{
    AuthenticatedPrincipal, IfMatch, OrganizationRole, RequestId, TraceId, WorkspaceDisplayName,
    WorkspaceModule, WorkspaceRole,
};

use crate::authoring::{AuthoredToolCompiler, PreparedAuthoredTool};
use crate::{
    AuthoredToolDescription, AuthoredToolSource, Error,
    LifecycleAction as PrivateToolLifecycleAction, PrivateToolChangeContext, PrivateToolDeletion,
    PrivateToolInspection, PrivateToolModule, PrivateToolPath, PrivateToolSave,
};

#[test]
fn fixed_private_tool_source_and_declaration_limits_accept_the_boundary_only() {
    let maximum_private_tool = "f".repeat(crate::AUTHORED_TOOL_SOURCE_MAX_BYTES);
    AuthoredToolSource::parse(&maximum_private_tool).expect("maximum Private Tool source");
    AuthoredToolSource::parse(&format!("{maximum_private_tool}f"))
        .expect_err("oversized Private Tool source");

    let maximum_declaration = "d".repeat(crate::AUTHORED_TOOL_DECLARATION_MAX_BYTES);
    crate::AuthoredToolDeclaration::from_compiler(&maximum_declaration)
        .expect("maximum Private Tool declaration");
    // The refusal reaches the author inside an authoring failure, so it
    // says what is too large and how to make it smaller.
    assert_eq!(
        crate::AuthoredToolDeclaration::from_compiler(&format!("{maximum_declaration}d")),
        Err(
            "the Tool declaration exceeds 262144 bytes. Return fewer fields, or annotate the return type."
                .to_string()
        )
    );
}

// Catches: a cursor that does not spell its key back, or text that is no key
// of a list taken for one.
#[test]
fn a_page_key_spells_its_cursor_and_reads_no_other_text() {
    fn spells<K: PageKey>(cursors: &[&str], refused: &[&str]) {
        for cursor in cursors {
            let key = K::from_cursor(cursor).unwrap_or_else(|| panic!("{cursor} is a key"));
            assert_eq!(key.to_cursor(), *cursor);
        }
        for text in refused {
            assert!(K::from_cursor(text).is_none(), "{text:?} is no key");
        }
    }
    let paths = ["alpha", "reports.total", "Middle.upper"];
    let no_paths = ["", "reports..total", ".alpha", "not a path", "1st", "a/b"];
    spells::<PrivateToolPath>(&paths, &no_paths);
    spells::<crate::SharedToolPath>(&paths, &no_paths);
    spells::<crate::SharedToolLifecycleEventKey>(
        &[
            "30/00000000-0000-4000-8000-000000000000",
            "-30/6f1e3a52-9c0b-4d1e-8a7f-2b5c4d3e2f10",
            "9223372036854775807/ffffffff-ffff-4fff-bfff-ffffffffffff",
        ],
        &[
            "",
            "30",
            "30/",
            "/00000000-0000-4000-8000-000000000000",
            "00000000-0000-4000-8000-000000000000",
            // One time and one identity each have one spelling.
            "030/00000000-0000-4000-8000-000000000000",
            "+30/00000000-0000-4000-8000-000000000000",
            "30.0/00000000-0000-4000-8000-000000000000",
            "9223372036854775808/00000000-0000-4000-8000-000000000000",
            "30/00000000-0000-4000-8000-00000000000G",
            "30/6F1E3A52-9C0B-4D1E-8A7F-2B5C4D3E2F10",
            "30/00000000-0000-4000-8000-000000000000/",
        ],
    );
}

#[tokio::test]
async fn private_tool_save_publishes_exact_authoring_record_and_event() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:tool-author").await;
    let source = r"
        interface Input { value: number }
        function double(value: number): number { return value * 2; }
        export default function calculate(input: Input): number {
            return double(input.value);
        }
    ";

    let saved = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 20, 'a'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("finance.calculate").expect("Private Tool Path"),
                AuthoredToolDescription::parse("Calculate a doubled finance value")
                    .expect("description"),
                AuthoredToolSource::parse(source).expect("source"),
            ),
        )
        .await
        .expect("save tool");

    assert!(saved.declaration().as_str().contains("interface Input"));
    assert!(saved.declaration().as_str().contains("export default"));
    let crate::AuthoredToolDeclarationFieldProjection::Ready(fields) =
        saved.declaration_field_projection()
    else {
        panic!("declaration fields must be ready");
    };
    assert!(fields.iter().any(|field| field.path() == "input.value"));
    let events = fixture
        .private_tools
        .list_lifecycle_events(&principal, &workspace_id)
        .await
        .expect("list author lifecycle Events");
    assert!(matches!(
        events.as_slice(),
        [event]
            if event.action() == PrivateToolLifecycleAction::Created
                && event.outcome() == crate::LifecycleOutcome::Accepted
                && event.target_id() == saved.id()
                && event.target_path() == saved.path()
                && event.changed_field_paths()
                    == [
                        "source",
                        "description",
                        "declaration",
                        "declaration_fields",
                        "diagnostics",
                    ]
    ));
}

// Catches: a save that loses the default its author declared, an update
// without a policy that resets it, or a change to it that its lifecycle
// Event does not name.
#[tokio::test]
async fn a_save_keeps_the_declared_default_until_one_changes_it_and_names_each_change() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:tool-policy").await;
    let saved_fields = [
        "source",
        "description",
        "declaration",
        "declaration_fields",
        "diagnostics",
    ];
    let with_default = [saved_fields.as_slice(), &["default_access_state"]].concat();
    // Each save in order: its path, the policy it declares, the default it
    // leaves, and whether its Event names the default.
    let steps = [
        ("lookup.plain", None, ToolAccessState::Enabled, false),
        ("lookup.plain", None, ToolAccessState::Enabled, false),
        (
            "lookup.plain",
            Some(ToolAccessState::ApprovalRequired),
            ToolAccessState::ApprovalRequired,
            true,
        ),
        (
            "lookup.plain",
            None,
            ToolAccessState::ApprovalRequired,
            false,
        ),
        (
            "lookup.plain",
            Some(ToolAccessState::ApprovalRequired),
            ToolAccessState::ApprovalRequired,
            false,
        ),
        (
            "lookup.plain",
            Some(ToolAccessState::Enabled),
            ToolAccessState::Enabled,
            true,
        ),
        (
            "lookup.blocked",
            Some(ToolAccessState::Disabled),
            ToolAccessState::Disabled,
            true,
        ),
        (
            "lookup.allowed",
            Some(ToolAccessState::Enabled),
            ToolAccessState::Enabled,
            false,
        ),
    ];
    for (step, (path, policy, default, names_default)) in steps.into_iter().enumerate() {
        let path = PrivateToolPath::parse(path).expect("path");
        let save = PrivateToolSave::new(
            path.clone(),
            AuthoredToolDescription::parse("Look up one value").expect("description"),
            AuthoredToolSource::parse(&format!("export default async () => {step};"))
                .expect("source"),
        )
        .declaring_default(policy);
        let at = 20 + i64::try_from(step).expect("step");
        fixture
            .private_tools
            .save(
                &private_tool_change(principal.clone(), at, 'a'),
                &workspace_id,
                save,
            )
            .await
            .expect("save");
        assert_eq!(
            fixture
                .private_tools
                .get(&principal, &workspace_id, &path)
                .await
                .expect("read the saved tool")
                .default_access_state(),
            default,
            "step {step}"
        );
        let events = fixture
            .private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list lifecycle Events");
        let event = events
            .iter()
            .max_by_key(|event| event.occurred_at())
            .expect("the save's Event");
        assert_eq!(
            event.changed_field_paths(),
            if names_default {
                with_default.as_slice()
            } else {
                saved_fields.as_slice()
            },
            "step {step}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle scenario covers failed save, replacement, and deletion atomicity"
)]
async fn failed_save_preserves_current_definition_and_accepted_changes_follow_transaction_order() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:tool-editor").await;
    let path = PrivateToolPath::parse("reports.total").expect("path");
    let original_source = "export default function total() { return 41; }";
    let original = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 100, 'b'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Original total").expect("description"),
                AuthoredToolSource::parse(original_source).expect("source"),
            ),
        )
        .await
        .expect("save original");

    let failed = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 200, 'c'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Broken replacement").expect("description"),
                AuthoredToolSource::parse("export default function (").expect("source"),
            ),
        )
        .await;
    assert!(matches!(
        failed,
        Err(Error::Admission(AuthoredToolKind::Private, _))
    ));
    let unchanged = fixture
        .private_tools
        .get(&principal, &workspace_id, &path)
        .await
        .expect("read unchanged current tool");
    assert_eq!(unchanged.id(), original.id());
    assert_eq!(unchanged.source().as_str(), original_source);
    assert_eq!(
        fixture
            .private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("events after rejection")
            .len(),
        1
    );

    let replacement_source = "export default function total() { return 42; }";
    let replacement = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 50, 'd'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Replacement total").expect("description"),
                AuthoredToolSource::parse(replacement_source).expect("source"),
            ),
        )
        .await
        .expect("replace despite older occurrence timestamp");
    assert_eq!(replacement.id(), original.id());
    assert_eq!(replacement.source().as_str(), replacement_source);
    assert_eq!(replacement.saved_at(), coral_db::UnixNanos::new(50));

    let deleted_id = fixture
        .private_tools
        .delete(
            &private_tool_change(principal.clone(), 40, 'e'),
            &workspace_id,
            &path,
        )
        .await
        .expect("delete despite older occurrence timestamp");
    assert_eq!(&deleted_id, original.id());
    assert!(matches!(
        fixture
            .private_tools
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    let events = fixture
        .private_tools
        .list_lifecycle_events(&principal, &workspace_id)
        .await
        .expect("complete lifecycle evidence");
    assert_eq!(events.len(), 3);
    assert!(events.iter().any(|event| {
        event.action() == PrivateToolLifecycleAction::Created && event.target_id() == original.id()
    }));
    assert!(events.iter().any(|event| {
        event.action() == PrivateToolLifecycleAction::Replaced && event.target_id() == original.id()
    }));
    assert!(events.iter().any(|event| {
        event.action() == PrivateToolLifecycleAction::Deleted
            && event.target_id() == original.id()
            && event.changed_field_paths()
                == [
                    "source",
                    "description",
                    "declaration",
                    "declaration_fields",
                    "diagnostics",
                ]
    }));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario compares both reads across replacement, deletion, and Workspaces"
)]
async fn entry_reads_return_current_private_tools_with_revision_and_declaration_fields() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, user) = fixture.user_workspace("user:entry-author").await;
    let agent = registered_agent(&fixture.workspaces, &user, &workspace_id).await;
    let (other_workspace_id, other_user) = fixture.user_workspace("user:entry-elsewhere").await;
    let save = |principal: &AuthenticatedPrincipal,
                workspace_id: &coral_workspace::WorkspaceId,
                path: &str,
                description: &str,
                at: i64| {
        let private_tools = fixture.private_tools.clone();
        let context = private_tool_change(principal.clone(), at, 'a');
        let workspace_id = workspace_id.clone();
        let save = PrivateToolSave::new(
            PrivateToolPath::parse(path).expect("path"),
            AuthoredToolDescription::parse(description).expect("description"),
            AuthoredToolSource::parse(
                "interface Input { value: number }\nexport default function run(input: Input): number { return input.value; }",
            )
            .expect("source"),
        );
        async move {
            private_tools
                .save(&context, &workspace_id, save)
                .await
                .expect("save tool")
        }
    };

    save(&user, &workspace_id, "reports.total", "Total a report", 20).await;
    let replaced = save(
        &user,
        &workspace_id,
        "reports.total",
        "Total a report again",
        30,
    )
    .await;
    let agent_private_tool = save(
        &agent,
        &workspace_id,
        "agent.answer",
        "Answer a question",
        40,
    )
    .await;
    let gone = save(&user, &workspace_id, "reports.gone", "Soon deleted", 50).await;
    fixture
        .private_tools
        .delete(
            &private_tool_change(user.clone(), 60, 'b'),
            &workspace_id,
            &PrivateToolPath::parse("reports.gone").expect("path"),
        )
        .await
        .expect("delete tool");
    let elsewhere = save(
        &other_user,
        &other_workspace_id,
        "reports.total",
        "Total elsewhere",
        70,
    )
    .await;

    let mut transaction = fixture.database.begin().await.expect("begin entry read");
    let every_entry = crate::authored_tool_entries_in_transaction(&mut transaction)
        .await
        .expect("read every Workspace's entries");
    for (workspace, tool, revision) in [
        (&workspace_id, replaced.id(), Some(2)),
        (&other_workspace_id, elsewhere.id(), Some(1)),
        (&other_workspace_id, replaced.id(), None),
        (&workspace_id, gone.id(), None),
    ] {
        assert_eq!(
            crate::authored_tool_entry_in_transaction(
                &mut transaction,
                workspace,
                &tool.clone().into()
            )
            .await
            .expect("read one tool's entry")
            .map(|entry| entry.revision()),
            revision
        );
    }
    transaction.rollback().await.expect("finish entry read");

    let mut summary = every_entry
        .iter()
        .map(|entry| {
            (
                entry.workspace_id().clone(),
                entry.principal().cloned(),
                entry.path().call_path(),
                entry.description().as_str().to_string(),
                entry.revision(),
            )
        })
        .collect::<Vec<_>>();
    summary.sort_by(|left, right| (&left.2, &left.3).cmp(&(&right.2, &right.3)));
    assert_eq!(
        summary,
        [
            (
                workspace_id.clone(),
                Some(agent.principal().clone()),
                "coral.private.agent.answer".to_string(),
                "Answer a question".to_string(),
                1,
            ),
            (
                workspace_id.clone(),
                Some(user.principal().clone()),
                "coral.private.reports.total".to_string(),
                "Total a report again".to_string(),
                2,
            ),
            (
                other_workspace_id.clone(),
                Some(other_user.principal().clone()),
                "coral.private.reports.total".to_string(),
                "Total elsewhere".to_string(),
                1,
            ),
        ],
        "every Principal's current tools of every Workspace, at their current revision"
    );
    for saved in [&replaced, &agent_private_tool, &elsewhere] {
        let read = every_entry
            .iter()
            .find(|entry| entry.id().private() == Some(saved.id()))
            .expect("current tool has an entry");
        assert_eq!(
            read.declaration_fields(),
            saved.declaration_field_projection().fields(),
            "the read agrees with what the save projected"
        );
        assert!(
            read.declaration_fields()
                .expect("ready declaration fields")
                .iter()
                .any(|field| field.path() == "input.value")
        );
    }

    crate::test_fixtures_repository::corrupt_declaration_fields(
        &fixture.database,
        &workspace_id,
        agent_private_tool.id(),
    )
    .await;
    let mut transaction = fixture.database.begin().await.expect("begin entry read");
    let corrupt = crate::authored_tool_entry_in_transaction(
        &mut transaction,
        &workspace_id,
        &agent_private_tool.id().clone().into(),
    )
    .await
    .expect("a corrupt projection does not fail the read")
    .expect("the tool stays readable by path and description");
    transaction.rollback().await.expect("finish entry read");
    assert_eq!(corrupt.declaration_fields(), None);
    assert_eq!(corrupt.description().as_str(), "Answer a question");
}

#[tokio::test]
async fn agent_and_user_private_tool_paths_are_principal_owned() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, user) = fixture.user_workspace("user:tool-owner").await;
    let agent = registered_agent(&fixture.workspaces, &user, &workspace_id).await;
    let path = PrivateToolPath::parse("private.answer").expect("path");
    let saved = fixture
        .private_tools
        .save(
            &private_tool_change(agent.clone(), 20, 'd'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Agent-private answer").expect("description"),
                AuthoredToolSource::parse("export default () => 42;").expect("source"),
            ),
        )
        .await
        .expect("Agent saves its own tool");

    assert!(matches!(
        fixture.private_tools.get(&user, &workspace_id, &path).await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(
        fixture
            .private_tools
            .get(&agent, &workspace_id, &path)
            .await
            .expect("Agent reads own current definition")
            .id(),
        saved.id()
    );
    fixture
        .private_tools
        .delete(
            &private_tool_change(agent.clone(), 30, 'e'),
            &workspace_id,
            &path,
        )
        .await
        .expect("Agent deletes its own tool");
    assert!(matches!(
        fixture
            .private_tools
            .get(&agent, &workspace_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert!(matches!(
        fixture
            .private_tools
            .list_lifecycle_events(&agent, &workspace_id)
            .await,
        Err(Error::PermissionDenied(_))
    ));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one visibility scenario compares a designated Owner with a Member and a non-Member Organization Owner"
)]
async fn only_designated_owners_inspect_and_delete_agent_private_tools_without_authoring_as_the_agent()
 {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, workspace_owner) = fixture.user_workspace("user:workspace-owner").await;
    let agent = registered_agent(&fixture.workspaces, &workspace_owner, &workspace_id).await;
    let agent_id = agent.principal().id().clone();
    let path = PrivateToolPath::parse("private.answer").expect("path");
    let agent_private_tool = fixture
        .private_tools
        .save(
            &private_tool_change(agent.clone(), 20, '7'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Agent-owned answer").expect("description"),
                AuthoredToolSource::parse("export default () => 'agent';").expect("source"),
            ),
        )
        .await
        .expect("Agent saves its tool");

    assert_eq!(
        fixture
            .inspection
            .get_agent(&workspace_owner, &workspace_id, &agent_id, &path)
            .await
            .expect("Workspace Owner inspects Agent tool")
            .id(),
        agent_private_tool.id()
    );
    assert_eq!(
        fixture
            .inspection
            .list_agent(&workspace_owner, &workspace_id, &agent_id)
            .await
            .expect("Workspace Owner lists Agent tools")
            .as_slice(),
        std::slice::from_ref(&agent_private_tool)
    );

    let owner_private_tool = fixture
        .private_tools
        .save(
            &private_tool_change(workspace_owner.clone(), 30, '8'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Owner's private answer").expect("description"),
                AuthoredToolSource::parse("export default () => 'owner';").expect("source"),
            ),
        )
        .await
        .expect("Owner authors only as the Owner's User");
    assert_ne!(owner_private_tool.id(), agent_private_tool.id());
    assert_eq!(
        fixture
            .inspection
            .get_agent(&workspace_owner, &workspace_id, &agent_id, &path)
            .await
            .expect("Agent definition remains unchanged")
            .id(),
        agent_private_tool.id()
    );

    let member_user = organization_user(
        fixture.database.clone(),
        "user:tool-member",
        OrganizationRole::Member,
    )
    .await;
    fixture
        .workspaces
        .set_membership(
            &change(
                workspace_owner
                    .authenticated_user()
                    .expect("Workspace Owner User")
                    .clone(),
                35,
                'a',
            ),
            &workspace_id,
            &member_user,
            WorkspaceRole::Member,
        )
        .await
        .expect("add ordinary Member");
    let member = AuthenticatedPrincipal::user(member_user);
    assert!(matches!(
        fixture
            .inspection
            .get_agent(&member, &workspace_id, &agent_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert!(matches!(
        fixture
            .inspection
            .list_agent(&member, &workspace_id, &agent_id)
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert!(matches!(
        fixture
            .delete_agent(
                &private_tool_change(member, 36, 'b'),
                &workspace_id,
                &agent_id,
                &path,
            )
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));

    let organization_owner = AuthenticatedPrincipal::user(
        organization_user(
            fixture.database.clone(),
            "user:organization-owner",
            OrganizationRole::Owner,
        )
        .await,
    );
    // An Organization Owner without a Membership is denied like any non-Member.
    assert!(matches!(
        fixture
            .inspection
            .list_agent(&organization_owner, &workspace_id, &agent_id)
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert!(matches!(
        fixture
            .inspection
            .get_agent(&organization_owner, &workspace_id, &agent_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert!(matches!(
        fixture
            .delete_agent(
                &private_tool_change(organization_owner, 40, '9'),
                &workspace_id,
                &agent_id,
                &path,
            )
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(
        fixture
            .private_tools
            .get(&agent, &workspace_id, &path)
            .await
            .expect("denied deletions leave the Agent tool current")
            .id(),
        agent_private_tool.id()
    );

    fixture
        .delete_agent(
            &private_tool_change(workspace_owner.clone(), 41, '9'),
            &workspace_id,
            &agent_id,
            &path,
        )
        .await
        .expect("designated Owner deletes Agent tool");

    let events = fixture
        .private_tools
        .list_lifecycle_events(&workspace_owner, &workspace_id)
        .await
        .expect("list the Owner's lifecycle Events");
    assert!(
        matches!(
            events.last(),
            Some(event)
                if event.action() == PrivateToolLifecycleAction::Deleted
                    && event.target_id() == agent_private_tool.id()
                    && event.actor() == workspace_owner.principal()
        ),
        "the Owner is the actor of the Agent tool's deletion: {events:?}"
    );

    assert!(matches!(
        fixture
            .private_tools
            .get(&agent, &workspace_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(
        fixture
            .private_tools
            .get(&workspace_owner, &workspace_id, &path)
            .await
            .expect("Owner's User tool stays private and current")
            .id(),
        owner_private_tool.id()
    );
}

// Catches: an Owner's read or delete of an Agent's Private Tool that selects
// a User's when the request names that User's Principal as the Agent.
#[tokio::test]
async fn naming_a_user_as_the_agent_selects_no_private_tool() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, owner) = fixture.user_workspace("user:workspace-owner").await;
    let path = PrivateToolPath::parse("private.answer").expect("path");
    let saved = fixture
        .private_tools
        .save(
            &private_tool_change(owner.clone(), 20, '7'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("A User's answer").expect("description"),
                AuthoredToolSource::parse("export default () => 'user';").expect("source"),
            ),
        )
        .await
        .expect("User saves their tool");
    let user_id = owner.principal().id();

    assert!(matches!(
        fixture
            .inspection
            .get_agent(&owner, &workspace_id, user_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert!(matches!(
        fixture
            .delete_agent(
                &private_tool_change(owner.clone(), 30, '8'),
                &workspace_id,
                user_id,
                &path,
            )
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(
        fixture
            .private_tools
            .get(&owner, &workspace_id, &path)
            .await
            .expect("the refused delete keeps the User's tool")
            .id(),
        saved.id()
    );
}

/// The Saved Function rows the release before Private Tools wrote, at step
/// 62, for one User and one Agent, read back after the upgrade as each
/// owner's Private Tools, with every part of the definition kept.
// Catches: an upgrade that loses or rewrites part of a definition, gives a
// Private Tool another owner, or leaves it without the default Tool Access
// State, the binding to the definition Coral performs, or its renamed table.
#[tokio::test]
async fn upgrade_keeps_each_saved_function_as_its_owners_private_tool() {
    let before_upgrade = coral_test_support::migration_plan_before(63);
    let fixture = PrivateToolTestModules::on_plan(&before_upgrade).await;
    // This version writes the columns of the resource version step when it
    // creates the Workspace and the Agent. The fixture holds them until the
    // upgrade.
    coral_test_support::add_resource_version_columns(&fixture.fixture).await;
    let (workspace_id, owner) = fixture.user_workspace("user:upgrade-owner").await;
    let agent = registered_agent(&fixture.workspaces, &owner, &workspace_id).await;
    let declaration = "export default function lookup(input: { id: string }): Promise<string>;";
    let diagnostics = r#"[{"category":"warning","code":6133,"message":"'unused' is declared but its value is never read.","line":2,"column":7}]"#;
    let stored = [
        (
            &owner,
            "00000000-0000-4000-8000-0000000000a1",
            "crm.lookup",
            3,
        ),
        (
            &agent,
            "00000000-0000-4000-8000-0000000000a2",
            "ops.lookup",
            7,
        ),
    ];
    for (principal, id, path, revision) in stored {
        fixture
            .fixture
            .execute_fixture_sql(&format!(
                "INSERT INTO saved_function_records (id, workspace_id, principal_id, \
                 principal_kind, path, description, source, declaration, \
                 declaration_fields_format, declaration_fields_status, \
                 declaration_fields_json, declaration_fields_failure, diagnostics_json, \
                 diagnostics_truncated, saved_at_unix_nanos, definition_revision) \
                 VALUES ('{id}', '{workspace_id}', '{}', '{}', '{path}', 'Look up {path}', \
                 'export default async function lookup(input) {{ return input.id; }}', \
                 '{declaration}', 'saved-function-declaration-fields-v1', 'ready', '[]', \
                 NULL, '{}', FALSE, 15, {revision})",
                principal.principal().id().as_str(),
                principal.principal().kind().as_storage_str(),
                diagnostics.replace('\'', "''"),
            ))
            .await
            .expect("write a Saved Function as an earlier Coral did");
    }

    coral_test_support::drop_resource_version_columns(&fixture.fixture).await;
    fixture
        .database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let agent_id = agent.principal().id().clone();
    for (principal, id, path, revision) in stored {
        let tool = fixture
            .private_tools
            .get(
                principal,
                &workspace_id,
                &PrivateToolPath::parse(path).expect("path"),
            )
            .await
            .expect("the owner reads its Private Tool");
        assert_eq!(tool.id().to_string(), id);
        assert_eq!(tool.principal(), principal.principal());
        assert_eq!(tool.description().as_str(), format!("Look up {path}"));
        assert_eq!(
            tool.source().as_str(),
            "export default async function lookup(input) { return input.id; }"
        );
        assert_eq!(tool.declaration().as_str(), declaration);
        assert_eq!(
            serde_json::to_string(tool.diagnostics()).expect("encode diagnostics"),
            diagnostics
        );
        assert!(!tool.diagnostics_truncated());
        assert_eq!(tool.saved_at(), coral_db::UnixNanos::new(15));
        assert_eq!(tool.revision(), revision);
        assert_eq!(tool.default_access_state(), ToolAccessState::Enabled);
        assert_eq!(tool.execution_binding().definition(), tool.id().to_string());
        assert_eq!(tool.execution_binding().revision(), revision);
    }
    // A Workspace Owner still reads an Agent's Private Tool.
    assert_eq!(
        fixture
            .inspection
            .list_agent(&owner, &workspace_id, &agent_id)
            .await
            .expect("the Owner lists the Agent's Private Tools")
            .iter()
            .map(|tool| tool.path().as_str())
            .collect::<Vec<_>>(),
        ["ops.lookup"]
    );
}

#[tokio::test]
async fn private_tool_paths_cannot_overlap_virtual_namespaces() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:path-conflict").await;

    for path in ["reports.current", "finance.tax.calculate"] {
        fixture
            .private_tools
            .save(
                &private_tool_change(principal.clone(), 20, 'c'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).expect("path"),
                    AuthoredToolDescription::parse("Current callable").expect("description"),
                    AuthoredToolSource::parse("export default () => 42;").expect("source"),
                ),
            )
            .await
            .expect("save non-overlapping tool");
    }

    for path in ["reports.current.detail", "finance.tax"] {
        assert!(matches!(
            fixture
                .private_tools
                .save(
                    &private_tool_change(principal.clone(), 30, 'd'),
                    &workspace_id,
                    PrivateToolSave::new(
                        PrivateToolPath::parse(path).expect("path"),
                        AuthoredToolDescription::parse("Conflicting callable")
                            .expect("description"),
                        AuthoredToolSource::parse("export default () => 0;").expect("source"),
                    ),
                )
                .await,
            Err(Error::PathConflict(AuthoredToolKind::Private))
        ));
        assert!(
            matches!(
                fixture
                    .private_tools
                    .get(
                        &principal,
                        &workspace_id,
                        &PrivateToolPath::parse(path).expect("path"),
                    )
                    .await,
                Err(Error::NotFound(AuthoredToolKind::Private))
            ),
            "the refused save of {path} saved nothing"
        );
    }
    assert!(matches!(
        fixture
            .private_tools
            .get(
                &principal,
                &workspace_id,
                &PrivateToolPath::parse("reports.current").expect("path"),
            )
            .await,
        Ok(tool) if tool.source().as_str() == "export default () => 42;"
    ));
}

#[tokio::test]
async fn a_compile_failure_keeps_its_cause_and_saves_nothing() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:compile-failures").await;
    let path = PrivateToolPath::parse("compile.failure").expect("path");

    for (failure, kind, rejection) in [
        (
            CompileFailure::Rejected("scripted rejection".to_string()),
            "private_tool_admission",
            Some("private_tool_admission_failed"),
        ),
        (
            CompileFailure::Busy,
            "compiler_busy",
            Some("authoring_compiler_busy"),
        ),
        (
            CompileFailure::TimedOut,
            "private_tool_compilation_timed_out",
            Some("private_tool_compilation_timed_out"),
        ),
        (CompileFailure::Stopped, "compiler_worker_stopped", None),
        (
            CompileFailure::NoCompiler,
            "no_compiler",
            Some("authoring_compiler_unavailable"),
        ),
        (
            CompileFailure::Failed("engine"),
            "compiler_unavailable",
            None,
        ),
    ] {
        let module = PrivateToolModule::with_compiler(
            fixture.database.clone(),
            fixture.workspaces.clone(),
            Arc::new(GatedAuthoredToolCompiler::failing(failure)),
        );
        let error = module
            .save(
                &private_tool_change(principal.clone(), 20, 'e'),
                &workspace_id,
                PrivateToolSave::new(
                    path.clone(),
                    AuthoredToolDescription::parse("Fails to compile").expect("description"),
                    AuthoredToolSource::parse("export default () => 1;").expect("source"),
                ),
            )
            .await
            .expect_err("the compile fails");
        assert_eq!(error.code(), kind);
        assert_eq!(
            error.safe_rejection().map(|(code, _message)| code),
            rejection
        );
        assert!(matches!(
            fixture
                .private_tools
                .get(&principal, &workspace_id, &path)
                .await,
            Err(Error::NotFound(AuthoredToolKind::Private))
        ));
    }
}

/// One refused authoring call: what it asked, what it returned, and whether
/// that is its own variant.
type Refusal = (&'static str, Result<(), Error>, fn(&Error) -> bool);

// Catches: a create-only or replace-only save that compiles before it finds
// its path taken or missing, so it costs a compile and can report the
// compile's failure instead of its own; a refusal under any variant but its
// own; or a refused save or delete that changes the tool, creates one, or
// records an Event.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table checks each refusal's variant and that it changed nothing"
)]
async fn a_save_whose_intent_the_path_refuses_never_compiles_and_changes_nothing() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:intent-before-compile").await;
    let path = PrivateToolPath::parse("reports.current").expect("path");
    let save = |path: &PrivateToolPath| {
        PrivateToolSave::new(
            path.clone(),
            AuthoredToolDescription::parse("Report the current value").expect("description"),
            AuthoredToolSource::parse("export default () => 42;").expect("source"),
        )
    };
    fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 10, 'a'),
            &workspace_id,
            save(&path),
        )
        .await
        .expect("the first save");
    // Every compile times out, so only a check before it can refuse.
    let timing_out = PrivateToolModule::with_compiler(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        Arc::new(GatedAuthoredToolCompiler::failing(CompileFailure::TimedOut)),
    );
    let missing = PrivateToolPath::parse("reports.missing").expect("path");
    let current = fixture
        .private_tools
        .get(&principal, &workspace_id, &path)
        .await
        .expect("the first save is current");
    let events = fixture
        .private_tools
        .list_lifecycle_events(&principal, &workspace_id)
        .await
        .expect("Events")
        .len();
    let context = private_tool_change(principal.clone(), 20, 'b');
    let refusals: [Refusal; 4] = [
        (
            "create-only at a taken path",
            timing_out
                .save(&context, &workspace_id, save(&path).create_only())
                .await
                .map(drop),
            |error| matches!(error, Error::PrivateToolExists),
        ),
        (
            "replace-only at a missing path",
            timing_out
                .save(&context, &workspace_id, save(&missing).replace_only(None))
                .await
                .map(drop),
            |error| matches!(error, Error::NotFound(AuthoredToolKind::Private)),
        ),
        (
            "replace-only at a moved revision",
            timing_out
                .save(&context, &workspace_id, save(&path).replace_only(Some(2)))
                .await
                .map(drop),
            |error| {
                matches!(
                    error,
                    Error::RevisionConflict {
                        kind: AuthoredToolKind::Private,
                        current: 1
                    }
                )
            },
        ),
        (
            "delete at a missing path",
            timing_out
                .delete(&context, &workspace_id, &missing)
                .await
                .map(drop),
            |error| matches!(error, Error::NotFound(AuthoredToolKind::Private)),
        ),
    ];
    for (name, refused, expected) in refusals {
        assert!(refused.as_ref().is_err_and(expected), "{name}: {refused:?}");
        assert_eq!(
            fixture
                .private_tools
                .get(&principal, &workspace_id, &path)
                .await
                .expect("still current"),
            current,
            "{name}: the tool is unchanged"
        );
        assert!(
            matches!(
                fixture
                    .private_tools
                    .get(&principal, &workspace_id, &missing)
                    .await,
                Err(Error::NotFound(AuthoredToolKind::Private))
            ),
            "{name}: nothing was created"
        );
        assert_eq!(
            fixture
                .private_tools
                .list_lifecycle_events(&principal, &workspace_id)
                .await
                .expect("Events")
                .len(),
            events,
            "{name}: no Event"
        );
    }
    assert!(matches!(
        timing_out
            .save(
                &private_tool_change(principal.clone(), 30, 'c'),
                &workspace_id,
                save(&path).replace_only(Some(1)),
            )
            .await,
        Err(Error::CompilationTimedOut(AuthoredToolKind::Private))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_saves_serialize_virtual_namespace_conflicts() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:path-conflict-race").await;
    let private_tools = PrivateToolModule::with_compiler(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        Arc::new(GatedAuthoredToolCompiler::paired()),
    );

    let first_module = private_tools.clone();
    let first_workspace = workspace_id.clone();
    let first_principal = principal.clone();
    let first = tokio::spawn(async move {
        first_module
            .save(
                &private_tool_change(first_principal, 20, 'e'),
                &first_workspace,
                PrivateToolSave::new(
                    PrivateToolPath::parse("concurrent.path").expect("path"),
                    AuthoredToolDescription::parse("Concurrent parent").expect("description"),
                    AuthoredToolSource::parse("export default () => 1;").expect("source"),
                ),
            )
            .await
    });
    let second_module = private_tools;
    let second = tokio::spawn(async move {
        second_module
            .save(
                &private_tool_change(principal, 21, 'f'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse("concurrent.path.child").expect("path"),
                    AuthoredToolDescription::parse("Concurrent child").expect("description"),
                    AuthoredToolSource::parse("export default () => 2;").expect("source"),
                ),
            )
            .await
    });

    let (first, second) = tokio::join!(first, second);
    let results = [
        first.expect("join parent save"),
        second.expect("join child save"),
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::PathConflict(AuthoredToolKind::Private))))
            .count(),
        1
    );
}

#[tokio::test]
async fn private_tool_compilation_holds_no_write_transaction_and_rechecks_active_workspace() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:authoring-race").await;
    let (compiler, compiling, release) = GatedAuthoredToolCompiler::held();
    let blocked_private_tools = PrivateToolModule::with_compiler(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        Arc::new(compiler),
    );
    let saving_module = blocked_private_tools.clone();
    let saving_workspace = workspace_id.clone();
    let saving_principal = principal.clone();
    let save = tokio::spawn(async move {
        saving_module
            .save(
                &private_tool_change(saving_principal, 30, 'a'),
                &saving_workspace,
                PrivateToolSave::new(
                    PrivateToolPath::parse("races.compile").expect("path"),
                    AuthoredToolDescription::parse("Compile outside transaction")
                        .expect("description"),
                    AuthoredToolSource::parse("export default () => 42;").expect("source"),
                ),
            )
            .await
    });
    compiling.notified().await;

    let user = principal
        .authenticated_user()
        .expect("User Principal")
        .clone();
    tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .workspaces
            .archive(&change(user, 40, 'b'), &workspace_id, &IfMatch::any()),
    )
    .await
    .expect("archive must not wait for authoring compilation")
    .expect("archive while compilation is blocked");
    release.send(()).expect("release authoring compilation");
    assert!(matches!(
        save.await.expect("join save"),
        Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
    ));
    assert!(matches!(
        blocked_private_tools
            .get(
                &principal,
                &workspace_id,
                &PrivateToolPath::parse("races.compile").expect("path"),
            )
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(
        blocked_private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list rejected authoring evidence"),
        [] as [crate::AdministrativeLifecycleEvent; 0]
    );
}

#[tokio::test]
async fn private_tool_authoring_keeps_semantic_advice_but_rejects_unresolved_public_types() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:typed-tool-author").await;
    let path = PrivateToolPath::parse("typed.advisory").expect("path");
    let advisory = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 20, '2'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Keep strict semantic advice").expect("description"),
                AuthoredToolSource::parse(
                    r"
                        interface Input { value: number }
                        export default function advise(input: Input): number {
                            const ordinarySemanticError: string = input.value;
                            void unresolvedImplementationOnly;
                            return input.value;
                        }
                    ",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("ordinary semantic diagnostics are advisory");
    for code in [2322, 2304] {
        assert!(
            advisory
                .diagnostics()
                .iter()
                .any(|diagnostic| diagnostic.code() == code)
        );
    }
    assert!(advisory.declaration().as_str().contains("interface Input"));

    let rejected = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 30, '3'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Unresolved public type").expect("description"),
                AuthoredToolSource::parse(
                    "export default function invalid(input: MissingPublicType): number { return 0; }",
                )
                .expect("source"),
            ),
        )
        .await;
    assert!(
        matches!(
            rejected,
            Err(Error::Admission(AuthoredToolKind::Private, _))
        ),
        "unexpected unresolved-public-type result: {rejected:?}"
    );
    for invalid_source in [
        "export default 42;",
        "export const extra = 1; export default function valid() { return 42; }",
        "import value from 'package'; export default function imported() { return value; }",
        "export default function secret() { return process.env.SECRET; }",
        "declare function missingRuntime(): number; export default missingRuntime;",
        "declare const missingRuntime: () => number; export { missingRuntime as default };",
        "export default function missingBody(): number;",
        "function typeOnlyDefault(): number { return 42; } export { type typeOnlyDefault as default };",
    ] {
        assert!(matches!(
            fixture
                .private_tools
                .save(
                    &private_tool_change(principal.clone(), 31, '4'),
                    &workspace_id,
                    PrivateToolSave::new(
                        path.clone(),
                        AuthoredToolDescription::parse("Invalid export shape")
                            .expect("description"),
                        AuthoredToolSource::parse(invalid_source).expect("source"),
                    ),
                )
                .await,
            Err(Error::Admission(AuthoredToolKind::Private, _))
        ));
    }
    let unchanged = fixture
        .private_tools
        .get(&principal, &workspace_id, &path)
        .await
        .expect("advisory definition remains current");
    assert_eq!(unchanged.id(), advisory.id());
    assert_eq!(unchanged.source(), advisory.source());
    assert_eq!(
        fixture
            .private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("one accepted authoring Event")
            .len(),
        1
    );
}

// Catches: a save that accepts a Private Tool that does not take one object,
// or whose argument or result cannot cross a Tool call as data, anywhere in
// its declaration; a refusal whose path does not start at `input` whatever
// the parameter is called; or one that refuses a Tool whose types say nothing
// about the data (`any`, `unknown`), that is optional, or takes nothing.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of every parameter shape and value the data rule covers"
)]
async fn a_save_refuses_a_declaration_whose_inputs_or_result_are_not_data() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:data-only-author").await;
    let path = PrivateToolPath::parse("data.only").expect("path");
    let save = |source: &str| {
        PrivateToolSave::new(
            path.clone(),
            AuthoredToolDescription::parse("Takes and returns data").expect("description"),
            AuthoredToolSource::parse(source).expect("source"),
        )
    };
    for (source, reason) in [
        (
            "export default (callback: () => number) => callback();",
            "`input` has type () => number; take one object instead",
        ),
        (
            "export default ({ callback }: { callback: () => number }) => callback();",
            "`input.callback` is a function",
        ),
        (
            "export default (input: { at: Date }) => input.at.getTime();",
            "`input.at` is a Date",
        ),
        (
            "export default (name: string) => name;",
            "`input` has type string; take one object instead",
        ),
        (
            "export default (ids: string[]) => ids.length;",
            "`input` has type string[]; take one object instead",
        ),
        (
            "export default (input: { a: string } | string) => input;",
            "`input` has type string | { a: string; }; take one object instead",
        ),
        (
            "export default (a: { x: number }, b: { y: number }) => a.x + b.y;",
            "it takes 2 parameters; take one object instead",
        ),
        (
            "export default (...rest: { x: number }[]) => rest.length;",
            "`input` is a rest parameter; take one object instead",
        ),
        ("export default () => () => 1;", "`result` is a function"),
        (
            "class Box { constructor(public value: number) {} } export default (): Box => new Box(1);",
            "`result` is an instance of class Box",
        ),
        (
            "export default async (): Promise<{ when: Date }> => ({ when: new Date() });",
            "`result.when` is a Date",
        ),
        (
            "export default (): Map<string, number> => new Map();",
            "`result` is a Map",
        ),
        (
            "export default ({ values }: { values: Set<string>[] }) => values.length;",
            "`input.values[]` is a Set",
        ),
        (
            "export default (args: { value: bigint }) => Number(args.value);",
            "`input.value` is a bigint",
        ),
        (
            "export default (): { id: symbol } => ({ id: Symbol() });",
            "`result.id` is a symbol",
        ),
    ] {
        let refused = fixture
            .private_tools
            .save(
                &private_tool_change(principal.clone(), 20, 'd'),
                &workspace_id,
                save(source),
            )
            .await;
        assert!(
            matches!(&refused, Err(Error::NotData(AuthoredToolKind::Private, message)) if message.contains(reason)),
            "{source}: {refused:?}"
        );
    }
    assert!(matches!(
        fixture
            .private_tools
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));

    for source in [
        "export default async (input: any): Promise<any> => input;",
        "export default (input: unknown): unknown => input;",
        "export default async (input: { name: string; tags: readonly string[]; at?: [number, string | null] }): Promise<Record<string, boolean>> => ({ [input.name]: true });",
        "export default async (): Promise<void> => {};",
        "export default async (input?: { limit: number }): Promise<number> => input?.limit ?? 0;",
        "export default async ({ id }: { id: string }): Promise<string> => id;",
        "export default async (input: object): Promise<number> => Object.keys(input).length;",
        "export default async <T extends { id: string }>(input: T): Promise<string> => input.id;",
    ] {
        fixture
            .private_tools
            .save(
                &private_tool_change(principal.clone(), 30, 'e'),
                &workspace_id,
                save(source),
            )
            .await
            .unwrap_or_else(|error| panic!("{source}: {error}"));
    }
}

#[tokio::test]
async fn private_tool_save_rejects_missing_and_computed_dependencies_atomically() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:dependency-author").await;
    let path = PrivateToolPath::parse("reports.compose").expect("path");

    let missing = fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 20, '5'),
            &workspace_id,
            PrivateToolSave::new(
                path.clone(),
                AuthoredToolDescription::parse("Compose one report").expect("description"),
                AuthoredToolSource::parse(
                    "export default function compose() {\n  return coral.private.missing.value();\n}",
                )
                .expect("source"),
            ),
        )
        .await;
    assert!(
        matches!(
            &missing,
            Err(Error::Admission(AuthoredToolKind::Private, message))
                if message == "coral.private.reports.compose:2:10: \
                    coral.private.missing.value names no Tool that you can call. \
                    Find one with coral.search."
        ),
        "{missing:?}"
    );

    for source in [
        "const dependency = 'value'; export default function compose() { return coral.private.reports[dependency](); }",
        "const dependency = 'value'; export default function compose() { return (coral.private.reports as any)[dependency](); }",
        "const reports = coral.private.reports; export default function compose() { return reports.value(); }",
        "function pick(root: any, key: string) { return root[key]; } export default function compose() { return pick(coral.private, 'reports')(); }",
    ] {
        let rejected = fixture
            .private_tools
            .save(
                &private_tool_change(principal.clone(), 20, '5'),
                &workspace_id,
                PrivateToolSave::new(
                    path.clone(),
                    AuthoredToolDescription::parse("Compose one report").expect("description"),
                    AuthoredToolSource::parse(source).expect("source"),
                ),
            )
            .await;
        assert!(matches!(
            rejected,
            Err(Error::Admission(AuthoredToolKind::Private, message))
                if message.starts_with("coral.private.reports.compose:")
                    && (message.contains(" at 1:") || message.contains(":1:"))
        ));
    }

    assert!(matches!(
        fixture
            .private_tools
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(
        fixture
            .private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list lifecycle evidence"),
        [] as [crate::AdministrativeLifecycleEvent; 0]
    );
}

#[tokio::test]
async fn accepted_private_tool_diagnostics_are_bounded() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:diagnostic-bound").await;
    let mut source = (0..=crate::AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT)
        .map(|index| format!("const invalid{index}: string = {index};"))
        .collect::<Vec<_>>()
        .join("\n");
    source.push_str("\nexport default function bounded(): number { return 42; }");

    let saved = fixture
        .private_tools
        .save(
            &private_tool_change(principal, 20, 'c'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("diagnostics.bounded").expect("path"),
                AuthoredToolDescription::parse("Bound compiler diagnostics").expect("description"),
                AuthoredToolSource::parse(&source).expect("source"),
            ),
        )
        .await
        .expect("save with bounded advisory diagnostics");

    assert_eq!(
        saved.diagnostics().len(),
        crate::AUTHORED_TOOL_DIAGNOSTIC_MAX_COUNT
    );
    assert!(saved.diagnostics_truncated());
}

/// Declares `repos.get` for every caller.
pub(crate) struct DeclaresReposGet;

#[async_trait::async_trait]
impl crate::ToolDeclarationReader for DeclaresReposGet {
    async fn read(
        &self,
        call_paths: &[String],
    ) -> Result<Vec<crate::ToolDeclaration>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(call_paths
            .iter()
            .filter(|path| *path == "coral.providers.github.repos.get")
            .map(|path| {
                crate::ToolDeclaration::new(
                    path.clone(),
                    "export default function (input: { \"owner\": string }): Promise<{ data: string }>;".to_owned(),
                )
            })
            .collect())
    }
}

/// Never answers.
struct StalledReader;

#[async_trait::async_trait]
impl crate::ToolDeclarationReader for StalledReader {
    async fn read(
        &self,
        _call_paths: &[String],
    ) -> Result<Vec<crate::ToolDeclaration>, Box<dyn std::error::Error + Send + Sync>> {
        std::future::pending().await
    }
}

/// A reader that never answers leaves Tools untyped, and the save still
/// compiles in time.
#[tokio::test]
async fn a_stalled_declaration_read_saves_untyped_in_time() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:stalled-typing").await;
    let readers: [(&str, &dyn crate::ToolDeclarationReader, Vec<u64>); 2] = [
        ("a reader that answers", &DeclaresReposGet, vec![2353]),
        ("a reader that never answers", &StalledReader, vec![]),
    ];
    for (name, reader, expected) in readers {
        let started = std::time::Instant::now();
        let prepared = fixture
            .private_tools
            .prepare_save(
                &private_tool_change(principal.clone(), 20, 'd'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse("typing.stalled").expect("path"),
                    AuthoredToolDescription::parse("Typed").expect("description"),
                    AuthoredToolSource::parse(
                        "export default async function f() { return coral.providers.github.repos.get({ owner: \"a\", nope: 1 }); }",
                    )
                    .expect("source"),
                ),
                Some(reader),
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: typing never fails a save: {error:?}"));
        assert!(
            started.elapsed() < crate::COMPILATION_TIMEOUT,
            "{name}: {:?}",
            started.elapsed()
        );
        let mut transaction = fixture.database.begin().await.expect("begin save");
        let saved = fixture
            .private_tools
            .save_in_transaction(&mut transaction, prepared)
            .await
            .expect("save");
        assert_eq!(
            saved
                .diagnostics()
                .iter()
                .map(crate::AuthoredToolDiagnostic::code)
                .collect::<Vec<_>>(),
            expected,
            "{name}"
        );
    }
}

// Catches: a save that asks the compiler to analyze or prepare it more than
// once, so the Run Executor link carries a message for each. Which
// declarations type it is tested by
// `a_stalled_declaration_read_saves_untyped_in_time`.
#[tokio::test]
async fn a_typed_save_analyzes_once_and_prepares_once() {
    use coral_run_executor::testing::{CompilerCalls, CountingCompiler};
    let compiler = Arc::new(CountingCompiler::new(Arc::new(
        coral_run_executor::InProcessCompiler::new(crate::IsolateLimits::default())
            .expect("start Private Tool compiler"),
    )));
    let fixture =
        PrivateToolTestModules::compiling_through(coral_db::migration_plan(), compiler.clone())
            .await;
    let (workspace_id, principal) = fixture.user_workspace("user:counted-save").await;
    fixture
        .private_tools
        .save(
            &private_tool_change(principal.clone(), 20, 'a'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("typing.dependency").expect("path"),
                AuthoredToolDescription::parse("Dependency").expect("description"),
                AuthoredToolSource::parse(
                    "export default function dependency(): number { return 1; }",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save dependency");
    compiler.take();

    fixture
        .private_tools
        .prepare_save(
            &private_tool_change(principal.clone(), 21, 'b'),
            &workspace_id,
            PrivateToolSave::new(
                PrivateToolPath::parse("typing.counted").expect("path"),
                AuthoredToolDescription::parse("Typed").expect("description"),
                AuthoredToolSource::parse(
                    "export default async function f() { return [await coral.providers.github.repos.get({ owner: \"a\", nope: 1 }), await coral.private.typing.dependency({})]; }",
                )
                .expect("source"),
            ),
            Some(&DeclaresReposGet),
        )
        .await
        .expect("prepare typed save");
    assert_eq!(
        compiler.take(),
        CompilerCalls {
            analyze: vec![1],
            compile: 0,
            prepare: 1,
        }
    );
}

#[tokio::test]
async fn one_users_private_tools_at_the_same_path_stay_scoped_to_each_workspace() {
    let fixture = PrivateToolTestModules::new().await;
    let (first, principal) = fixture.user_workspace("user:two-workspaces").await;
    let second = fixture
        .workspaces
        .create(
            &change(principal.authenticated_user().unwrap().clone(), 11, 'e'),
            WorkspaceDisplayName::parse("Second").unwrap(),
        )
        .await
        .unwrap()
        .id()
        .clone();
    let path = PrivateToolPath::parse("reports.total").unwrap();
    let mut saved = Vec::new();
    for (workspace, source) in [
        (&first, "export default () => 1;"),
        (&second, "export default () => 2;"),
    ] {
        saved.push(
            fixture
                .private_tools
                .save(
                    &private_tool_change(principal.clone(), 20, 'a'),
                    workspace,
                    PrivateToolSave::new(
                        path.clone(),
                        AuthoredToolDescription::parse("Total").unwrap(),
                        AuthoredToolSource::parse(source).unwrap(),
                    ),
                )
                .await
                .unwrap(),
        );
    }
    let [in_first, in_second] = <[_; 2]>::try_from(saved).unwrap();
    assert_ne!(in_first.id(), in_second.id());
    let first_events = fixture
        .private_tools
        .list_lifecycle_events(&principal, &first)
        .await
        .unwrap();
    let get = |workspace: &coral_workspace::WorkspaceId| {
        let (tools, principal, workspace, path) =
            (&fixture.private_tools, &principal, workspace.clone(), &path);
        async move { tools.get(principal, &workspace, path).await }
    };
    assert_eq!(get(&second).await.unwrap(), in_second);

    let context = private_tool_change(principal.clone(), 30, 'b');
    let delete = || fixture.private_tools.delete(&context, &second, &path);
    assert_eq!(&delete().await.unwrap(), in_second.id());
    assert!(matches!(
        get(&second).await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    // Deleting again finds nothing and never falls back to the first Workspace.
    assert!(matches!(
        delete().await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(get(&first).await.unwrap(), in_first);
    assert_eq!(
        fixture
            .private_tools
            .list_lifecycle_events(&principal, &first)
            .await
            .unwrap(),
        first_events
    );
}

/// Compiles every source to one fixed declaration once its gate opens. The
/// gate blocks, so it waits on a blocking worker as a compiler does.
struct GatedAuthoredToolCompiler {
    gate: Arc<dyn Fn() -> Result<(), CompileFailure> + Send + Sync>,
}

impl GatedAuthoredToolCompiler {
    /// Each compilation waits until a second one reaches the gate.
    fn paired() -> Self {
        let barrier = Barrier::new(2);
        Self {
            gate: Arc::new(move || {
                barrier.wait();
                Ok(())
            }),
        }
    }

    /// Compilation notifies `compiling` when it starts and waits for `release`.
    fn held() -> (Self, Arc<Notify>, mpsc::Sender<()>) {
        let compiling = Arc::new(Notify::new());
        let started = compiling.clone();
        let (release, released) = mpsc::channel();
        let released = Mutex::new(released);
        let compiler = Self {
            gate: Arc::new(move || {
                started.notify_one();
                released
                    .lock()
                    .map_err(|error| error.to_string())?
                    .recv()
                    .map_err(|error| error.to_string())?;
                Ok(())
            }),
        };
        (compiler, compiling, release)
    }

    /// Compilation always ends in `failure`.
    fn failing(failure: CompileFailure) -> Self {
        Self {
            gate: Arc::new(move || Err(failure.clone())),
        }
    }
}

#[async_trait::async_trait]
impl AuthoredToolCompiler for GatedAuthoredToolCompiler {
    async fn analyze(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
    ) -> Option<coral_v8::SourceReferences> {
        None
    }

    async fn prepare(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
        _declarations: Option<&coral_v8::CoralDeclarations>,
    ) -> Result<PreparedAuthoredTool, CompileFailure> {
        let gate = Arc::clone(&self.gate);
        tokio::task::spawn_blocking(move || gate())
            .await
            .map_err(|_join_error| CompileFailure::Stopped)??;
        Ok(PreparedAuthoredTool {
            declaration: crate::AuthoredToolDeclaration::from_compiler(
                "declare const _default: () => number;\nexport default _default;\n",
            )?,
            declaration_field_projection: crate::AuthoredToolDeclarationFieldProjection::Ready(
                Vec::new(),
            ),
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            linked: Vec::new(),
            tools: coral_v8::ToolReferences::default(),
            not_data: None,
        })
    }
}

struct PrivateToolTestModules {
    fixture: TestDatabase,
    database: Arc<coral_db::Database>,
    workspaces: WorkspaceModule,
    private_tools: PrivateToolModule,
    /// What the management API holds in place of the Module.
    inspection: PrivateToolInspection,
    deletion: PrivateToolDeletion,
}

impl PrivateToolTestModules {
    async fn new() -> Self {
        Self::on_plan(coral_db::migration_plan()).await
    }

    async fn on_plan(plan: &coral_db::MigrationPlan) -> Self {
        let compiler = coral_run_executor::InProcessCompiler::new(crate::IsolateLimits::default())
            .expect("start Private Tool compiler");
        Self::compiling_through(plan, Arc::new(compiler)).await
    }

    async fn compiling_through(
        plan: &coral_db::MigrationPlan,
        compiler: Arc<dyn coral_run_executor::CompilerClient>,
    ) -> Self {
        let fixture = TestDatabase::new(plan)
            .await
            .expect("create Private Tool test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let private_tools = PrivateToolModule::with_compiler_client(
            database.clone(),
            workspaces.clone(),
            compiler,
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        );
        Self {
            fixture,
            inspection: PrivateToolInspection::new(database.clone(), workspaces.clone()),
            deletion: PrivateToolDeletion::new(workspaces.clone()),
            database,
            workspaces,
            private_tools,
        }
    }

    /// Deletes an Agent's Private Tool as a caller that owns the
    /// transaction does, and commits.
    async fn delete_agent(
        &self,
        context: &PrivateToolChangeContext,
        workspace_id: &coral_workspace::WorkspaceId,
        agent_id: &coral_workspace::PrincipalId,
        path: &PrivateToolPath,
    ) -> Result<crate::PrivateToolId, Error> {
        let mut transaction = self.database.begin().await?;
        let deleted = self
            .deletion
            .delete_agent_in_transaction(&mut transaction, context, workspace_id, agent_id, path)
            .await?;
        transaction.commit().await?;
        Ok(deleted)
    }

    /// Saves `source` as `author`'s Private Tool at `path` in `workspace`,
    /// declaring `default` as its default Tool Access State, if any.
    async fn save_tool(
        &self,
        author: &AuthenticatedPrincipal,
        workspace: &coral_workspace::WorkspaceId,
        path: &str,
        source: &str,
        default: Option<ToolAccessState>,
    ) -> crate::PrivateTool {
        self.private_tools
            .save(
                &private_tool_change(author.clone(), 20, 'b'),
                workspace,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).expect("path"),
                    AuthoredToolDescription::parse("A private draft").expect("description"),
                    AuthoredToolSource::parse(source).expect("source"),
                )
                .declaring_default(default),
            )
            .await
            .unwrap_or_else(|error| panic!("save the Private Tool {path}: {error}"))
    }

    async fn user_workspace(
        &self,
        user_id: &str,
    ) -> (coral_workspace::WorkspaceId, AuthenticatedPrincipal) {
        let user =
            organization_user(self.database.clone(), user_id, OrganizationRole::Member).await;
        let workspace = self
            .workspaces
            .create(
                &change(user.clone(), 10, 'f'),
                WorkspaceDisplayName::parse("Private Tools").expect("display name"),
            )
            .await
            .expect("create Workspace");
        (workspace.id().clone(), AuthenticatedPrincipal::user(user))
    }
}

async fn registered_agent(
    workspaces: &WorkspaceModule,
    owner: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
) -> AuthenticatedPrincipal {
    let authenticated_at = coral_db::UnixNanos::new(11);
    agent_registered_until(workspaces, owner, workspace_id, None, authenticated_at).await
}

/// An Agent whose Registration ends at `expires_at`, if it ends, as
/// authentication admitted it at `authenticated_at`.
async fn agent_registered_until(
    workspaces: &WorkspaceModule,
    owner: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    expires_at: Option<coral_db::UnixNanos>,
    authenticated_at: coral_db::UnixNanos,
) -> AuthenticatedPrincipal {
    let issued = workspaces
        .create_agent(
            &change(
                owner.authenticated_user().expect("Owner User").clone(),
                10,
                '1',
            ),
            &RequestId::generate(),
            workspace_id,
            coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
            expires_at,
        )
        .await
        .expect("create Agent");
    workspaces
        .authenticate_agent(workspace_id, issued.credential.expose(), authenticated_at)
        .await
        .expect("authenticate Agent")
}

fn private_tool_change(
    actor: AuthenticatedPrincipal,
    at: i64,
    digit: char,
) -> PrivateToolChangeContext {
    PrivateToolChangeContext::new(
        actor,
        coral_db::UnixNanos::new(at),
        TraceId::parse(&digit.to_string().repeat(32)).expect("trace id"),
    )
}

/// Each Module reads the clock it was given, and the inspection and the
/// deletion that a Private Tool Module holds read that Module's clock. An
/// Agent whose Registration ends an hour after the clock's start, in 2001,
/// saves, reads and deletes its Private Tools and lists the Shared Tools.
/// Once the clock reaches the Registration's end, each is refused.
///
/// Catches: a Module, or the inspection or the deletion inside one, reading
/// the host's clock, a quarter of a century later. It would refuse the Agent
/// from the start.
#[tokio::test]
async fn an_agents_authority_is_checked_on_the_modules_clock() {
    // 2001-01-01.
    let start = coral_db::UnixNanos::new(978_307_200 * 1_000_000_000);
    let expiry = start.checked_add(Duration::from_hours(1)).unwrap();
    let clock = coral_test_support::ManualClock::starting_at(start);
    let fixture = PrivateToolTestModules::new().await;
    let private_tools = fixture
        .private_tools
        .clone()
        .with_clock(Arc::new(clock.clone()));
    let shared_tools = crate::SharedToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start the Shared Tool compiler")
    .with_clock(Arc::new(clock.clone()));
    let (workspace_id, owner) = fixture.user_workspace("user:clock-owner").await;
    let workspaces = &fixture.workspaces;
    let agent =
        agent_registered_until(workspaces, &owner, &workspace_id, Some(expiry), start).await;
    let get = async |path: &str| {
        private_tools
            .get(
                &agent,
                &workspace_id,
                &PrivateToolPath::parse(path).unwrap(),
            )
            .await
    };
    let save = async |path: &str| {
        private_tools
            .save(
                &private_tool_change(agent.clone(), start.get(), '2'),
                &workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).unwrap(),
                    AuthoredToolDescription::parse("Answer").unwrap(),
                    AuthoredToolSource::parse("export default () => 42;").unwrap(),
                ),
            )
            .await
    };
    let delete = async |path: &str| {
        private_tools
            .delete(
                &private_tool_change(agent.clone(), start.get(), '3'),
                &workspace_id,
                &PrivateToolPath::parse(path).unwrap(),
            )
            .await
    };
    let list_shared = async || {
        shared_tools
            .list(&agent, &workspace_id, &page_request(None, 100))
            .await
    };

    let kept = save("kept")
        .await
        .expect("the Registration lasts on the Module's clock");
    save("spare").await.unwrap();
    let read = get("kept").await;
    assert_eq!(read.expect("inspection reads the Module's clock"), kept);
    delete("spare")
        .await
        .expect("deletion reads the Module's clock");
    list_shared()
        .await
        .expect("the Shared Tool Module reads its clock");

    clock.set(expiry);
    let expired = |result: Result<(), Error>| {
        matches!(
            result,
            Err(Error::Workspace(
                coral_workspace::Error::AgentRegistrationExpired
            ))
        )
    };
    assert!(expired(save("late").await.map(drop)));
    assert!(expired(delete("kept").await.map(drop)));
    assert!(expired(get("kept").await.map(drop)));
    assert!(expired(list_shared().await.map(drop)));
    assert_eq!(
        fixture
            .inspection
            .list_agent(&owner, &workspace_id, agent.principal().id())
            .await
            .unwrap(),
        [kept],
        "a refusal changes nothing"
    );
}

#[tokio::test]
async fn agent_authoring_rechecks_lifecycle_and_keeps_previous_source_and_events() {
    for (disabled, lost_authority) in [
        (true, coral_workspace::Error::AgentDisabled),
        (false, coral_workspace::Error::AgentRegistrationExpired),
    ] {
        let fixture = PrivateToolTestModules::new().await;
        let (workspace_id, owner) = fixture.user_workspace("user:owner").await;
        let agent = registered_agent(&fixture.workspaces, &owner, &workspace_id).await;
        let path = PrivateToolPath::parse("private.answer").unwrap();
        let save = PrivateToolSave::new(
            path.clone(),
            AuthoredToolDescription::parse("Answer").unwrap(),
            AuthoredToolSource::parse("export default () => 42;").unwrap(),
        );
        let original = fixture
            .private_tools
            .save(
                &private_tool_change(agent.clone(), 20, '1'),
                &workspace_id,
                save.clone(),
            )
            .await
            .unwrap();
        let (compiler, compiling, release) = GatedAuthoredToolCompiler::held();
        let module = PrivateToolModule::with_compiler(
            fixture.database.clone(),
            fixture.workspaces.clone(),
            Arc::new(compiler),
        );
        let caller = agent.clone();
        let target = workspace_id.clone();
        let saving = tokio::spawn(async move {
            module
                .save(&private_tool_change(caller, 30, '2'), &target, save)
                .await
        });
        compiling.notified().await;
        let context = change(owner.authenticated_user().unwrap().clone(), 31, '3');
        if disabled {
            fixture
                .workspaces
                .set_agent_state(
                    &context,
                    &workspace_id,
                    agent.principal().id(),
                    &IfMatch::any(),
                    coral_workspace::AgentState::Disabled,
                )
                .await
                .unwrap();
        } else {
            let registration = fixture
                .workspaces
                .get_agent_registration(context.actor(), &workspace_id, agent.principal().id())
                .await
                .unwrap();
            fixture
                .workspaces
                .update_agent_registration(
                    &context,
                    &workspace_id,
                    registration.id(),
                    &IfMatch::any(),
                    true,
                    Some(coral_db::UnixNanos::new(0)),
                )
                .await
                .unwrap();
        }
        release.send(()).unwrap();
        let denied = |result: Result<(), Error>| {
            matches!(result, Err(Error::Workspace(error))
                if std::mem::discriminant(&error) == std::mem::discriminant(&lost_authority))
        };
        assert!(denied(saving.await.unwrap().map(drop)));
        assert_eq!(
            fixture
                .inspection
                .get_agent(&owner, &workspace_id, agent.principal().id(), &path)
                .await
                .unwrap(),
            original
        );
        assert!(denied(
            fixture
                .private_tools
                .delete(&private_tool_change(agent, 40, '4'), &workspace_id, &path)
                .await
                .map(drop)
        ));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one listing scenario compares the owner, another Member, an Agent, another Workspace, and access loss"
)]
async fn list_pages_only_the_callers_own_private_tools_in_one_workspace() {
    let fixture = PrivateToolTestModules::new().await;
    let organization = coral_workspace::OrganizationModule::new(fixture.database.clone());
    let administrator = organization_user(
        fixture.database.clone(),
        "user:list-admin",
        OrganizationRole::Owner,
    )
    .await;
    let (workspace_id, owner) = fixture.user_workspace("user:list-owner").await;
    let owner_user = owner.authenticated_user().expect("owner User").clone();
    let (other_workspace, _) = fixture.user_workspace("user:list-elsewhere").await;
    let member_user = organization_user(
        fixture.database.clone(),
        "user:list-member",
        OrganizationRole::Member,
    )
    .await;
    for workspace in [&workspace_id, &other_workspace] {
        let workspace_owner = if workspace == &workspace_id {
            owner_user.clone()
        } else {
            coral_workspace::UserId::parse("user:list-elsewhere")
                .map(|id| {
                    coral_workspace::AuthenticatedUser::organization_member(
                        id,
                        OrganizationRole::Member,
                    )
                })
                .expect("elsewhere owner")
        };
        fixture
            .workspaces
            .set_membership(
                &change(workspace_owner, 12, 'a'),
                workspace,
                &member_user,
                WorkspaceRole::Member,
            )
            .await
            .expect("add Member");
    }
    let member = AuthenticatedPrincipal::user(member_user.clone());
    let agent = registered_agent(&fixture.workspaces, &owner, &workspace_id).await;
    let save = |actor: &AuthenticatedPrincipal, workspace: &coral_workspace::WorkspaceId, path| {
        let (tools, actor, workspace) = (&fixture.private_tools, actor.clone(), workspace.clone());
        async move {
            tools
                .save(
                    &private_tool_change(actor, 20, 'c'),
                    &workspace,
                    PrivateToolSave::new(
                        PrivateToolPath::parse(path).expect("path"),
                        AuthoredToolDescription::parse(&format!("Describe {path}"))
                            .expect("description"),
                        AuthoredToolSource::parse("export default () => 1;").expect("source"),
                    ),
                )
                .await
                .expect("save tool")
        }
    };
    // An uppercase path sorts first on both databases, as in Rust string order.
    for path in ["reports.total", "alpha.first", "zeta.last", "Middle.upper"] {
        save(&member, &workspace_id, path).await;
    }
    save(&member, &other_workspace, "elsewhere.only").await;
    save(&owner, &workspace_id, "owner.private").await;
    save(&agent, &workspace_id, "agent.work").await;

    let inspection =
        crate::PrivateToolInspection::new(fixture.database.clone(), fixture.workspaces.clone());
    let list = |actor: &AuthenticatedPrincipal, after: Option<&str>, page_size| {
        let (tools, actor, workspace) = (&inspection, actor.clone(), workspace_id.clone());
        let after = after.map(|path| PrivateToolPath::parse(path).expect("after path"));
        async move {
            tools
                .list(&actor, &workspace, &page_request(after, page_size))
                .await
        }
    };
    let paths = |page: &Page<crate::PrivateToolSummary, PrivateToolPath>| {
        page.items
            .iter()
            .map(|summary| summary.path().as_str().to_owned())
            .collect::<Vec<_>>()
    };

    let first = list(&member, None, 2).await.expect("first page");
    assert_eq!(paths(&first), ["Middle.upper", "alpha.first"]);
    let summary = first.items.last().expect("summary");
    assert_eq!(summary.description().as_str(), "Describe alpha.first");
    assert_eq!(summary.saved_at(), coral_db::UnixNanos::new(20));
    let first_after = first.next_after;
    let after = first_after.as_ref().expect("more pages").as_str();
    let second = list(&member, Some(after), 2).await.expect("second page");
    // A page that ends exactly at the last tool has no continuation.
    assert_eq!(paths(&second), ["reports.total", "zeta.last"]);
    assert_eq!(second.next_after, None);

    // Pages of every size hold the same Private Tools in the same order.
    let whole = ["Middle.upper", "alpha.first", "reports.total", "zeta.last"];
    for size in [1, 2, 3, 4, 100] {
        let pages = read_pages(size, async |request| {
            inspection.list(&member, &workspace_id, &request).await
        })
        .await;
        let read = pages
            .iter()
            .map(|page| {
                page.iter()
                    .map(|summary| summary.path().as_str())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expected = whole
            .chunks(usize::try_from(size).expect("size"))
            .map(<[_]>::to_vec)
            .collect::<Vec<_>>();
        assert_eq!(read, expected, "pages of {size}");
    }
    // The path of another Principal's Private Tool, or of the caller's own in
    // another Workspace, only positions the page: it shows the caller's own
    // Private Tools of this Workspace after it.
    for (case, lister, workspace, foreign) in [
        ("another Principal", &owner, &workspace_id, "owner.private"),
        (
            "another Workspace",
            &member,
            &other_workspace,
            "elsewhere.only",
        ),
    ] {
        let theirs = inspection
            .list(lister, workspace, &page_request(None, 100))
            .await
            .expect(case);
        assert_eq!(paths(&theirs), [foreign], "{case}");
        let positioned = list(&member, Some(foreign), 100).await.expect(case);
        assert_eq!(paths(&positioned), ["reports.total", "zeta.last"], "{case}");
    }
    // A page sends as many statements for four Private Tools as for one.
    let sent = async |workspace| {
        let before = fixture.fixture.statements_sent();
        let request = page_request(None, 100);
        let page = inspection.list(&member, workspace, &request).await;
        page.expect("a page");
        fixture.fixture.statements_sent() - before
    };
    assert_eq!(sent(&workspace_id).await, sent(&other_workspace).await);
    // The list fetches a page's rows and the one after them, whatever the
    // collection holds. It fetches no row beyond those: this one fails any
    // fetch of it.
    let corrupt = async |from: &str, to: &str| {
        let changed = fixture
            .fixture
            .execute_fixture_sql(&format!(
                "UPDATE private_tool_records SET path = '{to}' WHERE path = '{from}'"
            ))
            .await
            .expect("change the path");
        assert_eq!(changed, 1);
    };
    corrupt("zeta.last", "zeta.not a path").await;
    let bounded = list(&member, None, 2).await.expect("a page before the row");
    assert_eq!(paths(&bounded), ["Middle.upper", "alpha.first"]);
    assert_eq!(bounded.next_after, first_after);
    let reached = list(&member, Some("alpha.first"), 2).await;
    assert!(matches!(reached, Err(Error::CorruptData(_))), "{reached:?}");
    corrupt("zeta.not a path", "zeta.last").await;

    // An Owner sees only their own tools: never a Member's or an Agent's.
    // An Agent likewise sees only its own.
    for (actor, own) in [(&owner, "owner.private"), (&agent, "agent.work")] {
        assert_eq!(
            paths(&list(actor, None, 100).await.expect("own page")),
            [own]
        );
    }
    // A disabled Agent loses its reads at once, though its session is authenticated.
    fixture
        .workspaces
        .set_agent_state(
            &change(owner_user.clone(), 25, 'f'),
            &workspace_id,
            agent.principal().id(),
            &IfMatch::any(),
            coral_workspace::AgentState::Disabled,
        )
        .await
        .expect("disable Agent");
    let denied = list(&agent, None, 100).await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::AgentDisabled))
        ),
        "{denied:?}"
    );
    // A page size holds one or more, so only a size over the maximum
    // reaches the list.
    assert!(matches!(
        list(&member, None, crate::PRIVATE_TOOL_PAGE_MAX_SIZE + 1).await,
        Err(Error::InvalidInput(_))
    ));

    let entries = || async {
        let mut transaction = fixture
            .database
            .begin_read_snapshot()
            .await
            .expect("read snapshot");
        let entries = crate::authored_tool_entries_in_transaction(&mut transaction)
            .await
            .expect("entries");
        transaction.commit().await.expect("commit read");
        entries
    };
    let before = entries().await;
    let membership = organization
        .provision_login(&change(member_user.clone(), 30, 'd'))
        .await
        .expect("current Organization Membership");
    organization
        .set_enabled(
            &change(administrator, 31, 'e'),
            member_user.user_id(),
            membership.revision(),
            false,
        )
        .await
        .expect("disable Member");
    assert!(matches!(
        list(&member, None, 100).await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert!(matches!(
        fixture
            .private_tools
            .get(
                &member,
                &workspace_id,
                &PrivateToolPath::parse("alpha.first").expect("path"),
            )
            .await,
        Err(Error::NotFound(AuthoredToolKind::Private))
    ));
    assert_eq!(entries().await, before);
}

/// What a narrow read returns of a whole Private Tool.
fn record_of(tool: &crate::PrivateTool) -> crate::repository::PrivateToolRecord {
    crate::repository::PrivateToolRecord {
        workspace_id: tool.workspace_id().clone(),
        owner: tool.principal().clone(),
        id: tool.id().clone(),
        path: tool.path().clone(),
        revision: tool.revision(),
        default_access_state: tool.default_access_state(),
    }
}

/// A Workspace of `owner_id` with a Member and an Agent, each of which can
/// save Private Tools: the Workspace, then the Owner, the Member and the
/// Agent.
async fn workspace_with_a_member_and_an_agent(
    fixture: &PrivateToolTestModules,
    owner_id: &str,
    member_id: &str,
) -> (coral_workspace::WorkspaceId, [AuthenticatedPrincipal; 3]) {
    let (workspace_id, owner) = fixture.user_workspace(owner_id).await;
    let owner_user = owner.authenticated_user().expect("owner User").clone();
    let member_user = organization_user(
        fixture.database.clone(),
        member_id,
        OrganizationRole::Member,
    )
    .await;
    fixture
        .workspaces
        .set_membership(
            &change(owner_user, 12, 'a'),
            &workspace_id,
            &member_user,
            WorkspaceRole::Member,
        )
        .await
        .expect("add Member");
    let agent = registered_agent(&fixture.workspaces, &owner, &workspace_id).await;
    (
        workspace_id,
        [owner, AuthenticatedPrincipal::user(member_user), agent],
    )
}

const TAKES_NOTHING: &str =
    "export default async function remind(): Promise<string> { return 'draft'; }";
const TAKES_A_NAME: &str = "export default async function greet(input: { name: string }): Promise<string> { return input.name; }";

// Catches: a read of a Private Tool's path, identity, revision, default or
// declaration that answers differently from the whole definition it stands
// in for: with another owner's or another Workspace's Private Tool, a
// replaced one, or a path that names nothing.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture compares every narrow read with the whole read beside it"
)]
async fn each_narrow_private_tool_read_agrees_with_the_whole_definition() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, [owner, member, agent]) =
        workspace_with_a_member_and_an_agent(&fixture, "user:narrow-owner", "user:narrow-member")
            .await;
    let elsewhere = fixture
        .workspaces
        .create(
            &change(
                owner.authenticated_user().expect("owner User").clone(),
                14,
                'a',
            ),
            WorkspaceDisplayName::parse("Elsewhere").expect("display name"),
        )
        .await
        .expect("create another Workspace")
        .id()
        .clone();
    // Each save: its author, Workspace, path, source and declared default.
    // The Owner's `mail.send` is saved twice, so it is at revision 2.
    for (author, workspace, path, source, default) in [
        (&owner, &workspace_id, "draft", TAKES_NOTHING, None),
        (&owner, &workspace_id, "mail.send", TAKES_NOTHING, None),
        (
            &owner,
            &workspace_id,
            "mail.send",
            TAKES_A_NAME,
            Some(ToolAccessState::ApprovalRequired),
        ),
        (&member, &workspace_id, "draft", TAKES_A_NAME, None),
        (&agent, &workspace_id, "agent.work", TAKES_NOTHING, None),
        (&owner, &elsewhere, "draft", TAKES_A_NAME, None),
    ] {
        fixture
            .save_tool(author, workspace, path, source, default)
            .await;
    }

    let paths = ["agent.work", "draft", "mail.send", "nothing.here"]
        .map(|path| PrivateToolPath::parse(path).expect("path"));
    let named = paths.iter().collect::<Vec<_>>();
    let mut transaction = fixture.database.begin_read_snapshot().await.expect("begin");
    // (owner, Workspace, the paths it owns there)
    let owners: [(
        &AuthenticatedPrincipal,
        &coral_workspace::WorkspaceId,
        &[&str],
    ); 5] = [
        (&owner, &workspace_id, &["draft", "mail.send"]),
        (&member, &workspace_id, &["draft"]),
        (&agent, &workspace_id, &["agent.work"]),
        (&owner, &elsewhere, &["draft"]),
        (&member, &elsewhere, &[]),
    ];
    for (principal, workspace, owned) in owners {
        let principal = principal.principal();
        let case = format!("{principal:?} in {workspace}");
        let whole =
            crate::repository::list_current_in_transaction(&mut transaction, workspace, principal)
                .await
                .expect("list whole Private Tools");
        assert_eq!(
            whole
                .iter()
                .map(|tool| tool.path().as_str())
                .collect::<Vec<_>>(),
            owned,
            "{case}"
        );
        assert_eq!(
            crate::repository::list_current_records_in_transaction(
                &mut transaction,
                workspace,
                principal
            )
            .await
            .expect("list records"),
            whole.iter().map(record_of).collect::<Vec<_>>(),
            "{case}"
        );
        let mut declarations = std::collections::BTreeMap::new();
        for path in &paths {
            let whole = crate::repository::find_current_in_transaction(
                &mut transaction,
                workspace,
                principal,
                path,
            )
            .await
            .expect("find the whole Private Tool");
            assert_eq!(
                crate::repository::find_current_record_in_transaction(
                    &mut transaction,
                    workspace,
                    principal,
                    path
                )
                .await
                .expect("find the record"),
                whole.as_ref().map(record_of),
                "{case}: {path:?}"
            );
            let path = crate::AuthoredToolPath::Private(path.clone());
            assert_eq!(
                crate::authored_tool_key_in_transaction(
                    &mut transaction,
                    workspace,
                    principal,
                    &path
                )
                .await
                .expect("read the Tool Key"),
                crate::select_linked_tool_in_transaction(
                    &mut transaction,
                    workspace,
                    principal,
                    &path
                )
                .await
                .expect("select")
                .map(|selection| selection.tool_key().clone()),
                "{case}: {path:?}"
            );
            if let Some(whole) = whole {
                declarations.insert(whole.path().clone(), whole.declaration().clone());
            }
        }
        assert_eq!(
            crate::repository::current_declarations_in_transaction(
                &mut transaction,
                workspace,
                principal,
                &named
            )
            .await
            .expect("read declarations"),
            declarations,
            "{case}"
        );
    }
    transaction.rollback().await.expect("finish");
}

// Catches: a save, a delete, a policy target or an access removal that
// decodes a definition it only names, so that one Private Tool that Coral
// cannot read fails work that never reads it.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture takes each caller of a narrow read past the same unreadable definitions"
)]
async fn private_tool_work_that_only_names_a_tool_decodes_no_definition() {
    let fixture = PrivateToolTestModules::new().await;
    let (workspace_id, [owner, member, agent]) =
        workspace_with_a_member_and_an_agent(&fixture, "user:names-owner", "user:names-member")
            .await;
    let path = |path| PrivateToolPath::parse(path).expect("path");
    let mut ids = std::collections::BTreeMap::new();
    for (author, at) in [
        (&owner, "draft"),
        (&owner, "mail.send"),
        (&owner, "mail.digest"),
        (&member, "draft"),
        (&agent, "agent.work"),
    ] {
        let saved = fixture
            .save_tool(author, &workspace_id, at, TAKES_NOTHING, None)
            .await;
        // Its source is now one byte over the authoring bound, so the whole
        // definition cannot be read.
        crate::test_fixtures_repository::corrupt_source(
            &fixture.database,
            &workspace_id,
            saved.id(),
        )
        .await;
        ids.insert(
            (author.principal().id().as_str().to_owned(), at),
            saved.id().clone(),
        );
    }
    let id_of = |author: &AuthenticatedPrincipal, at| {
        ids.get(&(author.principal().id().as_str().to_owned(), at))
            .expect("a saved Private Tool")
    };
    let whole = fixture
        .private_tools
        .get(&owner, &workspace_id, &path("draft"))
        .await;
    assert!(matches!(whole, Err(Error::CorruptData(_))), "{whole:?}");

    let save = |at, source| {
        PrivateToolSave::new(
            path(at),
            AuthoredToolDescription::parse("A private draft").expect("description"),
            AuthoredToolSource::parse(source).expect("source"),
        )
    };

    // A save at a free path decides its namespace by the paths its author
    // owns, and is typed by the declaration of the Private Tool it names:
    // it passes that Tool an argument it does not take.
    let typed = fixture
        .private_tools
        .save(
            &private_tool_change(owner.clone(), 40, 'c'),
            &workspace_id,
            save(
                "typed",
                "export default async function typed() { return coral.private.draft(1); }",
            ),
        )
        .await
        .expect("save beside unreadable definitions");
    assert_eq!(
        typed
            .diagnostics()
            .iter()
            .map(crate::AuthoredToolDiagnostic::code)
            .collect::<Vec<_>>(),
        [2554],
        "{:?}",
        typed.diagnostics()
    );

    // A save in a namespace the author's paths take is refused by those
    // paths, and saves nothing.
    let events = async || {
        fixture
            .private_tools
            .list_lifecycle_events(&owner, &workspace_id)
            .await
            .expect("list lifecycle Events")
    };
    let before = events().await;
    let conflict = fixture
        .private_tools
        .save(
            &private_tool_change(owner.clone(), 41, 'c'),
            &workspace_id,
            save("mail", TAKES_NOTHING),
        )
        .await;
    assert!(
        matches!(
            conflict,
            Err(Error::PathConflict(AuthoredToolKind::Private))
        ),
        "{conflict:?}"
    );
    let saved = fixture
        .private_tools
        .get(&owner, &workspace_id, &path("mail"))
        .await;
    assert!(
        matches!(saved, Err(Error::NotFound(AuthoredToolKind::Private))),
        "{saved:?}"
    );
    assert_eq!(events().await, before);

    // A conditional save reads the revision it expects, and replaces the
    // Private Tool under its identity.
    let replaced = fixture
        .private_tools
        .save(
            &private_tool_change(owner.clone(), 42, 'c'),
            &workspace_id,
            save("mail.send", TAKES_A_NAME).replace_only(Some(1)),
        )
        .await
        .expect("replace an unreadable definition");
    assert_eq!(
        (replaced.id(), replaced.revision()),
        (id_of(&owner, "mail.send"), 2)
    );

    // A policy target names the Private Tool by its Tool Key, where a
    // linked selection of it reads its whole definition.
    let digest = id_of(&owner, "mail.digest");
    let named = crate::AuthoredToolPath::Private(path("mail.digest"));
    let mut transaction = fixture.database.begin_read_snapshot().await.expect("begin");
    let selection = crate::select_linked_tool_in_transaction(
        &mut transaction,
        &workspace_id,
        owner.principal(),
        &named,
    )
    .await;
    let key = crate::authored_tool_key_in_transaction(
        &mut transaction,
        &workspace_id,
        owner.principal(),
        &named,
    )
    .await
    .expect("read the Tool Key");
    transaction.rollback().await.expect("finish");
    assert!(
        matches!(selection, Err(Error::CorruptData(_))),
        "{selection:?}"
    );
    assert_eq!(key, Some(digest.tool_key()));

    // A delete names the Private Tool: the owner's own, an Agent's for a
    // Workspace Owner, and each one of a Member who loses access.
    assert_eq!(
        &fixture
            .private_tools
            .delete(
                &private_tool_change(owner.clone(), 45, 'c'),
                &workspace_id,
                &path("mail.digest")
            )
            .await
            .expect("delete an unreadable Private Tool"),
        digest
    );
    assert_eq!(
        &fixture
            .delete_agent(
                &private_tool_change(owner.clone(), 46, 'c'),
                &workspace_id,
                agent.principal().id(),
                &path("agent.work"),
            )
            .await
            .expect("delete an Agent's unreadable Private Tool"),
        id_of(&agent, "agent.work")
    );
    let departure = change(
        owner.authenticated_user().expect("owner User").clone(),
        47,
        'f',
    );
    let mut transaction = fixture.database.begin().await.expect("begin");
    let prepared = fixture
        .workspaces
        .prepare_access_removal_in_transaction(
            &mut transaction,
            &departure,
            coral_workspace::AccessRemovalTarget::WorkspaceMember(
                &workspace_id,
                member.authenticated_user().expect("member User").user_id(),
            ),
        )
        .await
        .expect("prepare the Member's removal");
    let lost = crate::delete_current_for_access_removal_in_transaction(
        &mut transaction,
        &departure,
        &prepared,
    )
    .await
    .expect("delete the departing Member's unreadable Private Tools");
    transaction.rollback().await.expect("finish");
    assert_eq!(
        lost,
        [(workspace_id.clone(), vec![id_of(&member, "draft").clone()])]
    );
}
