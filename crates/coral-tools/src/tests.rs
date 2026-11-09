use coral_test_support::change;
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::Notify;

use coral_catalog::ToolAccessState;
use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_v8::CompileFailure;
use coral_workspace::{
    AuthenticatedPrincipal, OrganizationRole, TraceId, WorkspaceDisplayName, WorkspaceModule,
    WorkspaceRole,
};

use crate::authoring::{AuthoredToolCompiler, PreparedAuthoredTool};
use crate::{
    AuthoredToolDescription, AuthoredToolSource, Error,
    LifecycleAction as PrivateToolLifecycleAction, PrivateToolChangeContext, PrivateToolModule,
    PrivateToolPath, PrivateToolSave,
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
    assert!(matches!(
        crate::AuthoredToolDeclaration::from_compiler(&format!("{maximum_declaration}d")),
        Err(Error::PackageTooLarge {
            component: "Tool declaration",
            ..
        })
    ));
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
    assert!(
        fields
            .iter()
            .any(|field| field.path() == "input.input.value")
    );
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
            .max_by_key(|event| event.occurred_at_unix_nanos())
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
    assert!(matches!(failed, Err(Error::PrivateToolAdmission(_))));
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
    assert_eq!(replacement.saved_at_unix_nanos(), 50);

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
        Err(Error::PrivateToolNotFound)
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
    let workspace_entries =
        crate::private_tool_entries_in_transaction(&mut transaction, &workspace_id)
            .await
            .expect("read one Workspace's entries");
    let every_entry = crate::all_private_tool_entries_in_transaction(&mut transaction)
        .await
        .expect("read every Workspace's entries");
    for (workspace, tool, revision) in [
        (&workspace_id, replaced.id(), Some(2)),
        (&other_workspace_id, elsewhere.id(), Some(1)),
        (&other_workspace_id, replaced.id(), None),
        (&workspace_id, gone.id(), None),
    ] {
        assert_eq!(
            crate::private_tool_entry_in_transaction(&mut transaction, workspace, tool)
                .await
                .expect("read one tool's entry")
                .map(|entry| entry.revision()),
            revision
        );
    }
    transaction.rollback().await.expect("finish entry read");

    let summary = |entries: &[crate::PrivateToolEntry]| {
        let mut summary = entries
            .iter()
            .map(|entry| {
                (
                    entry.workspace_id().clone(),
                    entry.principal().clone(),
                    entry.path().as_str().to_string(),
                    entry.description().as_str().to_string(),
                    entry.revision(),
                )
            })
            .collect::<Vec<_>>();
        summary.sort_by(|left, right| (&left.2, &left.3).cmp(&(&right.2, &right.3)));
        summary
    };
    assert_eq!(
        summary(&workspace_entries),
        [
            (
                workspace_id.clone(),
                agent.principal().clone(),
                "agent.answer".to_string(),
                "Answer a question".to_string(),
                1,
            ),
            (
                workspace_id.clone(),
                user.principal().clone(),
                "reports.total".to_string(),
                "Total a report again".to_string(),
                2,
            ),
        ],
        "every Principal's current tools of this Workspace, at their current revision"
    );
    let mut expected = summary(&workspace_entries);
    expected.push((
        other_workspace_id.clone(),
        other_user.principal().clone(),
        "reports.total".to_string(),
        "Total elsewhere".to_string(),
        1,
    ));
    assert_eq!(
        summary(&every_entry),
        expected,
        "the cross-Workspace read adds the other Workspace's tool"
    );
    for (entries, saved) in [
        (&workspace_entries, &replaced),
        (&workspace_entries, &agent_private_tool),
        (&every_entry, &replaced),
        (&every_entry, &agent_private_tool),
        (&every_entry, &elsewhere),
    ] {
        let read = entries
            .iter()
            .find(|entry| entry.id() == saved.id())
            .expect("current tool has an entry");
        assert_eq!(
            read,
            &crate::PrivateToolEntry::from_saved(saved),
            "the read agrees with what a save hands to search"
        );
        assert_eq!(read.declaration(), saved.declaration());
        assert!(
            read.declaration_fields()
                .expect("ready declaration fields")
                .iter()
                .any(|field| field.path() == "input.input.value")
        );
    }

    crate::test_fixtures_repository::corrupt_declaration_fields(
        &fixture.database,
        &workspace_id,
        agent_private_tool.id(),
    )
    .await;
    let mut transaction = fixture.database.begin().await.expect("begin entry read");
    let entries = crate::private_tool_entries_in_transaction(&mut transaction, &workspace_id)
        .await
        .expect("a corrupt projection does not fail the read");
    transaction.rollback().await.expect("finish entry read");
    let corrupt = entries
        .iter()
        .find(|entry| entry.id() == agent_private_tool.id())
        .expect("the tool stays readable by path and description");
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
        Err(Error::PrivateToolNotFound)
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
        Err(Error::PrivateToolNotFound)
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
            .private_tools
            .get_agent(&workspace_owner, &workspace_id, &agent_id, &path)
            .await
            .expect("Workspace Owner inspects Agent tool")
            .id(),
        agent_private_tool.id()
    );
    assert_eq!(
        fixture
            .private_tools
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
            .private_tools
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
            .private_tools
            .get_agent(&member, &workspace_id, &agent_id, &path)
            .await,
        Err(Error::PrivateToolNotFound)
    ));
    assert!(matches!(
        fixture
            .private_tools
            .list_agent(&member, &workspace_id, &agent_id)
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert!(matches!(
        fixture
            .private_tools
            .delete_agent(
                &private_tool_change(member, 36, 'b'),
                &workspace_id,
                &agent_id,
                &path,
            )
            .await,
        Err(Error::PrivateToolNotFound)
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
            .private_tools
            .list_agent(&organization_owner, &workspace_id, &agent_id)
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert!(matches!(
        fixture
            .private_tools
            .get_agent(&organization_owner, &workspace_id, &agent_id, &path)
            .await,
        Err(Error::PrivateToolNotFound)
    ));
    assert!(matches!(
        fixture
            .private_tools
            .delete_agent(
                &private_tool_change(organization_owner, 40, '9'),
                &workspace_id,
                &agent_id,
                &path,
            )
            .await,
        Err(Error::PrivateToolNotFound)
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
        .private_tools
        .delete_agent(
            &private_tool_change(workspace_owner.clone(), 41, '9'),
            &workspace_id,
            &agent_id,
            &path,
        )
        .await
        .expect("designated Owner deletes Agent tool");

    assert!(matches!(
        fixture
            .private_tools
            .get(&agent, &workspace_id, &path)
            .await,
        Err(Error::PrivateToolNotFound)
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

/// The Saved Function rows the release before Private Tools wrote, at step
/// 59, for one User and one Agent, read back after the upgrade as each
/// owner's Private Tools, with every part of the definition kept.
// Catches: an upgrade that loses or rewrites part of a definition, gives a
// Private Tool another owner, or leaves it without the default Tool Access
// State, the binding to the definition Coral performs, or its renamed table.
#[tokio::test]
async fn upgrade_keeps_each_saved_function_as_its_owners_private_tool() {
    let before_upgrade = coral_test_support::migration_plan_before(60);
    let fixture = PrivateToolTestModules::on_plan(&before_upgrade).await;
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
        assert_eq!(tool.saved_at_unix_nanos(), 15);
        assert_eq!(tool.revision(), revision);
        assert_eq!(tool.default_access_state(), ToolAccessState::Enabled);
        assert_eq!(tool.execution_binding().definition(), tool.id().to_string());
        assert_eq!(tool.execution_binding().revision(), revision);
    }
    // A Workspace Owner still reads an Agent's Private Tool.
    assert_eq!(
        fixture
            .private_tools
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
            Err(Error::PrivateToolPathConflict)
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
                Err(Error::PrivateToolNotFound)
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
            Err(Error::PrivateToolNotFound)
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
            |error| matches!(error, Error::PrivateToolNotFound),
        ),
        (
            "replace-only at a moved revision",
            timing_out
                .save(&context, &workspace_id, save(&path).replace_only(Some(2)))
                .await
                .map(drop),
            |error| matches!(error, Error::PrivateToolRevisionConflict { current: 1 }),
        ),
        (
            "delete at a missing path",
            timing_out
                .delete(&context, &workspace_id, &missing)
                .await
                .map(drop),
            |error| matches!(error, Error::PrivateToolNotFound),
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
                Err(Error::PrivateToolNotFound)
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
        Err(Error::PrivateToolCompilationTimedOut)
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
            .filter(|result| matches!(result, Err(Error::PrivateToolPathConflict)))
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
            .archive(&change(user, 40, 'b'), &workspace_id),
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
        Err(Error::PrivateToolNotFound)
    ));
    assert!(
        blocked_private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list rejected authoring evidence")
            .is_empty()
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
        matches!(rejected, Err(Error::PrivateToolAdmission(_))),
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
            Err(Error::PrivateToolAdmission(_))
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

// Catches: a save that accepts a Private Tool whose arguments or result
// cannot cross a Tool call as data, anywhere in its declaration, or that
// refuses one whose types say nothing about the data (`any`, `unknown`).
#[tokio::test]
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
            "`input.callback` is a function",
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
            "export default (values: Set<string>[]) => values.length;",
            "`input.values[]` is a Set",
        ),
        (
            "export default (value: bigint) => Number(value);",
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
            matches!(&refused, Err(Error::PrivateToolNotData(message)) if message.contains(reason)),
            "{source}: {refused:?}"
        );
    }
    assert!(matches!(
        fixture
            .private_tools
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::PrivateToolNotFound)
    ));

    for source in [
        "export default async (input: any): Promise<any> => input;",
        "export default (input: unknown): unknown => input;",
        "export default async (input: { name: string; tags: readonly string[]; at?: [number, string | null] }): Promise<Record<string, boolean>> => ({ [input.name]: true });",
        "export default async (): Promise<void> => {};",
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
            Err(Error::PrivateToolAdmission(message))
                if message == "Private Tool coral.private.reports.compose:2:10: \
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
            Err(Error::PrivateToolAdmission(message))
                if message.contains("Private Tool coral.private.reports.compose")
                    && (message.contains(" at 1:") || message.contains(":1:"))
        ));
    }

    assert!(matches!(
        fixture
            .private_tools
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::PrivateToolNotFound)
    ));
    assert!(
        fixture
            .private_tools
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list lifecycle evidence")
            .is_empty()
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
struct DeclaresReposGet;

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
        Err(Error::PrivateToolNotFound)
    ));
    // Deleting again finds nothing and never falls back to the first Workspace.
    assert!(matches!(delete().await, Err(Error::PrivateToolNotFound)));
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

/// Compiles every source to one fixed declaration once its gate opens.
struct GatedAuthoredToolCompiler {
    gate: Box<dyn Fn() -> Result<(), CompileFailure> + Send + Sync>,
}

impl GatedAuthoredToolCompiler {
    /// Each compilation waits until a second one reaches the gate.
    fn paired() -> Self {
        let barrier = Barrier::new(2);
        Self {
            gate: Box::new(move || {
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
            gate: Box::new(move || {
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
            gate: Box::new(move || Err(failure.clone())),
        }
    }
}

impl AuthoredToolCompiler for GatedAuthoredToolCompiler {
    fn call_paths(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
    ) -> Option<coral_v8::NamedCallPaths> {
        None
    }

    fn prepare(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
        _declarations: Option<&coral_v8::CoralDeclarations>,
    ) -> Result<PreparedAuthoredTool, CompileFailure> {
        (self.gate)()?;
        Ok(PreparedAuthoredTool {
            declaration: crate::AuthoredToolDeclaration::from_compiler(
                "declare const _default: () => number;\nexport default _default;\n",
            )
            .map_err(|error| error.to_string())?,
            declaration_field_projection: crate::AuthoredToolDeclarationFieldProjection::Ready(
                Vec::new(),
            ),
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            private_references: Vec::new(),
            shared_references: Vec::new(),
            not_data: None,
        })
    }
}

struct PrivateToolTestModules {
    fixture: TestDatabase,
    database: Arc<coral_db::Database>,
    workspaces: WorkspaceModule,
    private_tools: PrivateToolModule,
}

impl PrivateToolTestModules {
    async fn new() -> Self {
        Self::on_plan(coral_db::migration_plan()).await
    }

    async fn on_plan(plan: &coral_db::MigrationPlan) -> Self {
        let fixture = TestDatabase::new(plan)
            .await
            .expect("create Private Tool test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let private_tools = PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("start Private Tool compiler");
        Self {
            fixture,
            database,
            workspaces,
            private_tools,
        }
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
    let issued = workspaces
        .create_agent(
            &change(
                owner.authenticated_user().expect("Owner User").clone(),
                10,
                '1',
            ),
            workspace_id,
            coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
            None,
        )
        .await
        .expect("create Agent");
    workspaces
        .authenticate_agent(workspace_id, issued.credential.expose(), 11)
        .await
        .expect("authenticate Agent")
}

fn private_tool_change(
    actor: AuthenticatedPrincipal,
    occurred_at_unix_nanos: i64,
    digit: char,
) -> PrivateToolChangeContext {
    PrivateToolChangeContext::new(
        actor,
        occurred_at_unix_nanos,
        TraceId::parse(&digit.to_string().repeat(32)).expect("trace id"),
    )
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
                    true,
                    Some(0),
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
                .private_tools
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
                .list(&actor, &workspace, after.as_ref(), page_size)
                .await
        }
    };
    let paths = |page: &crate::PrivateToolPage| {
        page.summaries()
            .iter()
            .map(|summary| summary.path().as_str().to_owned())
            .collect::<Vec<_>>()
    };

    let first = list(&member, None, 2).await.expect("first page");
    assert_eq!(paths(&first), ["Middle.upper", "alpha.first"]);
    let summary = first.summaries().last().expect("summary");
    assert_eq!(summary.description().as_str(), "Describe alpha.first");
    assert_eq!(summary.saved_at_unix_nanos(), 20);
    let after = first.next_after().expect("more pages").as_str().to_owned();
    let second = list(&member, Some(&after), 2).await.expect("second page");
    // A page that ends exactly at the last tool has no continuation.
    assert_eq!(paths(&second), ["reports.total", "zeta.last"]);
    assert_eq!(second.next_after(), None);

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
    for page_size in [0, crate::PRIVATE_TOOL_PAGE_MAX_SIZE + 1] {
        assert!(matches!(
            list(&member, None, page_size).await,
            Err(Error::InvalidInput(_))
        ));
    }

    let entries = || async {
        let mut transaction = fixture
            .database
            .begin_read_snapshot()
            .await
            .expect("read snapshot");
        let entries = crate::private_tool_entries_in_transaction(&mut transaction, &workspace_id)
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
        Err(Error::PrivateToolNotFound)
    ));
    assert_eq!(entries().await, before);
}
