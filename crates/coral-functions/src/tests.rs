use coral_test_support::change;
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::Notify;

use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_v8::CompileFailure;
use coral_workspace::{
    AuthenticatedPrincipal, OrganizationRole, TraceId, WorkspaceDisplayName, WorkspaceModule,
    WorkspaceRole,
};

use crate::saved_function::{PreparedSavedFunction, SavedFunctionCompiler};
use crate::{
    Error, LifecycleAction as SavedFunctionLifecycleAction, SavedFunctionChangeContext,
    SavedFunctionDescription, SavedFunctionModule, SavedFunctionPath, SavedFunctionSave,
    SavedFunctionSource,
};

#[test]
fn fixed_saved_function_source_and_declaration_limits_accept_the_boundary_only() {
    let maximum_function = "f".repeat(crate::SAVED_FUNCTION_SOURCE_MAX_BYTES);
    SavedFunctionSource::parse(&maximum_function).expect("maximum Saved Function source");
    SavedFunctionSource::parse(&format!("{maximum_function}f"))
        .expect_err("oversized Saved Function source");

    let maximum_declaration = "d".repeat(crate::SAVED_FUNCTION_DECLARATION_MAX_BYTES);
    crate::SavedFunctionDeclaration::from_compiler(&maximum_declaration)
        .expect("maximum Saved Function declaration");
    assert!(matches!(
        crate::SavedFunctionDeclaration::from_compiler(&format!("{maximum_declaration}d")),
        Err(Error::PackageTooLarge {
            component: "Saved Function declaration",
            ..
        })
    ));
}

#[tokio::test]
async fn saved_function_save_publishes_exact_authoring_record_and_event() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:function-author").await;
    let source = r"
        interface Input { value: number }
        function double(value: number): number { return value * 2; }
        export default function calculate(input: Input): number {
            return double(input.value);
        }
    ";

    let saved = fixture
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 20, 'a'),
            &workspace_id,
            SavedFunctionSave::new(
                SavedFunctionPath::parse("finance.calculate").expect("Saved Function Path"),
                SavedFunctionDescription::parse("Calculate a doubled finance value")
                    .expect("description"),
                SavedFunctionSource::parse(source).expect("source"),
            ),
        )
        .await
        .expect("save function");

    assert!(saved.declaration().as_str().contains("interface Input"));
    assert!(saved.declaration().as_str().contains("export default"));
    let crate::SavedFunctionDeclarationFieldProjection::Ready(fields) =
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
        .saved_functions
        .list_lifecycle_events(&principal, &workspace_id)
        .await
        .expect("list author lifecycle Events");
    assert!(matches!(
        events.as_slice(),
        [event]
            if event.action() == SavedFunctionLifecycleAction::Created
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

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle scenario covers failed save, replacement, and deletion atomicity"
)]
async fn failed_save_preserves_current_definition_and_accepted_changes_follow_transaction_order() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:function-editor").await;
    let path = SavedFunctionPath::parse("reports.total").expect("path");
    let original_source = "export default function total() { return 41; }";
    let original = fixture
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 100, 'b'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Original total").expect("description"),
                SavedFunctionSource::parse(original_source).expect("source"),
            ),
        )
        .await
        .expect("save original");

    let failed = fixture
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 200, 'c'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Broken replacement").expect("description"),
                SavedFunctionSource::parse("export default function (").expect("source"),
            ),
        )
        .await;
    assert!(matches!(failed, Err(Error::SavedFunctionAdmission(_))));
    let unchanged = fixture
        .saved_functions
        .get(&principal, &workspace_id, &path)
        .await
        .expect("read unchanged current function");
    assert_eq!(unchanged.id(), original.id());
    assert_eq!(unchanged.source().as_str(), original_source);
    assert_eq!(
        fixture
            .saved_functions
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("events after rejection")
            .len(),
        1
    );

    let replacement_source = "export default function total() { return 42; }";
    let replacement = fixture
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 50, 'd'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Replacement total").expect("description"),
                SavedFunctionSource::parse(replacement_source).expect("source"),
            ),
        )
        .await
        .expect("replace despite older occurrence timestamp");
    assert_eq!(replacement.id(), original.id());
    assert_eq!(replacement.source().as_str(), replacement_source);
    assert_eq!(replacement.saved_at_unix_nanos(), 50);

    let deleted_id = fixture
        .saved_functions
        .delete(
            &saved_function_change(principal.clone(), 40, 'e'),
            &workspace_id,
            &path,
        )
        .await
        .expect("delete despite older occurrence timestamp");
    assert_eq!(&deleted_id, original.id());
    assert!(matches!(
        fixture
            .saved_functions
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    let events = fixture
        .saved_functions
        .list_lifecycle_events(&principal, &workspace_id)
        .await
        .expect("complete lifecycle evidence");
    assert_eq!(events.len(), 3);
    assert!(events.iter().any(|event| {
        event.action() == SavedFunctionLifecycleAction::Created
            && event.target_id() == original.id()
    }));
    assert!(events.iter().any(|event| {
        event.action() == SavedFunctionLifecycleAction::Replaced
            && event.target_id() == original.id()
    }));
    assert!(events.iter().any(|event| {
        event.action() == SavedFunctionLifecycleAction::Deleted
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
async fn entry_reads_return_current_functions_with_revision_and_declaration_fields() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, user) = fixture.user_workspace("user:entry-author").await;
    let agent = registered_agent(&fixture.workspaces, &user, &workspace_id).await;
    let (other_workspace_id, other_user) = fixture.user_workspace("user:entry-elsewhere").await;
    let save = |principal: &AuthenticatedPrincipal,
                workspace_id: &coral_workspace::WorkspaceId,
                path: &str,
                description: &str,
                at: i64| {
        let saved_functions = fixture.saved_functions.clone();
        let context = saved_function_change(principal.clone(), at, 'a');
        let workspace_id = workspace_id.clone();
        let save = SavedFunctionSave::new(
            SavedFunctionPath::parse(path).expect("path"),
            SavedFunctionDescription::parse(description).expect("description"),
            SavedFunctionSource::parse(
                "interface Input { value: number }\nexport default function run(input: Input): number { return input.value; }",
            )
            .expect("source"),
        );
        async move {
            saved_functions
                .save(&context, &workspace_id, save)
                .await
                .expect("save function")
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
    let agent_function = save(
        &agent,
        &workspace_id,
        "agent.answer",
        "Answer a question",
        40,
    )
    .await;
    let gone = save(&user, &workspace_id, "reports.gone", "Soon deleted", 50).await;
    fixture
        .saved_functions
        .delete(
            &saved_function_change(user.clone(), 60, 'b'),
            &workspace_id,
            &SavedFunctionPath::parse("reports.gone").expect("path"),
        )
        .await
        .expect("delete function");
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
        crate::saved_function_entries_in_transaction(&mut transaction, &workspace_id)
            .await
            .expect("read one Workspace's entries");
    let every_entry = crate::all_saved_function_entries_in_transaction(&mut transaction)
        .await
        .expect("read every Workspace's entries");
    for (workspace, function, revision) in [
        (&workspace_id, replaced.id(), Some(2)),
        (&other_workspace_id, elsewhere.id(), Some(1)),
        (&other_workspace_id, replaced.id(), None),
        (&workspace_id, gone.id(), None),
    ] {
        assert_eq!(
            crate::saved_function_index_revision_in_transaction(
                &mut transaction,
                workspace,
                function,
            )
            .await
            .expect("read one function's revision"),
            revision
        );
    }
    transaction.rollback().await.expect("finish entry read");

    let summary = |entries: &[crate::SavedFunctionEntry]| {
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
        "every Principal's current functions of this Workspace, at their current revision"
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
        "the cross-Workspace read adds the other Workspace's function"
    );
    for (entries, saved) in [
        (&workspace_entries, &replaced),
        (&workspace_entries, &agent_function),
        (&every_entry, &replaced),
        (&every_entry, &agent_function),
        (&every_entry, &elsewhere),
    ] {
        let read = entries
            .iter()
            .find(|entry| entry.id() == saved.id())
            .expect("current function has an entry");
        assert_eq!(
            read,
            &crate::SavedFunctionEntry::from_saved(saved),
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
        agent_function.id(),
    )
    .await;
    let mut transaction = fixture.database.begin().await.expect("begin entry read");
    let entries = crate::saved_function_entries_in_transaction(&mut transaction, &workspace_id)
        .await
        .expect("a corrupt projection does not fail the read");
    transaction.rollback().await.expect("finish entry read");
    let corrupt = entries
        .iter()
        .find(|entry| entry.id() == agent_function.id())
        .expect("the function stays readable by path and description");
    assert_eq!(corrupt.declaration_fields(), None);
    assert_eq!(corrupt.description().as_str(), "Answer a question");
}

#[tokio::test]
async fn agent_and_user_saved_function_paths_are_principal_owned() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, user) = fixture.user_workspace("user:function-owner").await;
    let agent = registered_agent(&fixture.workspaces, &user, &workspace_id).await;
    let path = SavedFunctionPath::parse("private.answer").expect("path");
    let saved = fixture
        .saved_functions
        .save(
            &saved_function_change(agent.clone(), 20, 'd'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Agent-private answer").expect("description"),
                SavedFunctionSource::parse("export default () => 42;").expect("source"),
            ),
        )
        .await
        .expect("Agent saves its own function");

    assert!(matches!(
        fixture
            .saved_functions
            .get(&user, &workspace_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert_eq!(
        fixture
            .saved_functions
            .get(&agent, &workspace_id, &path)
            .await
            .expect("Agent reads own current definition")
            .id(),
        saved.id()
    );
    fixture
        .saved_functions
        .delete(
            &saved_function_change(agent.clone(), 30, 'e'),
            &workspace_id,
            &path,
        )
        .await
        .expect("Agent deletes its own function");
    assert!(matches!(
        fixture
            .saved_functions
            .get(&agent, &workspace_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert!(matches!(
        fixture
            .saved_functions
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
async fn only_designated_owners_inspect_and_delete_agent_functions_without_authoring_as_the_agent()
{
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, workspace_owner) = fixture.user_workspace("user:workspace-owner").await;
    let agent = registered_agent(&fixture.workspaces, &workspace_owner, &workspace_id).await;
    let agent_id = agent.principal().id().clone();
    let path = SavedFunctionPath::parse("private.answer").expect("path");
    let agent_function = fixture
        .saved_functions
        .save(
            &saved_function_change(agent.clone(), 20, '7'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Agent-owned answer").expect("description"),
                SavedFunctionSource::parse("export default () => 'agent';").expect("source"),
            ),
        )
        .await
        .expect("Agent saves its function");

    assert_eq!(
        fixture
            .saved_functions
            .get_agent(&workspace_owner, &workspace_id, &agent_id, &path)
            .await
            .expect("Workspace Owner inspects Agent function")
            .id(),
        agent_function.id()
    );
    assert_eq!(
        fixture
            .saved_functions
            .list_agent(&workspace_owner, &workspace_id, &agent_id)
            .await
            .expect("Workspace Owner lists Agent functions")
            .as_slice(),
        std::slice::from_ref(&agent_function)
    );

    let owner_function = fixture
        .saved_functions
        .save(
            &saved_function_change(workspace_owner.clone(), 30, '8'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Owner's private answer").expect("description"),
                SavedFunctionSource::parse("export default () => 'owner';").expect("source"),
            ),
        )
        .await
        .expect("Owner authors only as the Owner's User");
    assert_ne!(owner_function.id(), agent_function.id());
    assert_eq!(
        fixture
            .saved_functions
            .get_agent(&workspace_owner, &workspace_id, &agent_id, &path)
            .await
            .expect("Agent definition remains unchanged")
            .id(),
        agent_function.id()
    );

    let member_user = organization_user(
        fixture.database.clone(),
        "user:function-member",
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
            .saved_functions
            .get_agent(&member, &workspace_id, &agent_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert!(matches!(
        fixture
            .saved_functions
            .list_agent(&member, &workspace_id, &agent_id)
            .await,
        Err(Error::PermissionDenied(_))
    ));
    assert!(matches!(
        fixture
            .saved_functions
            .delete_agent(
                &saved_function_change(member, 36, 'b'),
                &workspace_id,
                &agent_id,
                &path,
            )
            .await,
        Err(Error::SavedFunctionNotFound)
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
            .saved_functions
            .list_agent(&organization_owner, &workspace_id, &agent_id)
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert!(matches!(
        fixture
            .saved_functions
            .get_agent(&organization_owner, &workspace_id, &agent_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert!(matches!(
        fixture
            .saved_functions
            .delete_agent(
                &saved_function_change(organization_owner, 40, '9'),
                &workspace_id,
                &agent_id,
                &path,
            )
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert_eq!(
        fixture
            .saved_functions
            .get(&agent, &workspace_id, &path)
            .await
            .expect("denied deletions leave the Agent function current")
            .id(),
        agent_function.id()
    );

    fixture
        .saved_functions
        .delete_agent(
            &saved_function_change(workspace_owner.clone(), 41, '9'),
            &workspace_id,
            &agent_id,
            &path,
        )
        .await
        .expect("designated Owner deletes Agent function");

    assert!(matches!(
        fixture
            .saved_functions
            .get(&agent, &workspace_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert_eq!(
        fixture
            .saved_functions
            .get(&workspace_owner, &workspace_id, &path)
            .await
            .expect("Owner's User function stays private and current")
            .id(),
        owner_function.id()
    );
}

#[tokio::test]
async fn saved_function_paths_cannot_overlap_virtual_namespaces() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:path-conflict").await;

    for path in ["reports.current", "finance.tax.calculate"] {
        fixture
            .saved_functions
            .save(
                &saved_function_change(principal.clone(), 20, 'c'),
                &workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse(path).expect("path"),
                    SavedFunctionDescription::parse("Current callable").expect("description"),
                    SavedFunctionSource::parse("export default () => 42;").expect("source"),
                ),
            )
            .await
            .expect("save non-overlapping function");
    }

    for path in ["reports.current.detail", "finance.tax"] {
        assert!(matches!(
            fixture
                .saved_functions
                .save(
                    &saved_function_change(principal.clone(), 30, 'd'),
                    &workspace_id,
                    SavedFunctionSave::new(
                        SavedFunctionPath::parse(path).expect("path"),
                        SavedFunctionDescription::parse("Conflicting callable")
                            .expect("description"),
                        SavedFunctionSource::parse("export default () => 0;").expect("source"),
                    ),
                )
                .await,
            Err(Error::SavedFunctionAdmission(message))
                if message.contains("virtual namespace")
        ));
    }
    assert!(matches!(
        fixture
            .saved_functions
            .get(
                &principal,
                &workspace_id,
                &SavedFunctionPath::parse("reports.current").expect("path"),
            )
            .await,
        Ok(function) if function.source().as_str() == "export default () => 42;"
    ));
}

#[tokio::test]
async fn a_compile_failure_keeps_its_cause_and_saves_nothing() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:compile-failures").await;
    let path = SavedFunctionPath::parse("compile.failure").expect("path");

    for (failure, kind, rejection) in [
        (
            CompileFailure::Rejected("scripted rejection".to_string()),
            "saved_function_admission",
            Some("saved_function_admission_failed"),
        ),
        (
            CompileFailure::Busy,
            "compiler_busy",
            Some("saved_function_compiler_busy"),
        ),
        (
            CompileFailure::TimedOut,
            "saved_function_compilation_timed_out",
            Some("saved_function_compilation_timed_out"),
        ),
        (CompileFailure::Stopped, "compiler_worker_stopped", None),
        (
            CompileFailure::Failed("engine"),
            "compiler_unavailable",
            None,
        ),
    ] {
        let module = SavedFunctionModule::with_compiler(
            fixture.database.clone(),
            fixture.workspaces.clone(),
            Arc::new(GatedSavedFunctionCompiler::failing(failure)),
        );
        let error = module
            .save(
                &saved_function_change(principal.clone(), 20, 'e'),
                &workspace_id,
                SavedFunctionSave::new(
                    path.clone(),
                    SavedFunctionDescription::parse("Fails to compile").expect("description"),
                    SavedFunctionSource::parse("export default () => 1;").expect("source"),
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
                .saved_functions
                .get(&principal, &workspace_id, &path)
                .await,
            Err(Error::SavedFunctionNotFound)
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_saves_serialize_virtual_namespace_conflicts() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:path-conflict-race").await;
    let saved_functions = SavedFunctionModule::with_compiler(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        Arc::new(GatedSavedFunctionCompiler::paired()),
    );

    let first_module = saved_functions.clone();
    let first_workspace = workspace_id.clone();
    let first_principal = principal.clone();
    let first = tokio::spawn(async move {
        first_module
            .save(
                &saved_function_change(first_principal, 20, 'e'),
                &first_workspace,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse("concurrent.path").expect("path"),
                    SavedFunctionDescription::parse("Concurrent parent").expect("description"),
                    SavedFunctionSource::parse("export default () => 1;").expect("source"),
                ),
            )
            .await
    });
    let second_module = saved_functions;
    let second = tokio::spawn(async move {
        second_module
            .save(
                &saved_function_change(principal, 21, 'f'),
                &workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse("concurrent.path.child").expect("path"),
                    SavedFunctionDescription::parse("Concurrent child").expect("description"),
                    SavedFunctionSource::parse("export default () => 2;").expect("source"),
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
            .filter(|result| matches!(result, Err(Error::SavedFunctionAdmission(message)) if message.contains("virtual namespace")))
            .count(),
        1
    );
}

#[tokio::test]
async fn saved_function_compilation_holds_no_write_transaction_and_rechecks_active_workspace() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:authoring-race").await;
    let (compiler, compiling, release) = GatedSavedFunctionCompiler::held();
    let blocked_functions = SavedFunctionModule::with_compiler(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        Arc::new(compiler),
    );
    let saving_module = blocked_functions.clone();
    let saving_workspace = workspace_id.clone();
    let saving_principal = principal.clone();
    let save = tokio::spawn(async move {
        saving_module
            .save(
                &saved_function_change(saving_principal, 30, 'a'),
                &saving_workspace,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse("races.compile").expect("path"),
                    SavedFunctionDescription::parse("Compile outside transaction")
                        .expect("description"),
                    SavedFunctionSource::parse("export default () => 42;").expect("source"),
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
        blocked_functions
            .get(
                &principal,
                &workspace_id,
                &SavedFunctionPath::parse("races.compile").expect("path"),
            )
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert!(
        blocked_functions
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list rejected authoring evidence")
            .is_empty()
    );
}

#[tokio::test]
async fn saved_function_authoring_keeps_semantic_advice_but_rejects_unresolved_public_types() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:typed-function-author").await;
    let path = SavedFunctionPath::parse("typed.advisory").expect("path");
    let advisory = fixture
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 20, '2'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Keep strict semantic advice")
                    .expect("description"),
                SavedFunctionSource::parse(
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
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 30, '3'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Unresolved public type").expect("description"),
                SavedFunctionSource::parse(
                    "export default function invalid(input: MissingPublicType): number { return 0; }",
                )
                .expect("source"),
            ),
        )
        .await;
    assert!(
        matches!(rejected, Err(Error::SavedFunctionAdmission(_))),
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
                .saved_functions
                .save(
                    &saved_function_change(principal.clone(), 31, '4'),
                    &workspace_id,
                    SavedFunctionSave::new(
                        path.clone(),
                        SavedFunctionDescription::parse("Invalid export shape")
                            .expect("description"),
                        SavedFunctionSource::parse(invalid_source).expect("source"),
                    ),
                )
                .await,
            Err(Error::SavedFunctionAdmission(_))
        ));
    }
    let unchanged = fixture
        .saved_functions
        .get(&principal, &workspace_id, &path)
        .await
        .expect("advisory definition remains current");
    assert_eq!(unchanged.id(), advisory.id());
    assert_eq!(unchanged.source(), advisory.source());
    assert_eq!(
        fixture
            .saved_functions
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("one accepted authoring Event")
            .len(),
        1
    );
}

#[tokio::test]
async fn saved_function_save_rejects_missing_and_computed_dependencies_atomically() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:dependency-author").await;
    let path = SavedFunctionPath::parse("reports.compose").expect("path");

    let missing = fixture
        .saved_functions
        .save(
            &saved_function_change(principal.clone(), 20, '5'),
            &workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Compose one report").expect("description"),
                SavedFunctionSource::parse(
                    "export default function compose() {\n  return coral.functions.missing.value();\n}",
                )
                .expect("source"),
            ),
        )
        .await;
    assert!(matches!(
        missing,
        Err(Error::SavedFunctionAdmission(message))
            if message.contains("coral.functions.reports.compose")
                && message.contains("coral.functions.missing.value")
    ));

    for source in [
        "const dependency = 'value'; export default function compose() { return coral.functions.reports[dependency](); }",
        "const dependency = 'value'; export default function compose() { return (coral.functions.reports as any)[dependency](); }",
        "const reports = coral.functions.reports; export default function compose() { return reports.value(); }",
        "function pick(root: any, key: string) { return root[key]; } export default function compose() { return pick(coral.functions, 'reports')(); }",
    ] {
        let rejected = fixture
            .saved_functions
            .save(
                &saved_function_change(principal.clone(), 20, '5'),
                &workspace_id,
                SavedFunctionSave::new(
                    path.clone(),
                    SavedFunctionDescription::parse("Compose one report").expect("description"),
                    SavedFunctionSource::parse(source).expect("source"),
                ),
            )
            .await;
        assert!(matches!(
            rejected,
            Err(Error::SavedFunctionAdmission(message))
                if message.contains("Saved Function coral.functions.reports.compose")
                    && (message.contains(" at 1:") || message.contains(":1:"))
        ));
    }

    assert!(matches!(
        fixture
            .saved_functions
            .get(&principal, &workspace_id, &path)
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert!(
        fixture
            .saved_functions
            .list_lifecycle_events(&principal, &workspace_id)
            .await
            .expect("list lifecycle evidence")
            .is_empty()
    );
}

#[tokio::test]
async fn accepted_saved_function_diagnostics_are_bounded() {
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:diagnostic-bound").await;
    let mut source = (0..=crate::SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT)
        .map(|index| format!("const invalid{index}: string = {index};"))
        .collect::<Vec<_>>()
        .join("\n");
    source.push_str("\nexport default function bounded(): number { return 42; }");

    let saved = fixture
        .saved_functions
        .save(
            &saved_function_change(principal, 20, 'c'),
            &workspace_id,
            SavedFunctionSave::new(
                SavedFunctionPath::parse("diagnostics.bounded").expect("path"),
                SavedFunctionDescription::parse("Bound compiler diagnostics").expect("description"),
                SavedFunctionSource::parse(&source).expect("source"),
            ),
        )
        .await
        .expect("save with bounded advisory diagnostics");

    assert_eq!(
        saved.diagnostics().len(),
        crate::SAVED_FUNCTION_DIAGNOSTIC_MAX_COUNT
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
    let fixture = SavedFunctionTestModules::new().await;
    let (workspace_id, principal) = fixture.user_workspace("user:stalled-typing").await;
    let readers: [(&str, &dyn crate::ToolDeclarationReader, Vec<u64>); 2] = [
        ("a reader that answers", &DeclaresReposGet, vec![2353]),
        ("a reader that never answers", &StalledReader, vec![]),
    ];
    for (name, reader, expected) in readers {
        let started = std::time::Instant::now();
        let prepared = fixture
            .saved_functions
            .prepare_save(
                &saved_function_change(principal.clone(), 20, 'd'),
                &workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse("typing.stalled").expect("path"),
                    SavedFunctionDescription::parse("Typed").expect("description"),
                    SavedFunctionSource::parse(
                        "export default async function f() { return coral.providers.github.repos.get({ owner: \"a\", nope: 1 }); }",
                    )
                    .expect("source"),
                ),
                Some(reader),
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: typing never fails a save: {error:?}"));
        assert!(
            started.elapsed() < coral_v8::COMPILATION_TIMEOUT,
            "{name}: {:?}",
            started.elapsed()
        );
        let mut transaction = fixture.database.begin().await.expect("begin save");
        let saved = fixture
            .saved_functions
            .save_in_transaction(&mut transaction, prepared)
            .await
            .expect("save");
        assert_eq!(
            saved
                .diagnostics()
                .iter()
                .map(crate::SavedFunctionDiagnostic::code)
                .collect::<Vec<_>>(),
            expected,
            "{name}"
        );
    }
}

#[tokio::test]
async fn one_users_functions_at_the_same_path_stay_scoped_to_each_workspace() {
    let fixture = SavedFunctionTestModules::new().await;
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
    let path = SavedFunctionPath::parse("reports.total").unwrap();
    let mut saved = Vec::new();
    for (workspace, source) in [
        (&first, "export default () => 1;"),
        (&second, "export default () => 2;"),
    ] {
        saved.push(
            fixture
                .saved_functions
                .save(
                    &saved_function_change(principal.clone(), 20, 'a'),
                    workspace,
                    SavedFunctionSave::new(
                        path.clone(),
                        SavedFunctionDescription::parse("Total").unwrap(),
                        SavedFunctionSource::parse(source).unwrap(),
                    ),
                )
                .await
                .unwrap(),
        );
    }
    let [in_first, in_second] = <[_; 2]>::try_from(saved).unwrap();
    assert_ne!(in_first.id(), in_second.id());
    let first_events = fixture
        .saved_functions
        .list_lifecycle_events(&principal, &first)
        .await
        .unwrap();
    let get = |workspace: &coral_workspace::WorkspaceId| {
        let (functions, principal, workspace, path) = (
            &fixture.saved_functions,
            &principal,
            workspace.clone(),
            &path,
        );
        async move { functions.get(principal, &workspace, path).await }
    };
    assert_eq!(get(&second).await.unwrap(), in_second);

    let context = saved_function_change(principal.clone(), 30, 'b');
    let delete = || fixture.saved_functions.delete(&context, &second, &path);
    assert_eq!(&delete().await.unwrap(), in_second.id());
    assert!(matches!(
        get(&second).await,
        Err(Error::SavedFunctionNotFound)
    ));
    // Deleting again finds nothing and never falls back to the first Workspace.
    assert!(matches!(delete().await, Err(Error::SavedFunctionNotFound)));
    assert_eq!(get(&first).await.unwrap(), in_first);
    assert_eq!(
        fixture
            .saved_functions
            .list_lifecycle_events(&principal, &first)
            .await
            .unwrap(),
        first_events
    );
}

/// Compiles every source to one fixed declaration once its gate opens.
struct GatedSavedFunctionCompiler {
    gate: Box<dyn Fn() -> Result<(), CompileFailure> + Send + Sync>,
}

impl GatedSavedFunctionCompiler {
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

impl SavedFunctionCompiler for GatedSavedFunctionCompiler {
    fn call_paths(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
    ) -> Option<coral_v8::SavedFunctionCallPaths> {
        None
    }

    fn prepare(
        &self,
        _deadline: std::time::Instant,
        _source: &str,
        _declarations: Option<&coral_v8::CoralDeclarations>,
    ) -> Result<PreparedSavedFunction, CompileFailure> {
        (self.gate)()?;
        Ok(PreparedSavedFunction {
            declaration: crate::SavedFunctionDeclaration::from_compiler(
                "declare const _default: () => number;\nexport default _default;\n",
            )
            .map_err(|error| error.to_string())?,
            declaration_field_projection: crate::SavedFunctionDeclarationFieldProjection::Ready(
                Vec::new(),
            ),
            diagnostics: Vec::new(),
            diagnostics_truncated: false,
            references: Vec::new(),
        })
    }
}

struct SavedFunctionTestModules {
    _fixture: TestDatabase,
    database: Arc<coral_db::Database>,
    workspaces: WorkspaceModule,
    saved_functions: SavedFunctionModule,
}

impl SavedFunctionTestModules {
    async fn new() -> Self {
        let fixture = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("create Saved Function test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let saved_functions = SavedFunctionModule::new(
            database.clone(),
            workspaces.clone(),
            crate::IsolateLimits::default(),
        )
        .expect("start Saved Function compiler");
        Self {
            _fixture: fixture,
            database,
            workspaces,
            saved_functions,
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
                WorkspaceDisplayName::parse("Saved Functions").expect("display name"),
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

fn saved_function_change(
    actor: AuthenticatedPrincipal,
    occurred_at_unix_nanos: i64,
    digit: char,
) -> SavedFunctionChangeContext {
    SavedFunctionChangeContext::new(
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
        let fixture = SavedFunctionTestModules::new().await;
        let (workspace_id, owner) = fixture.user_workspace("user:owner").await;
        let agent = registered_agent(&fixture.workspaces, &owner, &workspace_id).await;
        let path = SavedFunctionPath::parse("private.answer").unwrap();
        let save = SavedFunctionSave::new(
            path.clone(),
            SavedFunctionDescription::parse("Answer").unwrap(),
            SavedFunctionSource::parse("export default () => 42;").unwrap(),
        );
        let original = fixture
            .saved_functions
            .save(
                &saved_function_change(agent.clone(), 20, '1'),
                &workspace_id,
                save.clone(),
            )
            .await
            .unwrap();
        let (compiler, compiling, release) = GatedSavedFunctionCompiler::held();
        let module = SavedFunctionModule::with_compiler(
            fixture.database.clone(),
            fixture.workspaces.clone(),
            Arc::new(compiler),
        );
        let caller = agent.clone();
        let target = workspace_id.clone();
        let saving = tokio::spawn(async move {
            module
                .save(&saved_function_change(caller, 30, '2'), &target, save)
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
                .saved_functions
                .get_agent(&owner, &workspace_id, agent.principal().id(), &path)
                .await
                .unwrap(),
            original
        );
        assert!(denied(
            fixture
                .saved_functions
                .delete(&saved_function_change(agent, 40, '4'), &workspace_id, &path)
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
async fn list_pages_only_the_callers_own_functions_in_one_workspace() {
    let fixture = SavedFunctionTestModules::new().await;
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
        let (functions, actor, workspace) =
            (&fixture.saved_functions, actor.clone(), workspace.clone());
        async move {
            functions
                .save(
                    &saved_function_change(actor, 20, 'c'),
                    &workspace,
                    SavedFunctionSave::new(
                        SavedFunctionPath::parse(path).expect("path"),
                        SavedFunctionDescription::parse(&format!("Describe {path}"))
                            .expect("description"),
                        SavedFunctionSource::parse("export default () => 1;").expect("source"),
                    ),
                )
                .await
                .expect("save function")
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
        crate::SavedFunctionInspection::new(fixture.database.clone(), fixture.workspaces.clone());
    let list = |actor: &AuthenticatedPrincipal, after: Option<&str>, page_size| {
        let (functions, actor, workspace) = (&inspection, actor.clone(), workspace_id.clone());
        let after = after.map(|path| SavedFunctionPath::parse(path).expect("after path"));
        async move {
            functions
                .list(&actor, &workspace, after.as_ref(), page_size)
                .await
        }
    };
    let paths = |page: &crate::SavedFunctionPage| {
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
    // A page that ends exactly at the last function has no continuation.
    assert_eq!(paths(&second), ["reports.total", "zeta.last"]);
    assert_eq!(second.next_after(), None);

    // An Owner sees only their own functions: never a Member's or an Agent's.
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
    for page_size in [0, crate::SAVED_FUNCTION_PAGE_MAX_SIZE + 1] {
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
        let entries = crate::saved_function_entries_in_transaction(&mut transaction, &workspace_id)
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
            .saved_functions
            .get(
                &member,
                &workspace_id,
                &SavedFunctionPath::parse("alpha.first").expect("path"),
            )
            .await,
        Err(Error::SavedFunctionNotFound)
    ));
    assert_eq!(entries().await, before);
}
