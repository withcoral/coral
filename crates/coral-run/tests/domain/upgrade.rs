use super::*;

/// A database from before the Built-in Tools moved onto `coral` and Private
/// Tools to `coral.private`: the upgrade moves the Built-in Provider's
/// Call Root and every indexed Call Path to the new roots, so search returns
/// each callable where a Program calls it.
#[tokio::test]
async fn upgrade_moves_the_builtin_tools_and_private_tools_to_the_new_roots() {
    let fixture = TestModules::without_migration(
        60,
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
    )
    .await;
    let (workspace_id, owner) = builtin_tools_a_rule_and_a_private_tool(&fixture).await;
    // What Coral stored before the move.
    for downgrade in [
        "UPDATE provider_records SET namespace = 'coral.workspace' WHERE namespace = 'coral'",
        "UPDATE search_entities SET call_path = 'coral.workspace.' || substr(call_path, 7) \
         WHERE call_path NOT LIKE 'coral.providers.%' AND call_path NOT LIKE 'coral.private.%'",
        "UPDATE search_entities SET call_path = 'coral.functions.' || substr(call_path, 15) \
         WHERE call_path LIKE 'coral.private.%'",
    ] {
        fixture
            .test_database
            .execute_fixture_sql(downgrade)
            .await
            .expect("write the earlier roots");
    }

    fixture
        .database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let run = fixture
        .runs
        .start(
            &owner,
            &workspace_id,
            submission(
                "const paths = async (query: string) => \
                 (await coral.search({ query })).data.hits.map((hit: any) => hit.call_path);\n\
                 return { \
                 functions: await paths('greet person'), \
                 tools: await paths('list providers'), \
                 greeting: await coral.private.people.greet('Ada'), \
                 disabled: await coral.getProvider({ namespace: 'absent' }).then( \
                 () => 'called', (error) => error.code) };",
            ),
        )
        .await
        .expect("accept a Program on the new roots");
    let terminal = wait_for_terminal(&fixture.runs, &owner, &workspace_id, run.run_id()).await;
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = terminal.terminal_outcome()
    else {
        panic!("the Program succeeds: {terminal:?}");
    };
    let value = value.as_json();
    let paths = |list: &str| -> Vec<&str> {
        value[list]
            .as_array()
            .unwrap_or_else(|| panic!("{list} in {value}"))
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect()
    };
    assert!(
        paths("functions").contains(&"coral.private.people.greet"),
        "{value}"
    );
    assert!(paths("tools").contains(&"coral.listProviders"), "{value}");
    assert!(
        ["functions", "tools"]
            .iter()
            .flat_map(|list| paths(list))
            .all(|path| {
                !path.starts_with("coral.workspace.") && !path.starts_with("coral.functions.")
            }),
        "{value}"
    );
    assert_eq!(value["greeting"], serde_json::json!("hello Ada"));
    assert_eq!(value["disabled"], serde_json::json!("tool_disabled"));
}

/// A Workspace with its Built-in Provider, an Owner's rule that disables
/// `getProvider`, and one Private Tool, `people.greet`, all indexed, and its
/// Owner.
async fn builtin_tools_a_rule_and_a_private_tool(
    fixture: &TestModules,
) -> (coral_workspace::WorkspaceId, AuthenticatedPrincipal) {
    let (workspace_id, owner) = fixture
        .user_scope("user:upgrade-roots", "Find tools at their new roots")
        .await;
    let mut transaction = fixture.database.begin().await.unwrap();
    fixture
        .providers
        .install_builtin_in_transaction(
            &mut transaction,
            &change(owner.authenticated_user().unwrap().clone(), 11, '2'),
            &workspace_id,
        )
        .await
        .expect("install the Built-in Provider")
        .skip_search();
    transaction.commit().await.unwrap();
    PrivateToolModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler")
    .save(
        &private_tool_change(owner.clone(), 20, '2'),
        &workspace_id,
        PrivateToolSave::new(
            PrivateToolPath::parse("people.greet").expect("path"),
            PrivateToolDescription::parse("Greet one person by name.").expect("description"),
            PrivateToolSource::parse(
                "export default function greet(name: string): string { return `hello ${name}`; }",
            )
            .expect("source"),
        ),
    )
    .await
    .expect("save a function");
    coral_search::SearchIndex::new(fixture.database.clone(), Duration::from_mins(5))
        .reconcile()
        .await;
    // An Owner's rule keys a Built-in Tool by its Provider and Tool Key,
    // which the move leaves as they were.
    let mut transaction = fixture.database.begin().await.unwrap();
    let get_provider = coral_provider::resolve_invocation_tool_in_transaction(
        &mut transaction,
        &workspace_id,
        coral_provider::InvocationCaller::new(&fixture.workspaces, &owner, 12),
        "coral.getProvider",
    )
    .await
    .unwrap()
    .expect("the Built-in Tool resolves");
    transaction.rollback().await.unwrap();
    fixture
        .policies
        .set_access_state(
            &change(owner.authenticated_user().unwrap().clone(), 30, '3'),
            ToolPolicyTarget::new(
                workspace_id.clone(),
                get_provider.provider_access().id().clone(),
                get_provider.tool().key().clone(),
            ),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable getProvider");
    (workspace_id, owner)
}
