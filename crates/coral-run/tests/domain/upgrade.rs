use super::*;

/// A database from before Operations were Provider Tools: the upgrade renames
/// the Coral failure a Run and its Invocation retained, and the error class a
/// Saved Function catches, so each reads the way new ones do.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one upgrade covers what a Run, its Invocation and a Saved Function stored"
)]
async fn upgrade_renames_the_tool_failures_runs_and_saved_functions_stored() {
    let fixture = TestModules::without_migration(46, Arc::new(ImmediateEngine::default())).await;
    let saved_functions = SavedFunctionModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
    )
    .expect("start Saved Function compiler");
    let runs = fixture.live_runs(None);
    let tool = fixture
        .user_tool("user:upgrade", "Upgrade stored tool names")
        .await;
    saved_functions
        .save(
            &saved_function_change(tool.principal.clone(), 20, '2'),
            &tool.workspace_id,
            SavedFunctionSave::new(
                SavedFunctionPath::parse("upgrade.code").expect("path"),
                SavedFunctionDescription::parse("Return the forecast's failure code")
                    .expect("description"),
                SavedFunctionSource::parse(
                    "export default async function code(): Promise<string> { try { await coral.providers.weather.getForecast({}); return 'ok'; } catch (error) { return error instanceof coral.ToolError ? error.code : 'other'; } }",
                )
                .expect("source"),
            ),
        )
        .await
        .expect("save a function that catches a Tool Error");
    fixture
        .policies
        .set_access_state(
            &change(tool.user.clone(), 30, '3'),
            tool.policy_target.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable the forecast");
    let failed = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("await coral.providers.weather.getForecast({});"),
        )
        .await
        .expect("accept a call to the disabled Tool");
    wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, failed.run_id()).await;
    // What Coral stored before the rename.
    for downgrade in [
        "UPDATE program_run_records \
         SET failure_message = REPLACE(failure_message, \
         'ToolError: Tool ', 'OperationError: Operation '), \
         failure_error_json = REPLACE(REPLACE(REPLACE(REPLACE(failure_error_json, \
         '\"name\":\"ToolError\"', '\"name\":\"OperationError\"'), \
         '\"message\":\"Tool ', '\"message\":\"Operation '), \
         '\"tool\":{', '\"operation\":{'), \
         '\"tool_disabled\"', '\"operation_disabled\"') \
         WHERE failure_error_json IS NOT NULL",
        "UPDATE tool_invocations \
         SET failure_code = 'operation_disabled', \
         failure_message = REPLACE(failure_message, 'Tool ', 'Operation ') \
         WHERE failure_code = 'tool_disabled'",
        "UPDATE saved_function_records \
         SET source = REPLACE(source, 'coral.ToolError', 'coral.OperationError')",
    ] {
        fixture
            .test_database
            .execute_fixture_sql(downgrade)
            .await
            .expect("write the earlier names");
    }

    fixture
        .database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let terminal =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, failed.run_id()).await;
    let Some(RunTerminalOutcome::Failed(failure)) = terminal.terminal_outcome() else {
        panic!("the call to a disabled Tool fails its Run: {terminal:?}");
    };
    assert_eq!(failure.message(), "ToolError: Tool is disabled");
    let error = failure.error().expect("retained Program error");
    assert_eq!(
        (error.name.as_str(), error.message.as_str()),
        ("ToolError", "Tool is disabled")
    );
    assert_eq!(
        error.tool.as_ref().map(|tool| tool.code.as_str()),
        Some("tool_disabled")
    );
    let [invocation] = fixture
        .invocation_rows(&tool.workspace_id, failed.run_id())
        .await
        .try_into()
        .expect("one Invocation");
    let detail = runs
        .inspect_invocation(&tool.principal, &tool.workspace_id, &invocation.id)
        .await
        .expect("inspect the Invocation");
    assert!(
        matches!(
            &detail.outcome,
            Some(crate::InvocationOutcome::CoralFailure { code, message })
                if code == "tool_disabled" && message == "Tool is disabled"
        ),
        "{:?}",
        detail.outcome
    );
    let caught = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("return await coral.functions.upgrade.code();"),
        )
        .await
        .expect("accept a call to the upgraded Saved Function");
    let caught =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, caught.run_id()).await;
    assert!(
        matches!(
            caught.terminal_outcome(),
            Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) })
                if value.as_json() == &serde_json::json!("tool_disabled")
        ),
        "{caught:?}"
    );
}
