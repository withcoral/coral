//! Authored source locations recorded by real V8 execution and read back.
use super::*;

use crate::{RunEntry, SourceDocument, SourceLocation};

#[expect(
    clippy::unnecessary_wraps,
    reason = "rows compare the optional location each record carries"
)]
fn program_at(line: u32, column: u32) -> Option<SourceLocation> {
    Some(SourceLocation {
        document: SourceDocument::Program,
        line,
        column,
    })
}

const ACCEPTED_SAVED_FUNCTION: &str = "export default async function forecast(city: string) {\n    console.warn(city);\n    return coral.providers.weather.getForecast({ city });\n}";

fn saved_functions(fixture: &TestModules) -> SavedFunctionModule {
    SavedFunctionModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
    )
    .expect("start Saved Function compiler")
}

/// Runs one real-V8 Program that calls a linked Saved Function, emits in a
/// loop and fans out Tool calls, and waits for it to succeed.
async fn run_located_program() -> (TestModules, ToolFixture, crate::RunId) {
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        Arc::new(ScriptedUpstream::json(200, serde_json::json!(null))),
    )
    .await;
    let tool = fixture
        .user_tool("user:locations", "Record authored locations")
        .await;
    saved_functions(&fixture)
        .save(
            &saved_function_change(tool.principal.clone(), 30, 'a'),
            &tool.workspace_id,
            SavedFunctionSave::new(
                SavedFunctionPath::parse("weather.forecast").expect("path"),
                SavedFunctionDescription::parse("Forecast one city").expect("description"),
                SavedFunctionSource::parse(ACCEPTED_SAVED_FUNCTION).expect("source"),
            ),
        )
        .await
        .expect("save Saved Function");

    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                "const cities: string[] = [\"Paris\", \"Rome\"];\nfor (const city of cities) {\n    await coral.emit({ city });\n}\nawait coral.functions.weather.forecast(\"Oslo\");\nawait Promise.all(cities.map((city) => coral.providers.weather.getForecast({ city })));",
            ),
        )
        .await
        .expect("accepted");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(
        matches!(
            terminal.terminal_outcome(),
            Some(crate::RunTerminalOutcome::Succeeded { .. })
        ),
        "{:?}",
        terminal.terminal_outcome()
    );
    let run_id = accepted.run_id().clone();
    (fixture, tool, run_id)
}

/// Each record's kind and location, in the order every read path returns it.
struct ReadLocations {
    events: Vec<(&'static str, Option<SourceLocation>)>,
    rows: Vec<(&'static str, Option<SourceLocation>)>,
    snapshot_id: String,
    snapshot_source: String,
}

async fn read_locations(
    fixture: &TestModules,
    tool: &ToolFixture,
    run_id: &crate::RunId,
) -> ReadLocations {
    let detail = fixture
        .runs
        .inspect_run(
            &tool.principal,
            &tool.workspace_id,
            run_id,
            &crate::RunInspection {
                event_limit: 100,
                ..Default::default()
            },
        )
        .await
        .expect("inspect Run");
    let [snapshot] = detail.linked_functions.as_slice() else {
        panic!("one linked snapshot expected")
    };
    let events = detail
        .events
        .iter()
        .filter_map(|event| {
            let kind = match event.kind() {
                RunEventKind::ProgramEmitted { .. } => "emitted",
                RunEventKind::ProgramLogged { .. } => "logged",
                // Other Events never carry a location.
                _ => {
                    assert_eq!(event.source_location(), None, "{event:?}");
                    return None;
                }
            };
            Some((kind, event.source_location().cloned()))
        })
        .collect();
    let entries = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            run_id,
            &crate::RunEntryWindow::Start,
            100,
            None,
        )
        .await
        .expect("activity entries");
    let mut rows = Vec::new();
    for entry in &entries.entries {
        match entry {
            RunEntry::Invocation(summary) => {
                // Exact Invocation evidence agrees with its row.
                let exact = fixture
                    .runs
                    .inspect_invocation(&tool.principal, &tool.workspace_id, &summary.id)
                    .await
                    .expect("inspect Invocation");
                assert_eq!(exact.summary.source_location, summary.source_location);
                rows.push(("invocation", summary.source_location.clone()));
            }
            RunEntry::Event {
                kind,
                source_location,
                ..
            } => {
                if kind.starts_with("program.") {
                    rows.push(("output", source_location.clone()));
                } else {
                    assert_eq!(source_location, &None, "{kind}");
                }
            }
        }
    }
    ReadLocations {
        events,
        rows,
        snapshot_id: snapshot.saved_function_id.clone(),
        snapshot_source: snapshot.source.clone(),
    }
}

#[tokio::test]
async fn each_record_keeps_the_authored_location_of_its_own_call() {
    let (fixture, tool, run_id) = run_located_program().await;
    // Replacing and then deleting the current Saved Function cannot move a
    // recorded location off the Run's snapshot.
    let functions = saved_functions(&fixture);
    let path = SavedFunctionPath::parse("weather.forecast").expect("path");
    functions
        .save(
            &saved_function_change(tool.principal.clone(), 40, 'b'),
            &tool.workspace_id,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Replaced").expect("description"),
                SavedFunctionSource::parse("\n\nexport default () => console.info('replaced');")
                    .expect("source"),
            ),
        )
        .await
        .expect("replace Saved Function");
    functions
        .delete(
            &saved_function_change(tool.principal.clone(), 41, 'c'),
            &tool.workspace_id,
            &path,
        )
        .await
        .expect("delete Saved Function");
    let read = read_locations(&fixture, &tool, &run_id).await;
    assert_eq!(read.snapshot_source, ACCEPTED_SAVED_FUNCTION);
    let in_snapshot = |line, column| {
        Some(SourceLocation {
            document: SourceDocument::SavedFunction {
                saved_function_id: read.snapshot_id.clone(),
            },
            line,
            column,
        })
    };
    assert_eq!(
        read.events,
        vec![
            ("emitted", program_at(3, 17)),
            ("emitted", program_at(3, 17)),
            ("logged", in_snapshot(2, 13)),
        ]
    );
    // Activity rows carry the same locations, Tool calls in admission order.
    assert_eq!(
        read.rows,
        vec![
            ("output", program_at(3, 17)),
            ("output", program_at(3, 17)),
            ("output", in_snapshot(2, 13)),
            ("invocation", in_snapshot(3, 36)),
            ("invocation", program_at(6, 64)),
            ("invocation", program_at(6, 64)),
        ]
    );
}

#[tokio::test]
async fn records_without_a_recorded_location_read_back_as_unavailable() {
    let (fixture, tool, run_id) = run_located_program().await;
    let recorded = read_locations(&fixture, &tool, &run_id).await;
    // Records written before Coral captured locations keep NULL columns.
    for table in ["tool_invocations", "program_run_events"] {
        fixture
            .inject_fault(&format!(
                "UPDATE {table} SET source_document = NULL, source_saved_function_id = NULL, \
                 source_line = NULL, source_column = NULL WHERE run_id = '{run_id}'"
            ))
            .await;
    }
    let read = read_locations(&fixture, &tool, &run_id).await;
    let unavailable = |records: &[(&'static str, Option<SourceLocation>)]| {
        records
            .iter()
            .map(|(kind, _)| (*kind, None))
            .collect::<Vec<_>>()
    };
    assert_eq!(read.events, unavailable(&recorded.events));
    assert_eq!(read.rows, unavailable(&recorded.rows));
}

#[tokio::test]
async fn storage_rejects_an_incomplete_or_misplaced_location() {
    let (fixture, tool, run_id) = run_located_program().await;
    let before = read_locations(&fixture, &tool, &run_id).await;
    for statement in [
        // A document without a position.
        format!("UPDATE tool_invocations SET source_line = NULL WHERE run_id = '{run_id}'"),
        // A position without a document.
        format!(
            "UPDATE program_run_events SET source_document = NULL \
             WHERE run_id = '{run_id}' AND event_type = 'program.emitted'"
        ),
        // A Saved Function location without its snapshot identity.
        format!(
            "UPDATE tool_invocations SET source_saved_function_id = NULL \
             WHERE run_id = '{run_id}' AND source_document = 'saved_function'"
        ),
        // Lifecycle Events never carry a location.
        format!(
            "UPDATE program_run_events SET source_document = 'program', source_line = 1, \
             source_column = 1 WHERE run_id = '{run_id}' AND event_type = 'run.started'"
        ),
    ] {
        let error = fixture
            .test_database
            .execute_fixture_sql(&statement)
            .await
            .expect_err(&statement)
            .to_string();
        assert!(
            error.to_lowercase().contains("check constraint"),
            "{statement}: {error}"
        );
    }
    let after = read_locations(&fixture, &tool, &run_id).await;
    assert_eq!(after.events, before.events);
    assert_eq!(after.rows, before.rows);
}
