use coral_db::UnixNanos;
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_provider::ProviderModule;
use coral_run::{CoralInstanceId, RuntimeLimits};
use coral_search::UniversalSearchModule;
use coral_task::TaskModule;
use coral_test_support::{TEST_COMPILATION_TIMEOUT, TestDatabase, change, provision_user};
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceModule};
use serde_json::{Value, json};

use super::*;

const POLICY: DestinationPolicy =
    DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny);

/// The Sub-agent's tools over the real modules, and a lead's caller in a
/// Task of its own.
struct Harness {
    /// Serves the Runs on a Run Executor in this process.
    _serving: coral_run::testing::Serving,
    _fixture: TestDatabase,

    tasks: TaskModule,
    tools: RunTools,
    caller: Caller,
}

impl Harness {
    async fn new() -> Self {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = fixture.database();
        let user = provision_user(database.clone(), "lead", OrganizationRole::Owner).await;
        let workspaces = WorkspaceModule::new(database.clone());
        let name = WorkspaceDisplayName::parse("Lead").unwrap();
        let created = change(user.clone(), 1, 'a');
        let workspace = workspaces.create(&created, name).await.unwrap();
        let workspace_id = workspace.id().clone();
        let providers =
            ProviderModule::with_running_catalog(POLICY, database.clone(), workspaces.clone());
        let runs = RunModule::new(
            POLICY,
            database.clone(),
            workspaces.clone(),
            providers,
            RuntimeLimits::default().with_compilation_timeout(TEST_COMPILATION_TIMEOUT),
            CoralInstanceId::generate(),
        )
        .unwrap();
        let (runs, serving) = coral_run::testing::serve_on_run_executor(runs).await;
        let tasks = TaskModule::new(database.clone(), workspaces.clone());
        let search = UniversalSearchModule::new(database.clone(), workspaces);
        let commands = TaskCommands::new(tasks.clone(), search, runs.clone());
        let principal = AuthenticatedPrincipal::user(user);
        let mut harness = Self {
            _fixture: fixture,
            _serving: serving,
            tasks,
            tools: RunTools::new(database, commands, runs),
            caller: Caller {
                principal,
                workspace_id,
                task_id: TaskId::parse("placeholder-task-word-id").unwrap(),
                started_runs: std::sync::Mutex::default(),
            },
        };
        harness.caller.task_id = harness.task().await;
        harness
    }

    /// A new Task of the lead's.
    async fn task(&self) -> TaskId {
        let goal = TaskIntent::parse("Count the birds").unwrap();
        let caller = &self.caller;
        let started = self.tasks.start(
            &caller.principal,
            &caller.workspace_id,
            goal,
            UnixNanos::new(2),
        );
        started.await.unwrap().id().clone()
    }

    async fn call(&self, name: &str, arguments: &Value) -> String {
        let arguments = arguments.to_string();
        self.tools
            .call(&self.caller, name, &arguments)
            .await
            .content
    }

    /// How many attempts `task_id` has recorded: one for each `exec` that
    /// reached the Task.
    async fn attempts(&self, task_id: &TaskId) -> usize {
        let caller = &self.caller;
        let attempts =
            self.tasks
                .interactions(&caller.principal, &caller.workspace_id, task_id, None, 10);
        attempts.await.unwrap().len()
    }

    fn exec(&self, program: &str) -> Value {
        let task_id = self.caller.task_id.as_str();
        json!({"task_id": task_id, "intent": "Read the birds", "program": program})
    }
}

// Catches: a result whose field names differ from those of Coral's MCP
// tools, or an `exec` that does not wait for the Run it starts.
#[tokio::test]
async fn exec_returns_the_final_value_under_the_mcp_field_names() {
    let harness = Harness::new().await;
    let program = "return ['pelican', 'heron'].join(' and ');";

    let text = harness.call("exec", &harness.exec(program)).await;

    let result: Value = serde_json::from_str(&text).unwrap();
    let mut fields: Vec<&str> = result
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort_unstable();
    let expected = [
        "events",
        "final_value",
        "has_more",
        "next_cursor",
        "required_actions",
        "run_id",
        "state",
    ];
    assert_eq!(fields, expected, "{text}");
    assert_eq!(result.get("state"), Some(&json!("succeeded")), "{text}");
    let final_value = json!("pelican and heron");
    assert_eq!(result.get("final_value"), Some(&final_value), "{text}");
    assert_eq!(result.get("has_more"), Some(&json!(false)), "{text}");
}

// Catches: an `exec` that starts its Run before it settles the wait, so a
// refused wait leaves a Run that the model never hears of.
#[tokio::test]
async fn a_refused_wait_starts_no_run() {
    let harness = Harness::new().await;
    let mut arguments = harness.exec("return 1;");
    let fields = arguments.as_object_mut().unwrap();
    fields.insert("wait_ms".to_owned(), json!(MAX_WAIT_MS + 1));

    let text = harness.call("exec", &arguments).await;

    let message = format!("wait_ms must be at most {MAX_WAIT_MS}.");
    let refused = json!({"error": {"code": "invalid_arguments", "message": message}});
    assert_eq!(text, refused.to_string());
    assert_eq!(harness.attempts(&harness.caller.task_id).await, 0);
}

// Catches: a Sub-agent that runs a Program in a Task other than the lead's.
#[tokio::test]
async fn a_task_other_than_the_leads_is_refused_and_starts_nothing() {
    let harness = Harness::new().await;
    let other = harness.task().await;
    let mut arguments = harness.exec("return 1;");
    let fields = arguments.as_object_mut().unwrap();
    fields.insert("task_id".to_owned(), json!(other.as_str()));

    let text = harness.call("exec", &arguments).await;

    let lead = harness.caller.task_id.as_str();
    let message = format!("Nothing ran. Every exec runs in your Task: pass task_id \"{lead}\".");
    let refused = json!({"error": {"code": "wrong_task", "message": message}});
    assert_eq!(text, refused.to_string());
    assert_eq!(harness.attempts(&other).await, 0);
    assert_eq!(harness.attempts(&harness.caller.task_id).await, 0);
}

// Catches: a long result cut inside a character, or cut without saying how
// much is gone.
#[test]
fn a_result_over_20_kib_keeps_its_first_and_last_10_kib() {
    // One byte, then two-byte characters: byte 10240 falls inside one.
    let text = format!("a{}", "é".repeat(15_000));
    let whole = "a".repeat(RESULT_MAX_BYTES);

    let cut = shortened(text);

    let head = format!("a{}", "é".repeat(5_119));
    let tail = "é".repeat(5_120);
    let marker = "\n[... 9522 bytes cut from the middle of this result ...]\n";
    assert_eq!(cut, format!("{head}{marker}{tail}"));
    assert_eq!(shortened(whole.clone()), whole);
}

// Catches: a failure that hides text core marks safe, such as a Program's
// diagnostics, or one that shows an internal detail.
#[tokio::test]
async fn failures_show_only_what_is_safe() {
    let harness = Harness::new().await;
    let internal = coral_run::Error::CorruptData("Run has unrecognized state 'bogus'".into());

    let rows = [
        (
            "unknown tool",
            harness.call("find", &json!({})).await,
            json!({"error": {"code": "unknown_tool", "message": "There is no tool named \"find\". Call exec, wait, cancel or search."}}),
        ),
        (
            "Program that does not compile",
            harness.call("exec", &harness.exec("return 1 +;")).await,
            json!({"error": {"code": "program_admission_failed", "message": "Program admission failed: TypeScript 1109 at 1:11: Expression expected."}}),
        ),
        (
            "internal error",
            run_failure(internal).text(),
            json!({"error": {"code": "internal_error", "message": "Coral could not complete the tool call."}}),
        ),
    ];

    for (case, text, expected) in rows {
        assert_eq!(text, expected.to_string(), "{case}");
    }
}
