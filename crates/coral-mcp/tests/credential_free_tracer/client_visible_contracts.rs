//! What a content-only client sees, and what an Agent can reach, over raw
//! Streamable HTTP on every served protocol version.
#![expect(
    clippy::indexing_slicing,
    reason = "assert known tool result JSON members"
)]

use coral_catalog::ToolKey;
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_run::{ProgramSource, RunObservation, RunState, RunSubmission, RunTimeout};
use coral_workspace::{AgentDisplayName, AuthenticatedPrincipal, OrganizationRole, WorkspaceRole};
use serde_json::{Value, json};

use super::{RawSession, Tracer, change, untraced};
use coral_mcp::SUPPORTED_PROTOCOL_VERSIONS;
use rmcp::model::ProtocolVersion;

const PARKED: &str = "return await coral.providers.weather.getForecast({});";
const ABSENT_RUN: &str = "00000000-0000-4000-8000-000000000000";

/// A raw failed tool result without its per-call `trace_id`, to compare two
/// calls. Its `content[0]` text is checked on receipt to be the JSON of
/// `structuredContent`, so the structured result stands for both.
fn untraced_result(result: &Value) -> Value {
    json!({
        "isError": result["isError"],
        "content": result["content"].as_array().map(Vec::len),
        "structuredContent": untraced(&result["structuredContent"]),
    })
}

/// The only part of a tool result that a content-only client reads.
fn text_json(result: &Value) -> Value {
    let text = result
        .pointer("/content/0/text")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("content[0] is text: {result}"));
    serde_json::from_str(text).expect("content[0] is JSON")
}

/// A content-only client continues attributed work from the JSON text, and
/// sees why Coral rejected a call instead of a generic error.
#[tokio::test(flavor = "multi_thread")]
async fn content_only_clients_read_results_and_actionable_rejections() {
    let tracer = Tracer::start().await;
    tracer
        .policies
        .set_access_state(
            &change(tracer.local_user.clone(), 60, 'b'),
            ToolPolicyTarget::new(
                tracer.workspace_a.clone(),
                tracer.provider_id.clone(),
                ToolKey::parse("lostWorker").expect("Tool Key"),
            ),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable an installed Tool");
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        let started = session
            .tool("start_task", json!({"intent":"Read results as text"}))
            .await;
        let task = text_json(&started)["task_id"].clone();
        let searched = session
            .tool(
                "search",
                json!({"task_id":task, "intent":"Continue from the text"}),
            )
            .await;
        assert_eq!(searched["isError"], false, "{version}: {searched}");
        // The alias rule applies once the aliased Saved Function exists.
        let saved = session
            .tool(
                "save_function",
                json!({
                    "task_id":task, "intent":"Save a function to alias",
                    "path":"linear.inbox", "description":"Count inbox items",
                    "source":"export default function inbox() { return { total: 0 }; }"
                }),
            )
            .await;
        assert_eq!(saved["isError"], false, "{version}: {saved}");

        for (program, code, message) in [
            (
                "return typeof fetch;",
                "program_admission_failed",
                "ambient fetch are not available at 1:15",
            ),
            (
                "return typeof setTimeout;",
                "program_admission_failed",
                "ambient setTimeout are not available at 1:15",
            ),
            (
                "return typeof coral;",
                "program_admission_failed",
                "Program:1:15: Coral namespace aliases are not supported",
            ),
            (
                "const f = coral.functions;\nreturn (await f.linear.inbox()).total;",
                "program_admission_failed",
                "Program:1:11: Saved Function namespace aliases are not supported",
            ),
            // The Program from the 2026-09-28 report. Admission owns the
            // wording; the text must carry the code and authored location.
            (
                "const f = coral.functions.linear;\nreturn (await f.inbox()).total;",
                "program_admission_failed",
                "Program:1:11",
            ),
        ] {
            for wait_ms in [0, 1000] {
                let rejected = session
                    .tool(
                        "exec",
                        json!({"task_id":task, "intent":"Break an authoring rule", "wait_ms":wait_ms, "program":program}),
                    )
                    .await;
                assert_eq!(rejected["isError"], true, "{version}: {program}");
                let visible = text_json(&rejected);
                assert_eq!(visible["code"], code, "{version}: {program}");
                assert!(
                    visible["message"]
                        .as_str()
                        .is_some_and(|text| text.contains(message)),
                    "{version}: {program}: {visible}"
                );
            }
        }

        check_concealed_descriptions(&session, &task, version).await;
    }
}

/// An Agent reaches only Runs it invoked. Another Principal's Run, including
/// a Workspace Owner's view of a member's private Run, looks absent and is
/// left unchanged.
#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one Workspace holds every Principal pairing the denial table compares"
)]
async fn agents_observe_and_cancel_only_their_own_runs() {
    let tracer = Tracer::start().await;
    tracer.require_approval().await;
    let other_agent = tracer
        .workspaces
        .create_agent(
            &change(tracer.local_user.clone(), 61, 'c'),
            &tracer.workspace_a,
            AgentDisplayName::parse("Other Agent").expect("Agent name"),
            None,
        )
        .await
        .expect("register another Agent");
    let member = coral_test_support::provision_user(
        tracer.database.database(),
        "user:tracer-member",
        OrganizationRole::Member,
    )
    .await;
    tracer
        .workspaces
        .set_membership(
            &change(tracer.local_user.clone(), 62, 'd'),
            &tracer.workspace_a,
            &member,
            WorkspaceRole::Member,
        )
        .await
        .expect("add a Workspace member");
    let member = AuthenticatedPrincipal::user(member);
    let private = tracer
        .runs
        .start(
            &member,
            &tracer.workspace_a,
            RunSubmission::new(
                ProgramSource::parse(PARKED).expect("Program"),
                RunTimeout::from_millis(60_000).expect("timeout"),
            ),
        )
        .await
        .expect("the member starts a private Run");
    let private = private.run_id().clone();
    let member_view = || async {
        tracer
            .runs
            .observe(
                &member,
                &tracer.workspace_a,
                &private,
                &RunObservation::immediate(None, 100).expect("observation"),
            )
            .await
            .expect("the member observes their Run")
            .state()
    };

    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let endpoint = tracer.endpoint();
        let owner = RawSession::open(&endpoint, version).await;
        let agent = RawSession::open_as(&endpoint, version, Some(&tracer.agent_credential)).await;
        let other =
            RawSession::open_as(&endpoint, version, Some(other_agent.credential.expose())).await;
        let owner_run = parked(&owner).await;
        let other_run = parked(&other).await;
        let own_run = parked(&agent).await;

        for tool in ["wait", "cancel"] {
            for (case, session, run) in [
                ("an Agent targets a User's Run", &agent, &owner_run),
                ("an Agent targets another Agent's Run", &agent, &other_run),
                (
                    "an Owner targets a member's private Run",
                    &owner,
                    &json!(private.to_string()),
                ),
            ] {
                let absent = run_tool(session, tool, &json!(ABSENT_RUN)).await;
                assert_eq!(absent["isError"], true, "{version} {tool}");
                assert_eq!(
                    absent["structuredContent"]["code"], "run_not_found",
                    "{version} {tool}"
                );
                assert_eq!(
                    untraced_result(&run_tool(session, tool, run).await),
                    untraced_result(&absent),
                    "{version} {tool}: {case} looks absent"
                );
            }
        }
        for (session, run) in [(&owner, &owner_run), (&other, &other_run)] {
            let observed = session
                .tool("wait", json!({"run_id":run, "wait_ms":0}))
                .await;
            assert_eq!(
                observed["structuredContent"]["state"], "running",
                "{version}: a denial changes nothing"
            );
        }
        assert_eq!(member_view().await, RunState::Running, "{version}");

        let observed = agent
            .tool("wait", json!({"run_id":own_run, "wait_ms":0}))
            .await;
        assert_eq!(observed["isError"], false, "{version}");
        assert_eq!(observed["structuredContent"]["state"], "running");
        let canceled = agent.tool("cancel", json!({"run_id":own_run})).await;
        assert_eq!(canceled["isError"], false, "{version}");
        assert_eq!(canceled["structuredContent"]["run_id"], own_run);
        assert_eq!(canceled["structuredContent"]["state"], "canceled");
        assert_eq!(canceled["structuredContent"]["required_actions"], json!([]));
        for (session, run) in [(&owner, &owner_run), (&other, &other_run)] {
            session.tool("cancel", json!({"run_id":run})).await;
        }
    }
    tracer
        .runs
        .cancel(
            &member,
            &tracer.workspace_a,
            &private,
            &RunObservation::immediate(None, 100).expect("observation"),
        )
        .await
        .expect("the member cancels their Run");
}

/// Starts a Run that waits for approval, so it stays running without work.
async fn parked(session: &RawSession) -> Value {
    let task = session
        .tool("start_task", json!({"intent":"Hold a Run open"}))
        .await["structuredContent"]["task_id"]
        .clone();
    let update = session
        .tool(
            "exec",
            json!({"task_id":task, "intent":"Wait for approval", "wait_ms":0, "program":PARKED}),
        )
        .await;
    assert_eq!(update["isError"], false, "{update}");
    update["structuredContent"]["run_id"].clone()
}

async fn run_tool(session: &RawSession, tool: &str, run: &Value) -> Value {
    let arguments = if tool == "wait" {
        json!({"run_id":run, "wait_ms":0})
    } else {
        json!({"run_id":run})
    };
    session.tool(tool, arguments).await
}

/// A disabled Tool and a path that never existed give one result.
async fn check_concealed_descriptions(
    session: &RawSession,
    task: &Value,
    version: &ProtocolVersion,
) {
    let describe = |path: &str| {
        session.tool(
            "describe",
            json!({"task_id":task, "intent":"Describe one exact path", "path":path}),
        )
    };
    let absent = describe("coral.providers.gmail.send_message").await;
    let concealed = describe("coral.providers.weather.lostWorker").await;
    assert_eq!(absent["isError"], true, "{version}");
    assert_eq!(
        text_json(&absent)["code"],
        "authoring_description_not_found",
        "{version}"
    );
    assert_eq!(
        untraced_result(&concealed),
        untraced_result(&absent),
        "{version}: a disabled Tool looks absent"
    );
}
