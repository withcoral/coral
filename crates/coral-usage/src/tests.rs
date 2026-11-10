#![expect(clippy::indexing_slicing, reason = "assert known report JSON members")]

use std::time::Duration;

use chrono::{TimeZone as _, Utc};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{AuthenticatedUser, OrganizationRole};
use serde_json::{Value, json};
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::{
    BlockingVariable, Build, Error, Host, HostAuth, HostKind, SendBlock, SendEnvironment,
    SendOutcome, UsageData,
};

fn at(day: u32, hour: u32) -> i64 {
    Utc.with_ymd_and_hms(2026, 11, day, hour, 0, 0)
        .single()
        .and_then(|time| time.timestamp_nanos_opt())
        .expect("a fixture time")
}

struct Fixture {
    db: TestDatabase,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
}

impl Fixture {
    async fn new() -> Self {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        let owner = provision_user(db.database(), "usage-owner", OrganizationRole::Owner).await;
        let member = provision_user(db.database(), "usage-member", OrganizationRole::Member).await;
        db.execute_fixture_sql(&format!(
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, created_at_unix_nanos, updated_at_unix_nanos) \
             VALUES ('ws1', 'One', 'active', 1, {created}, {created})",
            created = at(6, 9)
        ))
        .await
        .expect("workspace");
        Self { db, owner, member }
    }

    fn usage(&self, environment: &SendEnvironment, build: Build) -> UsageData {
        UsageData::new(
            self.db.database(),
            Host {
                version: "1.5.0+abc123".into(),
                kind: HostKind::Desktop,
                auth: HostAuth::Disabled,
                database: self.db.database().system(),
            },
            build,
            environment,
        )
    }

    fn sending(&self) -> UsageData {
        self.usage(&SendEnvironment::default(), Build::Release)
    }

    /// `usage` checks once a day at noon, as a running process does.
    async fn check_daily(&self, usage: &UsageData, days: std::ops::RangeInclusive<u32>) {
        for day in days {
            usage.make_due_reports(at(day, 12)).await.expect("reports");
        }
    }

    async fn sql(&self, sql: &str) {
        self.db.execute_fixture_sql(sql).await.expect(sql);
    }

    async fn installs(&self) -> i64 {
        let database = self.db.database();
        let mut tx = database.begin_read_snapshot().await.expect("read");
        let rows: Vec<(i64,)> = tx
            .fetch_all_sql::<(i64,)>(
                "SELECT COUNT(*) FROM usage_installs",
                sea_query::Values(Vec::new()),
            )
            .await
            .expect("installs");
        tx.commit().await.expect("commit");
        rows[0].0
    }

    async fn reports(&self) -> Vec<(String, Option<i64>)> {
        let database = self.db.database();
        let mut tx = database.begin_read_snapshot().await.expect("read");
        let rows: Vec<(String, Option<i64>)> = tx
            .fetch_all_sql::<(String, Option<i64>)>(
                "SELECT report_day, sent_at_unix_nanos FROM usage_reports ORDER BY report_day",
                sea_query::Values(Vec::new()),
            )
            .await
            .expect("reports");
        tx.commit().await.expect("commit");
        rows
    }

    async fn report(&self, day: &str) -> Value {
        let database = self.db.database();
        let mut tx = database.begin_read_snapshot().await.expect("read");
        let rows: Vec<(String, String)> = tx
            .fetch_all_sql::<(String, String)>(
                "SELECT report_day, body FROM usage_reports",
                sea_query::Values(Vec::new()),
            )
            .await
            .expect("reports");
        tx.commit().await.expect("commit");
        let body = rows
            .into_iter()
            .find(|(report_day, _)| report_day == day)
            .map_or_else(|| panic!("no report for {day}"), |(_, body)| body);
        serde_json::from_str(&body).expect("report JSON")
    }
}

fn client() -> coral_outbound::Client {
    coral_outbound::Client::new(
        DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        Duration::from_secs(5),
    )
    .expect("client")
}

#[tokio::test]
async fn sharing_is_on_until_an_owner_turns_it_off() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();

    // A refused first change writes nothing, not even the install row.
    let denied = usage.set_sharing(&fixture.member, false, at(9, 12)).await;
    assert!(matches!(denied, Err(Error::PermissionDenied)), "{denied:?}");
    assert_eq!(fixture.installs().await, 0);

    let member_view = usage.view(&fixture.member, at(9, 12)).await.expect("view");
    assert!(member_view.sharing);
    assert!(!member_view.can_change);
    assert!(
        usage
            .view(&fixture.owner, at(9, 12))
            .await
            .expect("view")
            .can_change
    );

    let denied = usage.set_sharing(&fixture.member, false, at(9, 12)).await;
    assert!(matches!(denied, Err(Error::PermissionDenied)), "{denied:?}");
    assert!(
        usage
            .view(&fixture.member, at(9, 12))
            .await
            .expect("view")
            .sharing
    );

    let off = usage
        .set_sharing(&fixture.owner, false, at(9, 12))
        .await
        .expect("off");
    assert!(!off.sharing);
}

#[tokio::test]
async fn each_checked_day_gets_one_report() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    let peer = fixture.sending();
    fixture.check_daily(&usage, 6..=8).await;

    let (first, second) = tokio::join!(
        usage.make_due_reports(at(9, 12)),
        peer.make_due_reports(at(9, 12))
    );
    first.expect("reports");
    second.expect("reports");

    let days: Vec<String> = fixture
        .reports()
        .await
        .into_iter()
        .map(|(day, _)| day)
        .collect();
    assert_eq!(days, ["2026-11-06", "2026-11-07", "2026-11-08"]);
    let report = fixture.report("2026-11-07").await;
    assert_eq!(report["day"]["date"], "2026-11-07");
    assert_eq!(report["build"], "release");
    assert_eq!(report["coral"]["version"], "1.5.0");
    assert_eq!(report["coral"]["kind"], "desktop");
    assert_eq!(report["day"]["clients"], json!({}));
    assert_eq!(report["totals"]["providers"]["recipe"], json!({}));
    assert_eq!(report["totals"]["workspaces"], 1);
    assert_eq!(report["totals"]["users"], 2);
    let install = report["install"].as_str().expect("install");
    assert!(uuid::Uuid::parse_str(install).is_ok(), "{install}");
    assert_eq!(fixture.report("2026-11-08").await["install"], install);

    // The page shows the oldest waiting report, byte for byte.
    let view = usage.view(&fixture.member, at(9, 12)).await.expect("view");
    let database = fixture.db.database();
    let mut tx = database.begin_read_snapshot().await.expect("read");
    let stored: Vec<(String,)> = tx
        .fetch_all_sql(
            "SELECT body FROM usage_reports WHERE report_day = '2026-11-06'",
            sea_query::Values(Vec::new()),
        )
        .await
        .expect("body");
    tx.commit().await.expect("commit");
    assert_eq!(view.next_report_json, stored[0].0);
}

/// The same members at every level as the collector's own example report.
/// Only the recipe map's keys vary from install to install.
fn same_shape(ours: &Value, theirs: &Value, at: &str) {
    match (ours, theirs) {
        (Value::Object(ours), Value::Object(theirs)) => {
            if at == "/totals/providers/recipe" {
                return;
            }
            let mut our_keys: Vec<&String> = ours.keys().collect();
            let mut their_keys: Vec<&String> = theirs.keys().collect();
            our_keys.sort_unstable();
            their_keys.sort_unstable();
            assert_eq!(our_keys, their_keys, "members of {at}");
            for (key, value) in ours {
                same_shape(value, &theirs[key], &format!("{at}/{key}"));
            }
        }
        (Value::Null | Value::String(_), Value::Null | Value::String(_))
        | (Value::Number(_), Value::Number(_)) => {}
        _ => panic!("{at}: {ours} is not the kind of {theirs}"),
    }
}

#[tokio::test]
async fn a_report_matches_the_collectors_example() {
    let collector: Value =
        serde_json::from_str(include_str!("collector-report.json")).expect("collector example");
    let fixture = Fixture::new().await;
    fixture.check_daily(&fixture.sending(), 6..=7).await;
    let mut report = fixture.report("2026-11-06").await;
    // The collector's example lists every client; Coral records none yet,
    // and a left-out client is 0.
    report["day"]["clients"] = collector["day"]["clients"].clone();
    same_shape(&report, &collector, "");
}

/// A Program error that coral-run marks as a Coral `ToolError`.
fn tool_error_json() -> String {
    serde_json::to_string(&coral_run::ProgramError {
        name: "ToolError".into(),
        message: "the upstream failed".into(),
        frames: Vec::new(),
        tool: Some(coral_run::ProgramToolError {
            code: "upstream_failed".into(),
            trace_id: None,
            status: None,
            body: None,
            body_truncated: false,
        }),
        cause: None,
        errors: Vec::new(),
        truncated: false,
        cause_omitted: false,
        omitted_errors: 0,
        circular: false,
    })
    .expect("Program error JSON")
}

/// One Run of `task`, ended at `ended`, as Run history keeps it. A failure
/// is written with coral-run's public code and Program error: if either
/// changes, coral-run refuses the row instead of miscounting it.
fn run(id: &str, task: &str, state: &str, accepted: i64, ended: i64, failure: &str) -> String {
    use coral_run::RunFailureCode;
    let (code, error) = match failure {
        "tool" => (
            Some(RunFailureCode::ProgramRejected),
            Some(tool_error_json()),
        ),
        "cpu" => (Some(RunFailureCode::CpuLimitExceeded), None),
        "program" => (Some(RunFailureCode::ProgramRejected), None),
        _ => (None, None),
    };
    let (code, message) = code.map_or_else(
        || ("NULL".to_owned(), "NULL"),
        |code| (format!("'{}'", code.code()), "'it failed'"),
    );
    let error = error.map_or_else(|| "NULL".to_owned(), |json| format!("'{json}'"));
    let cancel = if state == "canceled" {
        format!("{accepted}, 'usage-owner', 'user'")
    } else {
        "NULL, NULL, NULL".into()
    };
    format!(
        "INSERT INTO program_run_records (id, workspace_id, principal_id, principal_kind, state, \
            accepted_at_unix_nanos, deadline_at_unix_nanos, cancellation_requested_at_unix_nanos, \
            cancellation_principal_id, cancellation_principal_kind, terminal_at_unix_nanos, \
            failure_code, failure_message, failure_error_json, next_event_sequence, \
            accumulated_output_bytes, shutdown_grace_nanos, value_limits_json) \
         VALUES ('{id}', 'ws1', 'usage-owner', 'user', '{state}', {accepted}, {ended}, {cancel}, \
            {ended}, {code}, {message}, {error}, 2, 0, 0, '{{}}'); \
         INSERT INTO task_interactions (id, workspace_id, task_id, principal_id, principal_kind, kind, \
            intent, started_at_unix_nanos, completed_at_unix_nanos, outcome, evidence_json, run_id) \
         VALUES ('i-{id}', 'ws1', '{task}', 'usage-owner', 'user', 'submit', 'go', {accepted}, \
            {accepted}, 'succeeded', '{{\"kind\":\"run\"}}', '{id}');"
    )
}

/// One Tool Invocation of `run` that ended at `ended` with `result`.
fn invocation(id: &str, run: &str, ended: i64, result: &str) -> String {
    let (dispatched, result_json, failure) = match result {
        "upstream_outcome" => (ended.to_string(), "'{}'", "NULL, NULL"),
        "coral_failure" => ("NULL".to_owned(), "NULL", "'invalid_input', 'refused'"),
        _ => ("NULL".to_owned(), "NULL", "NULL, NULL"),
    };
    format!(
        "INSERT INTO tool_invocations (id, workspace_id, run_id, provider_account_setting_version, \
            credential_route, provider_id, provider_generation_id, tool_key, call_path, arguments_json, \
            state, admitted_at_unix_nanos, dispatch_started_at_unix_nanos, ended_at_unix_nanos, \
            result_kind, result_json, failure_code, failure_message) \
         VALUES ('{id}', 'ws1', '{run}', 1, 'none', 'p1', 'g1', 'getForecast', \
            'coral.providers.weather.getForecast', '{{}}', 'ended', {ended}, {dispatched}, {ended}, \
            '{result}', {result_json}, {failure});"
    )
}

fn task(id: &str, started: i64) -> String {
    format!(
        "INSERT INTO task_records (id, workspace_id, principal_id, principal_kind, intent, \
            started_at_unix_nanos, last_activity_at_unix_nanos) \
         VALUES ('{id}', 'ws1', 'usage-owner', 'user', 'go', {started}, {started});"
    )
}

/// `run`'s Tool Invocations at `at`: an upstream answer, a Coral failure, an
/// unknown result, and one the Run's end cut short. Two needed approval, and
/// one request settled each way it can. A closed one is not a decision.
fn tool_calls(run: &str, at: i64) -> String {
    let mut sql: Vec<String> = [
        ("v1", "upstream_outcome"),
        ("v2", "coral_failure"),
        ("v3", "indeterminate"),
        ("v4", "run_terminated"),
    ]
    .into_iter()
    .map(|(id, result)| invocation(id, run, at, result))
    .collect();
    sql.push(format!(
        "INSERT INTO invocation_approvals (id, workspace_id, invocation_id, run_id, \
            deadline_at_unix_nanos, created_at_unix_nanos, state, decided_at_unix_nanos, approver_id) \
         VALUES ('a1', 'ws1', 'v1', '{run}', {deadline}, {at}, 'approved', {at}, 'usage-owner'), \
            ('a2', 'ws1', 'v2', '{run}', {deadline}, {at}, 'pending', NULL, NULL);",
        deadline = at + 1,
    ));
    for state in ["approved", "declined", "expired", "closed"] {
        sql.push(format!(
            "INSERT INTO approval_lifecycle_events (id, workspace_id, request_id, state, \
                occurred_at_unix_nanos, trace_id) VALUES ('e-{state}', 'ws1', 'a1', '{state}', {at}, '{trace}');",
            trace = "0".repeat(31) + "1",
        ));
    }
    sql.concat()
}

#[tokio::test]
async fn a_report_counts_its_days_activity_and_keeps_milestones() {
    let fixture = Fixture::new().await;
    let (d7, d8) = (|h| at(7, h), |h| at(8, h));
    let mut sql: Vec<String> = Vec::new();
    sql.push(task("t1", d7(1)));
    sql.push(task("t2", d8(1)));
    sql.push(run("r1", "t1", "succeeded", d7(2), d7(4), ""));
    sql.push(run("r2", "t1", "failed", d7(5), d7(5), "tool"));
    sql.push(run("r3", "t1", "failed", d7(6), d7(6), "cpu"));
    sql.push(run("r4", "t1", "failed", d7(7), d7(7), "program"));
    sql.push(run("r5", "t1", "timed_out", d7(8), d7(8), ""));
    sql.push(run("r6", "t1", "interrupted", d7(9), d7(9), ""));
    sql.push(run("r7", "t1", "canceled", d7(10), d7(10), ""));
    // Saved during r1 (in t1), used again by t1 (r8) and then by t2 (r9).
    sql.push(format!(
        "INSERT INTO saved_function_lifecycle_events (id, workspace_id, actor_principal_id, \
            actor_principal_kind, action, target_saved_function_id, target_path, outcome, \
            changed_field_paths, occurred_at_unix_nanos, trace_id) \
         VALUES ('e1', 'ws1', 'usage-owner', 'user', 'saved_function.created', 'f1', 'triage', \
            'accepted', '[]', {saved}, '{trace}');",
        saved = d7(3),
        trace = "0".repeat(32)
    ));
    sql.push(run("r8", "t1", "succeeded", d7(11), d7(11), ""));
    sql.push(run("r9", "t2", "succeeded", d8(2), d8(2), ""));
    for run_id in ["r8", "r9"] {
        sql.push(format!(
            "INSERT INTO program_run_saved_function_snapshots (run_id, workspace_id, saved_function_id, path, source) \
             VALUES ('{run_id}', 'ws1', 'f1', 'triage', 'return 1');"
        ));
    }
    sql.push(tool_calls("r1", d7(3)));
    sql.push(invocation("v5", "r9", d8(2), "upstream_outcome"));
    for statement in sql.concat().split_terminator(';') {
        if !statement.trim().is_empty() {
            fixture.sql(statement).await;
        }
    }

    fixture.check_daily(&fixture.sending(), 6..=9).await;

    let day7 = fixture.report("2026-11-07").await;
    assert_eq!(
        day7["day"]["runs"],
        json!({"succeeded": 2, "failed": 4, "timed_out": 1, "canceled": 1})
    );
    assert_eq!(
        day7["day"]["run_failures"],
        json!({"compile": 0, "tool_error": 1, "timeout": 1, "cpu_budget": 1, "interrupted": 1})
    );
    assert_eq!(
        day7["day"]["tool_invocations"],
        json!({"succeeded": 1, "failed": 2, "approval_required": 2})
    );
    assert_eq!(
        day7["day"]["approvals"],
        json!({"approved": 1, "denied": 1, "expired": 1})
    );
    assert_eq!(day7["day"]["tasks"]["started"], 1);
    assert_eq!(
        day7["day"]["saved_functions"],
        json!({"saved": 1, "called": 1, "tasks_reusing_one": 0})
    );
    let day8 = fixture.report("2026-11-08").await;
    assert_eq!(day8["day"]["tool_invocations"]["succeeded"], 1);
    assert_eq!(
        day8["day"]["saved_functions"],
        json!({"saved": 0, "called": 1, "tasks_reusing_one": 1})
    );
    let milestones = json!({
        "first_successful_run": "2026-11-07",
        "first_saved_function": "2026-11-07",
        "function_reused_in_later_task": "2026-11-08",
    });
    assert_eq!(day8["milestones"], milestones);
    assert_eq!(
        fixture.report("2026-11-06").await["milestones"]["function_reused_in_later_task"],
        Value::Null
    );

    // Run history goes; the milestones stay.
    fixture.sql("DELETE FROM invocation_approvals").await;
    fixture.sql("DELETE FROM tool_invocations").await;
    fixture.sql("DELETE FROM task_interactions").await;
    fixture
        .sql("DELETE FROM program_run_saved_function_snapshots")
        .await;
    fixture.sql("DELETE FROM program_run_records").await;
    fixture
        .sending()
        .make_due_reports(at(10, 12))
        .await
        .expect("reports");
    assert_eq!(fixture.report("2026-11-09").await["milestones"], milestones);
}

#[tokio::test]
async fn sending_follows_the_collectors_answer() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    fixture.check_daily(&usage, 6..=9).await;
    let day6 = fixture.report("2026-11-06").await.to_string();
    let collector = MockServer::start().await;
    let url = format!("{}/v1/report", collector.uri());

    // A 5xx, a timeout or a rate limit keeps every report and stops.
    let waiting = fixture.reports().await;
    for status in [503, 408, 429] {
        collector.reset().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&collector)
            .await;
        let outcome = usage
            .send_due(&client(), &url, || at(9, 13))
            .await
            .expect("send");
        assert_eq!(outcome, SendOutcome::Retry, "{status}");
        assert_eq!(
            collector.received_requests().await.expect("requests").len(),
            1,
            "{status}"
        );
        assert_eq!(fixture.reports().await, waiting, "{status}");
    }

    // Any other 4xx drops that report; a 204 stores the others; only the last stays.
    collector.reset().await;
    Mock::given(method("POST"))
        .and(path("/v1/report"))
        .and(header("content-type", "application/json"))
        .and(body_string(
            serde_json::from_str::<Value>(&day6)
                .expect("json")
                .to_string(),
        ))
        .respond_with(ResponseTemplate::new(422))
        .mount(&collector)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&collector)
        .await;
    let outcome = usage
        .send_due(&client(), &url, || at(9, 14))
        .await
        .expect("send");
    assert_eq!(outcome, SendOutcome::Done);
    assert_eq!(
        collector.received_requests().await.expect("requests").len(),
        3
    );
    assert_eq!(
        fixture.reports().await,
        [("2026-11-08".to_owned(), Some(at(9, 14)))]
    );

    let view = usage.view(&fixture.member, at(9, 15)).await.expect("view");
    let last = view.last_sent.expect("last sent");
    assert_eq!(last.sent_at_unix_nanos, at(9, 14));
    assert_eq!(last.json, fixture.report("2026-11-08").await.to_string());
    assert_eq!(
        serde_json::from_str::<Value>(&view.next_report_json).expect("json")["day"]["date"],
        "2026-11-09"
    );
}

#[tokio::test]
async fn turning_sharing_off_drops_waiting_reports_and_on_counts_from_then() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    // Day 12: one Run while sharing is off, one after it turns on at noon.
    let runs = [
        task("t1", at(12, 9)),
        run("r-off", "t1", "succeeded", at(12, 10), at(12, 10), ""),
        run("r-on", "t1", "succeeded", at(12, 14), at(12, 14), ""),
    ]
    .concat();
    for statement in runs.split_terminator(';') {
        if !statement.trim().is_empty() {
            fixture.sql(statement).await;
        }
    }
    fixture.check_daily(&usage, 6..=9).await;
    usage
        .set_sharing(&fixture.owner, false, at(9, 12))
        .await
        .expect("off");
    assert!(fixture.reports().await.is_empty());

    usage
        .make_due_reports(at(12, 12))
        .await
        .expect("reports while off");
    assert!(fixture.reports().await.is_empty());

    usage
        .set_sharing(&fixture.owner, true, at(12, 12))
        .await
        .expect("on");
    fixture.check_daily(&usage, 13..=14).await;
    let days: Vec<String> = fixture
        .reports()
        .await
        .into_iter()
        .map(|(day, _)| day)
        .collect();
    assert_eq!(days, ["2026-11-12", "2026-11-13"]);
    assert_eq!(
        fixture.report("2026-11-12").await["day"]["runs"]["succeeded"],
        1
    );
}

/// An owner turns sharing off while a report is on its way. The reports
/// still waiting do not go.
#[tokio::test]
async fn turning_sharing_off_stops_the_next_send() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    fixture.check_daily(&usage, 6..=8).await;
    assert_eq!(fixture.reports().await.len(), 2);
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_millis(500)))
        .mount(&collector)
        .await;
    let url = format!("{}/v1/report", collector.uri());
    let client = client();

    let turn_off = async {
        while collector
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        usage
            .set_sharing(&fixture.owner, false, at(8, 13))
            .await
            .expect("off");
    };
    let (outcome, ()) = tokio::join!(usage.send_due(&client, &url, || at(8, 13)), turn_off);
    assert_eq!(outcome.expect("send"), SendOutcome::Done);
    assert_eq!(
        collector.received_requests().await.expect("requests").len(),
        1
    );
}

/// Another Coral Instance composed day 6's report before an owner turned
/// sharing off. Its claim then fails, so the report never lands after the
/// off dropped the waiting ones.
#[tokio::test]
async fn a_report_composed_before_sharing_turned_off_is_not_stored() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    fixture.check_daily(&usage, 6..=6).await;
    usage
        .set_sharing(&fixture.owner, false, at(7, 1))
        .await
        .expect("off");

    let day = |day| chrono::NaiveDate::from_ymd_opt(2026, 11, day).expect("day");
    let database = fixture.db.database();
    let mut tx = database.begin().await.expect("write");
    let claimed = crate::repository::install::claim_day(&mut tx, day(6), day(7))
        .await
        .expect("claim");
    tx.commit().await.expect("commit");
    assert!(!claimed);
}

#[tokio::test]
async fn a_blocked_process_makes_no_report_and_skips_no_day() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    let blocked = fixture.usage(
        &SendEnvironment {
            do_not_track: Some("1".into()),
            ..SendEnvironment::default()
        },
        Build::Release,
    );
    assert_eq!(
        blocked.block(),
        Some(SendBlock::Environment(BlockingVariable::DoNotTrack))
    );
    let collector = MockServer::start().await;
    let url = format!("{}/v1/report", collector.uri());
    let days = || async {
        fixture
            .reports()
            .await
            .into_iter()
            .map(|(day, _)| day)
            .collect::<Vec<String>>()
    };

    // Beside a sending process, a blocked one skips nothing.
    fixture.check_daily(&usage, 6..=7).await;
    fixture.check_daily(&blocked, 8..=8).await;
    fixture.check_daily(&usage, 8..=8).await;
    blocked
        .send_due(&client(), &url, || at(9, 1))
        .await
        .expect("send");
    assert!(
        collector
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
    assert_eq!(days().await, ["2026-11-06", "2026-11-07"]);

    // Days only blocked processes saw get no report.
    fixture.check_daily(&blocked, 9..=11).await;
    fixture.check_daily(&usage, 12..=12).await;
    assert_eq!(days().await, ["2026-11-06", "2026-11-07", "2026-11-08"]);

    let source = fixture.usage(&SendEnvironment::default(), Build::Source);
    let view = source
        .view(&fixture.member, at(12, 13))
        .await
        .expect("view");
    assert_eq!(view.block, Some(SendBlock::SourceBuild));
    assert_eq!(
        serde_json::from_str::<Value>(&view.next_report_json).expect("json")["build"],
        "release",
        "the oldest waiting report was made by the release process"
    );
}
