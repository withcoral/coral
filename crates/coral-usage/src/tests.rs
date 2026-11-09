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

    async fn sql(&self, sql: &str) {
        self.db.execute_fixture_sql(sql).await.expect(sql);
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
async fn each_finished_day_gets_one_report_from_the_first_workspace_on() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    let peer = fixture.sending();

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
    fixture
        .sending()
        .make_due_reports(at(7, 12))
        .await
        .expect("reports");
    let mut report = fixture.report("2026-11-06").await;
    // The collector's example lists every client; Coral records none yet,
    // and a left-out client is 0.
    report["day"]["clients"] = collector["day"]["clients"].clone();
    same_shape(&report, &collector, "");
}

/// One Run of `task`, ended at `ended`, as Run history keeps it.
fn run(id: &str, task: &str, state: &str, accepted: i64, ended: i64, failure: &str) -> String {
    let (code, message) = match failure {
        "tool" => ("'program_rejected'", "'ToolError: the upstream failed'"),
        "cpu" => ("'cpu_limit_exceeded'", "'CPU budget exceeded'"),
        "program" => ("'program_rejected'", "'Error: boom'"),
        _ => ("NULL", "NULL"),
    };
    let cancel = if state == "canceled" {
        format!("{accepted}, 'usage-owner', 'user'")
    } else {
        "NULL, NULL, NULL".into()
    };
    format!(
        "INSERT INTO program_run_records (id, workspace_id, principal_id, principal_kind, state, \
            accepted_at_unix_nanos, deadline_at_unix_nanos, cancellation_requested_at_unix_nanos, \
            cancellation_principal_id, cancellation_principal_kind, terminal_at_unix_nanos, \
            failure_code, failure_message, next_event_sequence, accumulated_output_bytes, \
            shutdown_grace_nanos, value_limits_json) \
         VALUES ('{id}', 'ws1', 'usage-owner', 'user', '{state}', {accepted}, {ended}, {cancel}, \
            {ended}, {code}, {message}, 2, 0, 0, '{{}}'); \
         INSERT INTO task_interactions (id, workspace_id, task_id, principal_id, principal_kind, kind, \
            intent, started_at_unix_nanos, completed_at_unix_nanos, outcome, evidence_json, run_id) \
         VALUES ('i-{id}', 'ws1', '{task}', 'usage-owner', 'user', 'submit', 'go', {accepted}, \
            {accepted}, 'succeeded', '{{\"kind\":\"run\"}}', '{id}');"
    )
}

fn task(id: &str, started: i64) -> String {
    format!(
        "INSERT INTO task_records (id, workspace_id, principal_id, principal_kind, intent, \
            started_at_unix_nanos, last_activity_at_unix_nanos) \
         VALUES ('{id}', 'ws1', 'usage-owner', 'user', 'go', {started}, {started});"
    )
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
    sql.push(format!(
        "INSERT INTO task_interactions (id, workspace_id, task_id, principal_id, principal_kind, kind, \
            intent, started_at_unix_nanos, completed_at_unix_nanos, outcome, evidence_json) \
         VALUES ('i-reject', 'ws1', 't1', 'usage-owner', 'user', 'submit', 'go', {at}, {at}, 'rejected', \
            '{{\"kind\":\"rejected\",\"failure\":{{\"code\":\"program_admission_failed\",\"message\":\"x\"}}}}');",
        at = d7(12)
    ));
    for statement in sql.concat().split_terminator(';') {
        if !statement.trim().is_empty() {
            fixture.sql(statement).await;
        }
    }

    fixture
        .sending()
        .make_due_reports(at(9, 12))
        .await
        .expect("reports");

    let day7 = fixture.report("2026-11-07").await;
    assert_eq!(
        day7["day"]["runs"],
        json!({"succeeded": 2, "failed": 4, "timed_out": 1, "canceled": 1})
    );
    assert_eq!(
        day7["day"]["run_failures"],
        json!({"compile": 1, "tool_error": 1, "timeout": 1, "cpu_budget": 1, "interrupted": 1})
    );
    assert_eq!(day7["day"]["tasks"]["started"], 1);
    assert_eq!(
        day7["day"]["saved_functions"],
        json!({"saved": 1, "called": 1, "tasks_reusing_one": 0})
    );
    let day8 = fixture.report("2026-11-08").await;
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
    usage.make_due_reports(at(9, 12)).await.expect("reports");
    let day6 = fixture.report("2026-11-06").await.to_string();
    let collector = MockServer::start().await;
    let url = format!("{}/v1/report", collector.uri());

    // A 5xx keeps every report and stops.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&collector)
        .await;
    let outcome = usage
        .send_due(&client(), &url, || at(9, 13))
        .await
        .expect("send");
    assert_eq!(outcome, SendOutcome::Retry);
    assert_eq!(
        collector.received_requests().await.expect("requests").len(),
        1
    );
    assert!(
        fixture
            .reports()
            .await
            .iter()
            .all(|(_, sent)| sent.is_none())
    );

    // A 4xx drops that report; a 204 stores the others; only the last stays.
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
async fn turning_sharing_off_drops_waiting_reports_and_on_starts_today() {
    let fixture = Fixture::new().await;
    let usage = fixture.sending();
    usage.make_due_reports(at(9, 12)).await.expect("reports");
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
    usage.make_due_reports(at(14, 12)).await.expect("reports");
    let days: Vec<String> = fixture
        .reports()
        .await
        .into_iter()
        .map(|(day, _)| day)
        .collect();
    assert_eq!(days, ["2026-11-12", "2026-11-13"]);
}

#[tokio::test]
async fn a_process_that_never_sends_skips_its_days() {
    let fixture = Fixture::new().await;
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
    blocked
        .send_due(&client(), &url, || at(9, 12))
        .await
        .expect("send");
    assert!(
        collector
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
    assert!(fixture.reports().await.is_empty());

    // A later release process starts from today.
    fixture
        .sending()
        .make_due_reports(at(10, 12))
        .await
        .expect("reports");
    let days: Vec<String> = fixture
        .reports()
        .await
        .into_iter()
        .map(|(day, _)| day)
        .collect();
    assert_eq!(days, ["2026-11-09"]);

    let source = fixture.usage(&SendEnvironment::default(), Build::Source);
    let view = source
        .view(&fixture.member, at(10, 12))
        .await
        .expect("view");
    assert_eq!(view.block, Some(SendBlock::SourceBuild));
    assert_eq!(
        serde_json::from_str::<Value>(&view.next_report_json).expect("json")["build"],
        "release",
        "the oldest waiting report was made by the release process"
    );
}
