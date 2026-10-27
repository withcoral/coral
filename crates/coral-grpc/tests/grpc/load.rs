//! Load harness for Run coordination (#2379, #2391).
//!
//! Three ignored cells measure what the rest of the server experiences while one
//! Workspace is busy. Each runs on the real gRPC fixture (real `RunModule`, V8 and
//! the production-shaped pool) and talks to Coral only through `RunModule` and the
//! fixture's modules, so it measures any coordination shape behind that seam
//! unchanged. Raw SQL on a connection of its own is measurement only.
//!
//! - `run_fan_out_load`: one Run makes `CORAL_LOAD_CALLS` parallel approval-gated
//!   calls, which are admitted, wait, and (in `approve` mode) dispatch.
//! - `run_emit_load`: one Program emits `CORAL_LOAD_EMITS` values; a poller on the
//!   measurement connection times each Emission to Recorded and an observer reading
//!   through `RunModule::observe` times it to visible.
//! - `run_concurrent_runs_load`: `CORAL_LOAD_RUNS` one-call, no-approval Runs start
//!   at once against the default pool.
//!
//! Ignored by default; `scripts/load.mjs` provisions the database, the optional
//! round-trip latency, and runs the cells with these knobs:
//!
//! | Variable | Meaning |
//! |---|---|
//! | `CORAL_LOAD_CALLS` | fan-out: parallel approval-gated calls in one Run (default 30) |
//! | `CORAL_LOAD_MODE` | fan-out: `approve` (default): approve every request once all are visible, then settle; `hold`: cancel after the steady phase |
//! | `CORAL_LOAD_HOLD_SECS` | fan-out: steady phase length once every request is visible (default 10) |
//! | `CORAL_LOAD_ADMIT_WAIT_SECS` | fan-out: longest wait for every request to become visible (default 120) |
//! | `CORAL_LOAD_EMITS` | emit: Emissions in one Run (default 1000) |
//! | `CORAL_LOAD_EMIT_BYTES` | emit: padding bytes in every emitted value (default 0, a value of about 40 bytes) |
//! | `CORAL_LOAD_RUNS` | concurrent: one-call Runs started at once (default 20) |
//! | `CORAL_LOAD_REPORT` | path of the JSON report (default: stderr only) |
//! | `CORAL_LOAD_SAMPLER_URL` | direct Postgres URL, outside any proxy, for measurement reads and `pg_stat_activity` sampling |
//! | `CORAL_LOAD_LABEL` | free text copied into the report, for example `+5ms` |
//! | `CORAL_LOAD_LOG` | tracing filter for the cell's stderr (default `coral_run=warn`: the Run Owner's suspect, self-interrupt and retry lines) |
//!
//! Probes, each on its own task for the whole cell:
//! - `begin`: an unrelated `begin()` + commit every 250 ms (pool acquisition wait).
//! - `sign_in`: the sign-in transaction shape, `begin()` + the profile-update hold +
//!   commit, every 500 ms (pool wait plus the global organization guard row).
//! - `ready`: `ping()` with the 1.5 s readiness budget every 500 ms (`bootstrap/health.rs`).
//! - `neighbor`: a second Workspace runs a one-call, no-approval Run back to back
//!   (another tenant's Run latency).
#![allow(
    clippy::print_stderr,
    reason = "a manual load harness reports to the operator on stderr"
)]
use super::{AuthenticatedPrincipal, Fixture, change};
use coral_db::DatabaseOptions;
use coral_run::{RunId, RunObservation, RunState, RunSubmission, RunTimeout};
use coral_test_support::provider_setup_fixtures;
use coral_workspace::{Workspace, WorkspaceDisplayName, WorkspaceRole};
use serde_json::{Value, json};
use sqlx::Connection as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

provider_setup_fixtures!(coral_provider);

#[path = "load/concurrent.rs"]
mod concurrent;
#[path = "load/emit.rs"]
mod emit;
#[path = "load/fan_out.rs"]
mod fan_out;

const RUN_ROW_LOCK: &str = r#"UPDATE "program_run_records"%"#;
const READINESS_BUDGET: Duration = Duration::from_millis(1500);
/// One `pg_stat_activity` sample row: client backends, active, lock waits, Run-row
/// waits, ungranted locks, oldest transaction age in seconds, committed transactions.
type PgActivityRow = (i64, i64, i64, i64, i64, f64, i64);
/// One Invocation row as the probe reads it: admitted at, dispatch started
/// at, ended at, state, result kind.
pub(crate) type InvocationRow = (i64, Option<i64>, Option<i64>, String, Option<String>);
/// How long `Probes::finish` lets a probe finish its current operation.
const PROBE_STOP_GRACE: Duration = Duration::from_secs(10);

#[expect(
    clippy::disallowed_methods,
    reason = "the harness reads its knobs once at test start, like test-support reads its selection"
)]
fn setting(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn setting_u64(name: &str, default: u64) -> u64 {
    setting(name)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone)]
struct Sample {
    at: Duration,
    took: Duration,
    ok: bool,
    error: Option<String>,
}

#[derive(Default, Clone)]
struct Series(Arc<Mutex<Vec<Sample>>>);

impl Series {
    fn push(&self, sample: Sample) {
        self.0.lock().unwrap().push(sample);
    }
    fn within(&self, from: Duration, to: Duration) -> Vec<Sample> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.at >= from && s.at < to)
            .cloned()
            .collect()
    }
}

#[derive(Clone)]
struct PgSample {
    at: Duration,
    connections: i64,
    active: i64,
    lock_waits: i64,
    run_row_waits: i64,
    ungranted_locks: i64,
    max_xact_secs: f64,
    xact_per_s: f64,
}

struct Phase {
    name: &'static str,
    from: Duration,
    to: Duration,
}

fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// p50/p95/p99/max over durations in milliseconds, or null when empty.
fn distribution(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(f64::total_cmp);
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "percentile ranks over a small sample vector"
    )]
    let pct = |p: f64| {
        values
            .get(((values.len() as f64 - 1.0) * p).round() as usize)
            .copied()
            .unwrap_or_default()
    };
    json!({
        "n": values.len(),
        "p50_ms": pct(0.5),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "max_ms": values.last().copied().unwrap_or_default(),
    })
}

fn probe_summary(samples: &[Sample]) -> Value {
    let errors: Vec<&String> = samples.iter().filter_map(|s| s.error.as_ref()).collect();
    let mut summary = distribution(samples.iter().map(|s| millis(s.took)).collect());
    if let Value::Object(map) = &mut summary {
        map.insert(
            "failed".into(),
            json!(samples.iter().filter(|s| !s.ok).count()),
        );
        map.insert("errors".into(), json!(errors.len()));
        map.insert("first_error".into(), json!(errors.first()));
    } else {
        summary = json!({ "n": 0, "failed": 0, "errors": 0 });
    }
    summary
}

fn pg_summary(samples: &[PgSample]) -> Value {
    if samples.is_empty() {
        return Value::Null;
    }
    let max_i = |f: fn(&PgSample) -> i64| samples.iter().map(f).max().unwrap_or_default();
    let max_f = |f: fn(&PgSample) -> f64| {
        samples
            .iter()
            .map(f)
            .fold(0.0_f64, |acc, v| if v > acc { v } else { acc })
    };
    #[expect(clippy::cast_precision_loss, reason = "mean over a small sample count")]
    let (mean_xacts, mean_lock_waits) = (
        samples.iter().map(|s| s.xact_per_s).sum::<f64>() / samples.len() as f64,
        samples.iter().map(|s| s.lock_waits).sum::<i64>() as f64 / samples.len() as f64,
    );
    json!({
        "samples": samples.len(),
        "max_connections": max_i(|s| s.connections),
        "max_active": max_i(|s| s.active),
        "max_lock_waits": max_i(|s| s.lock_waits),
        "max_run_row_waits": max_i(|s| s.run_row_waits),
        "max_ungranted_locks": max_i(|s| s.ungranted_locks),
        "max_transaction_age_s": max_f(|s| s.max_xact_secs),
        "mean_xact_per_s": mean_xacts,
        "mean_lock_waits": mean_lock_waits,
    })
}

struct Probes {
    stop: Arc<AtomicBool>,
    begin: Series,
    sign_in: Series,
    ready: Series,
    neighbor: Series,
    pg: Arc<Mutex<Vec<PgSample>>>,
    /// Why the Postgres sampler stopped early, when it did; its phases then report `pg: null`.
    sampler_error: Arc<Mutex<Option<String>>>,
    /// Probes still inside an operation when the cell ended; their last sample is missing.
    aborted_in_flight: usize,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Probes {
    #[expect(
        clippy::too_many_lines,
        reason = "spawns the five probe tasks in one place"
    )]
    fn start(f: &Fixture, epoch: Instant, neighbor: Neighbor, sampler_url: Option<String>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let db = f.database.database();
        let mut tasks = Vec::new();
        let begin = Series::default();
        {
            let (db, stop, series) = (db.clone(), stop.clone(), begin.clone());
            tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let t = Instant::now();
                    let result = match db.begin().await {
                        Ok(tx) => tx.commit().await.map_err(|e| e.to_string()),
                        Err(e) => Err(e.to_string()),
                    };
                    series.push(Sample {
                        at: t.duration_since(epoch),
                        took: t.elapsed(),
                        ok: result.is_ok(),
                        error: result.err(),
                    });
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }));
        }
        let sign_in = Series::default();
        {
            let (db, stop, series) = (db.clone(), stop.clone(), sign_in.clone());
            tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let t = Instant::now();
                    let result = async {
                        let mut tx = db.begin().await.map_err(|e| e.to_string())?;
                        coral_workspace::OrganizationModule::hold_profile_updates_in_transaction(
                            &mut tx,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        tx.commit().await.map_err(|e| e.to_string())
                    }
                    .await;
                    series.push(Sample {
                        at: t.duration_since(epoch),
                        took: t.elapsed(),
                        ok: result.is_ok(),
                        error: result.err(),
                    });
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }));
        }
        let ready = Series::default();
        {
            let (db, stop, series) = (db.clone(), stop.clone(), ready.clone());
            tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let t = Instant::now();
                    let result = match tokio::time::timeout(READINESS_BUDGET, db.ping()).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(_) => Err("readiness budget exceeded".to_owned()),
                    };
                    series.push(Sample {
                        at: t.duration_since(epoch),
                        took: t.elapsed(),
                        ok: result.is_ok(),
                        error: result.err(),
                    });
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }));
        }
        let neighbor_series = Series::default();
        {
            let (stop, series) = (stop.clone(), neighbor_series.clone());
            tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let t = Instant::now();
                    let result = neighbor.run_once().await;
                    series.push(Sample {
                        at: t.duration_since(epoch),
                        took: t.elapsed(),
                        ok: result.is_ok(),
                        error: result.err(),
                    });
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }));
        }
        let pg = Arc::new(Mutex::new(Vec::new()));
        let sampler_error = Arc::new(Mutex::new(None));
        if let Some(url) = sampler_url {
            let (stop, pg, sampler_error) = (stop.clone(), pg.clone(), sampler_error.clone());
            tasks.push(tokio::spawn(async move {
                let mut connection = match sqlx::PgConnection::connect(&url).await {
                    Ok(connection) => connection,
                    Err(error) => {
                        eprintln!("load: pg sampler could not connect: {error}");
                        *sampler_error.lock().unwrap() = Some(format!("connect: {error}"));
                        return;
                    }
                };
                let mut last: Option<(i64, Instant)> = None;
                while !stop.load(Ordering::SeqCst) {
                    let sampled: Result<PgActivityRow, sqlx::Error> = sqlx::query_as(
                        "select
                           count(*) filter (where backend_type = 'client backend'),
                           count(*) filter (where state = 'active'),
                           count(*) filter (where wait_event_type = 'Lock'),
                           count(*) filter (where wait_event_type = 'Lock' and query like $1),
                           (select count(*) from pg_locks where not granted),
                           coalesce(max(extract(epoch from now() - xact_start)) filter (where state <> 'idle'), 0)::float8,
                           (select (xact_commit + xact_rollback)::bigint from pg_stat_database where datname = current_database())
                         from pg_stat_activity
                         where datname = current_database() and pid <> pg_backend_pid()",
                    )
                    .bind(RUN_ROW_LOCK)
                    .fetch_one(&mut connection)
                    .await;
                    let row = match sampled {
                        Ok(row) => row,
                        Err(error) => {
                            eprintln!("load: pg sampler query failed: {error}");
                            *sampler_error.lock().unwrap() = Some(format!("query: {error}"));
                            return;
                        }
                    };
                    let now = Instant::now();
                    #[expect(clippy::cast_precision_loss, reason = "transaction counter delta")]
                    let xact_per_s = last.map_or(0.0, |(x, at)| {
                        (row.6 - x) as f64 / now.duration_since(at).as_secs_f64()
                    });
                    last = Some((row.6, now));
                    pg.lock().unwrap().push(PgSample {
                        at: now.duration_since(epoch),
                        connections: row.0,
                        active: row.1,
                        lock_waits: row.2,
                        run_row_waits: row.3,
                        ungranted_locks: row.4,
                        max_xact_secs: row.5,
                        xact_per_s,
                    });
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }));
        }
        Self {
            stop,
            begin,
            sign_in,
            ready,
            neighbor: neighbor_series,
            pg,
            sampler_error,
            aborted_in_flight: 0,
            tasks,
        }
    }

    /// Stops the probes. Each one finishes the operation it is in and records that
    /// sample first, so slow or failing operations at the end of a cell still count;
    /// a probe that outlives `PROBE_STOP_GRACE` is aborted and counted.
    async fn finish(mut self) -> Self {
        self.stop.store(true, Ordering::SeqCst);
        let mut aborted_in_flight = 0;
        for mut task in std::mem::take(&mut self.tasks) {
            if tokio::time::timeout(PROBE_STOP_GRACE, &mut task)
                .await
                .is_err()
            {
                task.abort();
                aborted_in_flight += 1;
            }
        }
        Self {
            aborted_in_flight,
            ..self
        }
    }

    fn report(&self, phase: &Phase) -> Value {
        let pg: Vec<PgSample> = self
            .pg
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.at >= phase.from && s.at < phase.to)
            .cloned()
            .collect();
        json!({
            "name": phase.name,
            "from_ms": millis(phase.from),
            "to_ms": millis(phase.to),
            "begin": probe_summary(&self.begin.within(phase.from, phase.to)),
            "sign_in": probe_summary(&self.sign_in.within(phase.from, phase.to)),
            "ready": probe_summary(&self.ready.within(phase.from, phase.to)),
            "neighbor": probe_summary(&self.neighbor.within(phase.from, phase.to)),
            "pg": pg_summary(&pg),
        })
    }
}

/// A second active Workspace: its Owner plus one acting Member, one no-approval
/// Tool, one-call Runs.
struct Neighbor {
    runs: coral_run::RunModule,
    actor: AuthenticatedPrincipal,
    workspace: Workspace,
}

impl Neighbor {
    async fn arrange(f: &Fixture, upstream: &str) -> Self {
        let workspace = f
            .workspaces
            .create(
                &change(&f.owner),
                WorkspaceDisplayName::parse("neighbor").unwrap(),
            )
            .await
            .unwrap();
        f.workspaces
            .set_membership(
                &change(&f.owner),
                workspace.id(),
                &f.member,
                WorkspaceRole::Member,
            )
            .await
            .unwrap();
        enable_http(f, &workspace, "neighbor", upstream).await;
        Self {
            runs: f.runs.clone(),
            actor: AuthenticatedPrincipal::user(f.member.clone()),
            workspace,
        }
    }

    async fn run_once(&self) -> Result<(), String> {
        let run = self
            .runs
            .start(
                &self.actor,
                self.workspace.id(),
                submission(
                    "return await coral.providers.neighbor.read({body: {}});",
                    60_000,
                ),
            )
            .await
            .map_err(|e| e.to_string())?
            .run_id()
            .clone();
        let state = settle(&self.runs, &self.actor, self.workspace.id(), &run, 60).await?;
        if state == RunState::Succeeded {
            Ok(())
        } else {
            Err(format!("neighbor Run ended {state:?}"))
        }
    }
}

fn submission(source: &str, timeout_ms: u64) -> RunSubmission {
    RunSubmission::new(
        coral_run::ProgramSource::parse(source).unwrap(),
        RunTimeout::from_millis(timeout_ms).unwrap(),
    )
}

/// Observes until terminal, each observation waiting on change notifications for up
/// to 30 s and never past the budget. A Run still live at the budget is cancelled,
/// so it cannot keep loading the fixture after its cell has moved on.
async fn settle(
    runs: &coral_run::RunModule,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    run: &RunId,
    budget_secs: u64,
) -> Result<RunState, String> {
    let deadline = Instant::now() + Duration::from_secs(budget_secs);
    loop {
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(30));
        let update = runs
            .observe(
                actor,
                workspace,
                run,
                &RunObservation::initial(wait).unwrap(),
            )
            .await
            .map_err(|e| e.to_string())?;
        if update.state().is_terminal() {
            return Ok(update.state());
        }
        if Instant::now() >= deadline {
            let state = update.state();
            let cancelled = runs
                .cancel(
                    actor,
                    workspace,
                    run,
                    &RunObservation::initial(Duration::from_secs(30)).unwrap(),
                )
                .await
                .map_or_else(
                    |error| format!("cancel failed: {error}"),
                    |update| format!("{:?}", update.state()),
                );
            return Err(format!(
                "Run still {state:?} after {budget_secs} s; cancelled ({cancelled})"
            ));
        }
    }
}

async fn publish_http(
    f: &Fixture,
    workspace: &Workspace,
    namespace: &str,
    url: &str,
) -> coral_provider::ProviderId {
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.database.database(),
        f.workspaces.clone(),
    );
    let context = change(&f.owner);
    let setup = providers.prepare_openapi_fixture(&context, workspace.id(),
        coral_catalog::OpenApiDocument::parse(&json!({
            "openapi": "3.1.0", "info": {"title": "Load", "version": "1"},
            "servers": [{"url": url}], "paths": {"/result": {"post": {
                "operationId": "read", "requestBody": {"content": {"application/json": {"schema": {"type": "object"}}}},
                "responses": {"200": {"description": "Native result", "content": {"application/json": {"schema": {"type": "object"}}}}}
            }}}
        }).to_string()).unwrap()
    ).await.unwrap();
    providers
        .publish_workspace_provider(
            &context,
            coral_provider::WorkspaceProviderPublicationRequest::new(
                coral_provider::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string())
                    .unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                coral_provider::ProviderNamespace::parse(namespace).unwrap(),
                coral_provider::CredentialRoute::None,
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .id()
        .clone()
}

/// Publishes an HTTP Provider and enables its one Tool without approval.
async fn enable_http(f: &Fixture, workspace: &Workspace, namespace: &str, url: &str) {
    let provider = publish_http(f, workspace, namespace, url).await;
    coral_policy::ToolPolicyModule::new(f.database.database(), f.workspaces.clone())
        .set_access_state(
            &change(&f.owner),
            coral_policy::ToolPolicyTarget::new(
                workspace.id().clone(),
                provider,
                coral_catalog::ToolKey::parse("read").unwrap(),
            ),
            coral_policy::ToolAccessState::Enabled,
        )
        .await
        .unwrap();
}

/// Measurement reads on a connection of their own. These read timestamps the Run
/// domain recorded; they arrange nothing and the harness never writes through them.
enum Probe {
    Postgres(sqlx::PgConnection),
    Sqlite(sqlx::SqliteConnection),
}

impl Probe {
    async fn open(options: &DatabaseOptions) -> Self {
        match options {
            DatabaseOptions::Postgres { url } => {
                Self::Postgres(sqlx::PgConnection::connect(url).await.unwrap())
            }
            DatabaseOptions::Sqlite { path } => Self::Sqlite(
                sqlx::SqliteConnection::connect_with(
                    &sqlx::sqlite::SqliteConnectOptions::new().filename(path),
                )
                .await
                .unwrap(),
            ),
        }
    }

    async fn count_approvals(&mut self, run: &RunId) -> i64 {
        let sql = "select count(*) from invocation_approvals where run_id = $1";
        match self {
            Self::Postgres(c) => sqlx::query_scalar(sql)
                .bind(run.to_string())
                .fetch_one(c)
                .await
                .unwrap(),
            Self::Sqlite(c) => sqlx::query_scalar(sql)
                .bind(run.to_string())
                .fetch_one(c)
                .await
                .unwrap(),
        }
    }

    async fn run_row(&mut self, run: &RunId) -> (i64, Option<i64>, String, Option<String>, i64) {
        let sql = "select accepted_at_unix_nanos, terminal_at_unix_nanos, state, termination_kind, chunk_sequence \
                   from program_run_records where id = $1";
        match self {
            Self::Postgres(c) => sqlx::query_as(sql)
                .bind(run.to_string())
                .fetch_one(c)
                .await
                .unwrap(),
            Self::Sqlite(c) => sqlx::query_as(sql)
                .bind(run.to_string())
                .fetch_one(c)
                .await
                .unwrap(),
        }
    }

    /// Emissions committed after `after_sequence`: sequence, occurred-at, output bytes.
    async fn emissions_after(&mut self, run: &RunId, after_sequence: i64) -> Vec<(i64, i64, i64)> {
        let sql = "select sequence, occurred_at_unix_nanos, output_bytes from program_run_events \
                   where run_id = $1 and sequence > $2 and event_type = 'program.emitted' \
                   order by sequence";
        match self {
            Self::Postgres(c) => sqlx::query_as(sql)
                .bind(run.to_string())
                .bind(after_sequence)
                .fetch_all(c)
                .await
                .unwrap(),
            Self::Sqlite(c) => sqlx::query_as(sql)
                .bind(run.to_string())
                .bind(after_sequence)
                .fetch_all(c)
                .await
                .unwrap(),
        }
    }

    async fn invocations(&mut self, run: &RunId) -> Vec<InvocationRow> {
        let sql = "select admitted_at_unix_nanos, dispatch_started_at_unix_nanos, ended_at_unix_nanos, state, result_kind \
                   from tool_invocations where run_id = $1";
        match self {
            Self::Postgres(c) => sqlx::query_as(sql)
                .bind(run.to_string())
                .fetch_all(c)
                .await
                .unwrap(),
            Self::Sqlite(c) => sqlx::query_as(sql)
                .bind(run.to_string())
                .fetch_all(c)
                .await
                .unwrap(),
        }
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "nanosecond differences to milliseconds"
)]
fn nanos_to_ms(delta: i64) -> f64 {
    delta as f64 / 1_000_000.0
}

fn counts<'a>(values: impl Iterator<Item = &'a str>) -> Value {
    let mut map = serde_json::Map::new();
    for value in values {
        let entry = map.entry(value).or_insert(json!(0));
        *entry = json!(entry.as_u64().unwrap_or_default() + 1);
    }
    Value::Object(map)
}

/// Wall-clock milliseconds since the Unix epoch, comparable with a Program's `Date.now()`.
fn unix_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

/// One arranged cell: the fixture, the upstream mock, the busy Workspace's actor,
/// the neighbor Workspace the probes use, and the report destination.
struct Harness {
    f: Fixture,
    backend: &'static str,
    actor: AuthenticatedPrincipal,
    upstream: MockServer,
    neighbor: Option<Neighbor>,
    sampler_url: Option<String>,
    label: String,
    report_path: Option<String>,
    epoch: Instant,
}

impl Harness {
    async fn arrange(cell: &str) -> Self {
        // The Run Owner's liveness transitions (suspect, self-interrupt) and
        // its retries are warn-level lines; the cell's log shows them, so a
        // run can say whether any happened. `CORAL_LOAD_LOG` narrows them.
        let _subscriber = tracing_subscriber::fmt()
            .with_env_filter(
                setting("CORAL_LOAD_LOG").unwrap_or_else(|| "coral_run=warn".to_string()),
            )
            .with_writer(std::io::stderr)
            .try_init();
        let f = Fixture::new().await;
        let backend = match f.database.options() {
            DatabaseOptions::Postgres { .. } => "postgres",
            DatabaseOptions::Sqlite { .. } => "sqlite",
        };
        let sampler_url = (backend == "postgres")
            .then(|| setting("CORAL_LOAD_SAMPLER_URL"))
            .flatten();
        let upstream = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&upstream)
            .await;
        let neighbor = Neighbor::arrange(&f, &upstream.uri()).await;
        let actor = AuthenticatedPrincipal::user(f.member.clone());
        let label = setting("CORAL_LOAD_LABEL").unwrap_or_default();
        eprintln!(
            "load: cell={cell} backend={backend} label={label:?} sampler={}",
            sampler_url.is_some()
        );
        Self {
            f,
            backend,
            actor,
            upstream,
            neighbor: Some(neighbor),
            sampler_url,
            label,
            report_path: setting("CORAL_LOAD_REPORT"),
            epoch: Instant::now(),
        }
    }

    /// Where measurement reads connect: the fixture's own storage (its schema rides
    /// in the URL's `search_path`), but on Postgres through the direct host and port
    /// of the sampler URL, outside any proxy, so added round-trip latency never
    /// blunts what they time.
    fn measurement_options(&self) -> DatabaseOptions {
        match (&self.sampler_url, self.f.database.options()) {
            (Some(direct), DatabaseOptions::Postgres { url }) => {
                let direct = url::Url::parse(direct).unwrap();
                let mut scoped = url::Url::parse(url).unwrap();
                scoped.set_host(direct.host_str()).unwrap();
                scoped.set_port(direct.port()).unwrap();
                DatabaseOptions::Postgres { url: scoped.into() }
            }
            (_, options) => options.clone(),
        }
    }

    async fn open_probe(&self) -> Probe {
        Probe::open(&self.measurement_options()).await
    }

    fn start_probes(&mut self) -> Probes {
        let neighbor = self
            .neighbor
            .take()
            .expect("the probes start once per cell");
        Probes::start(&self.f, self.epoch, neighbor, self.sampler_url.clone())
    }

    fn elapsed(&self) -> Duration {
        self.epoch.elapsed()
    }

    /// Writes the cell's report: the common envelope, the cell's own fields, and
    /// every probe per phase.
    fn report(&self, cell: &str, body: Value, probes: &Probes, phases: &[Phase]) {
        let mut report = serde_json::Map::new();
        report.insert("cell".into(), json!(cell));
        report.insert("label".into(), json!(self.label));
        report.insert("backend".into(), json!(self.backend));
        report.insert("debug_assertions".into(), json!(cfg!(debug_assertions)));
        if let Value::Object(fields) = body {
            report.extend(fields);
        }
        report.insert(
            "sampler_error".into(),
            json!(probes.sampler_error.lock().unwrap().clone()),
        );
        report.insert(
            "probes_aborted_in_flight".into(),
            json!(probes.aborted_in_flight),
        );
        report.insert(
            "phases".into(),
            phases.iter().map(|p| probes.report(p)).collect(),
        );
        let report = Value::Object(report);
        eprintln!("{}", serde_json::to_string_pretty(&report).unwrap());
        if let Some(path) = &self.report_path {
            std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
            eprintln!("load: report written to {path}");
        }
    }
}
