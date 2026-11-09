//! Builds one day's report from stored data.

use chrono::{DateTime, NaiveDate, Utc};
use coral_db::Transaction;

use crate::Error;
use crate::environment::Build;
use crate::report::{
    Accounts, ApprovalCounts, Clients, CoralBlock, Day, Host, Milestones, Providers, RunCounts,
    RunFailures, SavedFunctionCounts, TaskCounts, ToolInvocationCounts, Totals, UsageReport, count,
};
use crate::repository::activity::{self, Window};
use crate::repository::install::InstallRow;

/// One composed report and the milestone days it found.
pub(crate) struct Composed {
    pub json: String,
    pub first_successful_run: Option<String>,
    pub first_saved_function: Option<String>,
    pub function_reused: Option<String>,
}

/// Builds the report for `day`, counting activity in `window`. A finished
/// day's window is the whole day; today's ends now.
pub(crate) async fn compose(
    tx: &mut Transaction<'_>,
    install: &InstallRow,
    host: &Host,
    build: Build,
    day: NaiveDate,
    window: Window,
) -> Result<Composed, Error> {
    let runs = activity::runs(tx, window).await?;
    let compile = activity::compile_failures(tx, window).await?;
    let tools = activity::tool_invocations(tx, window).await?;
    let approval_required = activity::approval_requests(tx, window).await?;
    let approvals = activity::approvals(tx, window).await?;
    let tasks = activity::tasks_started(tx, window).await?;
    let saved = activity::functions_saved(tx, window).await?;
    let called = activity::functions_called(tx, window).await?;
    let reuse_today = activity::reuse(tx, window).await?;
    let reuse_ever = activity::reuse(
        tx,
        Window {
            start: i64::MIN,
            end: window.end,
        },
    )
    .await?;
    let totals = activity::totals(tx).await?;

    let first_successful_run = earliest(
        install.first_successful_run_day.as_deref(),
        activity::first_successful_run(tx).await?,
    );
    let first_saved_function = earliest(
        install.first_saved_function_day.as_deref(),
        activity::first_saved_function(tx).await?,
    );
    let function_reused = earliest(install.function_reused_day.as_deref(), reuse_ever.first_at);

    let report = UsageReport {
        v: 1,
        install: &install.install_id,
        coral: CoralBlock::new(host),
        build: build.wire(),
        milestones: Milestones {
            first_successful_run: first_successful_run.clone(),
            first_saved_function: first_saved_function.clone(),
            function_reused_in_later_task: function_reused.clone(),
        },
        day: Day {
            date: day_string(day),
            runs: RunCounts {
                succeeded: count(runs.succeeded),
                failed: count(runs.failed),
                timed_out: count(runs.timed_out),
                canceled: count(runs.canceled),
            },
            run_failures: RunFailures {
                compile: count(compile),
                tool_error: count(runs.tool_error),
                timeout: count(runs.timed_out),
                cpu_budget: count(runs.cpu_budget),
                interrupted: count(runs.interrupted),
            },
            tool_invocations: ToolInvocationCounts {
                succeeded: count(tools.succeeded),
                failed: count(tools.failed),
                approval_required: count(approval_required),
            },
            approvals: ApprovalCounts {
                approved: count(approvals.approved),
                denied: count(approvals.denied),
                expired: count(approvals.expired),
            },
            saved_functions: SavedFunctionCounts {
                saved: count(saved),
                called: count(called),
                tasks_reusing_one: count(reuse_today.tasks),
            },
            tasks: TaskCounts {
                started: count(tasks),
            },
            clients: Clients {},
        },
        totals: Totals {
            workspaces: count(totals.workspaces),
            users: count(totals.users),
            agents: count(totals.agents),
            saved_functions: count(totals.saved_functions),
            providers: Providers {
                // Coral does not record which recipe made a provider yet, so
                // every provider counts by its kind.
                recipe: std::collections::BTreeMap::new(),
                custom_openapi: count(totals.openapi_providers),
                custom_mcp: count(totals.mcp_providers),
            },
            accounts: Accounts {
                oauth: count(totals.oauth_accounts),
                secret: count(totals.secret_accounts),
            },
        },
    };
    let json = serde_json::to_string(&report)
        .map_err(|_error| Error::CorruptData("the usage report does not serialize"))?;
    Ok(Composed {
        json,
        first_successful_run,
        first_saved_function,
        function_reused,
    })
}

/// The earlier of a kept day and a found time, as a UTC day.
fn earliest(kept: Option<&str>, found: Option<i64>) -> Option<String> {
    let found = found.map(|at| day_string(day_of(at)));
    match (kept, found) {
        (Some(kept), Some(found)) => Some(kept.min(found.as_str()).to_owned()),
        (Some(kept), None) => Some(kept.to_owned()),
        (None, found) => found,
    }
}

/// The UTC day of a unix-nanosecond time.
pub(crate) fn day_of(unix_nanos: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_nanos(unix_nanos).date_naive()
}

/// The unix-nanosecond start of a UTC day.
pub(crate) fn day_start(day: NaiveDate) -> i64 {
    day.and_hms_opt(0, 0, 0)
        .and_then(|start| start.and_utc().timestamp_nanos_opt())
        .unwrap_or(i64::MAX)
}

/// `YYYY-MM-DD`.
pub(crate) fn day_string(day: NaiveDate) -> String {
    day.format("%Y-%m-%d").to_string()
}

/// Parses a stored `YYYY-MM-DD` day.
pub(crate) fn parse_day(day: &str) -> Result<NaiveDate, Error> {
    NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .map_err(|_error| Error::CorruptData("a stored usage report day is not a date"))
}
