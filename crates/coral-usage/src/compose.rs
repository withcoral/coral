//! Builds one day's report from stored data.

use chrono::NaiveDate;
use coral_db::Transaction;

use crate::Error;
use crate::day::{day_of, day_string};
use crate::environment::Build;
use crate::report::{
    Accounts, ApprovalCounts, Clients, CoralBlock, Day, Host, Milestones, Providers, RunCounts,
    RunFailures, SavedFunctionCounts, TaskCounts, ToolInvocationCounts, Totals, UsageReport, count,
};
use crate::repository::activity::{self, Window};
use crate::repository::install::{Install, MilestoneDays};

/// One composed report and the milestone days it found.
pub(crate) struct Composed {
    pub json: String,
    pub milestones: MilestoneDays,
}

/// Builds the report for `day`, counting activity in `window`. A finished
/// day's window is the whole day; today's ends now.
pub(crate) async fn compose(
    tx: &mut Transaction<'_>,
    install: &Install,
    host: &Host,
    build: Build,
    day: NaiveDate,
    window: Window,
) -> Result<Composed, Error> {
    let runs = coral_run::usage_repository::run_endings(tx, window.start, window.end).await?;
    let compile: u64 =
        coral_task::usage_repository::rejected_submissions(tx, window.start, window.end)
            .await?
            .into_iter()
            .filter(|(code, _)| coral_run::Error::is_compile_rejection(code))
            .map(|(_, submissions)| submissions)
            .sum();
    let tools = activity::tool_invocations(tx, window).await?;
    let approval_required = activity::approval_requests(tx, window).await?;
    let approvals = activity::approvals(tx, window).await?;
    let tasks = activity::tasks_started(tx, window).await?;
    let saved = activity::functions_saved(tx, window).await?;
    let called = activity::functions_called(tx, window).await?;
    let reuse_today = activity::reuse(tx, window).await?;
    let totals = activity::totals(tx).await?;
    let providers = coral_provider::usage_repository::provider_totals(tx).await?;

    let milestones = milestones(tx, install.milestones, day, window.end).await?;

    let report = UsageReport {
        v: 1,
        install: &install.id,
        coral: CoralBlock::new(host),
        build: build.wire(),
        milestones: Milestones {
            first_successful_run: milestones.first_successful_run.map(day_string),
            first_saved_function: milestones.first_saved_function.map(day_string),
            function_reused_in_later_task: milestones.function_reused.map(day_string),
        },
        day: Day {
            date: day_string(day),
            runs: RunCounts {
                succeeded: count(runs.succeeded),
                // An interrupted Run counts as failed.
                failed: count(runs.failed.saturating_add(runs.interrupted)),
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
                custom_openapi: count(providers.openapi),
                custom_mcp: count(providers.mcp),
            },
            accounts: Accounts {
                oauth: count(providers.oauth_accounts),
                secret: count(providers.secret_accounts),
            },
        },
    };
    let json = serde_json::to_string(&report)
        .map_err(|_error| Error::CorruptData("the usage report does not serialize"))?;
    Ok(Composed { json, milestones })
}

/// Each milestone's first day as of the end of the report's day: the
/// collector rejects a milestone after `day.date`.
async fn milestones(
    tx: &mut Transaction<'_>,
    kept: MilestoneDays,
    day: NaiveDate,
    end: i64,
) -> Result<MilestoneDays, Error> {
    let reuse = activity::reuse(
        tx,
        Window {
            start: i64::MIN,
            end,
        },
    )
    .await?;
    Ok(MilestoneDays {
        first_successful_run: earliest(
            day,
            kept.first_successful_run,
            activity::first_successful_run(tx, end).await?,
        ),
        first_saved_function: earliest(
            day,
            kept.first_saved_function,
            activity::first_saved_function(tx, end).await?,
        ),
        function_reused: earliest(day, kept.function_reused, reuse.first_at),
    })
}

/// The earlier of a kept day no later than `day` and the day of a found
/// time.
fn earliest(day: NaiveDate, kept: Option<NaiveDate>, found: Option<i64>) -> Option<NaiveDate> {
    kept.filter(|kept| *kept <= day)
        .into_iter()
        .chain(found.map(day_of))
        .min()
}
