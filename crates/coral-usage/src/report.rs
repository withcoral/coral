//! The Usage Report, field for field as the collector accepts it.
//!
//! The collector rejects any unknown field, enum value or map key, so every
//! value here comes from a fixed list or is a count or a date. Serialization
//! keeps the field order below, and the stored JSON is what Coral sends and
//! what Reef shows, byte for byte.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde::Serialize;

/// The first day a Coral release sent a report. The collector rejects an
/// earlier `day.date`, so no report is made for one.
pub const FIRST_REPORT_DAY: NaiveDate = match NaiveDate::from_ymd_opt(2026, 11, 1) {
    Some(day) => day,
    None => panic!("2026-11-01 is a date"),
};

/// The largest count the collector accepts.
pub(crate) const MAX_COUNT: u64 = (1 << 31) - 1;

/// What this Coral process is, for the report's `coral` block. Bootstrap
/// decides it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    /// The release version, such as `1.5.0`.
    pub version: String,
    /// Desktop or a shared server.
    pub kind: HostKind,
    /// Whether people sign in.
    pub auth: HostAuth,
    /// The database system.
    pub database: coral_db::DatabaseSystem,
}

/// Where Coral runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostKind {
    /// Coral Desktop on one computer.
    Desktop,
    /// A shared server.
    Server,
}

/// How people reach Coral.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostAuth {
    /// One local user, no sign-in.
    Disabled,
    /// Sign-in for many people.
    Required,
}

#[derive(Serialize)]
pub(crate) struct UsageReport<'a> {
    pub v: u8,
    pub install: &'a str,
    pub coral: CoralBlock<'a>,
    pub build: &'static str,
    pub milestones: Milestones,
    pub day: Day,
    pub totals: Totals,
}

#[derive(Serialize)]
pub(crate) struct CoralBlock<'a> {
    pub version: &'a str,
    pub os: &'static str,
    pub arch: &'static str,
    pub kind: &'static str,
    pub auth: &'static str,
    pub database: &'static str,
}

impl<'a> CoralBlock<'a> {
    pub(crate) fn new(host: &'a Host) -> Self {
        Self {
            version: wire_version(&host.version),
            os: match std::env::consts::OS {
                "macos" => "macos",
                "windows" => "windows",
                _ => "linux",
            },
            arch: match std::env::consts::ARCH {
                "aarch64" => "arm64",
                _ => "x86_64",
            },
            kind: match host.kind {
                HostKind::Desktop => "desktop",
                HostKind::Server => "server",
            },
            auth: match host.auth {
                HostAuth::Disabled => "disabled",
                HostAuth::Required => "required",
            },
            database: match host.database {
                coral_db::DatabaseSystem::Sqlite => "sqlite",
                coral_db::DatabaseSystem::Postgres => "postgres",
            },
        }
    }
}

/// The version without build metadata (`1.5.0+abc123` is `1.5.0`), which
/// the collector does not accept.
fn wire_version(version: &str) -> &str {
    version
        .split_once('+')
        .map_or(version, |(version, _)| version)
}

#[derive(Serialize)]
pub(crate) struct Milestones {
    pub first_successful_run: Option<String>,
    pub first_saved_function: Option<String>,
    pub function_reused_in_later_task: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct Day {
    pub date: String,
    pub runs: RunCounts,
    pub run_failures: RunFailures,
    pub tool_invocations: ToolInvocationCounts,
    pub approvals: ApprovalCounts,
    pub saved_functions: SavedFunctionCounts,
    pub tasks: TaskCounts,
    pub clients: Clients,
}

#[derive(Serialize, Default)]
pub(crate) struct RunCounts {
    pub succeeded: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub canceled: u64,
}

#[derive(Serialize, Default)]
pub(crate) struct RunFailures {
    pub compile: u64,
    pub tool_error: u64,
    pub timeout: u64,
    pub cpu_budget: u64,
    pub interrupted: u64,
}

#[derive(Serialize, Default)]
pub(crate) struct ToolInvocationCounts {
    pub succeeded: u64,
    pub failed: u64,
    pub approval_required: u64,
}

#[derive(Serialize, Default)]
pub(crate) struct ApprovalCounts {
    pub approved: u64,
    pub denied: u64,
    pub expired: u64,
}

#[derive(Serialize, Default)]
pub(crate) struct SavedFunctionCounts {
    pub saved: u64,
    pub called: u64,
    pub tasks_reusing_one: u64,
}

#[derive(Serialize, Default)]
pub(crate) struct TaskCounts {
    pub started: u64,
}

/// Runs by MCP client. Coral does not record the client yet, so the map is
/// empty, which the collector reads as 0 for each client.
#[derive(Serialize, Default)]
pub(crate) struct Clients {}

#[derive(Serialize)]
pub(crate) struct Totals {
    pub workspaces: u64,
    pub users: u64,
    pub agents: u64,
    pub saved_functions: u64,
    pub providers: Providers,
    pub accounts: Accounts,
}

#[derive(Serialize, Default)]
pub(crate) struct Providers {
    pub recipe: BTreeMap<String, u64>,
    pub custom_openapi: u64,
    pub custom_mcp: u64,
}

#[derive(Serialize, Default)]
pub(crate) struct Accounts {
    pub oauth: u64,
    pub secret: u64,
}

/// A count the collector accepts: negative becomes 0, and a count past its
/// limit stops at the limit.
pub(crate) fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0).min(MAX_COUNT)
}

#[cfg(test)]
mod tests {
    use super::wire_version;

    #[test]
    fn the_version_drops_build_metadata() {
        assert_eq!(wire_version("1.5.0+abc123"), "1.5.0");
        assert_eq!(wire_version("1.6.0-rc.1"), "1.6.0-rc.1");
    }
}
