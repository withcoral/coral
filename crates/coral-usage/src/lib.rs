//! Anonymous usage data: one Usage Report per UTC day from each self-hosted
//! Coral, to measure activation.
//!
//! # Interface
//!
//! [`UsageData`] owns the deployment's one sharing setting, its random
//! install ID, and the reports to send. [`UsageData::view`] shows any
//! Organization Member the exact JSON of the next and the last report;
//! [`UsageData::set_sharing`] lets an Organization Owner turn sharing off or
//! on. [`UsageData::send_due`] is one pass: it makes each finished day's
//! report and sends it through the bootstrap-selected destination policy.
//! Bootstrap runs it at once, then every [`CHECK_INTERVAL`].
//!
//! # Invariants
//!
//! - Sharing is on until an Organization Owner turns it off. Off drops the
//!   reports not yet sent, and stops the next send. On starts again with
//!   today's report.
//! - A report holds counts, UTC dates, values from fixed lists, the random
//!   install ID and the release version. It never holds a Program, an
//!   argument, a result, a credential or a name.
//! - A report is stored as the JSON that is sent, and read back byte for
//!   byte. The collector rejects any field it does not know, so the types in
//!   `report` match its contract exactly. A report stored before an upgrade
//!   is sent as stored, in its own version: the collector accepts each
//!   version a release sent.
//! - A process with a [`SendBlock`] never makes or sends a report: an
//!   environment variable bootstrap read, or a source build. A day gets a
//!   report only when a process that may send checked on it, so a day that
//!   only blocked processes saw is never reported.
//! - A day's counts start at the latest of the day's start, when the
//!   install row was made, and when sharing last turned on.
//! - A missed day keeps its report until the collector answers. A 4xx but
//!   408 or 429 drops it, since the same report can never succeed. Only the
//!   last report sent is kept.
//! - A milestone is the earliest date in the history Coral keeps, even
//!   from while sharing was off. A report keeps it once found, because Run
//!   history is deleted.
//! - Several Coral Instances may check at once: a day's report is made once,
//!   and the collector stores a repeated report once.

mod compose;
mod day;
mod environment;
mod report;
mod repository;
mod sender;
mod usage_data;

#[cfg(test)]
mod tests;

pub use environment::{BlockingVariable, Build, SendBlock, SendEnvironment};
pub use report::{Host, HostAuth, HostKind};
pub use sender::{CHECK_INTERVAL, COLLECTOR_URL, REQUEST_TIMEOUT, SendOutcome};
pub use usage_data::{SentReport, UsageData, UsageDataView};

/// Why a usage data request failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The caller may not read or change usage data.
    #[error("permission denied")]
    PermissionDenied,
    /// The database failed.
    #[error("usage data storage failed: {0}")]
    Database(#[from] coral_db::Error),
    /// Stored usage data is not what Coral writes.
    #[error("usage data is corrupt: {0}")]
    CorruptData(&'static str),
    /// Organization authority could not be read.
    #[error("Organization authority is unavailable")]
    Unavailable,
}

impl Error {
    /// The failure's stable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::PermissionDenied => "permission_denied",
            Self::Database(_) => "database",
            Self::CorruptData(_) => "corrupt_data",
            Self::Unavailable => "unavailable",
        }
    }
}

impl From<coral_run::Error> for Error {
    fn from(error: coral_run::Error) -> Self {
        match error {
            coral_run::Error::Database(error) => Self::Database(error),
            _ => Self::CorruptData("Run history is not what Coral writes"),
        }
    }
}

impl From<coral_tools::Error> for Error {
    fn from(error: coral_tools::Error) -> Self {
        match error {
            coral_tools::Error::Database(error) => Self::Database(error),
            _ => Self::CorruptData("a Private Tool is not what Coral writes"),
        }
    }
}

impl From<coral_task::Error> for Error {
    fn from(error: coral_task::Error) -> Self {
        match error {
            coral_task::Error::Database(error) => Self::Database(error),
            _ => Self::CorruptData("a Task interaction is not what Coral writes"),
        }
    }
}

impl From<coral_provider::Error> for Error {
    fn from(error: coral_provider::Error) -> Self {
        match error {
            coral_provider::Error::Database(error) => Self::Database(error),
            _ => Self::CorruptData("an Account is not what Coral writes"),
        }
    }
}

impl From<coral_workspace::Error> for Error {
    fn from(error: coral_workspace::Error) -> Self {
        match error {
            coral_workspace::Error::Database(error) => Self::Database(error),
            coral_workspace::Error::PermissionDenied(_) => Self::PermissionDenied,
            _ => Self::Unavailable,
        }
    }
}
