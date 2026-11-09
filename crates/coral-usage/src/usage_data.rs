//! The deployment's usage data: the sharing setting, the reports to send,
//! and what the Usage data page shows.

use std::sync::Arc;

use chrono::Days;
use coral_db::Database;
use coral_workspace::{AuthenticatedUser, OrganizationModule, OrganizationRole};

use crate::Error;
use crate::compose::{compose, day_of, day_start, day_string, parse_day};
use crate::environment::{Build, SendBlock, SendEnvironment};
use crate::report::{FIRST_REPORT_DAY, Host, HostKind};
use crate::repository::activity::Window;
use crate::repository::install::{self, MilestoneDays};

/// What the Usage data page shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageDataView {
    /// On until an Organization Owner turns it off.
    pub sharing: bool,
    /// Whether the caller is an Organization Owner, who can change `sharing`.
    pub can_change: bool,
    /// What stops this process from sending, whatever `sharing` says.
    pub block: Option<SendBlock>,
    /// Where Coral runs.
    pub kind: HostKind,
    /// The next report, as the exact JSON Coral sends: the oldest one
    /// waiting, or else today's so far.
    pub next_report_json: String,
    /// The last report the collector stored.
    pub last_sent: Option<SentReport>,
}

/// A report the collector stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentReport {
    /// The exact JSON Coral sent.
    pub json: String,
    /// When the collector stored it, in unix nanoseconds.
    pub sent_at_unix_nanos: i64,
}

impl SentReport {
    /// When the collector stored it, in RFC 3339 UTC to the second.
    #[must_use]
    pub fn sent_at_rfc3339(&self) -> String {
        chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(self.sent_at_unix_nanos)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }
}

/// A report waiting to be sent.
pub(crate) struct Unsent {
    pub day: String,
    pub json: String,
}

/// The deployment's usage data.
#[derive(Clone)]
pub struct UsageData {
    inner: Arc<Inner>,
}

struct Inner {
    database: Arc<Database>,
    organization: OrganizationModule,
    host: Host,
    build: Build,
    block: Option<SendBlock>,
}

impl UsageData {
    /// Composes usage data for this process. Bootstrap reads `environment`
    /// and the build marker once.
    #[must_use]
    pub fn new(
        database: Arc<Database>,
        host: Host,
        build: Build,
        environment: &SendEnvironment,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                organization: OrganizationModule::new(Arc::clone(&database)),
                database,
                host,
                build,
                block: environment.block(build),
            }),
        }
    }

    /// What stops this process from sending, if anything.
    #[must_use]
    pub fn block(&self) -> Option<SendBlock> {
        self.inner.block
    }

    /// What the Usage data page shows. Any Organization Member can read it.
    ///
    /// # Errors
    ///
    /// Denies a caller without an enabled Organization Membership.
    pub async fn view(&self, actor: &AuthenticatedUser, now: i64) -> Result<UsageDataView, Error> {
        self.ensure_install(now).await?;
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        let role = self
            .inner
            .organization
            .check_inspection_in_transaction(&mut tx, actor)
            .await?;
        let install = install::install(&mut tx)
            .await?
            .ok_or(Error::CorruptData("the usage install row is missing"))?;
        let sharing = install.sharing != 0;
        let waiting = if sharing {
            install::unsent_reports(&mut tx).await?.into_iter().next()
        } else {
            None
        };
        let next_report_json = if let Some(report) = waiting {
            report.body
        } else {
            let today = day_of(now);
            let window = Window {
                start: day_start(today),
                end: now.saturating_add(1),
            };
            compose(
                &mut tx,
                &install,
                &self.inner.host,
                self.inner.build,
                today,
                window,
            )
            .await?
            .json
        };
        let last_sent = install::last_sent(&mut tx).await?.and_then(|row| {
            row.sent_at_unix_nanos.map(|sent_at_unix_nanos| SentReport {
                json: row.body,
                sent_at_unix_nanos,
            })
        });
        tx.commit().await?;
        Ok(UsageDataView {
            sharing,
            can_change: role == OrganizationRole::Owner,
            block: self.inner.block,
            kind: self.inner.host.kind,
            next_report_json,
            last_sent,
        })
    }

    /// Turns sharing on or off for the deployment. Turning it off drops the
    /// reports not yet sent; turning it on starts with today's report.
    ///
    /// # Errors
    ///
    /// Denies a caller who is not an enabled Organization Owner, and changes
    /// nothing then.
    pub async fn set_sharing(
        &self,
        actor: &AuthenticatedUser,
        sharing: bool,
        now: i64,
    ) -> Result<UsageDataView, Error> {
        self.ensure_install(now).await?;
        let mut tx = self.inner.database.begin().await?;
        self.inner
            .organization
            .check_owner_in_transaction(&mut tx, actor)
            .await?;
        install::set_sharing(&mut tx, sharing, &day_string(day_of(now)), now).await?;
        tx.commit().await?;
        self.view(actor, now).await
    }

    /// Makes the report of each finished day not made yet, oldest first,
    /// while sharing is on. A process that never sends skips those days
    /// instead, so a later process does not send them.
    ///
    /// # Errors
    ///
    /// Returns a database failure; the days made before it stay made.
    pub async fn make_due_reports(&self, now: i64) -> Result<(), Error> {
        self.ensure_install(now).await?;
        let today = day_of(now);
        loop {
            let mut read = self.inner.database.begin_read_snapshot().await?;
            let install = install::install(&mut read)
                .await?
                .ok_or(Error::CorruptData("the usage install row is missing"))?;
            let day = parse_day(&install.next_report_day)?;
            if day >= today {
                read.commit().await?;
                return Ok(());
            }
            let from = install.next_report_day.clone();
            if install.sharing == 0 || self.inner.block.is_some() {
                read.commit().await?;
                let mut tx = self.inner.database.begin().await?;
                install::advance_day(&mut tx, &from, &day_string(today)).await?;
                tx.commit().await?;
                return Ok(());
            }
            let next = day
                .checked_add_days(Days::new(1))
                .ok_or(Error::CorruptData("a usage report day has no next day"))?;
            let window = Window {
                start: day_start(day),
                end: day_start(next),
            };
            let composed = compose(
                &mut read,
                &install,
                &self.inner.host,
                self.inner.build,
                day,
                window,
            )
            .await?;
            read.commit().await?;
            let mut tx = self.inner.database.begin().await?;
            if install::advance_day(&mut tx, &from, &day_string(next)).await? {
                install::insert_report(&mut tx, &from, &composed.json).await?;
                install::keep_milestones(
                    &mut tx,
                    &MilestoneDays {
                        first_successful_run: composed.first_successful_run.as_deref(),
                        first_saved_function: composed.first_saved_function.as_deref(),
                        function_reused: composed.function_reused.as_deref(),
                    },
                )
                .await?;
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
        }
    }

    /// The reports to send now, oldest first: none while sharing is off or
    /// this process never sends.
    pub(crate) async fn unsent(&self) -> Result<Vec<Unsent>, Error> {
        if self.inner.block.is_some() {
            return Ok(Vec::new());
        }
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        let sharing = install::install(&mut tx)
            .await?
            .is_some_and(|install| install.sharing != 0);
        let reports = if sharing {
            install::unsent_reports(&mut tx).await?
        } else {
            Vec::new()
        };
        tx.commit().await?;
        Ok(reports
            .into_iter()
            .map(|row| Unsent {
                day: row.report_day,
                json: row.body,
            })
            .collect())
    }

    /// Records that the collector stored `day`'s report.
    pub(crate) async fn record_sent(&self, day: &str, now: i64) -> Result<(), Error> {
        let mut tx = self.inner.database.begin().await?;
        install::mark_sent(&mut tx, day, now).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Drops `day`'s report, which the collector will never accept.
    pub(crate) async fn record_rejected(&self, day: &str) -> Result<(), Error> {
        let mut tx = self.inner.database.begin().await?;
        install::delete_unsent(&mut tx, day).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Mints the install row on first use. Reports start on the day the
    /// first Workspace was created, and never before 2026-11-01.
    async fn ensure_install(&self, now: i64) -> Result<(), Error> {
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        let exists = install::install(&mut tx).await?.is_some();
        tx.commit().await?;
        if exists {
            return Ok(());
        }
        let today = day_of(now);
        let first = install::first_workspace_created(&self.inner.database)
            .await?
            .map_or(today, day_of)
            .min(today)
            .max(FIRST_REPORT_DAY);
        install::insert_install(
            &self.inner.database,
            &uuid::Uuid::new_v4().to_string(),
            &day_string(first),
            now,
        )
        .await
    }
}
