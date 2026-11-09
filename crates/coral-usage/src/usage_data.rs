//! The deployment's usage data: the sharing setting, the reports to send,
//! and what the Usage data page shows.

use std::sync::Arc;

use chrono::{Days, NaiveDate};
use coral_db::{Database, Transaction};
use coral_workspace::{AuthenticatedUser, OrganizationModule, OrganizationRole};

use crate::Error;
use crate::compose::{Composed, compose};
use crate::day::{day_of, day_start};
use crate::environment::{Build, SendBlock, SendEnvironment};
use crate::report::{Host, HostKind};
use crate::repository::activity::Window;
use crate::repository::install::{self, Install, Unsent};

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

fn missing_install() -> Error {
    Error::CorruptData("the usage install row is missing")
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
            .ok_or_else(missing_install)?;
        let waiting = if install.sharing {
            install::unsent_reports(&mut tx).await?.into_iter().next()
        } else {
            None
        };
        let next_report_json = if let Some(report) = waiting {
            report.json
        } else {
            let today = day_of(now);
            let window = Window {
                start: day_start(today),
                end: now.saturating_add(1),
            };
            self.compose(&mut tx, &install, today, window).await?.json
        };
        let last_sent = install::last_sent(&mut tx).await?;
        tx.commit().await?;
        Ok(UsageDataView {
            sharing: install.sharing,
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
        install::set_sharing(&mut tx, sharing, day_of(now), now).await?;
        tx.commit().await?;
        self.view(actor, now).await
    }

    /// Makes the report of each finished day not made yet, oldest first,
    /// then records that this process checked today. A day gets a report
    /// only when sharing is on and a process that may send checked on it, so
    /// a day that only blocked processes saw is skipped. A blocked process
    /// changes nothing. A report is composed in a snapshot and stored only if
    /// sharing is still on when its day is claimed.
    ///
    /// # Errors
    ///
    /// Returns a database failure; the days made before it stay made.
    pub async fn make_due_reports(&self, now: i64) -> Result<(), Error> {
        if self.inner.block.is_some() {
            return Ok(());
        }
        self.ensure_install(now).await?;
        let today = day_of(now);
        loop {
            let mut read = self.inner.database.begin_read_snapshot().await?;
            let install = install::install(&mut read)
                .await?
                .ok_or_else(missing_install)?;
            let day = install.next_report_day;
            if day >= today {
                read.commit().await?;
                break;
            }
            // Checks run hourly, so a process that may send checks on every
            // day it runs. Days after the last check had none.
            let seen = install.last_check_day.is_some_and(|checked| day <= checked);
            if !install.sharing || !seen {
                read.commit().await?;
                let mut tx = self.inner.database.begin().await?;
                install::advance_day(&mut tx, day, today).await?;
                tx.commit().await?;
                continue;
            }
            let next = day
                .checked_add_days(Days::new(1))
                .ok_or(Error::CorruptData("a usage report day has no next day"))?;
            let window = Window {
                start: day_start(day),
                end: day_start(next),
            };
            let composed = self.compose(&mut read, &install, day, window).await?;
            read.commit().await?;
            let mut tx = self.inner.database.begin().await?;
            if install::claim_day(&mut tx, day, next).await? {
                install::insert_report(&mut tx, day, &composed.json).await?;
                install::keep_milestones(&mut tx, composed.milestones).await?;
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
        }
        let mut tx = self.inner.database.begin().await?;
        install::mark_checked(&mut tx, today).await?;
        tx.commit().await?;
        Ok(())
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
            .is_some_and(|install| install.sharing);
        let reports = if sharing {
            install::unsent_reports(&mut tx).await?
        } else {
            Vec::new()
        };
        tx.commit().await?;
        Ok(reports)
    }

    /// Records that the collector stored `day`'s report.
    pub(crate) async fn record_sent(&self, day: NaiveDate, now: i64) -> Result<(), Error> {
        let mut tx = self.inner.database.begin().await?;
        install::mark_sent(&mut tx, day, now).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Drops `day`'s report, which the collector will never accept.
    pub(crate) async fn record_rejected(&self, day: NaiveDate) -> Result<(), Error> {
        let mut tx = self.inner.database.begin().await?;
        install::delete_unsent(&mut tx, day).await?;
        tx.commit().await?;
        Ok(())
    }

    /// The report for `day`, counting activity in `window`.
    async fn compose(
        &self,
        tx: &mut Transaction<'_>,
        install: &Install,
        day: NaiveDate,
        window: Window,
    ) -> Result<Composed, Error> {
        compose(tx, install, &self.inner.host, self.inner.build, day, window).await
    }

    /// Mints the install row on first use. Reports start today: an earlier
    /// day's report would carry today's version and totals.
    async fn ensure_install(&self, now: i64) -> Result<(), Error> {
        let mut tx = self.inner.database.begin_read_snapshot().await?;
        let exists = install::install(&mut tx).await?.is_some();
        tx.commit().await?;
        if exists {
            return Ok(());
        }
        let mut tx = self.inner.database.begin().await?;
        install::insert_install(&mut tx, &uuid::Uuid::new_v4().to_string(), day_of(now), now)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
