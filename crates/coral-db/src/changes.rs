//! Content-free, coalescing invalidation after explicitly marked commits.
//! This is a wake-up hint, not a durable event log or authorization evidence.
use std::time::Duration;

use sqlx::postgres::PgListener;
use tokio::sync::watch;

use crate::{Database, Error};

const CHANNEL_PREFIX: &str = "coral_committed_change_";
/// Default liveness period for the Postgres notification connection.
pub(crate) const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(5);

async fn postgres_listener(url: &str, channel: &str) -> Result<PgListener, Error> {
    // A dedicated connection, not a permanently borrowed query-pool slot.
    let mut listener = PgListener::connect(url).await?;
    sqlx::query("SELECT set_config('application_name', $1, false)")
        .bind(channel)
        .execute(&mut listener)
        .await?;
    listener.listen(channel).await?;
    listener.eager_reconnect(false);
    Ok(listener)
}

pub(crate) struct Changes {
    /// Each send is one coalescing wake-up; receivers track the version they last saw.
    pub(crate) changed: watch::Sender<()>,
    /// Whether the notification path can be trusted. Readers fail closed while false.
    available: watch::Sender<bool>,
    /// Postgres notifies through LISTEN so other workers observe the same commits.
    pub(crate) postgres_channel: Option<String>,
    listener: Option<tokio::task::JoinHandle<()>>,
}

impl Changes {
    pub(crate) fn local() -> Self {
        Self {
            changed: watch::channel(()).0,
            available: watch::channel(true).0,
            postgres_channel: None,
            listener: None,
        }
    }

    pub(crate) async fn postgres(
        url: &str,
        pool: &sqlx::PgPool,
        keepalive: Duration,
    ) -> Result<Self, Error> {
        // State namespaces can share one physical database. Hash the schema to keep
        // the channel and diagnostic application name within PostgreSQL's 63 bytes.
        let channel: String = sqlx::query_scalar("SELECT $1 || md5(current_schema()::text)")
            .bind(CHANNEL_PREFIX)
            .fetch_one(pool)
            .await?;
        let mut listener = postgres_listener(url, &channel).await?;
        let mut changes = Self::local();
        let wake = changes.changed.clone();
        let available = changes.available.clone();
        let url = url.to_owned();
        changes.postgres_channel = Some(channel.clone());
        changes.listener = Some(tokio::spawn(async move {
            let mut health =
                tokio::time::interval_at(tokio::time::Instant::now() + keepalive, keepalive);
            health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let connected = tokio::select! {
                    result = listener.try_recv() => {
                        if let Ok(Some(_)) = result { wake.send_replace(()); true } else { false }
                    }
                    _ = health.tick() => {
                        // Test this socket, not the query pool. A half-open LISTEN
                        // connection must not leave stale views alive indefinitely.
                        // Keepalives read no domain tables and never wake observers.
                        matches!(tokio::time::timeout(keepalive, sqlx::query("SELECT 1")
                            .execute(&mut listener)).await, Ok(Ok(_)))
                    }
                };
                if !connected {
                    available.send_replace(false);
                    // Never silently lose a notification gap. Readers fail closed;
                    // reconnection establishes LISTEN before announcing a resync.
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if let Ok(replacement) = postgres_listener(&url, &channel).await {
                            listener = replacement;
                            available.send_replace(true);
                            wake.send_replace(());
                            break;
                        }
                    }
                }
            }
        }));
        Ok(changes)
    }

    pub(crate) fn close(&self) {
        self.available.send_replace(false);
        if let Some(listener) = &self.listener {
            listener.abort();
        }
    }
}
impl Drop for Changes {
    fn drop(&mut self) {
        self.close();
    }
}

/// A bounded invalidation subscription. Subscribe before reading a snapshot.
/// Changes during that read remain pending; slow readers coalesce changes.
/// A disconnected backend invalidates the subscription rather than preserving stale data.
pub struct ChangeSubscription {
    changed: watch::Receiver<()>,
    available: watch::Receiver<bool>,
}
impl ChangeSubscription {
    /// Waits for a committed mutation or resynchronization. Cancel-safe.
    /// # Errors
    /// Returns an error when an outage begins; a later call waits for the resync.
    pub async fn changed(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                result = self.changed.changed() => {
                    result.map_err(|_closed| Error::ObservationUnavailable)?;
                    return self.check();
                }
                result = self.available.changed() => {
                    result.map_err(|_closed| Error::ObservationUnavailable)?;
                    self.check()?;
                }
            }
        }
    }

    /// Checks notification health without consuming a pending change.
    /// # Errors
    /// Returns an error during a notification connection outage.
    pub fn check(&self) -> Result<(), Error> {
        if *self.available.borrow() {
            Ok(())
        } else {
            Err(Error::ObservationUnavailable)
        }
    }
}
impl Database {
    /// Subscribes to explicit committed mutations, never reads or lock-only writes.
    /// No table names, row identities, payloads, or domain authority cross this interface.
    /// # Errors
    /// Returns an error when the notification connection is unavailable.
    pub fn subscribe_changes(&self) -> Result<ChangeSubscription, Error> {
        let changes = &self.access.changes;
        let subscription = ChangeSubscription {
            changed: changes.changed.subscribe(),
            available: changes.available.subscribe(),
        };
        subscription.check()?;
        Ok(subscription)
    }

    /// Terminates this database's Postgres notification connection, as an outage
    /// would. Other schemas' listeners in the same physical database keep running.
    /// `SQLite` has no notification connection, so this does nothing there.
    ///
    /// # Errors
    /// Returns an error when the termination query fails.
    #[cfg(feature = "test-support")]
    pub async fn interrupt_change_notifications(&self) -> Result<(), Error> {
        use sea_query::{Alias, Expr, ExprTrait as _, Query};
        let Some(channel) = self.access.changes.postgres_channel.clone() else {
            return Ok(());
        };
        self.execute(
            Query::select()
                .expr(Expr::cust("pg_terminate_backend(pid)"))
                .from(Alias::new("pg_stat_activity"))
                .and_where(Expr::cust("datname = current_database()"))
                .and_where(Expr::cust("pid <> pg_backend_pid()"))
                .and_where(Expr::col(Alias::new("application_name")).eq(channel))
                .to_owned(),
        )
        .await?;
        Ok(())
    }
}
