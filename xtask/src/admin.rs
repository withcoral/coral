//! Repository-side recovery for shared deployments no human can administer.
//!
//! Possession of the state database is the entire authority here. This module
//! authenticates and authorizes nobody, so recovering a `SQLite` deployment
//! requires filesystem access to the state directory, and recovering a
//! Postgres deployment requires the configured connection URL. No locality of
//! the host is claimed or checked.
//!
//! The tool is deliberately **non-migrating**. It reads and repairs an
//! existing repository database exactly as the server left it and never
//! applies a schema or state migration; bringing a database up to the current
//! schema is the server's job, not recovery's. `sqlx`'s `migrate` feature is
//! compiled into the linked crate regardless, so nothing in the type system
//! enforces this: no code path below may ever construct a migrator.
//!
//! The module compiles only under `xtask`'s off-by-default `admin` feature, so
//! the default `xtask` build compiles none of this code and resolves none of
//! the dependency edges it needs. No shipped Coral binary depends on `xtask` at
//! all, so none of this reaches a released artifact.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use etcetera::app_strategy::{AppStrategy as _, AppStrategyArgs, choose_native_strategy};
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::{
    AssertSqlSafe, ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Pool, Type,
};

/// The existing state database one recovery command operates on.
///
/// Configuration resolution follows the server's own rules and yields one of
/// these variants; every command then works against the already-migrated
/// database it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryDatabase {
    /// A `SQLite` state file inside the resolved state directory.
    Sqlite {
        /// Path to the existing state database file.
        path: PathBuf,
    },
    /// A Postgres database reached through the resolved connection URL.
    Postgres {
        /// Connection URL taken from the configured environment variable.
        url: String,
    },
}

/// The built-in local principal, which recovery refuses to appoint or rebind.
///
/// It is synthetic: it identifies the single-user local process rather than a
/// person who can authenticate against a shared deployment. Making it an owner
/// would leave the workspace exactly as unreachable as before.
///
/// This is a hand-copy of `coral-app`'s `identity::LOCAL_PRINCIPAL_ID`, which
/// is crate-private and so cannot be imported across the crate boundary. A test
/// pins the literal so a rename there fails loudly here instead of silently
/// classifying every local-owned workspace as human-owned.
pub(crate) const LOCAL_PRINCIPAL_ID: &str = "coral:local";

/// Membership role that carries workspace ownership.
const OWNER_ROLE: &str = "owner";

/// Environment variable naming a state-directory override, read by the server.
const CORAL_CONFIG_DIR: &str = "CORAL_CONFIG_DIR";

/// Placeholder printed for an absent or empty column value.
const ABSENT: &str = "-";

/// The refusal every command shares when the local principal is involved.
const REFUSES_LOCAL: &str = "`coral:local` is the built-in local principal, which identifies the \
     single-user process rather than a person who can authenticate against a shared deployment; \
     recovery refuses every operation involving it. Run `list-users` and pick an account marked \
     appointable";

/// The sentence that tells an operator no restart is coming.
const TAKES_EFFECT: &str = "membership is authorized per request, so this takes effect on that \
     user's next call; the server does not need restarting\n";

/// How long a repair waits for a write lock another process is holding.
///
/// Long enough to sit out any transaction a serving deployment takes, short
/// enough that an operator gets an answer instead of a hung terminal.
const WRITE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Explains the failure a repair hits when the server holds the write lock.
const WRITE_CONTENTION_HINT: &str = "the state database is held by another process, most likely \
     the running server; nothing was written, so retry, or stop the server for the moment the \
     repair takes";

/// Whether recovery may appoint this directory row as a workspace owner.
///
/// The built-in local principal is refused by `user_id` *and* by `issuer`: a
/// database whose synthetic row was hand-edited into a different id is still
/// the same unreachable principal.
fn appointable(user_id: &str, issuer: &str) -> bool {
    user_id != LOCAL_PRINCIPAL_ID && issuer != LOCAL_PRINCIPAL_ID
}

/// The current instant in the encoding the state database stores.
fn now_unix_nanos() -> Result<i64> {
    Utc::now()
        .timestamp_nanos_opt()
        .context("the system clock is outside the range this state database can store")
}

/// Lists every workspace in the state database with its ownership reachability.
///
/// Opens the database read-only, so it runs against a live deployment.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the `workspace-admin` subcommand that dispatches here lands in a follow-up change"
    )
)]
pub(crate) fn list_workspaces(config_dir_override: Option<PathBuf>) -> Result<String> {
    list_workspaces_on(&resolve_database(config_dir_override)?)
}

/// Lists every user in the state database, appointable accounts first.
///
/// The built-in local principal owns a real `users` row, so it is listed rather
/// than hidden: a workspace reported `local-owner-only` would otherwise name an
/// owner the listing denies exists. It is marked non-appointable and sorted
/// below every account `set-owner` will accept, because on a deployment where
/// nobody has authenticated yet its migration-stamped login is the most recent
/// one and it would otherwise head the listing as the single identity recovery
/// refuses.
///
/// Provider subjects are withheld unless `show_subjects` is set: the listing
/// exists to find a user id, not to publish everyone's upstream identity.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the `workspace-admin` subcommand that dispatches here lands in a follow-up change"
    )
)]
pub(crate) fn list_users(
    config_dir_override: Option<PathBuf>,
    show_subjects: bool,
) -> Result<String> {
    list_users_on(&resolve_database(config_dir_override)?, show_subjects)
}

/// Appoints an existing user as an owner of one workspace.
///
/// This is an add-or-promote and never a replacement: no other membership is
/// removed, demoted, or restamped, and re-running it once the user is already
/// an owner writes nothing at all. Membership is authorized per request, so the
/// appointment takes effect on that person's next call without a restart.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the `workspace-admin` subcommand that dispatches here lands in a follow-up change"
    )
)]
pub(crate) fn set_owner(
    config_dir_override: Option<PathBuf>,
    workspace_id: &str,
    user_id: &str,
) -> Result<String> {
    set_owner_on(
        &resolve_database(config_dir_override)?,
        workspace_id,
        user_id,
    )
}

/// Rebinds every user of one issuer to another after a provider rename.
///
/// Rebinding is an update of the `issuer` column and nothing else. The internal
/// `user_id` each membership points at is a primary key this never rewrites, so
/// no membership is orphaned and no second directory row is minted for the
/// person who already has one.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the `workspace-admin` subcommand that dispatches here lands in a follow-up change"
    )
)]
pub(crate) fn rebind_issuer(
    config_dir_override: Option<PathBuf>,
    from: &str,
    to: &str,
) -> Result<String> {
    rebind_issuer_on(&resolve_database(config_dir_override)?, from, to)
}

// The `*_on` forms below take an already-resolved database so the Postgres
// contracts can address the gate's server directly. Resolution reads process
// environment, which no test may mutate while its siblings run.

fn list_workspaces_on(database: &RecoveryDatabase) -> Result<String> {
    block_on(async {
        let connection = open(database, Access::ReadOnly).await?;
        let memberships = read_memberships(&connection).await?;
        Ok(render_workspaces(&summarize_workspaces(memberships)))
    })
}

fn list_users_on(database: &RecoveryDatabase, show_subjects: bool) -> Result<String> {
    block_on(async {
        let connection = open(database, Access::ReadOnly).await?;
        let mut users = read_users(&connection).await?;
        // Appointable accounts first, then most-recent login: the documented
        // recovery sequence is to have the intended owner authenticate once and
        // then appoint them, so the person who just logged in leads the list.
        users.sort_by(|left, right| {
            right
                .appointable()
                .cmp(&left.appointable())
                .then_with(|| {
                    right
                        .last_login_at_unix_nanos
                        .cmp(&left.last_login_at_unix_nanos)
                })
                .then_with(|| left.user_id.cmp(&right.user_id))
        });
        Ok(render_users(&users, show_subjects))
    })
}

fn set_owner_on(database: &RecoveryDatabase, workspace_id: &str, user_id: &str) -> Result<String> {
    // Refused before the database is even opened: the local principal is
    // synthetic, and appointing it would leave the workspace exactly as
    // unreachable as it is now.
    if user_id == LOCAL_PRINCIPAL_ID {
        bail!("{REFUSES_LOCAL}");
    }
    let now_unix_nanos = now_unix_nanos()?;
    let appointment = block_on(async {
        let connection = open(database, Access::Writable).await?;
        appoint(&connection, workspace_id, user_id, now_unix_nanos).await
    })?;

    Ok(match appointment {
        Appointment::AlreadyOwner => {
            format!("{user_id} already owns workspace `{workspace_id}`; nothing was written\n")
        }
        Appointment::Promoted { from } => format!(
            "promoted {user_id} from `{from}` to `{OWNER_ROLE}` of workspace `{workspace_id}`\n{TAKES_EFFECT}"
        ),
        Appointment::Added => {
            format!("appointed {user_id} as an owner of workspace `{workspace_id}`\n{TAKES_EFFECT}")
        }
    })
}

fn rebind_issuer_on(database: &RecoveryDatabase, from: &str, to: &str) -> Result<String> {
    if from == LOCAL_PRINCIPAL_ID || to == LOCAL_PRINCIPAL_ID {
        bail!("{REFUSES_LOCAL}");
    }
    if from == to {
        bail!("--from and --to name the same issuer `{from}`, so there is nothing to rebind");
    }
    let rebind = block_on(async {
        let connection = open(database, Access::Writable).await?;
        rebind(&connection, from, to).await
    })?;

    Ok(match rebind {
        Rebind::Rebound(rebound) => format!(
            "rebound {rebound} user(s) from issuer `{from}` to `{to}`; internal user ids and \
             every workspace membership are unchanged\n"
        ),
        Rebind::AlreadyRebound(bound) => format!(
            "no user is bound to issuer `{from}`, and {bound} user(s) are already bound to \
             `{to}`; nothing was written\n"
        ),
    })
}

/// Runs one recovery command's database work on a private current-thread runtime.
fn block_on<T>(work: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("start the recovery runtime")?
        .block_on(work)
}

// -- configuration resolution ------------------------------------------------

/// A `[database]` section as configured, before the environment is consulted.
///
/// This mirrors the server's own split between the configured and the resolved
/// form so the two can be compared rule by rule.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConfiguredDatabase {
    /// A `SQLite` file, already resolved against the state directory.
    Sqlite { path: PathBuf },
    /// A Postgres database whose URL lives in the named environment variable.
    Postgres { url_env: String },
}

/// Top-level `config.toml` shape, narrowed to the section recovery needs.
#[derive(Debug, Deserialize)]
struct PersistedConfig {
    #[serde(default)]
    database: Option<RawDatabaseSection>,
}

/// The `[database]` section exactly as written, before validation.
#[derive(Debug, Deserialize)]
struct RawDatabaseSection {
    #[serde(default)]
    backend: Option<PersistedBackend>,
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    url_env: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, toml::Value>,
}

/// The database backends the server accepts in `config.toml`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PersistedBackend {
    Sqlite,
    Postgres,
}

/// Resolves the state database the server would open on this host.
fn resolve_database(config_dir_override: Option<PathBuf>) -> Result<RecoveryDatabase> {
    let config_dir = resolve_config_dir(config_dir_override)?;
    match configured_database(&config_dir)? {
        ConfiguredDatabase::Sqlite { path } => Ok(RecoveryDatabase::Sqlite { path }),
        ConfiguredDatabase::Postgres { url_env } => {
            let url = crate::env::required_var(&url_env).with_context(|| {
                format!(
                    "database backend 'postgres' requires environment variable `{url_env}` to hold the connection URL"
                )
            })?;
            Ok(RecoveryDatabase::Postgres { url })
        }
    }
}

/// Picks the state directory the server would use: explicit override first,
/// then `CORAL_CONFIG_DIR`, then the platform-native application directory.
fn resolve_config_dir(config_dir_override: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(config_dir) = config_dir_override.or_else(config_dir_override_from_env) {
        return Ok(config_dir);
    }

    let strategy = choose_native_strategy(AppStrategyArgs {
        top_level_domain: "com".to_string(),
        author: "withcoral".to_string(),
        app_name: "coral".to_string(),
    })
    .context("locate this user's Coral state directory")?;
    #[cfg(target_os = "macos")]
    let config_dir = strategy.data_dir();
    #[cfg(not(target_os = "macos"))]
    let config_dir = strategy.config_dir();
    Ok(config_dir)
}

/// Reads `CORAL_CONFIG_DIR` as an OS string, exactly as the server does.
#[expect(
    clippy::disallowed_methods,
    reason = "recovery must accept a non-UTF-8 CORAL_CONFIG_DIR exactly as the server does, and xtask's env module exposes no optional OS-string accessor"
)]
fn config_dir_override_from_env() -> Option<PathBuf> {
    std::env::var_os(CORAL_CONFIG_DIR).map(PathBuf::from)
}

/// Applies the server's `[database]` rules to a state directory's `config.toml`.
fn configured_database(config_dir: &Path) -> Result<ConfiguredDatabase> {
    let config_file = config_dir.join("config.toml");
    let default_sqlite = || ConfiguredDatabase::Sqlite {
        path: config_dir.join("coral.db"),
    };

    if !config_file
        .try_exists()
        .with_context(|| format!("read {}", config_file.display()))?
    {
        return Ok(default_sqlite());
    }

    let raw = std::fs::read_to_string(&config_file)
        .with_context(|| format!("read {}", config_file.display()))?;
    let persisted: PersistedConfig =
        toml::from_str(&raw).with_context(|| format!("parse {}", config_file.display()))?;
    let Some(section) = persisted.database else {
        return Ok(default_sqlite());
    };

    if let Some(field) = section.extra.keys().next() {
        bail!("unsupported [database].{field} configuration key");
    }
    let Some(backend) = section.backend else {
        bail!("[database].backend is required when [database] is present");
    };

    match backend {
        PersistedBackend::Sqlite => {
            if section.url_env.is_some() {
                bail!("database backend 'sqlite' does not support [database].url_env");
            }
            Ok(section
                .path
                .map_or_else(default_sqlite, |path| ConfiguredDatabase::Sqlite {
                    path: if path.is_absolute() {
                        path
                    } else {
                        config_dir.join(path)
                    },
                }))
        }
        PersistedBackend::Postgres => {
            if section.path.is_some() {
                bail!("database backend 'postgres' does not support [database].path");
            }
            let Some(url_env) = section.url_env else {
                bail!("database backend 'postgres' requires [database].url_env");
            };
            Ok(ConfiguredDatabase::Postgres { url_env })
        }
    }
}

// -- opening the existing database -------------------------------------------

/// A handle on state the server owns.
enum RecoveryConnection {
    /// Pool over the `SQLite` state file.
    Sqlite(SqlitePool),
    /// Pool over the configured Postgres database.
    Postgres(PgPool),
}

/// How a command needs to hold the state database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    /// Listings, which take no lock a serving deployment can contend with.
    ReadOnly,
    /// Repairs, which hold the write lock for one transaction and no longer.
    Writable,
}

/// Opens the existing state database with exactly the access one command needs.
///
/// Read-only is both a safety property and a concurrency one. That handle takes
/// no write lock, so it coexists with a running server, and the operating
/// system refuses any write this process could still attempt. The one case it
/// cannot serve is a `SQLite` database left in WAL mode with no server running
/// and no `-shm` file present, which read-only connections cannot initialise.
///
/// A writable handle gives that guarantee up, so it buys back what it can. It
/// waits a bounded [`WRITE_LOCK_TIMEOUT`] for a contended `SQLite` file and
/// bounds a contended Postgres row with the same `lock_timeout`, because an
/// operator repairing a locked-out deployment is owed a clear refusal rather
/// than a terminal that never returns. Neither backend is asked to change its
/// journal mode or any other durable connection setting.
///
/// Nothing here creates a database, and nothing here runs a migration.
async fn open(database: &RecoveryDatabase, access: Access) -> Result<RecoveryConnection> {
    match database {
        RecoveryDatabase::Sqlite { path } => {
            if !path
                .try_exists()
                .with_context(|| format!("look for the state database at {}", path.display()))?
            {
                bail!(
                    "no state database at {}; recovery needs filesystem access to the deployment's state directory",
                    path.display()
                );
            }
            let options = SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false)
                .read_only(access == Access::ReadOnly)
                .busy_timeout(WRITE_LOCK_TIMEOUT);
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .with_context(|| format!("open the state database at {}", path.display()))?;
            Ok(RecoveryConnection::Sqlite(pool))
        }
        RecoveryDatabase::Postgres { url } => {
            let options = postgres_connect_options(url)?;
            let options = match access {
                Access::ReadOnly => options.options([("default_transaction_read_only", "on")]),
                Access::Writable => {
                    options.options([("lock_timeout", WRITE_LOCK_TIMEOUT.as_millis().to_string())])
                }
            };
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                // The URL carries credentials, so it never reaches an error message.
                .context("connect to the configured Postgres state database")?;
            Ok(RecoveryConnection::Postgres(pool))
        }
    }
}

/// Builds Postgres connect options under the server's transport rules.
///
/// The server refuses a remote URL that does not authenticate the server it
/// reaches; recovery holds the same line rather than quietly accepting weaker
/// transport for the same database.
fn postgres_connect_options(url: &str) -> Result<PgConnectOptions> {
    let parsed = url::Url::parse(url).context("the configured Postgres URL is not a valid URL")?;
    let mut explicit_ssl_mode = None;
    for (key, value) in parsed.query_pairs() {
        if key == "sslmode" || key == "ssl-mode" {
            explicit_ssl_mode = Some(
                value
                    .parse::<PgSslMode>()
                    .context("the configured Postgres URL sets an unknown sslmode")?,
            );
        }
    }
    let options = PgConnectOptions::from_str(parsed.as_str())
        .context("the configured Postgres URL is not a valid connection string")?;

    let local = options.get_socket().is_some()
        || options.get_host().starts_with('/')
        || options.get_host().eq_ignore_ascii_case("localhost")
        || options
            .get_host()
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !local && !matches!(explicit_ssl_mode, Some(PgSslMode::VerifyFull)) {
        bail!("remote Postgres database URLs must set sslmode=verify-full");
    }

    Ok(options)
}

// -- reading -----------------------------------------------------------------

/// One workspace row joined with one of its memberships, if it has any.
#[derive(Debug, sqlx::FromRow)]
struct MembershipRow {
    workspace_id: String,
    user_id: Option<String>,
    member_role: Option<String>,
}

/// One row of the `users` table.
#[derive(Debug, sqlx::FromRow)]
struct UserRow {
    user_id: String,
    issuer: String,
    subject: String,
    display_name: Option<String>,
    created_at_unix_nanos: i64,
    last_login_at_unix_nanos: i64,
}

impl UserRow {
    /// Whether `set-owner` would accept this row, by the same rule it applies.
    fn appointable(&self) -> bool {
        appointable(&self.user_id, &self.issuer)
    }
}

/// Every workspace with every membership it has, workspaces without one
/// included. The statement text is shared, but `sqlx` carries no backend-
/// agnostic driver, so each backend executes it against its own pool.
const MEMBERSHIPS_SQL: &str = "SELECT w.id AS workspace_id, m.user_id AS user_id, \
     m.role AS member_role FROM workspaces w \
     LEFT JOIN workspace_members m ON m.workspace_id = w.id";

/// Every user the deployment could appoint as an owner.
const USERS_SQL: &str = "SELECT user_id, issuer, subject, display_name, \
     created_at_unix_nanos, last_login_at_unix_nanos FROM users";

/// Explains a query failure that a pre-access-control database would produce.
const BEHIND_SCHEMA_HINT: &str = "if this reports a missing table, the state database predates the \
     workspace access-control migration; start the server once to migrate it, because recovery \
     never will";

async fn read_memberships(connection: &RecoveryConnection) -> Result<Vec<MembershipRow>> {
    match connection {
        RecoveryConnection::Sqlite(pool) => sqlx::query_as(MEMBERSHIPS_SQL).fetch_all(pool).await,
        RecoveryConnection::Postgres(pool) => sqlx::query_as(MEMBERSHIPS_SQL).fetch_all(pool).await,
    }
    .context(BEHIND_SCHEMA_HINT)
    .context("read workspace memberships")
}

async fn read_users(connection: &RecoveryConnection) -> Result<Vec<UserRow>> {
    match connection {
        RecoveryConnection::Sqlite(pool) => sqlx::query_as(USERS_SQL).fetch_all(pool).await,
        RecoveryConnection::Postgres(pool) => sqlx::query_as(USERS_SQL).fetch_all(pool).await,
    }
    .context(BEHIND_SCHEMA_HINT)
    .context("read users")
}

// -- repairing ---------------------------------------------------------------

/// How the shared statement text below spells a bound parameter.
///
/// `SQLite` accepts only `?` and Postgres only `$n`, so each statement is
/// written once with `?` and rewritten positionally per backend. This is exact
/// only because no statement here contains a literal `?` outside a placeholder;
/// keep it that way, and bind values rather than interpolating them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    /// `SQLite`, which takes the template unchanged.
    Positional,
    /// Postgres, which numbers each placeholder in order.
    Numbered,
}

/// Renders one statement template for a backend.
///
/// The result is asserted SQL-safe here rather than at each call site, and the
/// assertion holds by construction: the only inputs are the `&'static str`
/// templates below and a placeholder rewrite that copies every other character
/// through. No caller-supplied value reaches the statement text; values are
/// bound.
fn statement(template: &'static str, dialect: Dialect) -> AssertSqlSafe<String> {
    AssertSqlSafe(match dialect {
        Dialect::Positional => template.to_string(),
        Dialect::Numbered => {
            let mut rendered = String::with_capacity(template.len() + 8);
            let mut next = 1_u32;
            for character in template.chars() {
                if character == '?' {
                    rendered.push('$');
                    rendered.push_str(&next.to_string());
                    next += 1;
                } else {
                    rendered.push(character);
                }
            }
            rendered
        }
    })
}

/// Starts the one transaction a repair runs in.
///
/// `SQLite` takes the write lock up front so the bounded busy timeout applies:
/// a deferred transaction that upgrades from a read lock is refused instantly
/// instead of waiting, which turns ordinary contention into a spurious failure.
const SQLITE_BEGIN: &str = "BEGIN IMMEDIATE";
const POSTGRES_BEGIN: &str = "BEGIN";

/// The workspace a repair names, if the state database has it.
const WORKSPACE_BY_ID_SQL: &str = "SELECT id FROM workspaces WHERE id = ?";

/// The directory row a repair names, with the issuer that decides appointability.
const USER_BY_ID_SQL: &str = "SELECT user_id, issuer FROM users WHERE user_id = ?";

/// The role one user already holds in one workspace, if any.
const MEMBERSHIP_ROLE_SQL: &str =
    "SELECT role FROM workspace_members WHERE workspace_id = ? AND user_id = ?";

/// Promotes an existing membership, leaving its creation stamp alone.
const PROMOTE_MEMBER_SQL: &str =
    "UPDATE workspace_members SET role = ? WHERE workspace_id = ? AND user_id = ?";

/// Adds an owner membership the workspace does not have.
const ADD_OWNER_SQL: &str = "INSERT INTO workspace_members (workspace_id, user_id, role, \
     created_at_unix_nanos) VALUES (?, ?, ?, ?)";

/// Every directory row bound to one issuer, the local principal included so the
/// refusal can name it rather than silently skipping it.
const USERS_BY_ISSUER_SQL: &str = "SELECT user_id FROM users WHERE issuer = ?";

/// Rebinds users to the new issuer and names exactly the rows it moved.
///
/// `RETURNING` makes the report the rows themselves rather than a count taken
/// from a separate statement, and the `user_id` guard means the local principal
/// cannot be carried along even by a database that binds it to a real issuer.
const REBIND_ISSUER_SQL: &str =
    "UPDATE users SET issuer = ? WHERE issuer = ? AND user_id <> ? RETURNING user_id";

/// What appointing an owner turned out to mean for this workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Appointment {
    /// The user already owned the workspace, so nothing was written.
    AlreadyOwner,
    /// An existing membership was promoted from the role it held.
    Promoted { from: String },
    /// A membership was added where the user had none.
    Added,
}

/// What rebinding an issuer turned out to mean for this directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rebind {
    /// This many rows moved to the new issuer.
    Rebound(usize),
    /// Nothing was bound to the old issuer and this many rows already sit on
    /// the new one, which is what a repeated run of the same rebind looks like.
    AlreadyRebound(usize),
}

async fn appoint(
    connection: &RecoveryConnection,
    workspace_id: &str,
    user_id: &str,
    now_unix_nanos: i64,
) -> Result<Appointment> {
    match connection {
        RecoveryConnection::Sqlite(pool) => {
            appoint_owner(
                pool,
                Dialect::Positional,
                SQLITE_BEGIN,
                workspace_id,
                user_id,
                now_unix_nanos,
            )
            .await
        }
        RecoveryConnection::Postgres(pool) => {
            appoint_owner(
                pool,
                Dialect::Numbered,
                POSTGRES_BEGIN,
                workspace_id,
                user_id,
                now_unix_nanos,
            )
            .await
        }
    }
}

async fn rebind(connection: &RecoveryConnection, from: &str, to: &str) -> Result<Rebind> {
    match connection {
        RecoveryConnection::Sqlite(pool) => {
            rebind_issuer_rows(pool, Dialect::Positional, SQLITE_BEGIN, from, to).await
        }
        RecoveryConnection::Postgres(pool) => {
            rebind_issuer_rows(pool, Dialect::Numbered, POSTGRES_BEGIN, from, to).await
        }
    }
}

/// Adds or promotes one owner membership inside a single transaction.
///
/// Every refusal happens before the first write and abandons the transaction,
/// so a rejected appointment cannot leave a half-appointed workspace behind. No
/// statement here touches another workspace, another member, or a column the
/// caller did not ask to change: a promotion rewrites `role` alone and keeps
/// the membership's original creation stamp, which is what makes a second run
/// of the same appointment a genuine no-op rather than a quiet restamp.
///
/// One body serves both backends deliberately. Two hand-written copies of a
/// mutation this consequential would be two places for a fix to be applied to
/// only one of.
async fn appoint_owner<DB>(
    pool: &Pool<DB>,
    dialect: Dialect,
    begin: &'static str,
    workspace_id: &str,
    user_id: &str,
    now_unix_nanos: i64,
) -> Result<Appointment>
where
    DB: Database,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'a> &'a str: Encode<'a, DB> + Type<DB>,
    for<'a> i64: Encode<'a, DB> + Type<DB>,
    for<'a> String: Decode<'a, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
{
    let mut transaction = pool
        .begin_with(begin)
        .await
        .context(WRITE_CONTENTION_HINT)
        .context("open the appointment transaction")?;

    let workspace: Option<(String,)> = sqlx::query_as(statement(WORKSPACE_BY_ID_SQL, dialect))
        .bind(workspace_id)
        .fetch_optional(&mut *transaction)
        .await
        .context(BEHIND_SCHEMA_HINT)
        .context("look up the workspace")?;
    if workspace.is_none() {
        bail!("no workspace `{workspace_id}` in this state database; run `list-workspaces`");
    }

    let user: Option<(String, String)> = sqlx::query_as(statement(USER_BY_ID_SQL, dialect))
        .bind(user_id)
        .fetch_optional(&mut *transaction)
        .await
        .context(BEHIND_SCHEMA_HINT)
        .context("look up the user")?;
    let Some((_, issuer)) = user else {
        bail!(
            "no user `{user_id}` in this state database; recovery can only appoint someone who \
             already has a directory row, so have them authenticate against the deployment once \
             and then run `list-users`"
        );
    };
    if !appointable(user_id, &issuer) {
        bail!("{REFUSES_LOCAL}");
    }

    let existing: Option<(String,)> = sqlx::query_as(statement(MEMBERSHIP_ROLE_SQL, dialect))
        .bind(workspace_id)
        .bind(user_id)
        .fetch_optional(&mut *transaction)
        .await
        .context(BEHIND_SCHEMA_HINT)
        .context("look up the existing membership")?;

    let appointment = match existing {
        Some((role,)) if role == OWNER_ROLE => Appointment::AlreadyOwner,
        Some((role,)) => {
            sqlx::query(statement(PROMOTE_MEMBER_SQL, dialect))
                .bind(OWNER_ROLE)
                .bind(workspace_id)
                .bind(user_id)
                .execute(&mut *transaction)
                .await
                .context(WRITE_CONTENTION_HINT)
                .context("promote the existing membership")?;
            Appointment::Promoted { from: role }
        }
        None => {
            sqlx::query(statement(ADD_OWNER_SQL, dialect))
                .bind(workspace_id)
                .bind(user_id)
                .bind(OWNER_ROLE)
                .bind(now_unix_nanos)
                .execute(&mut *transaction)
                .await
                .context(WRITE_CONTENTION_HINT)
                .context("add the owner membership")?;
            Appointment::Added
        }
    };

    transaction
        .commit()
        .await
        .context(WRITE_CONTENTION_HINT)
        .context("commit the appointment")?;
    Ok(appointment)
}

/// Moves every user of one issuer to another inside a single transaction.
///
/// The `users` primary key is the internal `user_id` and memberships reference
/// it, so rebinding an issuer touches neither. A directory row is never
/// inserted here: minting a second row for someone who already has one is the
/// exact failure this command exists to avoid.
async fn rebind_issuer_rows<DB>(
    pool: &Pool<DB>,
    dialect: Dialect,
    begin: &'static str,
    from: &str,
    to: &str,
) -> Result<Rebind>
where
    DB: Database,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'a> &'a str: Encode<'a, DB> + Type<DB>,
    for<'a> String: Decode<'a, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
{
    let mut transaction = pool
        .begin_with(begin)
        .await
        .context(WRITE_CONTENTION_HINT)
        .context("open the rebind transaction")?;

    let bound: Vec<(String,)> = sqlx::query_as(statement(USERS_BY_ISSUER_SQL, dialect))
        .bind(from)
        .fetch_all(&mut *transaction)
        .await
        .context(BEHIND_SCHEMA_HINT)
        .context("read the users bound to the retiring issuer")?;
    if bound.iter().any(|(user_id,)| user_id == LOCAL_PRINCIPAL_ID) {
        bail!("issuer `{from}` binds the built-in local principal. {REFUSES_LOCAL}");
    }

    if bound.is_empty() {
        let already: Vec<(String,)> = sqlx::query_as(statement(USERS_BY_ISSUER_SQL, dialect))
            .bind(to)
            .fetch_all(&mut *transaction)
            .await
            .context(BEHIND_SCHEMA_HINT)
            .context("read the users already bound to the new issuer")?;
        if already.is_empty() {
            bail!(
                "no user is bound to issuer `{from}` and none is bound to `{to}` either; run \
                 `list-users` to see the issuers this state database actually holds"
            );
        }
        return Ok(Rebind::AlreadyRebound(already.len()));
    }

    let rebound: Vec<(String,)> = sqlx::query_as(statement(REBIND_ISSUER_SQL, dialect))
        .bind(to)
        .bind(from)
        .bind(LOCAL_PRINCIPAL_ID)
        .fetch_all(&mut *transaction)
        .await
        .context(WRITE_CONTENTION_HINT)
        .context("rebind the users of the retiring issuer")?;

    transaction
        .commit()
        .await
        .context(WRITE_CONTENTION_HINT)
        .context("commit the rebind")?;
    Ok(Rebind::Rebound(rebound.len()))
}

// -- summarising and rendering -----------------------------------------------

/// Whether a workspace's stored ownership can reach an authenticated caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnershipStatus {
    /// No owner at all: concealed from every ordinary caller, members included.
    ZeroOwners,
    /// Owned, but only by the synthetic local principal: validly owned in
    /// storage and equally unreachable by any authenticated user.
    LocalOwnerOnly,
    /// At least one human owner, so the workspace is reachable as designed.
    HumanOwned,
}

impl OwnershipStatus {
    /// The label the listing prints for this status.
    fn label(self) -> &'static str {
        match self {
            Self::ZeroOwners => "zero-owners",
            Self::LocalOwnerOnly => "local-owner-only",
            Self::HumanOwned => "human-owned",
        }
    }
}

/// One workspace's ownership as recovery reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceOwnership {
    id: String,
    owners: usize,
    members: usize,
    status: OwnershipStatus,
}

/// Tallies memberships per workspace and classifies each workspace's ownership.
fn summarize_workspaces(rows: Vec<MembershipRow>) -> Vec<WorkspaceOwnership> {
    /// Owners, human owners, and members counted for one workspace.
    #[derive(Default)]
    struct Tally {
        owners: usize,
        human_owners: usize,
        members: usize,
    }

    let mut tallies: BTreeMap<String, Tally> = BTreeMap::new();
    for row in rows {
        let tally = tallies.entry(row.workspace_id).or_default();
        // A workspace with no memberships still arrives, as a row of nulls.
        let Some(user_id) = row.user_id else {
            continue;
        };
        tally.members += 1;
        if row.member_role.as_deref() == Some(OWNER_ROLE) {
            tally.owners += 1;
            if user_id != LOCAL_PRINCIPAL_ID {
                tally.human_owners += 1;
            }
        }
    }

    tallies
        .into_iter()
        .map(|(id, tally)| WorkspaceOwnership {
            id,
            owners: tally.owners,
            members: tally.members,
            status: if tally.owners == 0 {
                OwnershipStatus::ZeroOwners
            } else if tally.human_owners == 0 {
                OwnershipStatus::LocalOwnerOnly
            } else {
                OwnershipStatus::HumanOwned
            },
        })
        .collect()
}

fn render_workspaces(workspaces: &[WorkspaceOwnership]) -> String {
    if workspaces.is_empty() {
        return "no workspaces recorded in this state database\n".to_string();
    }
    let rows: Vec<Vec<String>> = workspaces
        .iter()
        .map(|workspace| {
            vec![
                workspace.id.clone(),
                workspace.owners.to_string(),
                workspace.members.to_string(),
                workspace.status.label().to_string(),
            ]
        })
        .collect();
    render_table(&["WORKSPACE", "OWNERS", "MEMBERS", "REACHABILITY"], &rows)
}

/// The footer printed when the listing contains a row `set-owner` will refuse.
///
/// The column already says `no`; this says why, because the built-in principal
/// renders as an ordinary named account and an operator has no other way to
/// learn that the one identity at hand is the one that cannot be appointed.
const NON_APPOINTABLE_FOOTER: &str = "\nnot appointable: the built-in local principal identifies \
     the single-user process, not a person who can authenticate; `set-owner` refuses it\n";

fn render_users(users: &[UserRow], show_subjects: bool) -> String {
    if users.is_empty() {
        return "no users recorded in this state database\n".to_string();
    }
    let mut headers = vec![
        "USER ID",
        "DISPLAY NAME",
        "ISSUER",
        "APPOINTABLE",
        "LAST LOGIN",
        "CREATED",
    ];
    let rows: Vec<Vec<String>> = users
        .iter()
        .map(|user| {
            let mut row = vec![
                user.user_id.clone(),
                present(user.display_name.as_deref()),
                present(Some(user.issuer.as_str())),
            ];
            if show_subjects {
                row.push(present(Some(user.subject.as_str())));
            }
            row.push(if user.appointable() { "yes" } else { "no" }.to_string());
            row.push(timestamp(user.last_login_at_unix_nanos));
            row.push(timestamp(user.created_at_unix_nanos));
            row
        })
        .collect();
    if show_subjects {
        headers.insert(3, "SUBJECT");
    }

    let mut rendered = render_table(&headers, &rows);
    if users.iter().any(|user| !user.appointable()) {
        rendered.push_str(NON_APPOINTABLE_FOOTER);
    }
    rendered
}

/// Renders an optional column value, marking absent and empty ones.
fn present(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.is_empty() => value.to_string(),
        _ => ABSENT.to_string(),
    }
}

/// Renders a stored nanosecond timestamp as a UTC instant.
fn timestamp(unix_nanos: i64) -> String {
    DateTime::<Utc>::from_timestamp_nanos(unix_nanos).to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Renders headers and rows as a column-aligned table.
fn render_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers
        .iter()
        .map(|header| header.chars().count())
        .collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }

    let mut rendered = String::new();
    push_row(&mut rendered, &widths, headers.iter().copied());
    for row in rows {
        push_row(&mut rendered, &widths, row.iter().map(String::as_str));
    }
    rendered
}

fn push_row<'a>(rendered: &mut String, widths: &[usize], cells: impl IntoIterator<Item = &'a str>) {
    let mut line = String::new();
    for (width, cell) in widths.iter().zip(cells) {
        if !line.is_empty() {
            line.push_str("  ");
        }
        line.push_str(cell);
        for _ in cell.chars().count()..*width {
            line.push(' ');
        }
    }
    rendered.push_str(line.trim_end());
    rendered.push('\n');
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::fs;
    use std::time::Duration;

    use sqlx::migrate::Migrator;
    use tempfile::{TempDir, tempdir};

    use super::{
        ConfiguredDatabase, LOCAL_PRINCIPAL_ID, OWNER_ROLE, Path, PathBuf, PgPool, PgPoolOptions,
        RecoveryDatabase, SqliteConnectOptions, SqlitePool, SqlitePoolOptions, configured_database,
        list_users, list_users_on, list_workspaces, list_workspaces_on, postgres_connect_options,
        rebind_issuer, rebind_issuer_on, set_owner, set_owner_on,
    };

    /// The server's own migration set, read at runtime so the fixtures below
    /// carry real `sqlx` versions and checksums rather than imitations.
    const MIGRATIONS_DIR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../crates/coral-app/migrations"
    );

    /// Description of the migration that introduces users and memberships.
    const ACCESS_CONTROL_MIGRATION: &str = "workspace access control";

    /// A subject distinctive enough that finding it anywhere is conclusive.
    const SUBJECT_NEEDLE: &str = "needle-subject-4f19c7d2e8";

    /// The issuer the seeded human accounts authenticate through.
    const ISSUER: &str = "https://issuer.test";

    /// The issuer a provider rename moves them to.
    const RENAMED_ISSUER: &str = "https://issuer.test.example";

    /// The display name production writes for the built-in local principal
    /// (`coral-app`'s `LOCAL_USER_DISPLAY_NAME`). Seeding anything else - an
    /// absent name renders as a dash - would understate how ordinary that row
    /// looks in a listing an operator is reading to choose an owner.
    const LOCAL_DISPLAY_NAME: &str = "Local";

    /// A human account that already owns something.
    const ADA: &str = "11111111-1111-4111-8111-111111111111";

    /// A human account that starts as an ordinary member.
    const GRACE: &str = "22222222-2222-4222-8222-222222222222";

    /// A directory row carrying the local principal's issuer under a
    /// real-looking id, as a hand-edited or imported database can.
    const STRAY_LOCAL: &str = "44444444-4444-4444-8444-444444444444";

    /// Everything about a `SQLite` database that a migration would disturb.
    #[derive(Debug, PartialEq, Eq)]
    struct Snapshot {
        migrations: Vec<(i64, Vec<u8>, bool)>,
        schema: Vec<(String, String, Option<String>)>,
        bytes: Vec<u8>,
    }

    /// The literal that classifies a workspace as unreachable-but-owned.
    ///
    /// It is hand-copied from `coral-app`'s crate-private
    /// `identity::LOCAL_PRINCIPAL_ID`, and has already been renamed once
    /// (`local` to `coral:local`). Pinning it here turns the next rename into a
    /// failing test rather than a listing that silently reports every
    /// local-owned workspace as human-owned.
    #[test]
    fn local_principal_id_matches_the_app_identity_literal() {
        assert_eq!(LOCAL_PRINCIPAL_ID, "coral:local");
    }

    #[test]
    fn resolves_the_default_sqlite_path_without_a_config_file() {
        let temp = tempdir().expect("temp dir");

        let configured = configured_database(temp.path()).expect("resolve database config");

        assert_eq!(
            configured,
            ConfiguredDatabase::Sqlite {
                path: temp.path().join("coral.db")
            }
        );
    }

    #[test]
    fn resolves_configured_sqlite_and_postgres_backends_like_the_server() {
        let temp = tempdir().expect("temp dir");
        write_config(
            temp.path(),
            "[database]\nbackend = \"sqlite\"\npath = \"state/custom.db\"\n",
        );

        assert_eq!(
            configured_database(temp.path()).expect("resolve relative sqlite path"),
            ConfiguredDatabase::Sqlite {
                path: temp.path().join("state/custom.db")
            }
        );

        write_config(
            temp.path(),
            "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
        );

        assert_eq!(
            configured_database(temp.path()).expect("resolve postgres url env"),
            ConfiguredDatabase::Postgres {
                url_env: "CORAL_DATABASE_URL".to_string()
            }
        );
    }

    #[test]
    fn rejects_the_database_configurations_the_server_rejects() {
        let temp = tempdir().expect("temp dir");

        for (raw, expected) in [
            (
                "[database]\nurl_env = \"CORAL_DATABASE_URL\"\n",
                "[database].backend is required",
            ),
            (
                "[database]\nbackend = \"sqlite\"\nurl_env = \"CORAL_DATABASE_URL\"\n",
                "database backend 'sqlite' does not support [database].url_env",
            ),
            (
                "[database]\nbackend = \"postgres\"\npath = \"coral.db\"\nurl_env = \"X\"\n",
                "database backend 'postgres' does not support [database].path",
            ),
            (
                "[database]\nbackend = \"postgres\"\n",
                "database backend 'postgres' requires [database].url_env",
            ),
            (
                "[database]\nbackend = \"sqlite\"\nurl_environment = \"X\"\n",
                "unsupported [database].url_environment configuration key",
            ),
        ] {
            write_config(temp.path(), raw);

            let error = configured_database(temp.path())
                .expect_err("the server would reject this configuration");

            assert!(
                error.to_string().contains(expected),
                "unexpected error for {raw:?}: {error}"
            );
        }
    }

    /// Recovery holds the server's transport line: a remote Postgres URL that
    /// does not authenticate the server it reaches is refused before connecting.
    #[test]
    fn refuses_remote_postgres_urls_that_do_not_verify_the_server() {
        let error = postgres_connect_options("postgres://coral@db.example.com/coral")
            .expect_err("a remote URL without verify-full must be refused");
        assert!(
            error.to_string().contains("sslmode=verify-full"),
            "unexpected error: {error}"
        );

        postgres_connect_options("postgres://coral@db.example.com/coral?sslmode=verify-full")
            .expect("a verifying remote URL is accepted");
        postgres_connect_options("postgres://coral@localhost/coral")
            .expect("a loopback URL is accepted");
    }

    /// Ownership reachability is the whole point of the listing, so each of the
    /// three classes has to be told apart from the other two.
    #[test]
    fn list_sqlite_workspaces_separates_zero_owner_from_local_only_owner_rows() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);

        let rendered = list_workspaces(Some(config_dir)).expect("list workspaces");

        assert_eq!(
            workspace_row(&rendered, "abandoned"),
            "abandoned  0  0  zero-owners"
        );
        assert_eq!(
            workspace_row(&rendered, "demoted"),
            "demoted  0  1  zero-owners",
            "a workspace with members but no owner is as unreachable as an empty one"
        );
        assert_eq!(
            workspace_row(&rendered, "legacy"),
            "legacy  1  1  local-owner-only",
            "an owner that is only the synthetic local principal reaches nobody"
        );
        assert_eq!(
            workspace_row(&rendered, "shared"),
            "shared  1  2  human-owned"
        );
        assert_eq!(
            workspace_row(&rendered, "co_owned"),
            "co_owned  2  2  human-owned",
            "a human owner alongside the local principal is still reachable"
        );
    }

    /// The default listing must not publish upstream identities. Asserting the
    /// needle is absent is the load-bearing half; revealing it on request only
    /// proves the fixture really holds it.
    #[test]
    fn list_sqlite_users_withholds_subjects_unless_they_are_asked_for() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);

        let hidden = list_users(Some(config_dir.clone()), false).expect("list users");
        let revealed = list_users(Some(config_dir), true).expect("list users with subjects");

        assert!(
            !hidden.contains(SUBJECT_NEEDLE),
            "the default listing leaked a provider subject:\n{hidden}"
        );
        assert!(
            !hidden.contains("SUBJECT"),
            "the default listing offered a subject column:\n{hidden}"
        );
        assert!(
            hidden.contains("Ada Lovelace"),
            "the default listing still has to identify its users:\n{hidden}"
        );
        assert!(
            revealed.contains(SUBJECT_NEEDLE),
            "--show-subjects must reveal the subject:\n{revealed}"
        );
    }

    /// Recovery runs against a live, possibly damaged deployment, so opening
    /// its database must leave every byte of it exactly as it was.
    #[test]
    fn list_sqlite_listings_leave_a_current_state_database_byte_identical() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");
        let before = snapshot(&database);
        assert!(
            before
                .schema
                .iter()
                .any(|(_, name, _)| name == "workspace_members"),
            "the fixture must be fully migrated"
        );

        list_workspaces(Some(config_dir.clone())).expect("list workspaces");
        list_users(Some(config_dir.clone()), false).expect("list users");
        list_users(Some(config_dir), true).expect("list users with subjects");

        assert_eq!(
            before,
            snapshot(&database),
            "a listing changed the state database"
        );
    }

    /// A tool that only declines to migrate an already-current database has
    /// proven nothing. This one is deliberately behind the current schema: the
    /// migration that would bring it forward is the one recovery reads from,
    /// so the temptation to apply it is at its strongest here.
    #[test]
    fn list_sqlite_listings_never_advance_a_behind_schema_state_database() {
        let (_temp, config_dir) = state_dir(Migrations::BeforeAccessControl);
        let database = config_dir.join("coral.db");
        let before = snapshot(&database);
        assert!(
            !before
                .schema
                .iter()
                .any(|(_, name, _)| name == "workspace_members"),
            "the fixture must predate the access-control migration"
        );
        let applied_before = before.migrations.len();

        let workspaces_result = list_workspaces(Some(config_dir.clone()));
        let users_result = list_users(Some(config_dir), false);

        // The state of the database is asserted before the shape of the
        // failures, so that a listing which "worked" by migrating first is
        // reported as the damage it is rather than as a surprising success.
        let after = snapshot(&database);
        assert_eq!(
            after.migrations.len(),
            applied_before,
            "a migration was applied to a behind-schema database"
        );
        assert_eq!(
            before, after,
            "opening a behind-schema database advanced or altered it"
        );
        let workspaces_error = workspaces_result.expect_err("the tables do not exist yet");
        let users_error = users_result.expect_err("the tables do not exist yet");
        for error in [&workspaces_error, &users_error] {
            assert!(
                format!("{error:#}").contains("start the server once to migrate it"),
                "the failure must point at the server, not offer to migrate: {error:#}"
            );
        }
    }

    /// The recovery sequence has the operator run this against a deployment
    /// that is still serving, so a listing has to succeed while another
    /// connection holds an open write transaction on the same file.
    #[test]
    fn list_sqlite_listings_run_while_a_competing_writer_holds_the_database() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");

        // Kept alive for the whole test: the competing transaction must still
        // be open, and still holding its lock, while the listing runs.
        let runtime = runtime();
        let pool = runtime.block_on(writable_pool(&database));
        let writer = runtime.block_on(async {
            let mut writer = pool.begin().await.expect("begin the competing write");
            sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
                .bind("uncommitted")
                .bind(9_i64)
                .execute(&mut *writer)
                .await
                .expect("hold a write lock");
            writer
        });

        // Without this, "runs under contention" would be an untested slogan:
        // it proves the competing transaction is holding a real write lock
        // right now, because a third connection's write is refused.
        let contended = runtime.block_on(async {
            let contender = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(&database)
                        .create_if_missing(false)
                        .busy_timeout(Duration::from_millis(50)),
                )
                .await
                .expect("open a third connection");
            let refused =
                sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
                    .bind("contended")
                    .bind(9_i64)
                    .execute(&contender)
                    .await;
            contender.close().await;
            refused
        });
        assert!(
            contended.is_err(),
            "the competing transaction was not holding a write lock, so this proves nothing"
        );

        let rendered = list_workspaces(Some(config_dir)).expect("list while the server writes");

        assert!(
            rendered.contains("abandoned"),
            "the listing saw nothing:\n{rendered}"
        );
        assert!(
            !rendered.contains("uncommitted"),
            "recovery read another connection's uncommitted state:\n{rendered}"
        );
        runtime.block_on(async {
            writer
                .rollback()
                .await
                .expect("release the competing write");
            pool.close().await;
        });
    }

    /// The built-in local principal is a real, ordinary-looking directory row:
    /// display name `Local`, and on a deployment where nobody has authenticated
    /// yet the most recent login on it. Unmarked, it heads the very listing an
    /// operator reads to pick an owner, and it is the one identity `set-owner`
    /// refuses. Both halves are pinned here: the marking and the ordering.
    #[test]
    fn list_sqlite_users_marks_the_built_in_local_principal_non_appointable() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);

        let rendered = list_users(Some(config_dir), false).expect("list users");

        let rows: Vec<&str> = rendered
            .lines()
            .skip(1)
            .take_while(|line| !line.is_empty())
            .collect();
        let first = rows.first().expect("the listing must have rows");
        assert!(
            first.starts_with(ADA) || first.starts_with(GRACE),
            "a non-appointable row headed the listing:\n{rendered}"
        );
        assert_eq!(
            user_row(&rendered, LOCAL_PRINCIPAL_ID),
            format!(
                "{LOCAL_PRINCIPAL_ID}  {LOCAL_DISPLAY_NAME}  {LOCAL_PRINCIPAL_ID}  no  1970-01-01T00:00:00Z  1970-01-01T00:00:00Z"
            ),
            "the local principal must be listed and marked non-appointable"
        );
        assert!(
            user_row(&rendered, STRAY_LOCAL).contains("  no  "),
            "a row carrying the local issuer under another id is equally unappointable:\n{rendered}"
        );
        assert!(
            user_row(&rendered, GRACE).contains("  yes  "),
            "a human account must stay appointable:\n{rendered}"
        );
        assert!(
            rendered.contains("not appointable: the built-in local principal"),
            "the listing must say why the marked row cannot be appointed:\n{rendered}"
        );
    }

    /// The failure that matters is not a wrong appointment but a destroyed one.
    /// Every other membership in the workspace, every membership in every other
    /// workspace, and every directory row have to come through untouched -
    /// roles, creation stamps and all.
    #[test]
    fn mutation_sqlite_set_owner_promotes_one_row_and_disturbs_no_other() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");
        let before = contents(&database);
        assert_eq!(
            membership(&before, "shared", GRACE),
            Some(("member".to_string(), 33)),
            "the fixture must start with a promotable membership"
        );

        let report = set_owner(Some(config_dir), "shared", GRACE).expect("appoint an owner");

        assert!(
            report.contains("promoted") && report.contains("does not need restarting"),
            "unexpected report: {report}"
        );
        let mut expected = before.clone();
        for (workspace, user, role, _) in &mut expected.members {
            if workspace == "shared" && user == GRACE {
                *role = OWNER_ROLE.to_string();
            }
        }
        assert_eq!(
            expected,
            contents(&database),
            "the appointment changed something other than the one role it promotes"
        );
        assert_eq!(
            membership(&contents(&database), "shared", GRACE),
            Some((OWNER_ROLE.to_string(), 33)),
            "a promotion must keep the membership's original creation stamp"
        );
    }

    /// The three states the recovery sequence has to survive: a workspace with
    /// no owner at all, one owned only by the local principal, and one that
    /// already has a human owner. The local-only case must gain a human owner
    /// *beside* the synthetic one rather than in place of it - removing rows is
    /// not this command's job.
    #[test]
    fn mutation_sqlite_set_owner_repairs_ownerless_local_only_and_human_owned_workspaces() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");

        for workspace in ["abandoned", "legacy", "shared"] {
            set_owner(Some(config_dir.clone()), workspace, ADA)
                .unwrap_or_else(|error| panic!("appoint an owner of {workspace}: {error:#}"));
        }

        let after = contents(&database);
        for workspace in ["abandoned", "legacy", "shared"] {
            assert_eq!(
                membership(&after, workspace, ADA).map(|(role, _)| role),
                Some(OWNER_ROLE.to_string()),
                "{workspace} did not gain its human owner"
            );
        }
        assert_eq!(
            membership(&after, "legacy", LOCAL_PRINCIPAL_ID),
            Some((OWNER_ROLE.to_string(), 31)),
            "appointing a human owner removed the local principal's membership"
        );
        assert_eq!(
            membership(&after, "shared", ADA),
            Some((OWNER_ROLE.to_string(), 32)),
            "an already-owning member was rewritten instead of left alone"
        );
        let rendered = list_workspaces(Some(config_dir)).expect("list workspaces");
        for workspace in ["abandoned", "legacy", "shared"] {
            assert!(
                workspace_row(&rendered, workspace).ends_with("human-owned"),
                "{workspace} is still unreachable:\n{rendered}"
            );
        }
    }

    /// Idempotency has to mean "the second run wrote nothing", not "the second
    /// run also succeeded". The whole database is compared, so a re-appointment
    /// that quietly restamped a row would fail here.
    #[test]
    fn mutation_sqlite_set_owner_is_idempotent_down_to_the_stored_bytes() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");

        set_owner(Some(config_dir.clone()), "abandoned", ADA).expect("appoint an owner");
        let after_first = snapshot(&database);
        let report = set_owner(Some(config_dir), "abandoned", ADA).expect("re-appoint the owner");

        assert!(
            report.contains("already owns") && report.contains("nothing was written"),
            "unexpected report: {report}"
        );
        assert_eq!(
            after_first,
            snapshot(&database),
            "re-appointing the same owner wrote to the state database"
        );
    }

    /// Every refusal has to leave the database exactly as it found it: a
    /// half-applied repair on a deployment someone is trying to rescue is worse
    /// than no repair at all.
    #[test]
    fn mutation_sqlite_set_owner_refuses_the_local_principal_missing_users_and_missing_workspaces()
    {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");
        let before = snapshot(&database);

        let local = set_owner(Some(config_dir.clone()), "abandoned", LOCAL_PRINCIPAL_ID)
            .expect_err("the local principal is not appointable");
        let stray = set_owner(Some(config_dir.clone()), "abandoned", STRAY_LOCAL)
            .expect_err("a row bound to the local issuer is not appointable either");
        let missing_user = set_owner(Some(config_dir.clone()), "abandoned", "33333333-3333-3333")
            .expect_err("an unknown user is not appointable");
        let missing_workspace = set_owner(Some(config_dir), "no-such-workspace", ADA)
            .expect_err("an unknown workspace cannot be repaired");

        for error in [&local, &stray] {
            assert!(
                format!("{error:#}").contains("built-in local principal"),
                "the refusal must name what it refused: {error:#}"
            );
        }
        assert!(
            format!("{missing_user:#}").contains("authenticate against the deployment once"),
            "the refusal must point at the recovery sequence: {missing_user:#}"
        );
        assert!(
            format!("{missing_workspace:#}").contains("list-workspaces"),
            "the refusal must point at the listing: {missing_workspace:#}"
        );
        assert_eq!(
            before,
            snapshot(&database),
            "a refused appointment still wrote to the state database"
        );
    }

    /// Rebinding must move the `issuer` column and nothing else. Minting a
    /// second directory row for someone who already has one would orphan every
    /// membership pointing at the old internal id, which is precisely the
    /// damage this command exists to avoid.
    #[test]
    fn mutation_sqlite_rebind_issuer_preserves_internal_ids_and_every_membership() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");
        let before = contents(&database);

        let report =
            rebind_issuer(Some(config_dir), ISSUER, RENAMED_ISSUER).expect("rebind the issuer");

        assert!(
            report.contains("rebound 2 user(s)"),
            "unexpected report: {report}"
        );
        let mut expected = before.clone();
        for (user_id, issuer, ..) in &mut expected.users {
            if user_id == ADA || user_id == GRACE {
                *issuer = RENAMED_ISSUER.to_string();
            }
        }
        assert_eq!(
            expected,
            contents(&database),
            "the rebind changed something other than the issuer of the two rows it moved"
        );
        assert_eq!(
            before.members,
            contents(&database).members,
            "the rebind disturbed a membership"
        );
    }

    /// The local principal is refused from either direction, and a database
    /// that binds it to a human issuer does not smuggle it through the rebind.
    /// A rerun of a completed rebind reports itself as one rather than failing.
    #[test]
    fn mutation_sqlite_rebind_issuer_refuses_the_local_principal_and_reruns_cleanly() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");
        let before = snapshot(&database);

        let from_local =
            rebind_issuer(Some(config_dir.clone()), LOCAL_PRINCIPAL_ID, RENAMED_ISSUER)
                .expect_err("the local principal cannot be rebound");
        let to_local = rebind_issuer(Some(config_dir.clone()), ISSUER, LOCAL_PRINCIPAL_ID)
            .expect_err("nothing can be rebound onto the local principal");
        let same = rebind_issuer(Some(config_dir.clone()), ISSUER, ISSUER)
            .expect_err("rebinding an issuer to itself is a mistake, not a no-op");
        let unknown = rebind_issuer(
            Some(config_dir.clone()),
            "https://nobody.test",
            RENAMED_ISSUER,
        )
        .expect_err("an issuer nobody uses is a typo");

        for error in [&from_local, &to_local] {
            assert!(
                format!("{error:#}").contains("built-in local principal"),
                "the refusal must name what it refused: {error:#}"
            );
        }
        assert!(
            format!("{same:#}").contains("nothing to rebind"),
            "unexpected error: {same:#}"
        );
        assert!(
            format!("{unknown:#}").contains("list-users"),
            "the refusal must point at the listing: {unknown:#}"
        );
        assert_eq!(
            before,
            snapshot(&database),
            "a refused rebind still wrote to the state database"
        );

        // A database whose synthetic row was hand-edited onto a human issuer
        // must stop the rebind rather than carry the local principal across.
        runtime().block_on(async {
            let pool = writable_pool(&database).await;
            sqlx::query("UPDATE users SET issuer = ? WHERE user_id = ?")
                .bind(ISSUER)
                .bind(LOCAL_PRINCIPAL_ID)
                .execute(&pool)
                .await
                .expect("bind the local principal to a human issuer");
            pool.close().await;
        });
        let contaminated = snapshot(&database);

        let bound = rebind_issuer(Some(config_dir.clone()), ISSUER, RENAMED_ISSUER)
            .expect_err("an issuer binding the local principal cannot be rebound");

        assert!(
            format!("{bound:#}").contains("built-in local principal"),
            "unexpected error: {bound:#}"
        );
        assert_eq!(
            contaminated,
            snapshot(&database),
            "a refused rebind still wrote to the state database"
        );
    }

    /// Rerunning a completed rebind is a normal operator action, and it has to
    /// report itself rather than look like a failed one.
    #[test]
    fn mutation_sqlite_rebind_issuer_reports_an_already_completed_rebind() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");

        rebind_issuer(Some(config_dir.clone()), ISSUER, RENAMED_ISSUER).expect("rebind the issuer");
        let after_first = snapshot(&database);
        let report =
            rebind_issuer(Some(config_dir), ISSUER, RENAMED_ISSUER).expect("rerun the rebind");

        assert!(
            report.contains("already bound") && report.contains("nothing was written"),
            "unexpected report: {report}"
        );
        assert_eq!(
            after_first,
            snapshot(&database),
            "rerunning a completed rebind wrote to the state database"
        );
    }

    /// A repair against an already-current database cannot detect a wired-in
    /// migrator: running one on a current database is a no-op. This fixture is
    /// deliberately behind the schema, where a migrator would visibly bring it
    /// forward and turn both refusals into successes.
    #[test]
    fn mutation_sqlite_repairs_never_advance_a_behind_schema_state_database() {
        let (_temp, config_dir) = state_dir(Migrations::BeforeAccessControl);
        let database = config_dir.join("coral.db");
        // `workspaces` predates the access-control migration, so a real
        // workspace exists here. Without one the appointment would stop at a
        // missing workspace and never reach the tables the migration adds -
        // passing for the wrong reason.
        runtime().block_on(async {
            let pool = writable_pool(&database).await;
            sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
                .bind("abandoned")
                .bind(1_i64)
                .execute(&pool)
                .await
                .expect("seed a pre-migration workspace");
            pool.close().await;
        });
        let before = snapshot(&database);
        assert!(
            !before
                .schema
                .iter()
                .any(|(_, name, _)| name == "workspace_members"),
            "the fixture must predate the access-control migration"
        );

        let appointment = set_owner(Some(config_dir.clone()), "abandoned", ADA);
        let rebind = rebind_issuer(Some(config_dir), ISSUER, RENAMED_ISSUER);

        // Asserted before the shape of the failures, so a repair that "worked"
        // by migrating first is reported as the damage it is rather than as a
        // surprising success.
        let after = snapshot(&database);
        assert_eq!(
            before.migrations, after.migrations,
            "a migration was applied to a behind-schema database"
        );
        assert_eq!(
            before, after,
            "a repair against a behind-schema database advanced or altered it"
        );
        for error in [
            &appointment.expect_err("the tables do not exist yet"),
            &rebind.expect_err("the tables do not exist yet"),
        ] {
            assert!(
                format!("{error:#}").contains("start the server once to migrate it"),
                "the failure must point at the server, not offer to migrate: {error:#}"
            );
        }
    }

    /// Listings coexist with a serving deployment because they take no write
    /// lock. A repair cannot: it needs the lock the server may be holding. The
    /// contract chosen here is a bounded, actionable refusal that writes
    /// nothing - never an indefinite wait, and never a partial repair - and the
    /// same command has to succeed the moment the lock is released.
    #[test]
    fn mutation_sqlite_set_owner_refuses_a_held_write_lock_and_succeeds_once_it_clears() {
        let (_temp, config_dir) = state_dir(Migrations::Current);
        seed(&config_dir);
        let database = config_dir.join("coral.db");
        let before = contents(&database);

        let runtime = runtime();
        let pool = runtime.block_on(writable_pool(&database));
        let writer = runtime.block_on(async {
            let mut writer = pool.begin().await.expect("begin the competing write");
            sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
                .bind("uncommitted")
                .bind(9_i64)
                .execute(&mut *writer)
                .await
                .expect("hold a write lock");
            writer
        });

        let started = std::time::Instant::now();
        let refused = set_owner(Some(config_dir.clone()), "abandoned", ADA)
            .expect_err("a held write lock must refuse the repair, not wait forever");
        let waited = started.elapsed();

        assert!(
            format!("{refused:#}").contains("held by another process"),
            "the refusal must name the contention: {refused:#}"
        );
        assert!(
            waited < Duration::from_secs(30),
            "the repair waited {waited:?}, which is not a bounded refusal"
        );
        assert_eq!(
            before,
            contents(&database),
            "a repair refused for contention still wrote to the state database"
        );

        runtime.block_on(async {
            writer
                .rollback()
                .await
                .expect("release the competing write");
            pool.close().await;
        });

        set_owner(Some(config_dir), "abandoned", ADA)
            .expect("the same repair must succeed once the lock clears");
        assert_eq!(
            membership(&contents(&database), "abandoned", ADA).map(|(role, _)| role),
            Some(OWNER_ROLE.to_string()),
            "the retried repair did not appoint the owner"
        );
    }

    // -- Postgres recovery contracts -----------------------------------------
    //
    // Every name below carries the literal `contract_on_postgres`, which is
    // what `make postgres-tests` filters on, and the literal `postgres_contract`,
    // which is what the recipe's xtask invocation filters on. A Postgres test
    // named anything else compiles, exists, and is never executed by the
    // repository gate; this repository has already shipped one that way.
    //
    // They address the gate's database directly rather than through a config
    // file, because resolution reads process environment and no test may mutate
    // that while its siblings run.

    /// The Postgres database `make postgres-tests` provisions.
    ///
    /// This panics on an unset variable rather than returning early. An ignored
    /// contract that skips itself when the URL is missing exits 0 with zero
    /// coverage - reporting success precisely on the machines that cannot run
    /// it.
    fn postgres_database() -> RecoveryDatabase {
        let url = crate::env::required_var("CORAL_TEST_POSTGRES_URL")
            .expect("CORAL_TEST_POSTGRES_URL must be set; run these through `make postgres-tests`");
        assert!(
            !url.is_empty(),
            "CORAL_TEST_POSTGRES_URL must name a database, not an empty string"
        );
        runtime().block_on(async {
            let pool = postgres_pool(&url).await;
            // The fixture migrates, never the tool. `make postgres-tests` points
            // every contract at one database and sqlx takes an advisory lock, so
            // a sibling that got there first simply leaves nothing to apply.
            Migrator::new(Path::new(MIGRATIONS_DIR))
                .await
                .expect("read the server's migrations")
                .run(&pool)
                .await
                .expect("migrate the gate database");
            pool.close().await;
        });
        RecoveryDatabase::Postgres { url }
    }

    async fn postgres_pool(url: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await
            .expect("connect to the gate database")
    }

    /// One contract's own rows inside the shared gate database.
    ///
    /// Every identifier carries a fresh suffix and no assertion below counts
    /// rows instance-wide: sibling contracts run concurrently against this same
    /// database, so anything global would drift under their inserts.
    struct PostgresFixture {
        database: RecoveryDatabase,
        url: String,
        workspace: String,
        other: String,
        issuer: String,
        ada: String,
        grace: String,
    }

    impl PostgresFixture {
        /// Seeds a workspace with two ordinary members and a second workspace
        /// that no contract may disturb.
        fn seed() -> Self {
            let database = postgres_database();
            let RecoveryDatabase::Postgres { url } = database.clone() else {
                panic!("the gate database must be Postgres");
            };
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let fixture = Self {
                database,
                url,
                workspace: format!("ws_{suffix}"),
                other: format!("other_{suffix}"),
                issuer: format!("https://issuer.test/{suffix}"),
                ada: uuid::Uuid::new_v4().to_string(),
                grace: uuid::Uuid::new_v4().to_string(),
            };

            runtime().block_on(async {
                let pool = postgres_pool(&fixture.url).await;
                for id in [&fixture.workspace, &fixture.other] {
                    sqlx::query(
                        "INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ($1, $2)",
                    )
                    .bind(id)
                    .bind(1_i64)
                    .execute(&pool)
                    .await
                    .expect("seed workspace");
                }
                for (user_id, name) in [(&fixture.ada, "Ada"), (&fixture.grace, "Grace")] {
                    sqlx::query(
                        "INSERT INTO users (user_id, issuer, subject, display_name, \
                         created_at_unix_nanos, last_login_at_unix_nanos) \
                         VALUES ($1, $2, $3, $4, $5, $6)",
                    )
                    .bind(user_id)
                    .bind(&fixture.issuer)
                    .bind(format!("{name}-{suffix}"))
                    .bind(format!("{name} {suffix}"))
                    .bind(10_i64)
                    .bind(20_i64)
                    .execute(&pool)
                    .await
                    .expect("seed user");
                }
                for (workspace, user_id, role) in [
                    (&fixture.workspace, &fixture.ada, "member"),
                    (&fixture.workspace, &fixture.grace, "member"),
                    (&fixture.other, &fixture.ada, "owner"),
                    (&fixture.other, &fixture.grace, "member"),
                ] {
                    sqlx::query(
                        "INSERT INTO workspace_members (workspace_id, user_id, role, \
                         created_at_unix_nanos) VALUES ($1, $2, $3, $4)",
                    )
                    .bind(workspace)
                    .bind(user_id)
                    .bind(role)
                    .bind(30_i64)
                    .execute(&pool)
                    .await
                    .expect("seed membership");
                }
                pool.close().await;
            });

            fixture
        }

        /// This fixture's own rows, and nothing a sibling contract owns.
        fn contents(&self) -> Contents {
            runtime().block_on(async {
                let pool = postgres_pool(&self.url).await;
                let workspaces = sqlx::query_as(
                    "SELECT id, created_at_unix_nanos FROM workspaces WHERE id IN ($1, $2) \
                     ORDER BY id",
                )
                .bind(&self.workspace)
                .bind(&self.other)
                .fetch_all(&pool)
                .await
                .expect("read workspaces");
                let users = sqlx::query_as(
                    "SELECT user_id, issuer, subject, display_name, created_at_unix_nanos, \
                     last_login_at_unix_nanos FROM users WHERE user_id IN ($1, $2) ORDER BY user_id",
                )
                .bind(&self.ada)
                .bind(&self.grace)
                .fetch_all(&pool)
                .await
                .expect("read users");
                let members = sqlx::query_as(
                    "SELECT workspace_id, user_id, role, created_at_unix_nanos FROM \
                     workspace_members WHERE workspace_id IN ($1, $2) \
                     ORDER BY workspace_id, user_id",
                )
                .bind(&self.workspace)
                .bind(&self.other)
                .fetch_all(&pool)
                .await
                .expect("read memberships");
                pool.close().await;
                Contents {
                    workspaces,
                    users,
                    members,
                }
            })
        }
    }

    /// Listing has to work against a real Postgres deployment, not only against
    /// the `SQLite` file the other contracts use.
    #[test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the recovery contract against Postgres"]
    fn postgres_contract_on_postgres_lists_workspaces_and_users() {
        let fixture = PostgresFixture::seed();

        let workspaces =
            list_workspaces_on(&fixture.database).expect("list workspaces on Postgres");
        let users = list_users_on(&fixture.database, false).expect("list users on Postgres");

        assert_eq!(
            workspace_row(&workspaces, &fixture.workspace),
            format!("{}  0  2  zero-owners", fixture.workspace)
        );
        assert_eq!(
            workspace_row(&workspaces, &fixture.other),
            format!("{}  1  2  human-owned", fixture.other)
        );
        assert!(
            user_row(&users, &fixture.ada).contains("  yes  "),
            "a human account must be listed appointable:\n{users}"
        );
    }

    /// Recovery can only appoint someone who already has a directory row, and
    /// never the built-in local principal. Both refusals have to hold on
    /// Postgres and leave this fixture's rows exactly as they were.
    #[test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the recovery contract against Postgres"]
    fn postgres_contract_on_postgres_refuses_missing_users_and_the_local_principal() {
        let fixture = PostgresFixture::seed();
        let before = fixture.contents();

        let missing = set_owner_on(&fixture.database, &fixture.workspace, "no-such-user")
            .expect_err("an unknown user is not appointable");
        let local = set_owner_on(&fixture.database, &fixture.workspace, LOCAL_PRINCIPAL_ID)
            .expect_err("the local principal is not appointable");
        let workspace = set_owner_on(&fixture.database, "no-such-workspace", &fixture.ada)
            .expect_err("an unknown workspace cannot be repaired");

        assert!(
            format!("{missing:#}").contains("authenticate against the deployment once"),
            "unexpected error: {missing:#}"
        );
        assert!(
            format!("{local:#}").contains("built-in local principal"),
            "unexpected error: {local:#}"
        );
        assert!(
            format!("{workspace:#}").contains("list-workspaces"),
            "unexpected error: {workspace:#}"
        );
        assert_eq!(
            before,
            fixture.contents(),
            "a refused appointment wrote to the state database"
        );
    }

    /// The appointment is an add-or-promote that touches one row, and repeating
    /// it writes nothing. The second workspace is the control: nothing may
    /// reach it.
    #[test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the recovery contract against Postgres"]
    fn postgres_contract_on_postgres_appoints_idempotently_and_preserves_memberships() {
        let fixture = PostgresFixture::seed();
        let before = fixture.contents();

        set_owner_on(&fixture.database, &fixture.workspace, &fixture.grace)
            .expect("appoint an owner");
        let after_first = fixture.contents();
        let report = set_owner_on(&fixture.database, &fixture.workspace, &fixture.grace)
            .expect("re-appoint the same owner");

        assert!(
            report.contains("already owns") && report.contains("nothing was written"),
            "unexpected report: {report}"
        );
        let mut expected = before.clone();
        for (workspace, user, role, _) in &mut expected.members {
            if workspace == &fixture.workspace && user == &fixture.grace {
                *role = OWNER_ROLE.to_string();
            }
        }
        assert_eq!(
            expected, after_first,
            "the appointment changed something other than the one role it promotes"
        );
        assert_eq!(
            after_first,
            fixture.contents(),
            "re-appointing the same owner wrote to the state database"
        );
    }

    /// Rebinding must move the issuer and keep the internal `user_id` every
    /// membership points at. A new directory row here would orphan them.
    #[test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the recovery contract against Postgres"]
    fn postgres_contract_on_postgres_rebinds_an_issuer_preserving_internal_user_ids() {
        let fixture = PostgresFixture::seed();
        let before = fixture.contents();
        let renamed = format!("{}/renamed", fixture.issuer);

        let report = rebind_issuer_on(&fixture.database, &fixture.issuer, &renamed)
            .expect("rebind the issuer");

        assert!(
            report.contains("rebound 2 user(s)"),
            "unexpected report: {report}"
        );
        let mut expected = before.clone();
        for (_, issuer, ..) in &mut expected.users {
            *issuer = renamed.clone();
        }
        assert_eq!(
            expected,
            fixture.contents(),
            "the rebind changed something other than the issuer of the rows it moved"
        );
        assert_eq!(
            before.members,
            fixture.contents().members,
            "the rebind disturbed a membership"
        );
    }

    /// The recovery sequence runs against a deployment that is still serving.
    /// A repair therefore has to succeed while another connection holds an open
    /// transaction elsewhere, and to refuse - bounded, actionably, writing
    /// nothing - while that transaction holds the very row it needs.
    #[test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the recovery contract against Postgres"]
    fn postgres_contract_on_postgres_repairs_while_a_live_session_holds_the_database() {
        let fixture = PostgresFixture::seed();
        let runtime = runtime();
        let pool = runtime.block_on(postgres_pool(&fixture.url));

        // Open elsewhere: a live session writing to another workspace must not
        // stand between an operator and the workspace they are repairing.
        let elsewhere = runtime.block_on(async {
            let mut transaction = pool.begin().await.expect("begin the competing write");
            sqlx::query("UPDATE workspace_members SET role = $1 WHERE workspace_id = $2")
                .bind("member")
                .bind(&fixture.other)
                .execute(&mut *transaction)
                .await
                .expect("hold locks on another workspace");
            transaction
        });

        set_owner_on(&fixture.database, &fixture.workspace, &fixture.grace)
            .expect("a repair must run while the server transacts elsewhere");

        runtime.block_on(async { elsewhere.rollback().await.expect("release the write") });

        // On the row itself: bounded refusal rather than an unbounded wait.
        let contending = runtime.block_on(async {
            let mut transaction = pool.begin().await.expect("begin the competing write");
            sqlx::query(
                "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2 \
                 FOR UPDATE",
            )
            .bind(&fixture.workspace)
            .bind(&fixture.ada)
            .fetch_one(&mut *transaction)
            .await
            .expect("hold the row the repair needs");
            transaction
        });

        let started = std::time::Instant::now();
        let refused = set_owner_on(&fixture.database, &fixture.workspace, &fixture.ada)
            .expect_err("a held row must refuse the repair, not wait forever");
        let waited = started.elapsed();

        assert!(
            format!("{refused:#}").contains("held by another process"),
            "the refusal must name the contention: {refused:#}"
        );
        assert!(
            waited < Duration::from_secs(30),
            "the repair waited {waited:?}, which is not a bounded refusal"
        );
        assert_eq!(
            membership(&fixture.contents(), &fixture.workspace, &fixture.ada).map(|(role, _)| role),
            Some("member".to_string()),
            "a repair refused for contention still wrote to the state database"
        );

        runtime.block_on(async {
            contending.rollback().await.expect("release the row");
            pool.close().await;
        });

        set_owner_on(&fixture.database, &fixture.workspace, &fixture.ada)
            .expect("the same repair must succeed once the row clears");
        assert_eq!(
            membership(&fixture.contents(), &fixture.workspace, &fixture.ada).map(|(role, _)| role),
            Some(OWNER_ROLE.to_string()),
            "the retried repair did not appoint the owner"
        );
    }

    /// How far to migrate a fixture database.
    #[derive(Debug, Clone, Copy)]
    enum Migrations {
        /// Every migration the server ships.
        Current,
        /// Everything before users and memberships existed.
        BeforeAccessControl,
    }

    /// Builds a state directory holding a `SQLite` database at the given schema.
    fn state_dir(migrations: Migrations) -> (TempDir, PathBuf) {
        let temp = tempdir().expect("temp dir");
        let config_dir = temp.path().join("coral");
        fs::create_dir_all(&config_dir).expect("create the state directory");
        let database = config_dir.join("coral.db");

        runtime().block_on(async {
            let pool = writable_pool(&database).await;
            let mut migrator = Migrator::new(Path::new(MIGRATIONS_DIR))
                .await
                .expect("read the server's migrations");
            let access_control = migrator
                .migrations
                .iter()
                .find(|migration| migration.description.as_ref() == ACCESS_CONTROL_MIGRATION)
                .map(|migration| migration.version)
                .expect("the access-control migration must exist");
            if matches!(migrations, Migrations::BeforeAccessControl) {
                migrator.migrations = Cow::Owned(
                    migrator
                        .migrations
                        .iter()
                        .filter(|migration| migration.version < access_control)
                        .cloned()
                        .collect(),
                );
            }
            migrator.run(&pool).await.expect("migrate the fixture");
            pool.close().await;
        });

        (temp, config_dir)
    }

    /// Seeds one workspace of every reachability class, plus their users.
    ///
    /// The local principal is seeded exactly as production writes it: display
    /// name `Local`, and the most recent login on the deployment, because the
    /// ownership-bootstrap migration stamps it and nobody has authenticated
    /// yet. That is the arrangement in which the one identity recovery refuses
    /// would otherwise head a listing read to choose an owner.
    fn seed(config_dir: &Path) {
        runtime().block_on(async {
            let pool = writable_pool(&config_dir.join("coral.db")).await;
            for (id, created) in [
                ("abandoned", 1_i64),
                ("demoted", 2),
                ("legacy", 3),
                ("shared", 4),
                ("co_owned", 5),
                ("untouched", 6),
            ] {
                sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
                    .bind(id)
                    .bind(created)
                    .execute(&pool)
                    .await
                    .expect("seed workspace");
            }
            for (user_id, issuer, subject, display_name, created, last_login) in [
                (
                    LOCAL_PRINCIPAL_ID,
                    LOCAL_PRINCIPAL_ID,
                    "",
                    Some(LOCAL_DISPLAY_NAME),
                    10_i64,
                    100_i64,
                ),
                (ADA, ISSUER, SUBJECT_NEEDLE, Some("Ada Lovelace"), 11, 20),
                (GRACE, ISSUER, "other-subject", Some("Grace Hopper"), 12, 30),
                (
                    STRAY_LOCAL,
                    LOCAL_PRINCIPAL_ID,
                    "stray-local-subject",
                    Some("Stray Local"),
                    13,
                    40,
                ),
            ] {
                sqlx::query(
                    "INSERT INTO users (user_id, issuer, subject, display_name, \
                     created_at_unix_nanos, last_login_at_unix_nanos) VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(user_id)
                .bind(issuer)
                .bind(subject)
                .bind(display_name)
                .bind(created)
                .bind(last_login)
                .execute(&pool)
                .await
                .expect("seed user");
            }
            for (workspace_id, user_id, role, created) in [
                ("demoted", ADA, "member", 30_i64),
                ("legacy", LOCAL_PRINCIPAL_ID, "owner", 31),
                ("shared", ADA, "owner", 32),
                ("shared", GRACE, "member", 33),
                ("co_owned", LOCAL_PRINCIPAL_ID, "owner", 34),
                ("co_owned", GRACE, "owner", 35),
                ("untouched", ADA, "owner", 36),
                ("untouched", GRACE, "member", 37),
            ] {
                sqlx::query(
                    "INSERT INTO workspace_members (workspace_id, user_id, role, \
                     created_at_unix_nanos) VALUES (?, ?, ?, ?)",
                )
                .bind(workspace_id)
                .bind(user_id)
                .bind(role)
                .bind(created)
                .execute(&pool)
                .await
                .expect("seed membership");
            }
            pool.close().await;
        });
    }

    /// Every row a repair could disturb, in a stable order.
    ///
    /// This is the invariant the mutation contracts compare, not the file's
    /// bytes: a writable connection is entitled to rewrite pages a read-only
    /// one never touches, so raw bytes would prove either too little or, on a
    /// journal-mode change, the wrong thing. Row-for-row equality is the claim
    /// that actually matters - a repair changed what it said it changed and
    /// nothing else, timestamps included.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Contents {
        workspaces: Vec<(String, i64)>,
        users: Vec<(String, String, String, Option<String>, i64, i64)>,
        members: Vec<(String, String, String, i64)>,
    }

    fn contents(database: &Path) -> Contents {
        runtime().block_on(async {
            let pool = read_only_pool(database).await;
            let workspaces =
                sqlx::query_as("SELECT id, created_at_unix_nanos FROM workspaces ORDER BY id")
                    .fetch_all(&pool)
                    .await
                    .expect("read workspaces");
            let users = sqlx::query_as(
                "SELECT user_id, issuer, subject, display_name, created_at_unix_nanos, \
                 last_login_at_unix_nanos FROM users ORDER BY user_id",
            )
            .fetch_all(&pool)
            .await
            .expect("read users");
            let members = sqlx::query_as(
                "SELECT workspace_id, user_id, role, created_at_unix_nanos FROM \
                 workspace_members ORDER BY workspace_id, user_id",
            )
            .fetch_all(&pool)
            .await
            .expect("read memberships");
            pool.close().await;
            Contents {
                workspaces,
                users,
                members,
            }
        })
    }

    /// The membership row for one pair, or `None` when there is none.
    fn membership(contents: &Contents, workspace_id: &str, user_id: &str) -> Option<(String, i64)> {
        contents
            .members
            .iter()
            .find(|(workspace, user, _, _)| workspace == workspace_id && user == user_id)
            .map(|(_, _, role, created)| (role.clone(), *created))
    }

    /// Captures the migration ledger, the schema, and the raw file.
    fn snapshot(database: &Path) -> Snapshot {
        let bytes = fs::read(database).expect("read the database file");
        runtime().block_on(async {
            let pool = SqlitePoolOptions::new()
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(database)
                        .create_if_missing(false)
                        .read_only(true),
                )
                .await
                .expect("open the fixture read-only");
            let migrations = sqlx::query_as(
                "SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version",
            )
            .fetch_all(&pool)
            .await
            .expect("read the migration ledger");
            let schema =
                sqlx::query_as("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
                    .fetch_all(&pool)
                    .await
                    .expect("read the schema");
            pool.close().await;
            Snapshot {
                migrations,
                schema,
                bytes,
            }
        })
    }

    async fn read_only_pool(database: &Path) -> SqlitePool {
        SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(database)
                    .create_if_missing(false)
                    .read_only(true),
            )
            .await
            .expect("open the fixture read-only")
    }

    async fn writable_pool(database: &Path) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(database)
                    .create_if_missing(true),
            )
            .await
            .expect("open the fixture for writing")
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build a test runtime")
    }

    fn write_config(config_dir: &Path, raw: &str) {
        fs::write(config_dir.join("config.toml"), raw).expect("write config.toml");
    }

    /// The rendered row for one user, with column padding collapsed.
    fn user_row(rendered: &str, user_id: &str) -> String {
        row_for(rendered, user_id)
    }

    /// The rendered row for one workspace, with column padding collapsed.
    fn workspace_row(rendered: &str, workspace_id: &str) -> String {
        row_for(rendered, workspace_id)
    }

    fn row_for(rendered: &str, id: &str) -> String {
        rendered
            .lines()
            .find(|line| line.split_whitespace().next() == Some(id))
            .unwrap_or_else(|| panic!("no row for {id} in:\n{rendered}"))
            .split_whitespace()
            .collect::<Vec<_>>()
            .join("  ")
    }
}
