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

use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use etcetera::app_strategy::{AppStrategy as _, AppStrategyArgs, choose_native_strategy};
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

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
    let database = resolve_database(config_dir_override)?;
    block_on(async {
        let connection = open_read_only(&database).await?;
        let memberships = read_memberships(&connection).await?;
        Ok(render_workspaces(&summarize_workspaces(memberships)))
    })
}

/// Lists every user the state database can appoint as a workspace owner.
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
    let database = resolve_database(config_dir_override)?;
    block_on(async {
        let connection = open_read_only(&database).await?;
        let mut users = read_users(&connection).await?;
        // Most-recent login first: the documented recovery sequence is to have
        // the intended owner authenticate once and then appoint them.
        users.sort_by(|left, right| {
            right
                .last_login_at_unix_nanos
                .cmp(&left.last_login_at_unix_nanos)
                .then_with(|| left.user_id.cmp(&right.user_id))
        });
        Ok(render_users(&users, show_subjects))
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

/// A read-only handle on state the server owns.
enum RecoveryConnection {
    /// Pool over a `SQLite` file opened with `SQLITE_OPEN_READONLY`.
    Sqlite(SqlitePool),
    /// Pool over Postgres with read-only transactions forced on.
    Postgres(PgPool),
}

/// Opens the existing state database for reading and nothing else.
///
/// Read-only is both a safety property and a concurrency one. The `SQLite`
/// handle takes no write lock, so it coexists with a running server, and the
/// operating system refuses any write this process could still attempt. The
/// one case it cannot serve is a `SQLite` database left in WAL mode with no
/// server running and no `-shm` file present, which read-only connections
/// cannot initialise.
///
/// Nothing here creates a database, and nothing here runs a migration.
async fn open_read_only(database: &RecoveryDatabase) -> Result<RecoveryConnection> {
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
                .read_only(true);
            let pool = SqlitePoolOptions::new()
                .connect_with(options)
                .await
                .with_context(|| format!("open the state database at {}", path.display()))?;
            Ok(RecoveryConnection::Sqlite(pool))
        }
        RecoveryDatabase::Postgres { url } => {
            let options =
                postgres_connect_options(url)?.options([("default_transaction_read_only", "on")]);
            let pool = PgPoolOptions::new()
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

fn render_users(users: &[UserRow], show_subjects: bool) -> String {
    if users.is_empty() {
        return "no users recorded in this state database\n".to_string();
    }
    let mut headers = vec!["USER ID", "DISPLAY NAME", "ISSUER", "LAST LOGIN", "CREATED"];
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
            row.push(timestamp(user.last_login_at_unix_nanos));
            row.push(timestamp(user.created_at_unix_nanos));
            row
        })
        .collect();
    if show_subjects {
        headers.insert(3, "SUBJECT");
    }
    render_table(&headers, &rows)
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
        ConfiguredDatabase, LOCAL_PRINCIPAL_ID, Path, PathBuf, SqliteConnectOptions, SqlitePool,
        SqlitePoolOptions, configured_database, list_users, list_workspaces,
        postgres_connect_options,
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
    fn seed(config_dir: &Path) {
        runtime().block_on(async {
            let pool = writable_pool(&config_dir.join("coral.db")).await;
            for (id, created) in [
                ("abandoned", 1_i64),
                ("demoted", 2),
                ("legacy", 3),
                ("shared", 4),
                ("co_owned", 5),
            ] {
                sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, ?)")
                    .bind(id)
                    .bind(created)
                    .execute(&pool)
                    .await
                    .expect("seed workspace");
            }
            for (user_id, issuer, subject, display_name) in [
                (LOCAL_PRINCIPAL_ID, LOCAL_PRINCIPAL_ID, "", None),
                (
                    "11111111-1111-4111-8111-111111111111",
                    "https://issuer.test",
                    SUBJECT_NEEDLE,
                    Some("Ada Lovelace"),
                ),
                (
                    "22222222-2222-4222-8222-222222222222",
                    "https://issuer.test",
                    "other-subject",
                    Some("Grace Hopper"),
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
                .bind(10_i64)
                .bind(20_i64)
                .execute(&pool)
                .await
                .expect("seed user");
            }
            for (workspace_id, user_id, role) in [
                ("demoted", "11111111-1111-4111-8111-111111111111", "member"),
                ("legacy", LOCAL_PRINCIPAL_ID, "owner"),
                ("shared", "11111111-1111-4111-8111-111111111111", "owner"),
                ("shared", "22222222-2222-4222-8222-222222222222", "member"),
                ("co_owned", LOCAL_PRINCIPAL_ID, "owner"),
                ("co_owned", "22222222-2222-4222-8222-222222222222", "owner"),
            ] {
                sqlx::query(
                    "INSERT INTO workspace_members (workspace_id, user_id, role, \
                     created_at_unix_nanos) VALUES (?, ?, ?, ?)",
                )
                .bind(workspace_id)
                .bind(user_id)
                .bind(role)
                .bind(30_i64)
                .execute(&pool)
                .await
                .expect("seed membership");
            }
            pool.close().await;
        });
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

    /// The rendered row for one workspace, with column padding collapsed.
    fn workspace_row(rendered: &str, workspace_id: &str) -> String {
        rendered
            .lines()
            .find(|line| line.split_whitespace().next() == Some(workspace_id))
            .unwrap_or_else(|| panic!("no row for {workspace_id} in:\n{rendered}"))
            .split_whitespace()
            .collect::<Vec<_>>()
            .join("  ")
    }
}
