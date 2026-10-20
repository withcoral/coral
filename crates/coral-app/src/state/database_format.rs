//! Read-only database state-format checks performed before migrations.

use std::io;
use std::path::Path;

use coral_db::{DatabaseOptions, DatabaseSystem, ReadOnlyDatabase};
use rusqlite::OpenFlags;
use sea_query::{Alias, Expr, ExprTrait as _, JoinType, Order, Query, SelectStatement};

use super::migrations::rows_match_known_migration_prefix;
use crate::bootstrap::AppError;

const CURRENT_DATABASE_FORMAT: &str = "coral-state-v1";
const DATABASE_FORMAT_TABLE: &str = "coral_state_format";

/// Verifies that an existing configured database belongs to the current Coral
/// state format. An empty database is fresh and can be initialized later.
///
/// Every probe is read-only. The state-initialization owner calls this before
/// publishing the filesystem marker or opening a migration-capable pool.
pub(super) async fn validate_database_state(config: &DatabaseOptions) -> Result<(), AppError> {
    match config {
        DatabaseOptions::Sqlite { path } => validate_sqlite(path),
        DatabaseOptions::Postgres { url } => validate_postgres(url).await,
    }
}

fn validate_sqlite(path: &Path) -> Result<(), AppError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Err(pre_v1_database_error()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }

    let connection = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(pre_v1_database_probe_error)?;
    let marker_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
            [DATABASE_FORMAT_TABLE],
            |row| row.get(0),
        )
        .map_err(pre_v1_database_probe_error)?;
    if marker_exists {
        let rows = connection
            .prepare("SELECT id, format FROM coral_state_format ORDER BY id")
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(malformed_database_probe_error)?;
        validate_marker_rows(&rows)?;
        return validate_sqlite_migration_ledger(&connection);
    }

    let object_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )
        .map_err(pre_v1_database_probe_error)?;
    if object_count == 0 {
        Ok(())
    } else {
        Err(pre_v1_database_error())
    }
}

async fn validate_postgres(url: &str) -> Result<(), AppError> {
    let database = ReadOnlyDatabase::connect_read_only(DatabaseOptions::Postgres {
        url: url.to_string(),
    })
    .await?;
    let result = validate_postgres_on(&database).await;
    database.close().await;
    result
}

async fn validate_postgres_on(database: &ReadOnlyDatabase) -> Result<(), AppError> {
    let current_schema: Option<(String,)> = database
        .fetch_optional(
            Query::select()
                .expr(Expr::cust("current_schema()"))
                .to_owned(),
        )
        .await?;
    let current_schema = current_schema
        .ok_or_else(|| {
            AppError::Database(
                "database contains corrupt data: Postgres has no current schema".to_string(),
            )
        })?
        .0;
    if postgres_table_exists(database, &current_schema, DATABASE_FORMAT_TABLE).await? {
        let marker_rows = database
            .fetch_all(postgres_marker_rows())
            .await
            .map_err(malformed_database_probe_error)?;
        validate_marker_rows(&marker_rows)?;
        return validate_postgres_migration_ledger(database, &current_schema).await;
    }

    let schema_has_objects: Option<(i32,)> = database
        .fetch_optional(postgres_schema_objects(&current_schema))
        .await?;
    if schema_has_objects.is_some() {
        Err(pre_v1_database_error())
    } else {
        Ok(())
    }
}

async fn postgres_table_exists(
    database: &ReadOnlyDatabase,
    current_schema: &str,
    table_name: &str,
) -> Result<bool, AppError> {
    let row: Option<(i32,)> = database
        .fetch_optional(
            Query::select()
                .expr(Expr::val(1_i32))
                .from((Alias::new("information_schema"), Alias::new("tables")))
                .and_where(Expr::col(Alias::new("table_schema")).eq(current_schema))
                .and_where(Expr::col(Alias::new("table_name")).eq(table_name))
                .and_where(Expr::col(Alias::new("table_type")).eq("BASE TABLE"))
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}

fn postgres_marker_rows() -> SelectStatement {
    Query::select()
        .columns([Alias::new("id"), Alias::new("format")])
        .from(Alias::new(DATABASE_FORMAT_TABLE))
        .order_by(Alias::new("id"), Order::Asc)
        .to_owned()
}

fn postgres_schema_objects(current_schema: &str) -> SelectStatement {
    let dependency = Alias::new("dependency");
    let namespace = Alias::new("namespace");
    Query::select()
        .expr(Expr::val(1_i32))
        .from_as(
            (Alias::new("pg_catalog"), Alias::new("pg_depend")),
            dependency.clone(),
        )
        .join_as(
            JoinType::InnerJoin,
            (Alias::new("pg_catalog"), Alias::new("pg_namespace")),
            namespace.clone(),
            Expr::col((dependency.clone(), Alias::new("refobjid")))
                .equals((namespace.clone(), Alias::new("oid"))),
        )
        .and_where(Expr::cust(
            "dependency.refclassid = 'pg_catalog.pg_namespace'::regclass",
        ))
        .and_where(Expr::col((dependency, Alias::new("refobjsubid"))).eq(0_i32))
        .and_where(Expr::col((namespace, Alias::new("nspname"))).eq(current_schema))
        // An administrator may install an extension Coral requires, such as
        // pg_trgm, into Coral's schema before the first boot. The extension
        // and its member objects are not Coral state.
        .and_where(Expr::cust(
            "dependency.classid <> 'pg_catalog.pg_extension'::regclass",
        ))
        .and_where(Expr::cust(
            "NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS member \
             WHERE member.classid = dependency.classid \
             AND member.objid = dependency.objid \
             AND member.deptype = 'e')",
        ))
        .limit(1)
        .to_owned()
}

fn validate_sqlite_migration_ledger(connection: &rusqlite::Connection) -> Result<(), AppError> {
    let ledger_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations')",
            [],
            |row| row.get(0),
        )
        .map_err(malformed_database_probe_error)?;
    if !ledger_exists {
        return Err(malformed_database_marker_error());
    }
    let rows = connection
        .prepare("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(malformed_database_probe_error)?;
    validate_ledger_rows(&rows, DatabaseSystem::Sqlite)
}

async fn validate_postgres_migration_ledger(
    database: &ReadOnlyDatabase,
    current_schema: &str,
) -> Result<(), AppError> {
    if !postgres_table_exists(database, current_schema, "_sqlx_migrations").await? {
        return Err(malformed_database_marker_error());
    }
    let rows: Vec<(i64, Vec<u8>, bool)> = database
        .fetch_all(
            Query::select()
                .columns([
                    Alias::new("version"),
                    Alias::new("checksum"),
                    Alias::new("success"),
                ])
                .from(Alias::new("_sqlx_migrations"))
                .order_by(Alias::new("version"), Order::Asc)
                .to_owned(),
        )
        .await
        .map_err(malformed_database_probe_error)?;
    validate_ledger_rows(&rows, DatabaseSystem::Postgres)
}

/// An empty ledger or a failed migration is broken state, not a migration this
/// build does not recognize.
fn validate_ledger_rows(
    rows: &[(i64, Vec<u8>, bool)],
    system: DatabaseSystem,
) -> Result<(), AppError> {
    if rows.is_empty() || rows.iter().any(|(_, _, success)| !success) {
        Err(malformed_database_marker_error())
    } else if rows_match_known_migration_prefix(rows, system) {
        Ok(())
    } else {
        Err(unknown_migrations_error())
    }
}

fn validate_marker_rows(rows: &[(String, String)]) -> Result<(), AppError> {
    if matches!(rows, [(id, format)] if id == "current" && format == CURRENT_DATABASE_FORMAT) {
        Ok(())
    } else {
        Err(malformed_database_marker_error())
    }
}

fn pre_v1_database_probe_error(error: rusqlite::Error) -> AppError {
    let unavailable = matches!(
        &error,
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                rusqlite::ffi::ErrorCode::PermissionDenied
                    | rusqlite::ffi::ErrorCode::DatabaseBusy
                    | rusqlite::ffi::ErrorCode::DatabaseLocked
                    | rusqlite::ffi::ErrorCode::ReadOnly
                    | rusqlite::ffi::ErrorCode::SystemIoFailure
                    | rusqlite::ffi::ErrorCode::CannotOpen
                    | rusqlite::ffi::ErrorCode::FileLockingProtocolFailed
            )
    );
    drop(error);
    if unavailable {
        database_probe_unavailable_error()
    } else {
        pre_v1_database_error()
    }
}

fn database_probe_unavailable_error() -> AppError {
    AppError::FailedPrecondition(
        "the configured SQLite database could not be inspected read-only; correct its permissions or availability and retry; no state was changed"
            .to_string(),
    )
}

fn malformed_database_probe_error<E>(error: E) -> AppError {
    drop(error);
    malformed_database_marker_error()
}

fn pre_v1_database_error() -> AppError {
    AppError::FailedPrecondition(
        "pre-v1 Coral database state detected; move or remove it, or configure a fresh database before starting this version of Coral"
            .to_string(),
    )
}

fn unknown_migrations_error() -> AppError {
    AppError::FailedPrecondition(
        "the Coral database has applied migrations this version of Coral does not recognize: a migration was edited or reordered, or a newer version of Coral created the database; move or remove the database before starting this version of Coral"
            .to_string(),
    )
}

fn malformed_database_marker_error() -> AppError {
    AppError::FailedPrecondition(
        "the Coral database state format marker is malformed; move or remove the database before starting this version of Coral"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use sqlx::AssertSqlSafe;
    use sqlx::postgres::PgPoolOptions;

    use sea_query::{Alias, Order, Query};

    use super::validate_database_state;
    use crate::bootstrap;
    use coral_db::{Database, DatabaseOptions};
    use coral_test_support::TestDatabase;

    #[tokio::test]
    async fn ledger_problems_are_rejected_without_changing_state() {
        let last = "(SELECT MAX(version) FROM _sqlx_migrations)";
        // The prefix rule itself is tested in `migrations`; these cases pin
        // which error each kind of ledger problem reports.
        let cases = [
            (
                "edited migration",
                format!(
                    "UPDATE _sqlx_migrations SET checksum = \
                     (SELECT checksum FROM _sqlx_migrations WHERE version = {last}) \
                     WHERE version = (SELECT MIN(version) FROM _sqlx_migrations)"
                ),
                "does not recognize",
            ),
            (
                "migration from a newer Coral",
                format!(
                    "INSERT INTO _sqlx_migrations \
                     (version, description, success, checksum, execution_time) \
                     SELECT version + 1, 'from a newer Coral', success, checksum, execution_time \
                     FROM _sqlx_migrations WHERE version = {last}"
                ),
                "does not recognize",
            ),
            (
                "failed migration",
                format!("UPDATE _sqlx_migrations SET success = FALSE WHERE version = {last}"),
                "marker is malformed",
            ),
            (
                "empty ledger",
                "DELETE FROM _sqlx_migrations".to_string(),
                "marker is malformed",
            ),
        ];
        for (label, fault, expected) in cases {
            let fixture = TestDatabase::new(coral_db::migration_plan())
                .await
                .expect("apply current migrations");
            fixture
                .execute_fixture_sql(&fault)
                .await
                .expect("damage the ledger");
            let before = ledger_rows(&fixture).await;

            let error = validate_database_state(fixture.options())
                .await
                .expect_err("a damaged ledger must be rejected");

            assert!(error.to_string().contains(expected), "{label}: {error}");
            assert_eq!(
                ledger_rows(&fixture).await,
                before,
                "{label}: ledger changed"
            );
        }
    }

    async fn ledger_rows(fixture: &TestDatabase) -> Vec<(i64, Vec<u8>)> {
        fixture
            .database()
            .fetch_all(
                Query::select()
                    .columns([Alias::new("version"), Alias::new("checksum")])
                    .from(Alias::new("_sqlx_migrations"))
                    .order_by(Alias::new("version"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the ledger")
    }

    #[tokio::test]
    async fn database_state_format_contract_on_postgres() {
        coral_test_support::database_only!(Postgres);
        let url = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read Postgres test URL")
            .expect("the Postgres test selector requires CORAL_TEST_POSTGRES_URL");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("open Postgres");
        let fresh_schema = create_schema(&pool, "fresh").await;
        let retired_schema = create_schema(&pool, "retired").await;
        let marker_schema = create_schema(&pool, "marker").await;
        let malformed_schema = create_schema(&pool, "malformed").await;

        // As in a default deployment, `public` holds the extensions the
        // chain's search steps use.
        let fresh = postgres_config(&url, &format!("{fresh_schema},public"));
        validate_database_state(&fresh)
            .await
            .expect("recognize fresh Postgres state");
        let db = Database::connect(fresh.clone())
            .await
            .expect("open fresh Postgres schema");
        coral_search::prepare_storage(&db)
            .await
            .expect("check the search features");
        super::super::migrations::migrate_current(&db)
            .await
            .expect("migrate fresh Postgres schema");
        validate_database_state(&fresh)
            .await
            .expect("recognize current Postgres state");
        db.close().await;

        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {retired_schema}.legacy_sources (name TEXT NOT NULL)"
        )))
        .execute(&pool)
        .await
        .expect("create retired table");
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {retired_schema}.legacy_sources VALUES ('canary-retired-source')"
        )))
        .execute(&pool)
        .await
        .expect("seed retired table");
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {marker_schema}.coral_state_format (id TEXT, format TEXT)"
        )))
        .execute(&pool)
        .await
        .expect("create shadow marker table");
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {marker_schema}.coral_state_format VALUES ('current', 'coral-state-v1')"
        )))
        .execute(&pool)
        .await
        .expect("seed shadow marker");
        assert_forged_marker_is_rejected(&pool, &url, &marker_schema).await;
        let retired = postgres_config(&url, &format!("{retired_schema},{marker_schema}"));
        let retired_error = validate_database_state(&retired)
            .await
            .expect_err("a later-schema marker must not bless retired current-schema state");
        assert!(retired_error.to_string().contains("pre-v1"));
        assert!(!retired_error.to_string().contains("canary-retired-source"));
        let retired_rows: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {retired_schema}.legacy_sources"
        )))
        .fetch_one(&pool)
        .await
        .expect("count rejected retired rows");
        assert_eq!(retired_rows, 1, "rejection must not mutate retired state");

        assert_non_table_relations_are_rejected(&pool, &url).await;
        assert_malformed_marker_is_rejected(&pool, &url, &malformed_schema).await;

        for schema in [
            fresh_schema,
            retired_schema,
            marker_schema,
            malformed_schema,
        ] {
            sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
                .execute(&pool)
                .await
                .expect("drop state-format test schema");
        }
        pool.close().await;
    }

    #[tokio::test]
    async fn an_administrators_extension_alone_is_fresh_postgres_state() {
        coral_test_support::database_only!(Postgres);
        let url = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read Postgres test URL")
            .expect("the Postgres test selector requires CORAL_TEST_POSTGRES_URL");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("open Postgres");
        // A role that may not create extensions depends on an administrator
        // installing Universal Search's pg_trgm before the first boot, often in
        // Coral's own schema. The test uses `hstore` in its place: pg_trgm is
        // already in this test database's `public`, and an extension exists
        // once per database.
        let schema = create_schema(&pool, "extension").await;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE EXTENSION hstore SCHEMA {schema}"
        )))
        .execute(&pool)
        .await
        .expect("install an administrator's extension");
        let extension_only = validate_database_state(&postgres_config(&url, &schema)).await;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {schema}.legacy_sources (name TEXT NOT NULL)"
        )))
        .execute(&pool)
        .await
        .expect("create retired table beside the extension");
        let with_table = validate_database_state(&postgres_config(&url, &schema)).await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&pool)
            .await
            .expect("drop extension test schema");
        pool.close().await;

        extension_only.expect("extension objects alone are fresh state");
        let error = with_table.expect_err("a table beside the extension is pre-v1 state");
        assert!(error.to_string().contains("pre-v1"), "{error}");
    }

    async fn assert_forged_marker_is_rejected(pool: &sqlx::PgPool, url: &str, marker_schema: &str) {
        let marker_error = validate_database_state(&postgres_config(url, marker_schema))
            .await
            .expect_err("a marker without the current migration ledger must be rejected");
        assert!(marker_error.to_string().contains("marker is malformed"));
        let marker_rows: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {marker_schema}.coral_state_format"
        )))
        .fetch_one(pool)
        .await
        .expect("count rejected forged marker");
        assert_eq!(marker_rows, 1, "rejection must not mutate the marker");
    }

    async fn assert_malformed_marker_is_rejected(
        pool: &sqlx::PgPool,
        url: &str,
        malformed_schema: &str,
    ) {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {malformed_schema}.coral_state_format (id TEXT, format TEXT)"
        )))
        .execute(pool)
        .await
        .expect("create malformed marker table");
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {malformed_schema}.coral_state_format VALUES ('current', 'wrong')"
        )))
        .execute(pool)
        .await
        .expect("seed malformed marker");
        let malformed_error = validate_database_state(&postgres_config(url, malformed_schema))
            .await
            .expect_err("malformed marker must be rejected");
        assert!(malformed_error.to_string().contains("marker is malformed"));
        let malformed_rows: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {malformed_schema}.coral_state_format"
        )))
        .fetch_one(pool)
        .await
        .expect("count malformed marker rows");
        assert_eq!(malformed_rows, 1, "rejection must not mutate the marker");
    }

    async fn assert_non_table_relations_are_rejected(pool: &sqlx::PgPool, url: &str) {
        let view_schema = create_schema(pool, "view").await;
        let sequence_schema = create_schema(pool, "sequence").await;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE VIEW {view_schema}.legacy_sources AS SELECT 'github'::TEXT AS name"
        )))
        .execute(pool)
        .await
        .expect("create retired view");
        let view_error = validate_database_state(&postgres_config(url, &view_schema))
            .await
            .expect_err("a schema containing only a view must be rejected");
        assert!(view_error.to_string().contains("pre-v1"));
        assert_relation_still_exists(pool, &view_schema, "legacy_sources").await;

        sqlx::query(AssertSqlSafe(format!(
            "CREATE SEQUENCE {sequence_schema}.legacy_sequence"
        )))
        .execute(pool)
        .await
        .expect("create retired sequence");
        let sequence_error = validate_database_state(&postgres_config(url, &sequence_schema))
            .await
            .expect_err("a schema containing only a sequence must be rejected");
        assert!(sequence_error.to_string().contains("pre-v1"));
        assert_relation_still_exists(pool, &sequence_schema, "legacy_sequence").await;

        for schema in [view_schema, sequence_schema] {
            sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
                .execute(pool)
                .await
                .expect("drop non-table state-format test schema");
        }
    }

    async fn assert_relation_still_exists(pool: &sqlx::PgPool, schema: &str, relation: &str) {
        let qualified = format!("{schema}.{relation}");
        let still_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(qualified)
            .fetch_one(pool)
            .await
            .expect("inspect rejected relation");
        assert!(still_exists, "rejection must not remove {relation}");
    }

    async fn create_schema(pool: &sqlx::PgPool, label: &str) -> String {
        let schema = format!(
            "coral_state_format_{label}_{}",
            uuid::Uuid::new_v4().simple()
        );
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(pool)
            .await
            .expect("create state-format test schema");
        schema
    }

    fn postgres_config(url: &str, search_path: &str) -> DatabaseOptions {
        let mut url = url::Url::parse(url).expect("parse Postgres test URL");
        url.query_pairs_mut()
            .append_pair("options[search_path]", search_path);
        DatabaseOptions::Postgres { url: url.into() }
    }
}
