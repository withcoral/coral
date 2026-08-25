use super::backend::CoralDbBackend;
use super::{CoralDb, DbError};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

impl CoralDb {
    pub(crate) async fn migrate(&self) -> Result<(), DbError> {
        match &self.backend {
            CoralDbBackend::Sqlite(db) => {
                if sqlite_migrations_are_current(&db.pool).await? {
                    return Ok(());
                }
                MIGRATOR.run(&db.pool).await?;
            }
            CoralDbBackend::Postgres(db) => {
                if postgres_migrations_are_current(&db.pool).await? {
                    return Ok(());
                }
                MIGRATOR.run(&db.pool).await?;
            }
        }
        Ok(())
    }
}

async fn sqlite_migrations_are_current(pool: &sqlx::SqlitePool) -> Result<bool, DbError> {
    let table_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?;
    if table_count.0 == 0 {
        return Ok(false);
    }
    let rows: Vec<(i64, Vec<u8>, bool)> =
        sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(pool)
            .await?;
    Ok(rows_match_current_migrations(&rows))
}

async fn postgres_migrations_are_current(pool: &sqlx::PgPool) -> Result<bool, DbError> {
    let table_exists: (Option<String>,) =
        sqlx::query_as("SELECT to_regclass('_sqlx_migrations')::text")
            .fetch_one(pool)
            .await?;
    if table_exists.0.is_none() {
        return Ok(false);
    }
    let rows: Vec<(i64, Vec<u8>, bool)> =
        sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(pool)
            .await?;
    Ok(rows_match_current_migrations(&rows))
}

fn rows_match_current_migrations(rows: &[(i64, Vec<u8>, bool)]) -> bool {
    let migrations = MIGRATOR.migrations.as_ref();
    rows.len() == migrations.len()
        && rows
            .iter()
            .zip(migrations)
            .all(|((version, checksum, success), migration)| {
                *success
                    && *version == migration.version
                    && checksum.as_slice() == migration.checksum.as_ref()
            })
}

#[cfg(test)]
mod tests {
    use super::{MIGRATOR, rows_match_current_migrations};

    type IdentityRow<'a> = (&'a str, &'a str, Option<&'a str>, &'a str, Option<&'a str>);

    #[test]
    fn current_migration_rows_must_match_versions_checksums_and_success() {
        let current_rows: Vec<_> = MIGRATOR
            .migrations
            .iter()
            .map(|migration| {
                (
                    migration.version,
                    migration.checksum.as_ref().to_vec(),
                    true,
                )
            })
            .collect();
        assert!(rows_match_current_migrations(&current_rows));

        let mut missing_row = current_rows.clone();
        missing_row.pop();
        assert!(!rows_match_current_migrations(&missing_row));

        let mut failed_row = current_rows.clone();
        failed_row
            .first_mut()
            .expect("at least one embedded migration")
            .2 = false;
        assert!(!rows_match_current_migrations(&failed_row));

        let mut bad_checksum = current_rows;
        bad_checksum
            .first_mut()
            .expect("at least one embedded migration")
            .1 = b"not the embedded checksum".to_vec();
        assert!(!rows_match_current_migrations(&bad_checksum));
    }

    #[tokio::test]
    async fn sqlite_workspace_id_rejects_null() {
        let pool = sqlx::SqlitePool::connect(":memory:")
            .await
            .expect("sqlite pool");
        MIGRATOR.run(&pool).await.expect("run migrations");

        let error =
            sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (NULL, 0)")
                .execute(&pool)
                .await
                .expect_err("workspace id must reject null");

        assert!(
            error.to_string().contains("NOT NULL"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn sqlite_identity_specs_do_not_persist_derived_identity_type() {
        let pool = sqlx::SqlitePool::connect(":memory:")
            .await
            .expect("sqlite pool");
        MIGRATOR.run(&pool).await.expect("run migrations");

        let columns: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pragma_table_info('identity_specs') ORDER BY cid")
                .fetch_all(&pool)
                .await
                .expect("read identity_specs columns");

        assert!(
            columns.iter().all(|(column,)| column != "identity_type"),
            "identity_type must be derived from manifest_yaml"
        );
    }

    #[tokio::test]
    async fn sqlite_identity_schema_enforces_owner_and_spec_scope_structure() {
        let pool = sqlx::SqlitePool::connect(":memory:")
            .await
            .expect("sqlite pool");
        MIGRATOR.run(&pool).await.expect("run migrations");
        for workspace in ["alpha", "beta"] {
            sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES (?, 0)")
                .bind(workspace)
                .execute(&pool)
                .await
                .expect("seed workspace");
        }

        for row in [
            ("user", "local", None, "user-global", None),
            (
                "workspace",
                "alpha",
                Some("alpha"),
                "workspace-global",
                None,
            ),
            (
                "workspace",
                "alpha",
                Some("alpha"),
                "workspace-scoped",
                Some("alpha"),
            ),
        ] {
            insert_identity(&pool, row)
                .await
                .expect("valid identity row");
        }
        let identity_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM identities")
            .fetch_one(&pool)
            .await
            .expect("count identities");
        assert_eq!(identity_count, 3);

        for row in [
            ("workspace", "alpha", Some("beta"), "owner-mismatch", None),
            (
                "workspace",
                "missing",
                Some("missing"),
                "missing-workspace",
                None,
            ),
            ("user", "member", None, "user-scoped", Some("alpha")),
            (
                "workspace",
                "alpha",
                Some("alpha"),
                "cross-workspace",
                Some("beta"),
            ),
        ] {
            insert_identity(&pool, row)
                .await
                .expect_err("invalid identity row must be rejected");
        }

        sqlx::query("DELETE FROM workspaces WHERE id = 'alpha'")
            .execute(&pool)
            .await
            .expect("delete workspace");
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM identities")
            .fetch_one(&pool)
            .await
            .expect("count cascaded identity rows");
        assert_eq!(remaining, 1);
    }

    async fn insert_identity(
        pool: &sqlx::SqlitePool,
        (owner_kind, owner_key, workspace_id, name, identity_spec_workspace_id): IdentityRow<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO identities (
                owner_kind, owner_key, workspace_id, name,
                identity_spec_workspace_id, identity_spec_name,
                identity_spec_fingerprint, issuer, identity_type,
                created_at_unix_nanos, updated_at_unix_nanos
             ) VALUES (?, ?, ?, ?, ?, 'missing-spec', 'fingerprint', 'issuer', 'fixed_token', 1, 1)",
        )
        .bind(owner_kind)
        .bind(owner_key)
        .bind(workspace_id)
        .bind(name)
        .bind(identity_spec_workspace_id)
        .execute(pool)
        .await
        .map(|_| ())
    }
}
