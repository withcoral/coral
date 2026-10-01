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

    async fn migrated_sqlite_pool() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect(":memory:")
            .await
            .expect("sqlite pool");
        MIGRATOR.run(&pool).await.expect("run migrations");
        pool
    }

    async fn insert_user(pool: &sqlx::SqlitePool, user_id: &str, issuer: &str, subject: &str) {
        sqlx::query(
            "INSERT INTO users (\
                user_id, issuer, subject, display_name, \
                created_at_unix_nanos, last_login_at_unix_nanos\
            ) VALUES (?, ?, ?, NULL, 0, 0)",
        )
        .bind(user_id)
        .bind(issuer)
        .bind(subject)
        .execute(pool)
        .await
        .expect("insert user");
    }

    #[tokio::test]
    async fn sqlite_workspace_id_rejects_null() {
        let pool = migrated_sqlite_pool().await;

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
    async fn sqlite_migrations_provision_no_workspace() {
        let pool = migrated_sqlite_pool().await;

        let workspaces: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workspaces")
            .fetch_one(&pool)
            .await
            .expect("count workspaces");
        let members: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workspace_members")
            .fetch_one(&pool)
            .await
            .expect("count workspace members");

        assert_eq!(workspaces.0, 0);
        assert_eq!(members.0, 0);
    }

    #[tokio::test]
    async fn sqlite_users_reject_duplicate_subject() {
        let pool = migrated_sqlite_pool().await;
        insert_user(&pool, "user-1", "https://issuer.example", "subject-1").await;

        let error = sqlx::query(
            "INSERT INTO users (\
                user_id, issuer, subject, display_name, \
                created_at_unix_nanos, last_login_at_unix_nanos\
            ) VALUES ('user-2', 'https://other.example', 'subject-1', NULL, 0, 0)",
        )
        .execute(&pool)
        .await
        .expect_err("duplicate subject must be rejected");

        assert!(
            error.to_string().contains("UNIQUE constraint failed"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn sqlite_workspace_members_reject_unknown_role() {
        let pool = migrated_sqlite_pool().await;
        sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ('workspace-1', 0)")
            .execute(&pool)
            .await
            .expect("insert workspace");
        insert_user(&pool, "user-1", "https://issuer.example", "subject-1").await;

        for role in ["owner", "member"] {
            sqlx::query(
                "INSERT INTO workspace_members \
                    (workspace_id, user_id, role, created_at_unix_nanos) \
                 VALUES ('workspace-1', 'user-1', ?, 0) \
                 ON CONFLICT (workspace_id, user_id) DO UPDATE SET role = excluded.role",
            )
            .bind(role)
            .execute(&pool)
            .await
            .expect("supported role must be accepted");
        }

        let error = sqlx::query(
            "INSERT INTO workspace_members \
                (workspace_id, user_id, role, created_at_unix_nanos) \
             VALUES ('workspace-1', 'user-1', 'admin', 0)",
        )
        .execute(&pool)
        .await
        .expect_err("unknown role must be rejected");

        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn sqlite_workspace_deletion_cascades_membership() {
        let pool = migrated_sqlite_pool().await;
        sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ('workspace-1', 0)")
            .execute(&pool)
            .await
            .expect("insert workspace");
        insert_user(&pool, "user-1", "https://issuer.example", "subject-1").await;
        sqlx::query(
            "INSERT INTO workspace_members \
                (workspace_id, user_id, role, created_at_unix_nanos) \
             VALUES ('workspace-1', 'user-1', 'owner', 0)",
        )
        .execute(&pool)
        .await
        .expect("insert membership");

        sqlx::query("DELETE FROM workspaces WHERE id = 'workspace-1'")
            .execute(&pool)
            .await
            .expect("delete workspace");

        let members: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workspace_members")
            .fetch_one(&pool)
            .await
            .expect("count workspace members");
        let users: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(&pool)
            .await
            .expect("count users");

        assert_eq!(members.0, 0);
        assert_eq!(users.0, 1);
    }

    #[tokio::test]
    async fn sqlite_membership_requires_known_user() {
        let pool = migrated_sqlite_pool().await;
        sqlx::query("INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ('workspace-1', 0)")
            .execute(&pool)
            .await
            .expect("insert workspace");

        let error = sqlx::query(
            "INSERT INTO workspace_members \
                (workspace_id, user_id, role, created_at_unix_nanos) \
             VALUES ('workspace-1', 'missing-user', 'member', 0)",
        )
        .execute(&pool)
        .await
        .expect_err("unknown user must be rejected");

        assert!(
            error.to_string().contains("FOREIGN KEY constraint failed"),
            "unexpected error: {error}"
        );
    }
}
