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

    async fn migrated_sqlite_pool() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect(":memory:")
            .await
            .expect("sqlite pool");
        MIGRATOR.run(&pool).await.expect("run migrations");
        pool
    }

    async fn insert_user(
        pool: &sqlx::SqlitePool,
        user_id: &str,
        subject: &str,
    ) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
        sqlx::query(
            "INSERT INTO users (user_id, issuer, subject, created_at_unix_nanos, last_login_at_unix_nanos)
             VALUES (?, 'issuer', ?, 10, 10)",
        )
        .bind(user_id)
        .bind(subject)
        .execute(pool)
        .await
    }

    async fn insert_member(
        pool: &sqlx::SqlitePool,
        user_id: &str,
        role: &str,
    ) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role, created_at_unix_nanos)
             VALUES ('workspace-1', ?, ?, 10)",
        )
        .bind(user_id)
        .bind(role)
        .execute(pool)
        .await
    }

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
    async fn workspace_access_control_migration_enforces_schema_and_cascade() {
        let pool = migrated_sqlite_pool().await;

        let user_columns: (String,) = sqlx::query_as(
            "SELECT group_concat(name || ':' || \"notnull\" || ':' || pk, ',')
             FROM (SELECT * FROM pragma_table_info('users') ORDER BY cid)",
        )
        .fetch_one(&pool)
        .await
        .expect("users columns");
        assert_eq!(
            user_columns.0,
            "user_id:1:1,issuer:1:0,subject:1:0,display_name:0:0,created_at_unix_nanos:1:0,last_login_at_unix_nanos:1:0"
        );

        let member_columns: (String,) = sqlx::query_as(
            "SELECT group_concat(name || ':' || \"notnull\" || ':' || pk, ',')
             FROM (SELECT * FROM pragma_table_info('workspace_members') ORDER BY cid)",
        )
        .fetch_one(&pool)
        .await
        .expect("workspace member columns");
        assert_eq!(
            member_columns.0,
            "workspace_id:1:1,user_id:1:2,role:1:0,created_at_unix_nanos:1:0"
        );

        insert_user(&pool, "user-1", "subject-1")
            .await
            .expect("insert user");
        let duplicate_subject = insert_user(&pool, "user-2", "subject-1").await;
        assert!(duplicate_subject.is_err(), "raw subject must be unique");
        insert_user(&pool, "user-2", "subject-2")
            .await
            .expect("insert second user");

        sqlx::query(
            "INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ('workspace-1', 10)",
        )
        .execute(&pool)
        .await
        .expect("insert workspace");
        insert_member(&pool, "user-1", "owner")
            .await
            .expect("insert owner");

        let invalid_role = insert_member(&pool, "user-2", "admin").await;
        assert!(invalid_role.is_err(), "role must be owner or member");

        insert_member(&pool, "user-2", "member")
            .await
            .expect("insert member");

        let duplicate_membership = insert_member(&pool, "user-1", "member").await;
        assert!(
            duplicate_membership.is_err(),
            "workspace and user must form the primary key"
        );

        let missing_user = insert_member(&pool, "missing-user", "member").await;
        assert!(missing_user.is_err(), "membership user must exist");

        let member_indexes: (String,) = sqlx::query_as(
            "SELECT group_concat(name, ',') FROM (
                SELECT name FROM sqlite_master
                WHERE type = 'index' AND tbl_name = 'workspace_members' AND name LIKE 'idx_%'
                ORDER BY name
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("workspace member indexes");
        assert_eq!(
            member_indexes.0,
            "idx_workspace_members_user_workspaces,idx_workspace_members_workspace_role"
        );

        sqlx::query("DELETE FROM workspaces WHERE id = 'workspace-1'")
            .execute(&pool)
            .await
            .expect("delete workspace");
        let member_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workspace_members")
            .fetch_one(&pool)
            .await
            .expect("count memberships");
        assert_eq!(member_count.0, 0, "workspace deletion must cascade");
    }
}
