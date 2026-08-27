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
    use sea_query::{Expr, ExprTrait, Func, OnConflict, Query, SelectStatement};
    use tempfile::tempdir;

    use super::{MIGRATOR, rows_match_current_migrations};
    use crate::sources::SourceName;
    use crate::state::AppStateLayout;
    use crate::state::db::repositories::credential_documents::CredentialDocumentWrite;
    use crate::state::db::schema::{
        SourceManifests, SourceSecretKeys, SourceVariables, Sources, Workspaces,
    };
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, DbError, DbSession, ResolvedDatabaseConfig};
    use crate::telemetry::{
        StoredTraceInvocationKind, StoredTraceOperationKind, StoredTraceStatus, TraceSummaryRecord,
    };
    use crate::workspaces::WorkspaceName;

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

    #[tokio::test]
    async fn source_catalog_migration_contract_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let config = DatabaseConfig::load(&layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_source_catalog_migration_contract(&db).await;
    }

    #[expect(
        clippy::too_many_lines,
        reason = "The migration contract keeps seed, uniqueness, source cascade, and workspace cascade checks together so schema regressions are visible in one fixture."
    )]
    async fn assert_source_catalog_migration_contract(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let workspace_id = format!("workspace_{suffix}");
        let source_name = format!("source_{suffix}");
        let trace_id = format!("trace_{suffix}");
        let workspace = WorkspaceName::parse(&workspace_id).expect("workspace");
        let source = SourceName::parse(&source_name).expect("source name");
        let mut session = db.begin().await.expect("begin migration contract tx");

        insert_source_catalog_rows(&mut session, &workspace_id, &source_name)
            .await
            .expect("insert source catalog rows");
        session
            .credential_documents()
            .insert_if_absent(&workspace, &source, &credential_document_write(), 5)
            .await
            .expect("insert credential document row");
        session
            .trace_summaries()
            .upsert(
                &migration_trace_summary(&workspace_id, &trace_id),
                "migration-test-store",
            )
            .await
            .expect("insert trace summary row");
        assert_eq!(
            source_variable_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count variables"),
            1
        );
        assert_eq!(
            source_secret_key_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count secret keys"),
            1
        );
        assert_eq!(
            source_credential_revision(&mut session, &workspace_id, &source_name)
                .await
                .expect("read credential revision"),
            uuid::Uuid::nil().to_string()
        );
        assert_eq!(
            source_manifest_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count manifests"),
            1
        );
        session
            .commit()
            .await
            .expect("commit seed source catalog rows");

        assert_source_catalog_uniqueness_contract(db, &workspace_id, &source_name).await;

        let mut session = db
            .begin()
            .await
            .expect("begin cascade migration contract tx");

        let alternate_workspace_id = format!("alternate_workspace_{suffix}");
        insert_source_catalog_rows(&mut session, &alternate_workspace_id, &source_name)
            .await
            .expect("insert same source name in another workspace");
        assert_eq!(
            source_count(&mut session, &alternate_workspace_id)
                .await
                .expect("count alternate workspace source"),
            1
        );
        delete_workspace(&mut session, &alternate_workspace_id)
            .await
            .expect("delete alternate workspace");

        delete_source(&mut session, &workspace_id, &source_name)
            .await
            .expect("delete source");
        assert_eq!(
            source_variable_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count variables after source delete"),
            0
        );
        assert_eq!(
            source_secret_key_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count secret keys after source delete"),
            0
        );
        assert_eq!(
            source_manifest_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count manifests after source delete"),
            0
        );
        assert!(
            session
                .credential_documents()
                .get(&workspace, &source)
                .await
                .expect("get credential document after source delete")
                .is_none()
        );

        insert_source_catalog_rows(&mut session, &workspace_id, &source_name)
            .await
            .expect("reinsert source catalog rows");
        session
            .credential_documents()
            .insert_if_absent(&workspace, &source, &credential_document_write(), 5)
            .await
            .expect("reinsert credential document row");
        delete_workspace(&mut session, &workspace_id)
            .await
            .expect("delete workspace");
        assert_eq!(
            source_count(&mut session, &workspace_id)
                .await
                .expect("count sources after workspace delete"),
            0
        );
        assert_eq!(
            source_variable_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count variables after workspace delete"),
            0
        );
        assert_eq!(
            source_secret_key_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count secret keys after workspace delete"),
            0
        );
        assert_eq!(
            source_manifest_count(&mut session, &workspace_id, &source_name)
                .await
                .expect("count manifests after workspace delete"),
            0
        );
        assert!(
            session
                .credential_documents()
                .get(&workspace, &source)
                .await
                .expect("get credential document after workspace delete")
                .is_none()
        );
        assert!(
            session
                .trace_summaries()
                .get(&trace_id)
                .await
                .expect("get trace summary after workspace delete")
                .is_none()
        );
        session
            .commit()
            .await
            .expect("commit cascade migration contract tx");
    }

    async fn insert_source_catalog_rows<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        insert_workspace_row(session, workspace_id).await?;
        insert_source_row(session, workspace_id, source_name).await?;
        insert_source_variable_row(session, workspace_id, source_name, "REGION", "us-east-1")
            .await?;
        insert_source_secret_key_row(session, workspace_id, source_name, "API_TOKEN").await?;
        insert_source_manifest_row(session, workspace_id, source_name).await
    }

    async fn assert_source_catalog_uniqueness_contract(
        db: &CoralDb,
        workspace_id: &str,
        source_name: &str,
    ) {
        let mut tx = db.begin().await.expect("begin duplicate source tx");
        assert!(
            insert_source_row(&mut tx, workspace_id, source_name)
                .await
                .is_err(),
            "duplicate source identity should fail"
        );
        tx.rollback().await.expect("rollback duplicate source tx");

        let mut tx = db.begin().await.expect("begin duplicate variable tx");
        assert!(
            insert_source_variable_row(&mut tx, workspace_id, source_name, "REGION", "eu-west-1")
                .await
                .is_err(),
            "duplicate source variable key should fail"
        );
        tx.rollback().await.expect("rollback duplicate variable tx");

        let mut tx = db.begin().await.expect("begin duplicate secret key tx");
        assert!(
            insert_source_secret_key_row(&mut tx, workspace_id, source_name, "OTHER_TOKEN")
                .await
                .is_ok(),
            "distinct source secret keys should be allowed"
        );
        assert!(
            insert_source_secret_key_row(&mut tx, workspace_id, source_name, "API_TOKEN")
                .await
                .is_err(),
            "duplicate source secret key should fail"
        );
        tx.rollback()
            .await
            .expect("rollback duplicate secret key tx");
    }

    fn credential_document_write() -> CredentialDocumentWrite {
        CredentialDocumentWrite {
            ciphertext: b"ciphertext".to_vec(),
            nonce: b"nonce".to_vec(),
            wrapped_dek: b"wrapped-dek".to_vec(),
            wrapped_dek_nonce: b"wrapped-dek-nonce".to_vec(),
            key_id: "key-id".to_string(),
            algorithm: "AES-256-GCM".to_string(),
            aad_version: 1,
        }
    }

    async fn insert_workspace_row<S>(session: &mut S, workspace_id: &str) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(Workspaces::Table)
                    .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
                    .values_panic([Expr::val(workspace_id), Expr::val(1)])
                    .on_conflict(OnConflict::column(Workspaces::Id).do_nothing().to_owned())
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(Sources::Table)
                    .columns([
                        Sources::WorkspaceId,
                        Sources::Name,
                        Sources::Version,
                        Sources::OriginKind,
                        Sources::CredentialStorage,
                        Sources::CreatedAtUnixNanos,
                        Sources::UpdatedAtUnixNanos,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val("1.0.0"),
                        Expr::val("imported"),
                        Expr::val("file"),
                        Expr::val(2),
                        Expr::val(3),
                    ])
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_variable_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
        key: &str,
        value: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(SourceVariables::Table)
                    .columns([
                        SourceVariables::WorkspaceId,
                        SourceVariables::SourceName,
                        SourceVariables::Key,
                        SourceVariables::Value,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val(key),
                        Expr::val(value),
                    ])
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_secret_key_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
        key: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(SourceSecretKeys::Table)
                    .columns([
                        SourceSecretKeys::WorkspaceId,
                        SourceSecretKeys::SourceName,
                        SourceSecretKeys::Key,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val(key),
                    ])
                    .to_owned(),
            )
            .await
    }

    async fn insert_source_manifest_row<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::insert()
                    .into_table(SourceManifests::Table)
                    .columns([
                        SourceManifests::WorkspaceId,
                        SourceManifests::SourceName,
                        SourceManifests::ManifestYaml,
                        SourceManifests::ManifestHash,
                        SourceManifests::CreatedAtUnixNanos,
                    ])
                    .values_panic([
                        Expr::val(workspace_id),
                        Expr::val(source_name),
                        Expr::val("name: migration_contract\n"),
                        Expr::val("manifest-hash"),
                        Expr::val(4),
                    ])
                    .to_owned(),
            )
            .await
    }

    fn migration_trace_summary(workspace_id: &str, trace_id: &str) -> TraceSummaryRecord {
        TraceSummaryRecord {
            trace_id: trace_id.to_string(),
            workspace_id: Some(workspace_id.to_string()),
            root_span_id: "root".to_string(),
            name: "coral.query".to_string(),
            query: "SELECT 1".to_string(),
            status: StoredTraceStatus::Ok,
            start_time_unix_nanos: 1,
            end_time_unix_nanos: 2,
            duration_nanos: 1,
            span_count: 1,
            row_count: 1,
            row_count_recorded: true,
            operation_kind: StoredTraceOperationKind::Unspecified,
            operation_name: String::new(),
            invocation_kind: StoredTraceInvocationKind::Unspecified,
        }
    }

    async fn delete_source<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::delete()
                    .from_table(Sources::Table)
                    .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
                    .and_where(Expr::col(Sources::Name).eq(source_name))
                    .to_owned(),
            )
            .await
    }

    async fn delete_workspace<S>(session: &mut S, workspace_id: &str) -> Result<(), DbError>
    where
        S: DbSession,
    {
        session
            .execute(
                Query::delete()
                    .from_table(Workspaces::Table)
                    .and_where(Expr::col(Workspaces::Id).eq(workspace_id))
                    .to_owned(),
            )
            .await
    }

    async fn source_count<S>(session: &mut S, workspace_id: &str) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        fetch_count(
            session,
            Query::select()
                .expr(Func::count(Expr::val(1)))
                .from(Sources::Table)
                .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
                .to_owned(),
        )
        .await
    }

    async fn source_variable_count<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        fetch_count(
            session,
            Query::select()
                .expr(Func::count(Expr::val(1)))
                .from(SourceVariables::Table)
                .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_id))
                .and_where(Expr::col(SourceVariables::SourceName).eq(source_name))
                .to_owned(),
        )
        .await
    }

    async fn source_secret_key_count<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        fetch_count(
            session,
            Query::select()
                .expr(Func::count(Expr::val(1)))
                .from(SourceSecretKeys::Table)
                .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_id))
                .and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name))
                .to_owned(),
        )
        .await
    }

    async fn source_credential_revision<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<String, DbError>
    where
        S: DbSession,
    {
        Ok(session
            .fetch_all_scalars::<String>(
                Query::select()
                    .column(Sources::CredentialRevision)
                    .from(Sources::Table)
                    .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_id))
                    .and_where(Expr::col(Sources::Name).eq(source_name))
                    .to_owned(),
            )
            .await?
            .into_iter()
            .next()
            .expect("source row"))
    }

    async fn fetch_count<S>(session: &mut S, statement: SelectStatement) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        Ok(session
            .fetch_all_scalars::<i64>(statement)
            .await?
            .into_iter()
            .next()
            .expect("count row"))
    }

    async fn source_manifest_count<S>(
        session: &mut S,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<i64, DbError>
    where
        S: DbSession,
    {
        fetch_count(
            session,
            Query::select()
                .expr(Func::count(Expr::val(1)))
                .from(SourceManifests::Table)
                .and_where(Expr::col(SourceManifests::WorkspaceId).eq(workspace_id))
                .and_where(Expr::col(SourceManifests::SourceName).eq(source_name))
                .to_owned(),
        )
        .await
    }
}
