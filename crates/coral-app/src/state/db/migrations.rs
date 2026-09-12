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
    use sqlx::error::{DatabaseError, ErrorKind};
    use sqlx::{AssertSqlSafe, Column, Executor, SqlSafeStr};

    use super::{
        MIGRATOR, postgres_migrations_are_current, rows_match_current_migrations,
        sqlite_migrations_are_current,
    };
    use crate::bootstrap;

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

    async fn migrated_postgres_pool() -> Option<sqlx::PgPool> {
        let url = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())?;
        let pool = sqlx::PgPool::connect(&url).await.expect("postgres pool");
        MIGRATOR.run(&pool).await.expect("run migrations");
        Some(pool)
    }

    /// A migrated pool for one backend, so a schema contract can be asserted
    /// unchanged against both. Statements embed their literals rather than
    /// binding them because the two backends spell placeholders differently;
    /// every embedded value is generated here, never read from outside.
    enum ContractPool<'a> {
        Sqlite(&'a sqlx::SqlitePool),
        Postgres(&'a sqlx::PgPool),
    }

    impl ContractPool<'_> {
        /// Backend spelling of a one-byte binary literal.
        fn blob_literal(&self) -> &'static str {
            match self {
                ContractPool::Sqlite(_) => "X'00'",
                ContractPool::Postgres(_) => "'\\x00'::bytea",
            }
        }

        async fn try_execute(&self, sql: &str) -> Result<(), sqlx::Error> {
            match self {
                ContractPool::Sqlite(pool) => sqlx::query(AssertSqlSafe(sql))
                    .execute(*pool)
                    .await
                    .map(|_| ()),
                ContractPool::Postgres(pool) => sqlx::query(AssertSqlSafe(sql))
                    .execute(*pool)
                    .await
                    .map(|_| ()),
            }
        }

        async fn execute(&self, sql: &str) {
            if let Err(error) = self.try_execute(sql).await {
                panic!("statement must be accepted: {sql}: {error}");
            }
        }

        async fn expect_rejected(&self, sql: &str) -> sqlx::Error {
            match self.try_execute(sql).await {
                Ok(()) => panic!("statement must be rejected: {sql}"),
                Err(error) => error,
            }
        }

        async fn count(&self, sql: &str) -> i64 {
            match self {
                ContractPool::Sqlite(pool) => {
                    sqlx::query_scalar(AssertSqlSafe(sql))
                        .fetch_one(*pool)
                        .await
                }
                ContractPool::Postgres(pool) => {
                    sqlx::query_scalar(AssertSqlSafe(sql))
                        .fetch_one(*pool)
                        .await
                }
            }
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
        }

        async fn text(&self, sql: &str) -> String {
            match self {
                ContractPool::Sqlite(pool) => {
                    sqlx::query_scalar(AssertSqlSafe(sql))
                        .fetch_one(*pool)
                        .await
                }
                ContractPool::Postgres(pool) => {
                    sqlx::query_scalar(AssertSqlSafe(sql))
                        .fetch_one(*pool)
                        .await
                }
            }
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
        }

        async fn versions(&self, sql: &str) -> Vec<i64> {
            match self {
                ContractPool::Sqlite(pool) => {
                    sqlx::query_scalar(AssertSqlSafe(sql))
                        .fetch_all(*pool)
                        .await
                }
                ContractPool::Postgres(pool) => {
                    sqlx::query_scalar(AssertSqlSafe(sql))
                        .fetch_all(*pool)
                        .await
                }
            }
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
        }

        async fn column_names(&self, table: &str) -> Vec<String> {
            let sql = AssertSqlSafe(format!("SELECT * FROM {table}")).into_sql_str();
            match self {
                ContractPool::Sqlite(pool) => (*pool)
                    .describe(sql.clone())
                    .await
                    .map(|described| column_names(described.columns())),
                ContractPool::Postgres(pool) => (*pool)
                    .describe(sql.clone())
                    .await
                    .map(|described| column_names(described.columns())),
            }
            .unwrap_or_else(|error| panic!("describe {table}: {error}"))
        }
    }

    fn column_names<C: Column>(columns: &[C]) -> Vec<String> {
        columns
            .iter()
            .map(|column| column.name().to_owned())
            .collect()
    }

    /// The constraint a rejected statement violated, as classified by whichever
    /// backend rejected it.
    fn violated(error: &sqlx::Error) -> Option<ErrorKind> {
        error.as_database_error().map(DatabaseError::kind)
    }

    /// One column of a new-series table: a literal that is valid for it, and
    /// whether the schema must reject `NULL` there. Literals may use the
    /// `{workspace}`, `{source}`, and `{blob}` placeholders.
    struct ColumnContract {
        name: &'static str,
        value: &'static str,
        required: bool,
    }

    const fn required(name: &'static str, value: &'static str) -> ColumnContract {
        ColumnContract {
            name,
            value,
            required: true,
        }
    }

    const fn nullable(name: &'static str, value: &'static str) -> ColumnContract {
        ColumnContract {
            name,
            value,
            required: false,
        }
    }

    /// The shape one new-series table must have on every backend.
    struct TableContract {
        table: &'static str,
        primary_key: &'static [&'static str],
        columns: &'static [ColumnContract],
    }

    impl TableContract {
        /// A complete `INSERT` for this table, with `overrides` replacing the
        /// literal of the columns they name.
        fn insert_sql(&self, workspace: &str, blob: &str, overrides: &[(&str, &str)]) -> String {
            let names: Vec<&str> = self.columns.iter().map(|column| column.name).collect();
            let values: Vec<String> = self
                .columns
                .iter()
                .map(|column| {
                    overrides
                        .iter()
                        .find(|(name, _)| *name == column.name)
                        .map_or_else(
                            || resolve_literal(column, workspace, blob),
                            |(_, value)| (*value).to_owned(),
                        )
                })
                .collect();
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.table,
                names.join(", "),
                values.join(", ")
            )
        }

        /// A `WHERE` clause selecting exactly the row `insert_sql` writes.
        fn primary_key_predicate(&self, workspace: &str, blob: &str) -> String {
            let terms: Vec<String> = self
                .primary_key
                .iter()
                .map(|key| {
                    let column = self
                        .columns
                        .iter()
                        .find(|column| column.name == *key)
                        .unwrap_or_else(|| panic!("{}.{key} is not a declared column", self.table));
                    format!("{key} = {}", resolve_literal(column, workspace, blob))
                })
                .collect();
            terms.join(" AND ")
        }
    }

    /// A column's literal with its placeholders filled in.
    fn resolve_literal(column: &ColumnContract, workspace: &str, blob: &str) -> String {
        column
            .value
            .replace("{workspace}", workspace)
            .replace("{source}", SEEDED_SOURCE)
            .replace("{blob}", blob)
    }

    /// The source row every child-table contract row hangs off.
    const SEEDED_SOURCE: &str = "seeded-source";

    /// Every table the 0020/0021 series introduces, parents before children.
    const SOURCE_CONTRACTS: [TableContract; 6] = [
        TableContract {
            table: "sources",
            primary_key: &["workspace_id", "name"],
            columns: &[
                required("workspace_id", "'{workspace}'"),
                required("name", "'contract-source'"),
                nullable("version", "'1.2.3'"),
                required("origin_kind", "'imported'"),
                nullable("credential_storage", "'file'"),
                required(
                    "credential_revision",
                    "'11111111-1111-1111-1111-111111111111'",
                ),
                required("created_at_unix_nanos", "1"),
                required("updated_at_unix_nanos", "2"),
            ],
        },
        TableContract {
            table: "source_variables",
            primary_key: &["workspace_id", "source_name", "key"],
            columns: &[
                required("workspace_id", "'{workspace}'"),
                required("source_name", "'{source}'"),
                required("key", "'region'"),
                required("value", "'us-east-1'"),
            ],
        },
        TableContract {
            table: "source_secret_keys",
            primary_key: &["workspace_id", "source_name", "key"],
            columns: &[
                required("workspace_id", "'{workspace}'"),
                required("source_name", "'{source}'"),
                required("key", "'api_token'"),
            ],
        },
        // The source name below deliberately names no row in `sources`: a
        // tombstone is written after its source is gone, so this table must
        // carry the workspace foreign key and no other.
        TableContract {
            table: "source_tombstones",
            primary_key: &["workspace_id", "source_name"],
            columns: &[
                required("workspace_id", "'{workspace}'"),
                required("source_name", "'already-deleted-source'"),
                required("deleted_at_unix_nanos", "7"),
            ],
        },
        TableContract {
            table: "source_manifests",
            primary_key: &["workspace_id", "source_name"],
            columns: &[
                required("workspace_id", "'{workspace}'"),
                required("source_name", "'{source}'"),
                required("manifest_yaml", "'name: {source}'"),
                required("manifest_hash", "'sha256:abc'"),
                required("created_at_unix_nanos", "3"),
            ],
        },
        TableContract {
            table: "materializations",
            primary_key: &["workspace_id", "source_name"],
            columns: &[
                required("workspace_id", "'{workspace}'"),
                required("source_name", "'{source}'"),
                required("materialization_version", "'v4'"),
                nullable("fingerprint_yaml", "'fingerprint: v1'"),
                required("projections_yaml", "'projections: []'"),
                nullable("diagnostics_yaml", "'diagnostics: []'"),
                required("source_document_raw", "{blob}"),
                required("source_document_yaml", "'name: {source}'"),
                required("semantic_ir_yaml", "'ir: none'"),
                required("operation_metadata_yaml", "'op: none'"),
                required("created_at_unix_nanos", "4"),
            ],
        },
    ];

    /// Rows the 0020/0021 series owns, counted within one workspace so the
    /// assertions hold on a Postgres database shared with sibling tests.
    #[derive(Debug, PartialEq, Eq)]
    struct SourceRowCounts {
        sources: i64,
        variables: i64,
        secret_keys: i64,
        manifests: i64,
        materializations: i64,
        tombstones: i64,
    }

    async fn row_counts(pool: &ContractPool<'_>, workspace: &str) -> SourceRowCounts {
        let mut counts = [0_i64; SOURCE_CONTRACTS.len()];
        for (slot, contract) in counts.iter_mut().zip(&SOURCE_CONTRACTS) {
            *slot = pool
                .count(&format!(
                    "SELECT COUNT(*) FROM {} WHERE workspace_id = '{workspace}'",
                    contract.table
                ))
                .await;
        }
        let [
            sources,
            variables,
            secret_keys,
            tombstones,
            manifests,
            materializations,
        ] = counts;
        SourceRowCounts {
            sources,
            variables,
            secret_keys,
            manifests,
            materializations,
            tombstones,
        }
    }

    fn unique_workspace_id() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        format!("workspace_{}_{nanos}", std::process::id())
    }

    async fn seed_workspace(pool: &ContractPool<'_>, workspace: &str) {
        pool.execute(&format!(
            "INSERT INTO workspaces (id, created_at_unix_nanos) VALUES ('{workspace}', 0)"
        ))
        .await;
    }

    async fn seed_source(pool: &ContractPool<'_>, workspace: &str, source: &str) {
        pool.execute(&format!(
            "INSERT INTO sources \
                (workspace_id, name, origin_kind, created_at_unix_nanos, updated_at_unix_nanos) \
             VALUES ('{workspace}', '{source}', 'imported', 0, 0)"
        ))
        .await;
    }

    /// A source plus one row in each of its four child tables.
    async fn seed_source_with_children(pool: &ContractPool<'_>, workspace: &str, source: &str) {
        seed_source(pool, workspace, source).await;
        let blob = pool.blob_literal();
        for sql in [
            format!(
                "INSERT INTO source_variables (workspace_id, source_name, key, value) \
                 VALUES ('{workspace}', '{source}', 'region', 'us-east-1')"
            ),
            format!(
                "INSERT INTO source_secret_keys (workspace_id, source_name, key) \
                 VALUES ('{workspace}', '{source}', 'api_token')"
            ),
            format!(
                "INSERT INTO source_manifests \
                    (workspace_id, source_name, manifest_yaml, manifest_hash, \
                     created_at_unix_nanos) \
                 VALUES ('{workspace}', '{source}', 'name: {source}', 'sha256:abc', 0)"
            ),
            format!(
                "INSERT INTO materializations \
                    (workspace_id, source_name, materialization_version, projections_yaml, \
                     source_document_raw, source_document_yaml, semantic_ir_yaml, \
                     created_at_unix_nanos) \
                 VALUES ('{workspace}', '{source}', 'v4', 'projections: []', {blob}, '', '', 0)"
            ),
        ] {
            pool.execute(&sql).await;
        }
    }

    async fn seed_tombstone(pool: &ContractPool<'_>, workspace: &str, source: &str) {
        pool.execute(&format!(
            "INSERT INTO source_tombstones (workspace_id, source_name, deleted_at_unix_nanos) \
             VALUES ('{workspace}', '{source}', 7)"
        ))
        .await;
    }

    /// Deleting the workspace takes every row this suite wrote with it.
    async fn drop_workspace(pool: &ContractPool<'_>, workspace: &str) {
        pool.execute(&format!("DELETE FROM workspaces WHERE id = '{workspace}'"))
            .await;
    }

    /// One table's columns, `NOT NULL` columns, nullable columns, and primary
    /// key, asserted in that order against a live migrated database.
    async fn assert_table_contract(
        pool: &ContractPool<'_>,
        contract: &TableContract,
        workspace: &str,
    ) {
        let table = contract.table;
        let blob = pool.blob_literal();
        let expected = column_contract_names(contract);
        assert_eq!(pool.column_names(table).await, expected, "{table} columns");
        for key in contract.primary_key {
            assert!(
                expected.iter().any(|name| name == key),
                "{table} primary key names an absent column: {key}"
            );
        }

        for column in contract.columns.iter().filter(|column| column.required) {
            let sql = contract.insert_sql(workspace, blob, &[(column.name, "NULL")]);
            let error = pool.expect_rejected(&sql).await;
            assert_eq!(
                violated(&error),
                Some(ErrorKind::NotNullViolation),
                "{table}.{} must reject NULL: {error}",
                column.name
            );
        }

        // Writing the row with every optional column NULL proves they are
        // optional; writing it a second time proves the primary key.
        let optional_nulls: Vec<(&str, &str)> = contract
            .columns
            .iter()
            .filter(|column| !column.required)
            .map(|column| (column.name, "NULL"))
            .collect();
        let sparse_row = contract.insert_sql(workspace, blob, &optional_nulls);
        pool.execute(&sparse_row).await;
        let error = pool.expect_rejected(&sparse_row).await;
        assert_eq!(
            violated(&error),
            Some(ErrorKind::UniqueViolation),
            "{table} must reject a duplicate primary key: {error}"
        );

        pool.execute(&format!(
            "DELETE FROM {table} WHERE {}",
            contract.primary_key_predicate(workspace, blob)
        ))
        .await;
        pool.execute(&contract.insert_sql(workspace, blob, &[]))
            .await;
    }

    fn column_contract_names(contract: &TableContract) -> Vec<String> {
        contract
            .columns
            .iter()
            .map(|column| column.name.to_owned())
            .collect()
    }

    /// Every new table is anchored to its workspace, and the four child tables
    /// additionally to their source row.
    async fn assert_source_foreign_keys(pool: &ContractPool<'_>, workspace: &str) {
        let blob = pool.blob_literal();
        let absent_workspace = format!("'{workspace}-absent'");
        for contract in &SOURCE_CONTRACTS {
            let sql = contract.insert_sql(workspace, blob, &[("workspace_id", &absent_workspace)]);
            let error = pool.expect_rejected(&sql).await;
            assert_eq!(
                violated(&error),
                Some(ErrorKind::ForeignKeyViolation),
                "{} must require its workspace: {error}",
                contract.table
            );
        }

        for contract in SOURCE_CONTRACTS
            .iter()
            .filter(|contract| contract.table != "sources")
            .filter(|contract| contract.table != "source_tombstones")
        {
            let sql = contract.insert_sql(workspace, blob, &[("source_name", "'absent-source'")]);
            let error = pool.expect_rejected(&sql).await;
            assert_eq!(
                violated(&error),
                Some(ErrorKind::ForeignKeyViolation),
                "{} must require its source: {error}",
                contract.table
            );
        }
    }

    /// Columns the schema fills in for a writer that omits them.
    async fn assert_column_defaults(pool: &ContractPool<'_>, workspace: &str) {
        let blob = pool.blob_literal();
        pool.execute(&format!(
            "INSERT INTO sources \
                (workspace_id, name, origin_kind, created_at_unix_nanos, updated_at_unix_nanos) \
             VALUES ('{workspace}', 'defaulted-source', 'bundled', 0, 0)"
        ))
        .await;
        pool.execute(&format!(
            "INSERT INTO materializations \
                (workspace_id, source_name, materialization_version, projections_yaml, \
                 source_document_raw, source_document_yaml, semantic_ir_yaml, \
                 created_at_unix_nanos) \
             VALUES ('{workspace}', 'defaulted-source', 'v4', 'projections: []', {blob}, '', '', 0)"
        ))
        .await;

        assert_eq!(
            pool.text(&format!(
                "SELECT credential_revision FROM sources \
                 WHERE workspace_id = '{workspace}' AND name = 'defaulted-source'"
            ))
            .await,
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            pool.text(&format!(
                "SELECT operation_metadata_yaml FROM materializations \
                 WHERE workspace_id = '{workspace}' AND source_name = 'defaulted-source'"
            ))
            .await,
            String::new()
        );
    }

    async fn assert_source_schema_contract(pool: &ContractPool<'_>) {
        let workspace = unique_workspace_id();
        seed_workspace(pool, &workspace).await;
        seed_source(pool, &workspace, SEEDED_SOURCE).await;

        for contract in &SOURCE_CONTRACTS {
            assert_table_contract(pool, contract, &workspace).await;
        }
        assert_source_foreign_keys(pool, &workspace).await;
        assert_column_defaults(pool, &workspace).await;

        drop_workspace(pool, &workspace).await;
    }

    /// Deleting one source takes its catalog and artifact rows with it in a
    /// single statement, and leaves its sibling and its tombstone standing.
    async fn assert_source_delete_cascade(pool: &ContractPool<'_>) {
        let workspace = unique_workspace_id();
        seed_workspace(pool, &workspace).await;
        seed_source_with_children(pool, &workspace, "source-1").await;
        seed_source_with_children(pool, &workspace, "source-2").await;
        seed_tombstone(pool, &workspace, "source-1").await;

        pool.execute(&format!(
            "DELETE FROM sources WHERE workspace_id = '{workspace}' AND name = 'source-1'"
        ))
        .await;

        assert_eq!(
            row_counts(pool, &workspace).await,
            SourceRowCounts {
                sources: 1,
                variables: 1,
                secret_keys: 1,
                manifests: 1,
                materializations: 1,
                tombstones: 1,
            }
        );

        drop_workspace(pool, &workspace).await;
    }

    /// Deleting a workspace cascades through its sources to every catalog,
    /// artifact, and tombstone row in a single statement.
    async fn assert_workspace_delete_cascade(pool: &ContractPool<'_>) {
        let workspace = unique_workspace_id();
        seed_workspace(pool, &workspace).await;
        seed_source_with_children(pool, &workspace, "source-1").await;
        seed_source_with_children(pool, &workspace, "source-2").await;
        seed_tombstone(pool, &workspace, "already-deleted-source").await;

        assert_eq!(
            row_counts(pool, &workspace).await,
            SourceRowCounts {
                sources: 2,
                variables: 2,
                secret_keys: 2,
                manifests: 2,
                materializations: 2,
                tombstones: 1,
            }
        );

        drop_workspace(pool, &workspace).await;

        assert_eq!(
            row_counts(pool, &workspace).await,
            SourceRowCounts {
                sources: 0,
                variables: 0,
                secret_keys: 0,
                manifests: 0,
                materializations: 0,
                tombstones: 0,
            }
        );
    }

    /// A migrated database records every embedded migration, the 0020/0021
    /// source series included.
    async fn assert_source_series_recorded(pool: &ContractPool<'_>) {
        let recorded = pool
            .versions("SELECT version FROM _sqlx_migrations ORDER BY version")
            .await;
        let embedded: Vec<i64> = MIGRATOR
            .migrations
            .iter()
            .map(|migration| migration.version)
            .collect();

        assert_eq!(recorded, embedded);
        assert!(
            recorded.contains(&20) && recorded.contains(&21),
            "source series is not recorded: {recorded:?}"
        );
    }

    #[tokio::test]
    async fn sqlite_source_schema_contract() {
        let pool = migrated_sqlite_pool().await;

        assert_source_schema_contract(&ContractPool::Sqlite(&pool)).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the schema contract against Postgres"]
    async fn source_schema_contract_on_postgres() {
        let Some(pool) = migrated_postgres_pool().await else {
            return;
        };

        assert_source_schema_contract(&ContractPool::Postgres(&pool)).await;
    }

    #[tokio::test]
    async fn sqlite_source_delete_cascade() {
        let pool = migrated_sqlite_pool().await;

        assert_source_delete_cascade(&ContractPool::Sqlite(&pool)).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the source cascade contract against Postgres"]
    async fn source_delete_cascade_contract_on_postgres() {
        let Some(pool) = migrated_postgres_pool().await else {
            return;
        };

        assert_source_delete_cascade(&ContractPool::Postgres(&pool)).await;
    }

    #[tokio::test]
    async fn sqlite_workspace_delete_cascade() {
        let pool = migrated_sqlite_pool().await;

        assert_workspace_delete_cascade(&ContractPool::Sqlite(&pool)).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the workspace cascade contract against Postgres"]
    async fn workspace_delete_cascade_contract_on_postgres() {
        let Some(pool) = migrated_postgres_pool().await else {
            return;
        };

        assert_workspace_delete_cascade(&ContractPool::Postgres(&pool)).await;
    }

    #[tokio::test]
    async fn sqlite_source_series_is_current_after_migrating() {
        let pool = migrated_sqlite_pool().await;

        assert!(
            sqlite_migrations_are_current(&pool)
                .await
                .expect("sqlite currency check")
        );
        assert_source_series_recorded(&ContractPool::Sqlite(&pool)).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the currency contract against Postgres"]
    async fn source_series_currency_contract_on_postgres() {
        let Some(pool) = migrated_postgres_pool().await else {
            return;
        };

        assert!(
            postgres_migrations_are_current(&pool)
                .await
                .expect("postgres currency check")
        );
        assert_source_series_recorded(&ContractPool::Postgres(&pool)).await;
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
