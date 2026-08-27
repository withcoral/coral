#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "source catalog repository lands before manager wiring in the stacked PR sequence"
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use sea_query::{
    Condition, Expr, ExprTrait, Iden, JoinType, Order, Query, SelectStatement, UnionType,
};

use crate::credentials::CredentialStorageKind;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::db::schema::{SourceSecretKeys, SourceVariables, Sources};
use crate::state::db::{CoralTx, DbError, DbSession};
use crate::workspaces::WorkspaceName;

#[derive(Debug, sqlx::FromRow)]
struct SourceAggregateRow {
    name: String,
    version: Option<String>,
    origin_kind: String,
    credential_storage: Option<String>,
    credential_revision: String,
    child_key: Option<String>,
    variable_value: Option<String>,
}

#[derive(Iden)]
enum SourceChildren {
    Table,
    WorkspaceId,
    SourceName,
    ChildKey,
    VariableValue,
}

pub(crate) struct SourcesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> SourcesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn list_workspace_sources(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, DbError> {
        let rows = self.source_aggregate_rows(workspace_name, None).await?;
        installed_sources_from_rows(rows)
    }

    pub(crate) async fn list_workspace_source_names(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<String>, DbError> {
        let statement = Query::select()
            .column(Sources::Name)
            .from(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_name.as_str()))
            .order_by(Sources::Name, Order::Asc)
            .to_owned();
        let names: Vec<String> = self.session.fetch_all_scalars(statement).await?;
        names
            .into_iter()
            .map(|name| parse_source_name(&name).map(|name| name.as_str().to_string()))
            .collect()
    }

    pub(crate) async fn get_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<InstalledSource>, DbError> {
        Ok(installed_sources_from_rows(
            self.source_aggregate_rows(workspace_name, Some(source_name))
                .await?,
        )?
        .pop())
    }

    async fn source_aggregate_rows(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: Option<&SourceName>,
    ) -> Result<Vec<SourceAggregateRow>, DbError> {
        self.session
            .fetch_all(source_aggregate_statement(workspace_name, source_name))
            .await
    }

    async fn source_created_at(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<i64>, DbError> {
        let statement = Query::select()
            .column(Sources::CreatedAtUnixNanos)
            .from(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Sources::Name).eq(source_name.as_str()))
            .to_owned();
        let created_at: Option<(i64,)> = self.session.fetch_optional(statement).await?;
        Ok(created_at.map(|(created_at_unix_nanos,)| created_at_unix_nanos))
    }
}

fn source_aggregate_statement(
    workspace_name: &WorkspaceName,
    source_name: Option<&SourceName>,
) -> SelectStatement {
    let mut statement = Query::select();
    statement
        .columns([
            (Sources::Table, Sources::Name),
            (Sources::Table, Sources::Version),
            (Sources::Table, Sources::OriginKind),
            (Sources::Table, Sources::CredentialStorage),
            (Sources::Table, Sources::CredentialRevision),
        ])
        .expr_as(
            Expr::col((SourceChildren::Table, SourceChildren::ChildKey)),
            SourceChildren::ChildKey,
        )
        .expr_as(
            Expr::col((SourceChildren::Table, SourceChildren::VariableValue)),
            SourceChildren::VariableValue,
        )
        .from(Sources::Table)
        .join_subquery(
            JoinType::LeftJoin,
            source_children_statement(workspace_name, source_name),
            SourceChildren::Table,
            Condition::all()
                .add(
                    Expr::col((Sources::Table, Sources::WorkspaceId))
                        .equals((SourceChildren::Table, SourceChildren::WorkspaceId)),
                )
                .add(
                    Expr::col((Sources::Table, Sources::Name))
                        .equals((SourceChildren::Table, SourceChildren::SourceName)),
                ),
        )
        .and_where(Expr::col((Sources::Table, Sources::WorkspaceId)).eq(workspace_name.as_str()))
        .order_by((Sources::Table, Sources::Name), Order::Asc)
        .order_by(
            (SourceChildren::Table, SourceChildren::ChildKey),
            Order::Asc,
        );
    if let Some(source_name) = source_name {
        statement.and_where(Expr::col((Sources::Table, Sources::Name)).eq(source_name.as_str()));
    }
    statement
}

fn source_children_statement(
    workspace_name: &WorkspaceName,
    source_name: Option<&SourceName>,
) -> SelectStatement {
    let mut variables = Query::select();
    variables
        .expr_as(
            Expr::col(SourceVariables::WorkspaceId),
            SourceChildren::WorkspaceId,
        )
        .expr_as(
            Expr::col(SourceVariables::SourceName),
            SourceChildren::SourceName,
        )
        .expr_as(Expr::col(SourceVariables::Key), SourceChildren::ChildKey)
        .expr_as(
            Expr::col(SourceVariables::Value),
            SourceChildren::VariableValue,
        )
        .from(SourceVariables::Table)
        .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_name.as_str()));
    if let Some(source_name) = source_name {
        variables.and_where(Expr::col(SourceVariables::SourceName).eq(source_name.as_str()));
    }

    let mut secrets = Query::select();
    secrets
        .expr_as(
            Expr::col(SourceSecretKeys::WorkspaceId),
            SourceChildren::WorkspaceId,
        )
        .expr_as(
            Expr::col(SourceSecretKeys::SourceName),
            SourceChildren::SourceName,
        )
        .expr_as(Expr::col(SourceSecretKeys::Key), SourceChildren::ChildKey)
        .expr_as(
            Expr::val(Option::<String>::None),
            SourceChildren::VariableValue,
        )
        .from(SourceSecretKeys::Table)
        .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_name.as_str()));
    if let Some(source_name) = source_name {
        secrets.and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name.as_str()));
    }

    variables.union(UnionType::All, secrets);
    variables
}

fn installed_sources_from_rows(
    rows: Vec<SourceAggregateRow>,
) -> Result<Vec<InstalledSource>, DbError> {
    let mut sources = Vec::<SourceAggregate>::new();
    for row in rows {
        let SourceAggregateRow {
            name,
            version,
            origin_kind,
            credential_storage,
            credential_revision,
            child_key,
            variable_value,
        } = row;
        if sources
            .last()
            .is_none_or(|aggregate| aggregate.source.name.as_str() != name)
        {
            sources.push(SourceAggregate::new(
                &name,
                version,
                &origin_kind,
                credential_storage.as_deref(),
                &credential_revision,
            )?);
        }
        let aggregate = sources
            .last_mut()
            .expect("source aggregate exists after processing a database row");
        aggregate.add_child(child_key, variable_value)?;
    }
    Ok(sources
        .into_iter()
        .map(SourceAggregate::into_source)
        .collect())
}

struct SourceAggregate {
    source: InstalledSource,
    secrets: BTreeSet<String>,
}

impl SourceAggregate {
    fn new(
        name: &str,
        version: Option<String>,
        origin_kind: &str,
        credential_storage: Option<&str>,
        credential_revision: &str,
    ) -> Result<Self, DbError> {
        let name = parse_source_name(name)?;
        let credential_revision = parse_credential_revision(&name, credential_revision)?;
        Ok(Self {
            source: InstalledSource {
                name,
                version,
                variables: BTreeMap::new(),
                secrets: Vec::new(),
                credential_storage: credential_storage
                    .map(parse_credential_storage)
                    .transpose()?,
                credential_revision,
                origin: parse_source_origin(origin_kind)?,
            },
            secrets: BTreeSet::new(),
        })
    }

    fn add_child(
        &mut self,
        child_key: Option<String>,
        variable_value: Option<String>,
    ) -> Result<(), DbError> {
        // Variable values are NOT NULL in the database. Secret-key rows project
        // NULL here, while a left join with no child projects NULL for both fields.
        match (child_key, variable_value) {
            (None, None) => Ok(()),
            (Some(key), Some(value)) => {
                self.source.variables.insert(key, value);
                Ok(())
            }
            (Some(key), None) => {
                self.secrets.insert(key);
                Ok(())
            }
            (child_key, variable_value) => Err(DbError::CorruptData(format!(
                "invalid child row for source '{}': key={child_key:?}, variable_value={variable_value:?}",
                self.source.name
            ))),
        }
    }

    fn into_source(mut self) -> InstalledSource {
        self.source.secrets = self.secrets.into_iter().collect();
        self.source
    }
}

impl SourcesRepo<'_, CoralTx<'_>> {
    pub(crate) async fn upsert_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
        now_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let created_at_unix_nanos = self
            .source_created_at(workspace_name, &source.name)
            .await?
            .unwrap_or(now_unix_nanos);
        self.delete_source_catalog(workspace_name, &source.name)
            .await?;
        insert_source(
            self.session,
            workspace_name,
            source,
            created_at_unix_nanos,
            now_unix_nanos,
        )
        .await?;
        self.insert_source_variables(workspace_name, source).await?;
        self.insert_source_secret_keys(workspace_name, source).await
    }

    pub(crate) async fn remove_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<InstalledSource>, DbError> {
        let removed = self.get_source(workspace_name, source_name).await?;
        self.delete_source_catalog(workspace_name, source_name)
            .await?;
        Ok(removed)
    }

    async fn insert_source_variables(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(), DbError> {
        for (key, value) in &source.variables {
            insert_source_variable(self.session, workspace_name, &source.name, key, value).await?;
        }
        Ok(())
    }

    async fn insert_source_secret_keys(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(), DbError> {
        for key in &source.secrets {
            insert_source_secret_key(self.session, workspace_name, &source.name, key).await?;
        }
        Ok(())
    }

    async fn delete_source_catalog(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), DbError> {
        delete_source_secret_keys(self.session, workspace_name, source_name).await?;
        delete_source_variables(self.session, workspace_name, source_name).await?;
        delete_source(self.session, workspace_name, source_name).await
    }
}

async fn insert_source<S>(
    session: &mut S,
    workspace_name: &WorkspaceName,
    source: &InstalledSource,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
) -> Result<(), DbError>
where
    S: DbSession,
{
    let statement = Query::insert()
        .into_table(Sources::Table)
        .columns([
            Sources::WorkspaceId,
            Sources::Name,
            Sources::Version,
            Sources::OriginKind,
            Sources::CredentialStorage,
            Sources::CredentialRevision,
            Sources::CreatedAtUnixNanos,
            Sources::UpdatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(workspace_name.as_str().to_string()),
            Expr::val(source.name.as_str().to_string()),
            Expr::val(source.version.clone()),
            Expr::val(source.origin.as_config_value()),
            Expr::val(
                source
                    .credential_storage
                    .map(CredentialStorageKind::as_config_value),
            ),
            Expr::val(source.credential_revision.to_string()),
            Expr::val(created_at_unix_nanos),
            Expr::val(updated_at_unix_nanos),
        ])
        .to_owned();
    session.execute(statement).await
}

async fn insert_source_variable<S>(
    session: &mut S,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    key: &str,
    value: &str,
) -> Result<(), DbError>
where
    S: DbSession,
{
    let statement = Query::insert()
        .into_table(SourceVariables::Table)
        .columns([
            SourceVariables::WorkspaceId,
            SourceVariables::SourceName,
            SourceVariables::Key,
            SourceVariables::Value,
        ])
        .values_panic([
            Expr::val(workspace_name.as_str().to_string()),
            Expr::val(source_name.as_str().to_string()),
            Expr::val(key.to_string()),
            Expr::val(value.to_string()),
        ])
        .to_owned();
    session.execute(statement).await
}

async fn insert_source_secret_key<S>(
    session: &mut S,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    key: &str,
) -> Result<(), DbError>
where
    S: DbSession,
{
    let statement = Query::insert()
        .into_table(SourceSecretKeys::Table)
        .columns([
            SourceSecretKeys::WorkspaceId,
            SourceSecretKeys::SourceName,
            SourceSecretKeys::Key,
        ])
        .values_panic([
            Expr::val(workspace_name.as_str().to_string()),
            Expr::val(source_name.as_str().to_string()),
            Expr::val(key.to_string()),
        ])
        .to_owned();
    session.execute(statement).await
}

async fn delete_source_secret_keys<S>(
    session: &mut S,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<(), DbError>
where
    S: DbSession,
{
    let statement = Query::delete()
        .from_table(SourceSecretKeys::Table)
        .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_name.as_str()))
        .and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name.as_str()))
        .to_owned();
    session.execute(statement).await
}

async fn delete_source_variables<S>(
    session: &mut S,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<(), DbError>
where
    S: DbSession,
{
    let statement = Query::delete()
        .from_table(SourceVariables::Table)
        .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_name.as_str()))
        .and_where(Expr::col(SourceVariables::SourceName).eq(source_name.as_str()))
        .to_owned();
    session.execute(statement).await
}

async fn delete_source<S>(
    session: &mut S,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<(), DbError>
where
    S: DbSession,
{
    let statement = Query::delete()
        .from_table(Sources::Table)
        .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_name.as_str()))
        .and_where(Expr::col(Sources::Name).eq(source_name.as_str()))
        .to_owned();
    session.execute(statement).await
}

fn parse_source_name(name: &str) -> Result<SourceName, DbError> {
    SourceName::parse(name)
        .map_err(|error| DbError::CorruptData(format!("invalid source name '{name}': {error}")))
}

fn parse_source_origin(origin: &str) -> Result<SourceOrigin, DbError> {
    match origin {
        "bundled" => Ok(SourceOrigin::Bundled),
        "imported" => Ok(SourceOrigin::Imported),
        other => Err(DbError::CorruptData(format!(
            "invalid source origin '{other}'"
        ))),
    }
}

fn parse_credential_storage(storage: &str) -> Result<CredentialStorageKind, DbError> {
    match storage {
        "file" => Ok(CredentialStorageKind::File),
        "keychain" => Ok(CredentialStorageKind::Keychain),
        other => Err(DbError::CorruptData(format!(
            "invalid credential storage '{other}'"
        ))),
    }
}

fn parse_credential_revision(
    source_name: &SourceName,
    credential_revision: &str,
) -> Result<uuid::Uuid, DbError> {
    uuid::Uuid::parse_str(credential_revision).map_err(|error| {
        DbError::CorruptData(format!(
            "invalid credential revision '{credential_revision}' for source '{source_name}': {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use sea_query::{Expr, Query, SelectStatement};
    use sea_query_sqlx::SqlxBinder;
    use sqlx::FromRow;
    use sqlx::postgres::PgRow;
    use sqlx::sqlite::SqliteRow;
    use tempfile::{TempDir, tempdir};
    use tokio::sync::Barrier;

    use crate::bootstrap;
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::schema::Sources;
    use crate::state::db::session::{DbRepos, DbSession};
    use crate::state::db::{CoralDb, DatabaseConfig, DbError, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    struct ObservedReadSession<'a> {
        db: &'a CoralDb,
        interleave: Option<ReadInterleave>,
        read_count: usize,
    }

    struct ReadInterleave {
        first_read_done: Arc<Barrier>,
        writer_done: Arc<Barrier>,
    }

    impl<'a> ObservedReadSession<'a> {
        fn counting(db: &'a CoralDb) -> Self {
            Self {
                db,
                interleave: None,
                read_count: 0,
            }
        }

        fn interleaved(
            db: &'a CoralDb,
            first_read_done: Arc<Barrier>,
            writer_done: Arc<Barrier>,
        ) -> Self {
            Self {
                db,
                interleave: Some(ReadInterleave {
                    first_read_done,
                    writer_done,
                }),
                read_count: 0,
            }
        }

        async fn after_read(&mut self) {
            self.read_count += 1;
            if self.read_count == 1
                && let Some(interleave) = &self.interleave
            {
                interleave.first_read_done.wait().await;
                interleave.writer_done.wait().await;
            }
        }
    }

    impl DbSession for ObservedReadSession<'_> {
        async fn execute<S>(&mut self, statement: S) -> Result<(), DbError>
        where
            S: SqlxBinder,
        {
            let mut session = self.db;
            session.execute(statement).await
        }

        async fn execute_rows_affected<S>(&mut self, statement: S) -> Result<u64, DbError>
        where
            S: SqlxBinder,
        {
            let mut session = self.db;
            session.execute_rows_affected(statement).await
        }

        async fn fetch_optional<T>(
            &mut self,
            statement: SelectStatement,
        ) -> Result<Option<T>, DbError>
        where
            T: Send + Unpin,
            for<'r> T: FromRow<'r, SqliteRow>,
            for<'r> T: FromRow<'r, PgRow>,
        {
            let result = {
                let mut session = self.db;
                session.fetch_optional(statement).await
            };
            self.after_read().await;
            result
        }

        async fn fetch_all<T>(&mut self, statement: SelectStatement) -> Result<Vec<T>, DbError>
        where
            T: Send + Unpin,
            for<'r> T: FromRow<'r, SqliteRow>,
            for<'r> T: FromRow<'r, PgRow>,
        {
            let result = {
                let mut session = self.db;
                session.fetch_all(statement).await
            };
            self.after_read().await;
            result
        }

        async fn fetch_all_scalars<T>(
            &mut self,
            statement: SelectStatement,
        ) -> Result<Vec<T>, DbError>
        where
            T: Send + Unpin,
            for<'r> (T,): FromRow<'r, SqliteRow>,
            for<'r> (T,): FromRow<'r, PgRow>,
        {
            let result = {
                let mut session = self.db;
                session.fetch_all_scalars(statement).await
            };
            self.after_read().await;
            result
        }
    }

    #[tokio::test]
    async fn source_repository_round_trips_across_configured_backends() {
        let (_temp, databases) = configured_databases().await;
        for db in databases {
            assert_source_repository_round_trip(&db).await;
        }
    }

    #[tokio::test]
    async fn source_repository_reports_corrupt_persisted_source_names() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        let workspace = unique_workspace();

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 10)
            .await
            .expect("ensure workspace");
        tx.execute(
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
                    Expr::val(workspace.as_str().to_string()),
                    Expr::val("bad/source"),
                    Expr::val("1.0.0"),
                    Expr::val("imported"),
                    Expr::val("file"),
                    Expr::val(20),
                    Expr::val(30),
                ])
                .to_owned(),
        )
        .await
        .expect("insert corrupt source row");
        tx.commit().await.expect("commit corrupt row");

        let mut session = &db;
        let error = session
            .sources()
            .list_workspace_sources(&workspace)
            .await
            .expect_err("corrupt stored source name should fail decode");
        let DbError::CorruptData(detail) = error else {
            panic!("expected corrupt data error, got {error:?}");
        };
        assert!(detail.contains("invalid source name 'bad/source'"));
    }

    #[tokio::test]
    async fn source_repository_reports_corrupt_credential_revisions() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        let workspace = unique_workspace();
        let source_name = SourceName::parse("corrupt-revision").expect("source name");

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 10)
            .await
            .expect("ensure workspace");
        tx.execute(
            Query::insert()
                .into_table(Sources::Table)
                .columns([
                    Sources::WorkspaceId,
                    Sources::Name,
                    Sources::Version,
                    Sources::OriginKind,
                    Sources::CredentialStorage,
                    Sources::CredentialRevision,
                    Sources::CreatedAtUnixNanos,
                    Sources::UpdatedAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(workspace.as_str().to_string()),
                    Expr::val(source_name.as_str()),
                    Expr::val(Option::<String>::None),
                    Expr::val(SourceOrigin::Imported.as_config_value()),
                    Expr::val(Option::<String>::None),
                    Expr::val("not-a-uuid"),
                    Expr::val(20),
                    Expr::val(30),
                ])
                .to_owned(),
        )
        .await
        .expect("insert corrupt source row");
        tx.commit().await.expect("commit corrupt row");

        let mut session = &db;
        let error = session
            .sources()
            .get_source(&workspace, &source_name)
            .await
            .expect_err("corrupt credential revision should fail decode");
        let DbError::CorruptData(detail) = error else {
            panic!("expected corrupt data error, got {error:?}");
        };
        assert!(detail.contains("invalid credential revision 'not-a-uuid'"));
        assert!(detail.contains(source_name.as_str()));
    }

    #[tokio::test]
    async fn source_repository_rejects_source_without_workspace_across_configured_backends() {
        let (_temp, databases) = configured_databases().await;
        for db in databases {
            assert_source_repository_rejects_source_without_workspace(&db).await;
        }
    }

    #[tokio::test]
    async fn source_repository_rejects_invalid_persisted_source_name_across_configured_backends() {
        let (_temp, databases) = configured_databases().await;
        for db in databases {
            assert_source_repository_rejects_invalid_persisted_source_name(&db).await;
        }
    }

    async fn configured_databases() -> (TempDir, Vec<CoralDb>) {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let mut databases = vec![open_sqlite(&layout).await];
        if let Some(db) = open_configured_postgres().await {
            databases.push(db);
        }
        (temp, databases)
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    async fn open_configured_postgres() -> Option<CoralDb> {
        let url = postgres_test_url()?;
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        Some(db)
    }

    async fn assert_source_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace();
        let alpha = source("alpha", None, [], [], None, SourceOrigin::Bundled);
        let mut zeta = source(
            "zeta",
            Some("1.2.3"),
            [("z_var", "last"), ("a_var", "first")],
            ["api_key", "oauth_refresh"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );
        zeta.credential_revision = uuid::Uuid::from_u128(1);

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 10)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &zeta, 20)
            .await
            .expect("upsert zeta");
        tx.sources()
            .upsert_source(&workspace, &alpha, 30)
            .await
            .expect("upsert alpha");
        tx.commit().await.expect("commit tx");

        assert_eq!(
            source_names(db, &workspace).await,
            vec!["alpha".to_string(), "zeta".to_string()]
        );
        assert_eq!(
            sources(db, &workspace).await,
            vec![alpha.clone(), zeta.clone()]
        );

        let mut alpha_replacement = source(
            "alpha",
            Some("9.9.9"),
            [("only", "new")],
            ["replacement_secret"],
            Some(CredentialStorageKind::File),
            SourceOrigin::Imported,
        );
        alpha_replacement.credential_revision = uuid::Uuid::from_u128(2);
        let mut tx = db.begin().await.expect("begin replacement tx");
        tx.sources()
            .upsert_source(&workspace, &alpha_replacement, 40)
            .await
            .expect("replace alpha");
        tx.commit().await.expect("commit replacement");
        assert_eq!(
            get_source(db, &workspace, &alpha_replacement.name).await,
            Some(alpha_replacement.clone())
        );

        let mut tx = db.begin().await.expect("begin rollback tx");
        let rolled_back = source(
            "rolled-back",
            None,
            [("temporary", "value")],
            ["temporary_secret"],
            Some(CredentialStorageKind::File),
            SourceOrigin::Imported,
        );
        tx.sources()
            .upsert_source(&workspace, &rolled_back, 50)
            .await
            .expect("upsert rolled-back source");
        tx.rollback().await.expect("rollback tx");
        assert_eq!(get_source(db, &workspace, &rolled_back.name).await, None);

        let zeta_name = zeta.name.clone();
        assert_eq!(remove_source(db, &workspace, &zeta_name).await, Some(zeta));
        assert_eq!(get_source(db, &workspace, &zeta_name).await, None);

        assert_source_get_is_coherent_during_replacement(db).await;
        assert_source_list_is_coherent_during_delete(db).await;
        assert_source_name_list_uses_one_query(db).await;
    }

    async fn assert_source_get_is_coherent_during_replacement(db: &CoralDb) {
        let workspace = unique_workspace();
        let mut original = source(
            "coherent-replacement",
            Some("1.0.0"),
            [("generation", "original")],
            ["original_secret"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );
        original.credential_revision = uuid::Uuid::from_u128(10);
        let mut replacement = source(
            "coherent-replacement",
            Some("2.0.0"),
            [("generation", "replacement")],
            ["replacement_secret"],
            Some(CredentialStorageKind::File),
            SourceOrigin::Bundled,
        );
        replacement.credential_revision = uuid::Uuid::from_u128(20);
        write_source(db, &workspace, &original, 10).await;

        let first_read_done = Arc::new(Barrier::new(2));
        let writer_done = Arc::new(Barrier::new(2));
        let mut session = ObservedReadSession::interleaved(
            db,
            Arc::clone(&first_read_done),
            Arc::clone(&writer_done),
        );
        let reader = async {
            session
                .sources()
                .get_source(&workspace, &original.name)
                .await
                .expect("read source during replacement")
        };
        let writer = async {
            first_read_done.wait().await;
            write_source(db, &workspace, &replacement, 20).await;
            writer_done.wait().await;
        };
        let (read, ()) = tokio::join!(reader, writer);

        assert_eq!(read, Some(original));
        assert_eq!(session.read_count, 1);
        assert_eq!(
            get_source(db, &workspace, &replacement.name).await,
            Some(replacement)
        );
    }

    async fn assert_source_list_is_coherent_during_delete(db: &CoralDb) {
        let workspace = unique_workspace();
        let source = source(
            "coherent-delete",
            Some("1.0.0"),
            [("generation", "original")],
            ["original_secret"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );
        write_source(db, &workspace, &source, 10).await;

        let first_read_done = Arc::new(Barrier::new(2));
        let writer_done = Arc::new(Barrier::new(2));
        let mut session = ObservedReadSession::interleaved(
            db,
            Arc::clone(&first_read_done),
            Arc::clone(&writer_done),
        );
        let reader = async {
            session
                .sources()
                .list_workspace_sources(&workspace)
                .await
                .expect("list sources during delete")
        };
        let writer = async {
            first_read_done.wait().await;
            let mut tx = db.begin().await.expect("begin delete tx");
            tx.sources()
                .remove_source(&workspace, &source.name)
                .await
                .expect("remove source");
            tx.commit().await.expect("commit delete");
            writer_done.wait().await;
        };
        let (read, ()) = tokio::join!(reader, writer);

        assert_eq!(read, vec![source]);
        assert_eq!(session.read_count, 1);
        assert!(sources(db, &workspace).await.is_empty());
    }

    async fn assert_source_name_list_uses_one_query(db: &CoralDb) {
        let workspace = unique_workspace();
        let source = source(
            "names-only",
            None,
            [("unused", "binding")],
            ["unused_secret"],
            None,
            SourceOrigin::Bundled,
        );
        write_source(db, &workspace, &source, 10).await;

        let mut session = ObservedReadSession::counting(db);
        let names = session
            .sources()
            .list_workspace_source_names(&workspace)
            .await
            .expect("list source names");

        assert_eq!(names, vec![source.name.as_str().to_string()]);
        assert_eq!(session.read_count, 1);
    }

    async fn write_source(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source: &InstalledSource,
        now_unix_nanos: i64,
    ) {
        let mut tx = db.begin().await.expect("begin source write");
        tx.workspaces()
            .ensure(workspace.as_str(), now_unix_nanos)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(workspace, source, now_unix_nanos)
            .await
            .expect("upsert source");
        tx.commit().await.expect("commit source write");
    }

    async fn assert_source_repository_rejects_source_without_workspace(db: &CoralDb) {
        let workspace = unique_workspace();
        let source = source("orphan", None, [], [], None, SourceOrigin::Bundled);
        let mut tx = db.begin().await.expect("begin tx");

        let error = tx
            .sources()
            .upsert_source(&workspace, &source, 10)
            .await
            .expect_err("source rows must require an existing workspace");

        assert!(
            error.to_string().to_lowercase().contains("foreign key"),
            "unexpected error: {error}"
        );
        tx.rollback().await.expect("rollback failed tx");
    }

    async fn assert_source_repository_rejects_invalid_persisted_source_name(db: &CoralDb) {
        let workspace = unique_workspace();
        let invalid_source_name = "bad/name";
        let mut tx = db.begin().await.expect("begin invalid source-name tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 10)
            .await
            .expect("ensure workspace");
        tx.execute(
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
                    Expr::val(workspace.as_str().to_string()),
                    Expr::val(invalid_source_name),
                    Expr::val(Option::<String>::None),
                    Expr::val(SourceOrigin::Bundled.as_config_value()),
                    Expr::val(Option::<String>::None),
                    Expr::val(10),
                    Expr::val(10),
                ])
                .to_owned(),
        )
        .await
        .expect("insert invalid source-name row");

        let error = tx
            .sources()
            .list_workspace_source_names(&workspace)
            .await
            .expect_err("invalid persisted source name should fail");
        let DbError::CorruptData(message) = error else {
            panic!("unexpected error: {error}");
        };
        assert!(
            message.contains("invalid source name 'bad/name'"),
            "unexpected error: {message}"
        );
        tx.rollback()
            .await
            .expect("rollback invalid source-name tx");
    }

    async fn source_names(db: &CoralDb, workspace: &WorkspaceName) -> Vec<String> {
        let mut session = db;
        session
            .sources()
            .list_workspace_source_names(workspace)
            .await
            .expect("list source names")
    }

    async fn sources(db: &CoralDb, workspace: &WorkspaceName) -> Vec<InstalledSource> {
        let mut session = db;
        session
            .sources()
            .list_workspace_sources(workspace)
            .await
            .expect("list sources")
    }

    async fn get_source(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<InstalledSource> {
        let mut session = db;
        session
            .sources()
            .get_source(workspace, source_name)
            .await
            .expect("get source")
    }

    async fn remove_source(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<InstalledSource> {
        let mut tx = db.begin().await.expect("begin remove tx");
        let removed = tx
            .sources()
            .remove_source(workspace, source_name)
            .await
            .expect("remove source");
        tx.commit().await.expect("commit remove tx");
        removed
    }

    fn source<const VARIABLES: usize, const SECRETS: usize>(
        name: &str,
        version: Option<&str>,
        variables: [(&str, &str); VARIABLES],
        secrets: [&str; SECRETS],
        credential_storage: Option<CredentialStorageKind>,
        origin: SourceOrigin,
    ) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            version: version.map(str::to_string),
            variables: variables
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>(),
            secrets: secrets.into_iter().map(str::to_string).collect(),
            credential_storage,
            credential_revision: uuid::Uuid::from_u128(1),
            origin,
        }
    }

    fn unique_workspace() -> WorkspaceName {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        WorkspaceName::parse(&format!("source-repository-{nanos}")).expect("workspace name")
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
