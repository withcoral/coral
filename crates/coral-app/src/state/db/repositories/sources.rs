use std::collections::BTreeMap;

use sea_query::{Expr, ExprTrait, Order, Query};

use crate::credentials::CredentialStorageKind;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::db::schema::{SourceSecretKeys, SourceVariables, Sources};
use crate::state::db::{DbError, DbSession};
use crate::workspaces::WorkspaceName;

#[derive(Debug, sqlx::FromRow)]
struct SourceRow {
    name: String,
    version: Option<String>,
    origin_kind: String,
    credential_storage: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct SourceCreatedAtRow {
    created_at_unix_nanos: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct SourceVariableRow {
    key: String,
    value: String,
}

#[derive(Debug, sqlx::FromRow)]
struct SourceSecretKeyRow {
    key: String,
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
        let statement = Query::select()
            .columns([
                Sources::Name,
                Sources::Version,
                Sources::OriginKind,
                Sources::CredentialStorage,
            ])
            .from(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_name.as_str()))
            .order_by(Sources::Name, Order::Asc)
            .to_owned();
        let rows: Vec<SourceRow> = self.session.fetch_all(statement).await?;
        let mut sources = Vec::with_capacity(rows.len());
        for row in rows {
            sources.push(self.installed_source_from_row(workspace_name, row).await?);
        }
        Ok(sources)
    }

    pub(crate) async fn list_workspace_source_names(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<String>, DbError> {
        Ok(self
            .list_workspace_sources(workspace_name)
            .await?
            .into_iter()
            .map(|source| source.name.as_str().to_string())
            .collect())
    }

    pub(crate) async fn get_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<InstalledSource>, DbError> {
        let Some(row) = self.source_row(workspace_name, source_name).await? else {
            return Ok(None);
        };
        self.installed_source_from_row(workspace_name, row)
            .await
            .map(Some)
    }

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
        self.delete_source_rows(workspace_name, &source.name)
            .await?;
        self.insert_source(
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
        self.delete_source_rows(workspace_name, source_name).await?;
        Ok(removed)
    }

    async fn source_row(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<SourceRow>, DbError> {
        let statement = Query::select()
            .columns([
                Sources::Name,
                Sources::Version,
                Sources::OriginKind,
                Sources::CredentialStorage,
            ])
            .from(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Sources::Name).eq(source_name.as_str()))
            .to_owned();
        self.session.fetch_optional(statement).await
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
        let row: Option<SourceCreatedAtRow> = self.session.fetch_optional(statement).await?;
        Ok(row.map(|row| row.created_at_unix_nanos))
    }

    async fn installed_source_from_row(
        &mut self,
        workspace_name: &WorkspaceName,
        row: SourceRow,
    ) -> Result<InstalledSource, DbError> {
        let source_name = parse_source_name(&row.name)?;
        let variables = self
            .source_variables(workspace_name, &source_name)
            .await?
            .into_iter()
            .map(|row| (row.key, row.value))
            .collect::<BTreeMap<_, _>>();
        let secrets = self
            .source_secret_keys(workspace_name, &source_name)
            .await?
            .into_iter()
            .map(|row| row.key)
            .collect();
        Ok(InstalledSource {
            name: source_name,
            version: row.version,
            variables,
            secrets,
            credential_storage: row
                .credential_storage
                .as_deref()
                .map(parse_credential_storage)
                .transpose()?,
            origin: parse_source_origin(&row.origin_kind)?,
        })
    }

    async fn source_variables(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Vec<SourceVariableRow>, DbError> {
        let statement = Query::select()
            .columns([SourceVariables::Key, SourceVariables::Value])
            .from(SourceVariables::Table)
            .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceVariables::SourceName).eq(source_name.as_str()))
            .order_by(SourceVariables::Key, Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }

    async fn source_secret_keys(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Vec<SourceSecretKeyRow>, DbError> {
        let statement = Query::select()
            .column(SourceSecretKeys::Key)
            .from(SourceSecretKeys::Table)
            .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name.as_str()))
            .order_by(SourceSecretKeys::Position, Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }

    async fn insert_source(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
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
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(source.name.as_str().to_string()),
                Expr::val(source.version.clone()),
                Expr::val(source.origin.as_config_value()),
                Expr::val(
                    source
                        .credential_storage
                        .map(CredentialStorageKind::as_config_value),
                ),
                Expr::val(created_at_unix_nanos),
                Expr::val(updated_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await
    }

    async fn insert_source_variables(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(), DbError> {
        for (key, value) in &source.variables {
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
                    Expr::val(source.name.as_str().to_string()),
                    Expr::val(key.clone()),
                    Expr::val(value.clone()),
                ])
                .to_owned();
            self.session.execute(statement).await?;
        }
        Ok(())
    }

    async fn insert_source_secret_keys(
        &mut self,
        workspace_name: &WorkspaceName,
        source: &InstalledSource,
    ) -> Result<(), DbError> {
        for (position, key) in source.secrets.iter().enumerate() {
            let position = i64::try_from(position).map_err(|_error| {
                DbError::InvalidData(format!(
                    "too many secret declarations for '{}'",
                    source.name
                ))
            })?;
            let statement = Query::insert()
                .into_table(SourceSecretKeys::Table)
                .columns([
                    SourceSecretKeys::WorkspaceId,
                    SourceSecretKeys::SourceName,
                    SourceSecretKeys::Position,
                    SourceSecretKeys::Key,
                ])
                .values_panic([
                    Expr::val(workspace_name.as_str().to_string()),
                    Expr::val(source.name.as_str().to_string()),
                    Expr::val(position),
                    Expr::val(key.clone()),
                ])
                .to_owned();
            self.session.execute(statement).await?;
        }
        Ok(())
    }

    async fn delete_source_rows(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), DbError> {
        let secret_keys = Query::delete()
            .from_table(SourceSecretKeys::Table)
            .and_where(Expr::col(SourceSecretKeys::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceSecretKeys::SourceName).eq(source_name.as_str()))
            .to_owned();
        self.session.execute_delete(secret_keys).await?;

        let variables = Query::delete()
            .from_table(SourceVariables::Table)
            .and_where(Expr::col(SourceVariables::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceVariables::SourceName).eq(source_name.as_str()))
            .to_owned();
        self.session.execute_delete(variables).await?;

        let source = Query::delete()
            .from_table(Sources::Table)
            .and_where(Expr::col(Sources::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Sources::Name).eq(source_name.as_str()))
            .to_owned();
        self.session.execute_delete(source).await
    }
}

fn parse_source_name(name: &str) -> Result<SourceName, DbError> {
    SourceName::parse(name)
        .map_err(|error| DbError::InvalidData(format!("invalid source name '{name}': {error}")))
}

fn parse_source_origin(origin: &str) -> Result<SourceOrigin, DbError> {
    match origin {
        "bundled" => Ok(SourceOrigin::Bundled),
        "imported" => Ok(SourceOrigin::Imported),
        other => Err(DbError::InvalidData(format!(
            "invalid source origin '{other}'"
        ))),
    }
}

fn parse_credential_storage(storage: &str) -> Result<CredentialStorageKind, DbError> {
    match storage {
        "file" => Ok(CredentialStorageKind::File),
        "keychain" => Ok(CredentialStorageKind::Keychain),
        other => Err(DbError::InvalidData(format!(
            "invalid credential storage '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use crate::bootstrap;
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn source_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_source_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn source_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_source_repository_round_trip(&db).await;
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

    async fn assert_source_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace();
        let alpha = source("alpha", None, [], [], None, SourceOrigin::Bundled);
        let zeta = source(
            "zeta",
            Some("1.2.3"),
            [("z_var", "last"), ("a_var", "first")],
            ["api_key", "oauth_refresh"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );

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

        let alpha_replacement = source(
            "alpha",
            Some("9.9.9"),
            [("only", "new")],
            ["replacement_secret"],
            Some(CredentialStorageKind::File),
            SourceOrigin::Imported,
        );
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
        let mut session = db;
        session
            .sources()
            .remove_source(workspace, source_name)
            .await
            .expect("remove source")
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
}
