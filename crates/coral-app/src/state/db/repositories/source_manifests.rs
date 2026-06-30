use sea_query::{Expr, ExprTrait, Query};
use sha2::{Digest as _, Sha256};

use crate::sources::SourceName;
use crate::state::db::schema::SourceManifests;
use crate::state::db::{DbError, DbSession, DbWriteSession};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceManifestRecord {
    pub(crate) manifest_yaml: String,
    pub(crate) manifest_hash: String,
    pub(crate) created_at_unix_nanos: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct SourceManifestRow {
    manifest_yaml: String,
    manifest_hash: String,
    created_at_unix_nanos: i64,
}

impl From<SourceManifestRow> for SourceManifestRecord {
    fn from(value: SourceManifestRow) -> Self {
        Self {
            manifest_yaml: value.manifest_yaml,
            manifest_hash: value.manifest_hash,
            created_at_unix_nanos: value.created_at_unix_nanos,
        }
    }
}

pub(crate) struct SourceManifestsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> SourceManifestsRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn get(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<SourceManifestRecord>, DbError> {
        let statement = Query::select()
            .columns([
                SourceManifests::ManifestYaml,
                SourceManifests::ManifestHash,
                SourceManifests::CreatedAtUnixNanos,
            ])
            .from(SourceManifests::Table)
            .and_where(Expr::col(SourceManifests::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceManifests::SourceName).eq(source_name.as_str()))
            .to_owned();
        let row: Option<SourceManifestRow> = self.session.fetch_optional(statement).await?;
        Ok(row.map(Into::into))
    }
}

impl<S> SourceManifestsRepo<'_, S>
where
    S: DbWriteSession,
{
    pub(crate) async fn upsert(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: &str,
        created_at_unix_nanos: i64,
    ) -> Result<SourceManifestRecord, DbError> {
        self.delete_manifest(workspace_name, source_name).await?;
        let manifest_hash = sha256_hex(manifest_yaml.as_bytes());
        let statement = Query::insert()
            .into_table(SourceManifests::Table)
            .columns([
                SourceManifests::WorkspaceId,
                SourceManifests::SourceName,
                SourceManifests::ManifestYaml,
                SourceManifests::ManifestHash,
                SourceManifests::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(source_name.as_str().to_string()),
                Expr::val(manifest_yaml.to_string()),
                Expr::val(manifest_hash.clone()),
                Expr::val(created_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await?;
        Ok(SourceManifestRecord {
            manifest_yaml: manifest_yaml.to_string(),
            manifest_hash,
            created_at_unix_nanos,
        })
    }

    pub(crate) async fn remove(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<SourceManifestRecord>, DbError> {
        let removed = self.get(workspace_name, source_name).await?;
        self.delete_manifest(workspace_name, source_name).await?;
        Ok(removed)
    }

    async fn delete_manifest(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(SourceManifests::Table)
            .and_where(Expr::col(SourceManifests::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceManifests::SourceName).eq(source_name.as_str()))
            .to_owned();
        self.session.execute_delete(statement).await
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::{SourceManifestRecord, sha256_hex};
    use crate::bootstrap;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn source_manifest_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_source_manifest_repository_round_trip(&db).await;
    }

    #[tokio::test]
    async fn source_manifest_repository_rejects_manifest_without_source_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        let workspace = unique_workspace();
        let source_name = SourceName::parse("orphan").expect("source name");

        let mut tx = db.begin().await.expect("begin tx");
        let error = tx
            .source_manifests()
            .upsert(&workspace, &source_name, "name: orphan\n", 10)
            .await
            .expect_err("manifest rows must require an existing source");

        assert!(
            error.to_string().to_lowercase().contains("foreign key"),
            "unexpected error: {error}"
        );
        tx.rollback().await.expect("rollback failed tx");
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn source_manifest_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_source_manifest_repository_round_trip(&db).await;
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

    async fn assert_source_manifest_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace();
        let source_name = SourceName::parse("local_messages").expect("source name");
        let source = InstalledSource {
            name: source_name.clone(),
            version: Some("0.1.0".to_string()),
            variables: std::collections::BTreeMap::default(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        let first_manifest = "name: local_messages\nversion: 0.1.0\n";
        let replacement_manifest = "name: local_messages\nversion: 0.2.0\n";

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 10)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &source, 20)
            .await
            .expect("upsert source");
        let stored = tx
            .source_manifests()
            .upsert(&workspace, &source_name, first_manifest, 30)
            .await
            .expect("upsert manifest");
        tx.commit().await.expect("commit tx");

        assert_eq!(
            stored,
            SourceManifestRecord {
                manifest_yaml: first_manifest.to_string(),
                manifest_hash: sha256_hex(first_manifest.as_bytes()),
                created_at_unix_nanos: 30,
            }
        );
        assert_eq!(
            get_manifest(db, &workspace, &source_name).await,
            Some(stored)
        );

        let mut tx = db.begin().await.expect("begin replacement tx");
        let replacement = tx
            .source_manifests()
            .upsert(&workspace, &source_name, replacement_manifest, 40)
            .await
            .expect("replace manifest");
        tx.commit().await.expect("commit replacement");
        assert_eq!(
            get_manifest(db, &workspace, &source_name).await,
            Some(replacement.clone())
        );
        assert_eq!(
            replacement.manifest_hash,
            sha256_hex(replacement_manifest.as_bytes())
        );
        assert_eq!(replacement.created_at_unix_nanos, 40);

        let mut tx = db.begin().await.expect("begin rollback tx");
        tx.source_manifests()
            .upsert(&workspace, &source_name, first_manifest, 50)
            .await
            .expect("upsert rolled-back manifest");
        tx.rollback().await.expect("rollback tx");
        assert_eq!(
            get_manifest(db, &workspace, &source_name).await,
            Some(replacement.clone())
        );

        let mut tx = db.begin().await.expect("begin remove tx");
        let removed = tx
            .source_manifests()
            .remove(&workspace, &source_name)
            .await
            .expect("remove manifest");
        tx.commit().await.expect("commit remove tx");
        assert_eq!(removed, Some(replacement));
        assert_eq!(get_manifest(db, &workspace, &source_name).await, None);
    }

    async fn get_manifest(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<SourceManifestRecord> {
        let mut session = db;
        session
            .source_manifests()
            .get(workspace, source_name)
            .await
            .expect("get manifest")
    }

    fn unique_workspace() -> WorkspaceName {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        WorkspaceName::parse(&format!("source-manifest-repository-{nanos}"))
            .expect("workspace name")
    }
}
