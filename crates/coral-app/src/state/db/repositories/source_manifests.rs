//! Imported-source manifest persistence.
//!
//! An imported source's `manifest.yaml` lives here as the artifact of record;
//! the copy on a host's disk is a cache of this row. `manifest_hash` is what
//! that cache's freshness is decided against, so it is computed here rather
//! than taken from the caller: one hash function, applied to the one string
//! that was actually stored.

use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::hash::sha256_hex;
use crate::sources::SourceName;
use crate::state::db::DbError;
use crate::state::db::schema::SourceManifests;
use crate::state::db::session::DbSession;
use crate::workspaces::WorkspaceName;

/// One stored manifest, as the hydration pass reads it back.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct SourceManifestRecord {
    pub(crate) manifest_yaml: String,
    pub(crate) manifest_hash: String,
    pub(crate) created_at_unix_nanos: i64,
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

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the hydration pass reads through this next")
    )]
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
        self.session.fetch_optional(statement).await
    }

    /// Stores one source's manifest, replacing any manifest already held.
    ///
    /// `created_at_unix_nanos` is restated on every write, unlike the catalog
    /// row's: a manifest is replaced wholesale rather than amended, so the
    /// timestamp dates the manifest that is there, not the first one ever was.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the source manager writes through this next")
    )]
    pub(crate) async fn upsert(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: &str,
        now_unix_nanos: i64,
    ) -> Result<(), DbError> {
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
                Expr::val(workspace_name.as_str().to_owned()),
                Expr::val(source_name.as_str().to_owned()),
                Expr::val(manifest_yaml.to_owned()),
                Expr::val(sha256_hex(manifest_yaml.as_bytes())),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([SourceManifests::WorkspaceId, SourceManifests::SourceName])
                    .update_columns([
                        SourceManifests::ManifestYaml,
                        SourceManifests::ManifestHash,
                        SourceManifests::CreatedAtUnixNanos,
                    ])
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    /// Drops one source's manifest, reporting whether one was there to drop.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the source manager writes through this next")
    )]
    pub(crate) async fn remove(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<bool, DbError> {
        let statement = Query::delete()
            .from_table(SourceManifests::Table)
            .and_where(Expr::col(SourceManifests::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(SourceManifests::SourceName).eq(source_name.as_str()))
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::SourceManifestRecord;
    use crate::bootstrap;
    use crate::hash::sha256_hex;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    const MANIFEST: &str = "dsl_version: 4\nname: orders\n";
    const REPLACEMENT: &str = "dsl_version: 4\nname: shipments\n";

    #[tokio::test]
    async fn source_manifest_repository_round_trips_against_sqlite() {
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

        assert_source_manifest_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn source_manifest_repository_contract_on_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
        else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_source_manifest_round_trip(&db).await;
    }

    /// Exercises the whole surface against one backend: store, read back,
    /// replace in place, and drop.
    async fn assert_source_manifest_round_trip(db: &CoralDb) {
        let (workspace, source_name) = seed_source(db, "manifest-source").await;

        let mut session = db;
        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source_name)
                .await
                .expect("read absent manifest"),
            None
        );

        let mut tx = db.begin().await.expect("begin store tx");
        tx.source_manifests()
            .upsert(&workspace, &source_name, MANIFEST, 10)
            .await
            .expect("store manifest");
        tx.commit().await.expect("commit store tx");

        assert_eq!(
            session
                .source_manifests()
                .get(&workspace, &source_name)
                .await
                .expect("read stored manifest"),
            Some(SourceManifestRecord {
                manifest_yaml: MANIFEST.to_owned(),
                manifest_hash: sha256_hex(MANIFEST.as_bytes()),
                created_at_unix_nanos: 10,
            }),
            "the stored hash must be the hash of the stored manifest"
        );

        assert_replacement_is_singular(db, &workspace, &source_name).await;
        assert_remove_reports_what_it_dropped(db, &workspace, &source_name).await;
    }

    /// A second write replaces the manifest rather than adding a second row,
    /// and restates both the hash and the timestamp with it.
    async fn assert_replacement_is_singular(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let mut tx = db.begin().await.expect("begin replace tx");
        tx.source_manifests()
            .upsert(workspace, source_name, REPLACEMENT, 20)
            .await
            .expect("replace manifest");
        tx.commit().await.expect("commit replace tx");

        let mut session = db;
        assert_eq!(
            session
                .source_manifests()
                .get(workspace, source_name)
                .await
                .expect("read replaced manifest"),
            Some(SourceManifestRecord {
                manifest_yaml: REPLACEMENT.to_owned(),
                manifest_hash: sha256_hex(REPLACEMENT.as_bytes()),
                created_at_unix_nanos: 20,
            })
        );
    }

    /// The first drop reports the manifest it removed; a second reports none.
    async fn assert_remove_reports_what_it_dropped(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let mut tx = db.begin().await.expect("begin drop tx");
        assert!(
            tx.source_manifests()
                .remove(workspace, source_name)
                .await
                .expect("drop manifest")
        );
        assert!(
            !tx.source_manifests()
                .remove(workspace, source_name)
                .await
                .expect("drop absent manifest")
        );
        tx.commit().await.expect("commit drop tx");

        let mut session = db;
        assert_eq!(
            session
                .source_manifests()
                .get(workspace, source_name)
                .await
                .expect("read dropped manifest"),
            None
        );
    }

    /// Installs a workspace and one catalog row for the manifest to hang off:
    /// `source_manifests` is foreign-keyed to `sources`, so there is no such
    /// thing as a manifest for a source this database does not have.
    async fn seed_source(db: &CoralDb, name: &str) -> (WorkspaceName, SourceName) {
        let suffix = Uuid::new_v4().simple().to_string();
        let workspace =
            WorkspaceName::parse(&format!("workspace-{suffix}")).expect("parse workspace name");
        let source = InstalledSource {
            name: SourceName::parse(name).expect("parse source name"),
            version: None,
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            credential_revision: Uuid::nil(),
            origin: SourceOrigin::Imported,
        };

        let mut tx = db.begin().await.expect("begin seed tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(&workspace, &source, 1)
            .await
            .expect("install source");
        tx.commit().await.expect("commit seed tx");

        (workspace, source.name)
    }
}
