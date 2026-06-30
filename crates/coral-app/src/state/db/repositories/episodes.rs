use sea_query::{Expr, ExprTrait, Order, Query};

use crate::episode::EpisodeId;
use crate::state::db::schema::Episodes;
use crate::state::db::{DbError, DbSession, DbWriteSession};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EpisodeRecord {
    pub(crate) id: String,
    pub(crate) intent: String,
    pub(crate) parent_episode_id: Option<String>,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) record_bytes: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct EpisodeRow {
    id: String,
    intent: String,
    parent_episode_id: Option<String>,
    created_at_unix_nanos: i64,
    record_bytes: i64,
}

impl TryFrom<EpisodeRow> for EpisodeRecord {
    type Error = DbError;

    fn try_from(value: EpisodeRow) -> Result<Self, Self::Error> {
        validate_episode_id("episode id", &value.id)?;
        if let Some(parent_episode_id) = value.parent_episode_id.as_deref() {
            validate_episode_id("parent episode id", parent_episode_id)?;
        }
        if value.created_at_unix_nanos < 0 {
            return Err(DbError::InvalidData(format!(
                "episode '{}' has negative created_at_unix_nanos",
                value.id
            )));
        }
        if value.record_bytes < 0 {
            return Err(DbError::InvalidData(format!(
                "episode '{}' has negative record_bytes",
                value.id
            )));
        }
        Ok(Self {
            id: value.id,
            intent: value.intent,
            parent_episode_id: value.parent_episode_id,
            created_at_unix_nanos: value.created_at_unix_nanos,
            record_bytes: value.record_bytes,
        })
    }
}

fn validate_episode_id(field: &str, id: &str) -> Result<(), DbError> {
    EpisodeId::parse(id)
        .map(|_| ())
        .map_err(|error| DbError::InvalidData(format!("invalid {field} '{id}': {error}")))
}

pub(crate) struct EpisodesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> EpisodesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn get(
        &mut self,
        workspace_name: &WorkspaceName,
        id: &str,
    ) -> Result<Option<EpisodeRecord>, DbError> {
        let statement = Query::select()
            .columns(record_columns())
            .from(Episodes::Table)
            .and_where(Expr::col(Episodes::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Episodes::Id).eq(id))
            .to_owned();
        let row: Option<EpisodeRow> = self.session.fetch_optional(statement).await?;
        row.map(TryInto::try_into).transpose()
    }

    pub(crate) async fn list_workspace_episodes(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<EpisodeRecord>, DbError> {
        let statement = Query::select()
            .columns(record_columns())
            .from(Episodes::Table)
            .and_where(Expr::col(Episodes::WorkspaceId).eq(workspace_name.as_str()))
            .order_by(Episodes::CreatedAtUnixNanos, Order::Asc)
            .order_by(Episodes::Id, Order::Asc)
            .to_owned();
        let rows: Vec<EpisodeRow> = self.session.fetch_all(statement).await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }
}

impl<S> EpisodesRepo<'_, S>
where
    S: DbWriteSession,
{
    pub(crate) async fn insert(
        &mut self,
        workspace_name: &WorkspaceName,
        episode: &EpisodeRecord,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(Episodes::Table)
            .columns([
                Episodes::WorkspaceId,
                Episodes::Id,
                Episodes::Intent,
                Episodes::ParentEpisodeId,
                Episodes::CreatedAtUnixNanos,
                Episodes::RecordBytes,
            ])
            .values_panic([
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(episode.id.clone()),
                Expr::val(episode.intent.clone()),
                Expr::val(episode.parent_episode_id.clone()),
                Expr::val(episode.created_at_unix_nanos),
                Expr::val(episode.record_bytes),
            ])
            .to_owned();
        self.session.execute(statement).await
    }

    /// Replaces every episode row for one workspace.
    ///
    /// Callers MUST hold that workspace row's write lock in the same
    /// transaction, for example via `workspaces().ensure_write_locked(...)`,
    /// so concurrent servers cannot observe or race the delete-then-insert
    /// window.
    pub(crate) async fn replace_workspace_episodes(
        &mut self,
        workspace_name: &WorkspaceName,
        episodes: &[EpisodeRecord],
    ) -> Result<(), DbError> {
        self.delete_workspace_episodes(workspace_name).await?;
        for episode in episodes {
            self.insert(workspace_name, episode).await?;
        }
        Ok(())
    }

    async fn delete_workspace_episodes(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(Episodes::Table)
            .and_where(Expr::col(Episodes::WorkspaceId).eq(workspace_name.as_str()))
            .to_owned();
        self.session.execute_delete(statement).await
    }
}

fn record_columns() -> [Episodes; 5] {
    [
        Episodes::Id,
        Episodes::Intent,
        Episodes::ParentEpisodeId,
        Episodes::CreatedAtUnixNanos,
        Episodes::RecordBytes,
    ]
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, Query};
    use tempfile::tempdir;

    use super::EpisodeRecord;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::schema::Episodes;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, DbError, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn episode_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_episode_repository_round_trip(&db).await;
        assert_episode_repository_round_trip(&db).await;
        assert_episode_repository_rejects_invalid_rows(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn episode_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_episode_repository_round_trip(&db).await;
        assert_episode_repository_round_trip(&db).await;
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

    async fn assert_episode_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace("episode");
        let other_workspace = unique_workspace("episodeother");
        let root = EpisodeRecord {
            id: "ep_1".to_string(),
            intent: "root task".to_string(),
            parent_episode_id: None,
            created_at_unix_nanos: 10,
            record_bytes: 100,
        };
        let child = EpisodeRecord {
            id: "ep_2".to_string(),
            intent: "child task".to_string(),
            parent_episode_id: Some("ep_1".to_string()),
            created_at_unix_nanos: 11,
            record_bytes: 101,
        };

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.workspaces()
            .ensure(other_workspace.as_str(), 1)
            .await
            .expect("ensure other workspace");
        tx.episodes()
            .insert(&workspace, &root)
            .await
            .expect("insert root");
        tx.episodes()
            .insert(&workspace, &child)
            .await
            .expect("insert child");
        tx.episodes()
            .insert(&other_workspace, &root)
            .await
            .expect("insert isolated same id");
        tx.commit().await.expect("commit episodes");

        let mut session = db;
        assert_eq!(
            session
                .episodes()
                .get(&workspace, "ep_1")
                .await
                .expect("get root"),
            Some(root.clone())
        );
        assert_eq!(
            session
                .episodes()
                .list_workspace_episodes(&workspace)
                .await
                .expect("list workspace episodes"),
            vec![root.clone(), child.clone()]
        );

        let replacement = vec![child.clone()];
        let mut tx = db.begin().await.expect("begin replacement tx");
        tx.episodes()
            .replace_workspace_episodes(&workspace, &replacement)
            .await
            .expect("replace workspace episodes");
        tx.commit().await.expect("commit replacement");
        assert_eq!(
            session
                .episodes()
                .list_workspace_episodes(&workspace)
                .await
                .expect("list replaced episodes"),
            replacement
        );
        assert_eq!(
            session
                .episodes()
                .get(&other_workspace, "ep_1")
                .await
                .expect("other workspace remains isolated"),
            Some(root)
        );
        assert_episode_rejects_missing_workspace(db).await;
        assert_episode_cascades_with_workspace(db, &workspace, &child).await;
    }

    async fn assert_episode_rejects_missing_workspace(db: &CoralDb) {
        let workspace = unique_workspace("episodemissing");
        let episode = episode_record("orphan");
        let mut tx = db.begin().await.expect("begin orphan tx");

        let error = tx
            .episodes()
            .insert(&workspace, &episode)
            .await
            .expect_err("episodes must require an existing workspace");

        assert!(
            error.to_string().to_lowercase().contains("foreign key"),
            "unexpected error: {error}"
        );
        tx.rollback().await.expect("rollback orphan tx");
    }

    async fn assert_episode_repository_rejects_invalid_rows(db: &CoralDb) {
        for (id, parent, created, bytes, expected) in [
            ("bad id", None, 1, 1, "invalid episode id"),
            (
                "ep_bad_parent",
                Some("bad parent"),
                1,
                1,
                "invalid parent episode id",
            ),
            ("ep_bad_created", None, -1, 1, "created_at_unix_nanos"),
            ("ep_bad_bytes", None, 1, -1, "record_bytes"),
        ] {
            let workspace = unique_workspace("episodebad");
            insert_episode_row(db, &workspace, id, parent, created, bytes).await;
            let mut session = db;
            let error = session
                .episodes()
                .get(&workspace, id)
                .await
                .expect_err("invalid persisted episode row should fail");
            let DbError::InvalidData(message) = error else {
                panic!("unexpected error: {error}");
            };
            assert!(
                message.contains(expected),
                "expected {expected:?} in error: {message}"
            );
        }
    }

    async fn insert_episode_row(
        db: &CoralDb,
        workspace: &WorkspaceName,
        id: &str,
        parent_id: Option<&str>,
        created_at_unix_nanos: i64,
        record_bytes: i64,
    ) {
        let mut tx = db.begin().await.expect("begin invalid episode tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.execute(
            Query::insert()
                .into_table(Episodes::Table)
                .columns([
                    Episodes::WorkspaceId,
                    Episodes::Id,
                    Episodes::Intent,
                    Episodes::ParentEpisodeId,
                    Episodes::CreatedAtUnixNanos,
                    Episodes::RecordBytes,
                ])
                .values_panic([
                    Expr::val(workspace.as_str().to_string()),
                    Expr::val(id.to_string()),
                    Expr::val("intent"),
                    Expr::val(parent_id.map(str::to_string)),
                    Expr::val(created_at_unix_nanos),
                    Expr::val(record_bytes),
                ])
                .to_owned(),
        )
        .await
        .expect("insert invalid episode row");
        tx.commit().await.expect("commit invalid episode row");
    }

    async fn assert_episode_cascades_with_workspace(
        db: &CoralDb,
        workspace: &WorkspaceName,
        episode: &EpisodeRecord,
    ) {
        let mut tx = db.begin().await.expect("begin cascade tx");
        tx.workspaces()
            .remove(workspace.as_str())
            .await
            .expect("remove workspace");
        tx.commit().await.expect("commit cascade tx");

        let mut session = db;
        assert_eq!(
            session
                .episodes()
                .get(workspace, &episode.id)
                .await
                .expect("get cascaded episode"),
            None
        );
        assert!(
            session
                .episodes()
                .list_workspace_episodes(workspace)
                .await
                .expect("list cascaded episodes")
                .is_empty()
        );
    }

    fn episode_record(id: &str) -> EpisodeRecord {
        EpisodeRecord {
            id: id.to_string(),
            intent: "root task".to_string(),
            parent_episode_id: None,
            created_at_unix_nanos: 10,
            record_bytes: 100,
        }
    }

    fn unique_workspace(prefix: &str) -> WorkspaceName {
        WorkspaceName::parse(&format!("{prefix}{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace")
    }
}
