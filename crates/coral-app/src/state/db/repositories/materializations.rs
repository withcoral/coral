use sea_query::{Expr, ExprTrait, Order, Query, SelectStatement};

use crate::sources::SourceName;
use crate::state::db::schema::{MaterializationSurfaces as Surfaces, Materializations as Mats};
use crate::state::db::{DbError, DbSession, DbWriteSession};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializationRecord {
    pub(crate) materialization_version: String,
    pub(crate) fingerprint_yaml: String,
    pub(crate) projections_yaml: String,
    pub(crate) diagnostics_yaml: String,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) surfaces: Vec<MaterializationSurfaceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializationSurfaceRecord {
    pub(crate) surface_id: String,
    pub(crate) source_document_raw: Vec<u8>,
    pub(crate) source_document_yaml: String,
    pub(crate) semantic_ir_yaml: String,
}

#[derive(Debug, sqlx::FromRow)]
struct JoinedMaterializationRow {
    materialization_version: String,
    fingerprint_yaml: String,
    projections_yaml: String,
    diagnostics_yaml: String,
    created_at_unix_nanos: i64,
    surface_id: Option<String>,
    source_document_raw: Option<Vec<u8>>,
    source_document_yaml: Option<String>,
    semantic_ir_yaml: Option<String>,
}

pub(crate) struct MaterializationsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> MaterializationsRepo<'a, S>
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
    ) -> Result<Option<MaterializationRecord>, DbError> {
        let rows = self
            .session
            .fetch_all::<JoinedMaterializationRow>(materialization_get_statement(
                workspace_name,
                source_name,
            ))
            .await?;
        materialization_record_from_joined_rows(rows, source_name)
    }
}

fn materialization_get_statement(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> SelectStatement {
    Query::select()
        .columns([
            Mats::MaterializationVersion,
            Mats::FingerprintYaml,
            Mats::ProjectionsYaml,
            Mats::DiagnosticsYaml,
            Mats::CreatedAtUnixNanos,
        ])
        .columns([
            Surfaces::SurfaceId,
            Surfaces::SourceDocumentRaw,
            Surfaces::SourceDocumentYaml,
            Surfaces::SemanticIrYaml,
        ])
        .from(Mats::Table)
        .left_join(
            Surfaces::Table,
            Expr::col((Mats::Table, Mats::WorkspaceId))
                .equals((Surfaces::Table, Surfaces::WorkspaceId))
                .and(
                    Expr::col((Mats::Table, Mats::SourceName))
                        .equals((Surfaces::Table, Surfaces::SourceName)),
                ),
        )
        .and_where(Expr::col((Mats::Table, Mats::WorkspaceId)).eq(workspace_name.as_str()))
        .and_where(Expr::col((Mats::Table, Mats::SourceName)).eq(source_name.as_str()))
        .order_by((Surfaces::Table, Surfaces::SurfaceId), Order::Asc)
        .to_owned()
}

fn materialization_record_from_joined_rows(
    rows: Vec<JoinedMaterializationRow>,
    source_name: &SourceName,
) -> Result<Option<MaterializationRecord>, DbError> {
    let mut rows = rows.into_iter();
    let Some(first_row) = rows.next() else {
        return Ok(None);
    };
    let materialization_version = first_row.materialization_version.clone();
    let fingerprint_yaml = first_row.fingerprint_yaml.clone();
    let projections_yaml = first_row.projections_yaml.clone();
    let diagnostics_yaml = first_row.diagnostics_yaml.clone();
    let created_at_unix_nanos = first_row.created_at_unix_nanos;
    let mut surfaces = Vec::new();
    if let Some(surface) = row_surface(first_row, source_name)? {
        surfaces.push(surface);
    }
    for row in rows {
        if let Some(surface) = row_surface(row, source_name)? {
            surfaces.push(surface);
        }
    }
    Ok(Some(MaterializationRecord {
        materialization_version,
        fingerprint_yaml,
        projections_yaml,
        diagnostics_yaml,
        created_at_unix_nanos,
        surfaces,
    }))
}

fn row_surface(
    row: JoinedMaterializationRow,
    source_name: &SourceName,
) -> Result<Option<MaterializationSurfaceRecord>, DbError> {
    let Some(surface_id) = row.surface_id else {
        return Ok(None);
    };
    Ok(Some(MaterializationSurfaceRecord {
        surface_id,
        source_document_raw: required_surface_field(
            row.source_document_raw,
            source_name,
            "source_document_raw",
        )?,
        source_document_yaml: required_surface_field(
            row.source_document_yaml,
            source_name,
            "source_document_yaml",
        )?,
        semantic_ir_yaml: required_surface_field(
            row.semantic_ir_yaml,
            source_name,
            "semantic_ir_yaml",
        )?,
    }))
}

fn required_surface_field<T>(
    value: Option<T>,
    source_name: &SourceName,
    field: &str,
) -> Result<T, DbError> {
    value.ok_or_else(|| {
        DbError::InvalidData(format!(
            "materialization surface for source '{source_name}' is missing {field}"
        ))
    })
}

impl<S> MaterializationsRepo<'_, S>
where
    S: DbWriteSession,
{
    pub(crate) async fn upsert(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        record: &MaterializationRecord,
    ) -> Result<(), DbError> {
        self.delete_materialization(workspace_name, source_name)
            .await?;
        let statement = Query::insert()
            .into_table(Mats::Table)
            .columns([
                Mats::WorkspaceId,
                Mats::SourceName,
                Mats::MaterializationVersion,
                Mats::FingerprintYaml,
                Mats::ProjectionsYaml,
                Mats::DiagnosticsYaml,
                Mats::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(source_name.as_str().to_string()),
                Expr::val(record.materialization_version.clone()),
                Expr::val(record.fingerprint_yaml.clone()),
                Expr::val(record.projections_yaml.clone()),
                Expr::val(record.diagnostics_yaml.clone()),
                Expr::val(record.created_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await?;
        for surface in &record.surfaces {
            self.insert_surface(workspace_name, source_name, surface)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn remove(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<MaterializationRecord>, DbError> {
        let removed = self.get(workspace_name, source_name).await?;
        self.delete_materialization(workspace_name, source_name)
            .await?;
        Ok(removed)
    }

    async fn insert_surface(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface: &MaterializationSurfaceRecord,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(Surfaces::Table)
            .columns([
                Surfaces::WorkspaceId,
                Surfaces::SourceName,
                Surfaces::SurfaceId,
                Surfaces::SourceDocumentRaw,
                Surfaces::SourceDocumentYaml,
                Surfaces::SemanticIrYaml,
            ])
            .values_panic([
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(source_name.as_str().to_string()),
                Expr::val(surface.surface_id.clone()),
                Expr::val(surface.source_document_raw.clone()),
                Expr::val(surface.source_document_yaml.clone()),
                Expr::val(surface.semantic_ir_yaml.clone()),
            ])
            .to_owned();
        self.session.execute(statement).await
    }

    async fn delete_materialization(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(Mats::Table)
            .and_where(Expr::col(Mats::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Mats::SourceName).eq(source_name.as_str()))
            .to_owned();
        self.session.execute_delete(statement).await
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, Expr, ExprTrait, Func, Query};
    use tempfile::tempdir;

    use super::{MaterializationRecord, MaterializationSurfaceRecord};
    use crate::bootstrap;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::schema::MaterializationSurfaces as Surfaces;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbSession, DbWriteSession, ResolvedDatabaseConfig,
    };
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn materialization_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_materialization_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn materialization_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_materialization_repository_round_trip(&db).await;
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

    #[expect(clippy::too_many_lines, reason = "repository contract fixture")]
    async fn assert_materialization_repository_round_trip(db: &CoralDb) {
        let ws = WorkspaceName::parse("default").expect("workspace");
        let alt_ws = WorkspaceName::parse("alternate").expect("workspace");
        let name = SourceName::parse("github_v4").expect("source name");
        let source = InstalledSource {
            name: name.clone(),
            version: Some("0.1.0".to_string()),
            variables: std::collections::BTreeMap::default(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        let empty_surfaces = MaterializationRecord {
            surfaces: Vec::new(),
            ..materialization_record("v4-empty", 10)
        };
        let first = materialization_record("v4", 11);
        let alternate = materialization_record("v4-alternate", 12);
        let replaced = MaterializationRecord {
            materialization_version: "v4-replacement".to_string(),
            created_at_unix_nanos: 22,
            surfaces: vec![MaterializationSurfaceRecord {
                surface_id: "rest".to_string(),
                source_document_raw: b"replacement raw".to_vec(),
                source_document_yaml: "replacement: true\n".to_string(),
                semantic_ir_yaml: "replacement-ir: true\n".to_string(),
            }],
            ..materialization_record("v4-replacement", 22)
        };

        let mut tx = db.begin().await.expect("begin missing-source tx");
        tx.materializations()
            .upsert(&ws, &name, &first)
            .await
            .expect_err("materializations must require an existing source");
        tx.rollback().await.expect("rollback missing-source tx");

        let mut tx = db.begin().await.expect("begin tx");
        ensure_source(&mut tx, &ws, &source, 1).await;
        tx.materializations()
            .upsert(&ws, &name, &empty_surfaces)
            .await
            .expect("upsert empty-surface materialization");
        tx.commit()
            .await
            .expect("commit empty-surface materialization");

        let mut session = db;
        assert_mat(&mut session, &ws, &name, Some(empty_surfaces)).await;

        let mut tx = db.begin().await.expect("begin first tx");
        tx.materializations()
            .upsert(&ws, &name, &first)
            .await
            .expect("upsert first materialization");
        tx.commit().await.expect("commit first materialization");

        assert_mat(&mut session, &ws, &name, Some(first.clone())).await;

        let mut tx = db.begin().await.expect("begin isolation tx");
        ensure_source(&mut tx, &alt_ws, &source, 12).await;
        tx.materializations()
            .upsert(&alt_ws, &name, &alternate)
            .await
            .expect("upsert alternate-workspace materialization");
        tx.commit()
            .await
            .expect("commit alternate-workspace materialization");
        assert_mat(&mut session, &ws, &name, Some(first.clone())).await;
        assert_mat(&mut session, &alt_ws, &name, Some(alternate.clone())).await;

        let mut tx = db.begin().await.expect("begin replacement tx");
        tx.materializations()
            .upsert(&ws, &name, &replaced)
            .await
            .expect("upsert replacement materialization");
        tx.commit()
            .await
            .expect("commit replacement materialization");

        assert_mat(&mut session, &ws, &name, Some(replaced.clone())).await;
        assert_mat(&mut session, &alt_ws, &name, Some(alternate.clone())).await;

        let mut tx = db.begin().await.expect("begin remove tx");
        assert_eq!(
            tx.materializations()
                .remove(&ws, &name)
                .await
                .expect("remove materialization"),
            Some(replaced)
        );
        tx.commit().await.expect("commit remove materialization");
        assert_mat(&mut session, &ws, &name, None).await;
        assert_mat(&mut session, &alt_ws, &name, Some(alternate.clone())).await;

        let mut tx = db.begin().await.expect("begin source cascade tx");
        tx.materializations()
            .upsert(&ws, &name, &first)
            .await
            .expect("upsert source-cascade materialization");
        tx.sources()
            .remove_source(&ws, &name)
            .await
            .expect("remove source");
        tx.commit().await.expect("commit source cascade");
        assert_deleted(&mut session, &ws, &name).await;
        assert_mat(&mut session, &alt_ws, &name, Some(alternate.clone())).await;

        let mut tx = db.begin().await.expect("begin workspace cascade tx");
        ensure_source(&mut tx, &ws, &source, 23).await;
        tx.materializations()
            .upsert(&ws, &name, &first)
            .await
            .expect("upsert workspace-cascade materialization");
        tx.workspaces()
            .remove(ws.as_str())
            .await
            .expect("remove workspace");
        tx.commit().await.expect("commit workspace cascade");
        assert_deleted(&mut session, &ws, &name).await;
        assert_mat(&mut session, &alt_ws, &name, Some(alternate)).await;
    }

    async fn ensure_source<S>(
        session: &mut S,
        workspace: &WorkspaceName,
        source: &InstalledSource,
        timestamp: i64,
    ) where
        S: DbWriteSession + Sized,
    {
        session
            .workspaces()
            .ensure(workspace.as_str(), timestamp)
            .await
            .expect("ensure workspace");
        session
            .sources()
            .upsert_source(workspace, source, timestamp)
            .await
            .expect("upsert source");
    }

    async fn assert_deleted(
        session: &mut &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) {
        assert_mat(session, workspace, source_name, None).await;
        assert_eq!(surface_row_count(session, workspace, source_name).await, 0);
    }

    async fn assert_mat(
        session: &mut &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        expected: Option<MaterializationRecord>,
    ) {
        assert_eq!(
            get_materialization(session, workspace, source_name).await,
            expected
        );
    }

    async fn get_materialization(
        session: &mut &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<MaterializationRecord> {
        session
            .materializations()
            .get(workspace, source_name)
            .await
            .expect("get materialization")
    }

    #[derive(Debug, sqlx::FromRow)]
    struct CountRow {
        count: i64,
    }

    async fn surface_row_count(
        session: &mut &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> i64 {
        let row: CountRow = session
            .fetch_optional(
                Query::select()
                    .expr_as(Func::count(Expr::val(1)), Alias::new("count"))
                    .from(Surfaces::Table)
                    .and_where(Expr::col(Surfaces::WorkspaceId).eq(workspace.as_str()))
                    .and_where(Expr::col(Surfaces::SourceName).eq(source_name.as_str()))
                    .to_owned(),
            )
            .await
            .expect("count materialization surfaces")
            .expect("count row");
        row.count
    }

    fn materialization_record(version: &str, created_at_unix_nanos: i64) -> MaterializationRecord {
        MaterializationRecord {
            materialization_version: version.to_string(),
            fingerprint_yaml: "fingerprint: true\n".to_string(),
            projections_yaml: "projections: true\n".to_string(),
            diagnostics_yaml: "[]\n".to_string(),
            created_at_unix_nanos,
            surfaces: vec![
                MaterializationSurfaceRecord {
                    surface_id: "mcp".to_string(),
                    source_document_raw: b"mcp raw".to_vec(),
                    source_document_yaml: "mcp: true\n".to_string(),
                    semantic_ir_yaml: "mcp-ir: true\n".to_string(),
                },
                MaterializationSurfaceRecord {
                    surface_id: "rest".to_string(),
                    source_document_raw: b"rest raw".to_vec(),
                    source_document_yaml: "rest: true\n".to_string(),
                    semantic_ir_yaml: "rest-ir: true\n".to_string(),
                },
            ],
        }
    }
}
