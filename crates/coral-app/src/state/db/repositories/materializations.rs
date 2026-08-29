//! Materialized-source artifact persistence.
//!
//! One row reproduces one `materialized/v4` directory. It is singular by
//! construction: v4 is single-surface with a flat file layout, so there is no
//! child table and no surface id to key one by.
//!
//! Requiredness mirrors the v4 loader rather than the file system: the
//! projections, the source document (raw and parsed), the semantic IR and the
//! operation metadata are always present, while the fingerprint and the
//! diagnostics are optional there and so nullable here.

use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::sources::SourceName;
use crate::state::db::DbError;
use crate::state::db::schema::Materializations;
use crate::state::db::session::DbSession;
use crate::workspaces::WorkspaceName;

/// One stored materialization, as the hydration pass reads it back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializationRecord {
    pub(crate) materialization_version: String,
    pub(crate) fingerprint_yaml: Option<String>,
    pub(crate) projections_yaml: String,
    pub(crate) diagnostics_yaml: Option<String>,
    pub(crate) source_document_raw: Vec<u8>,
    pub(crate) source_document_yaml: String,
    pub(crate) semantic_ir_yaml: String,
    pub(crate) operation_metadata_yaml: String,
}

#[derive(sqlx::FromRow)]
struct MaterializationRow {
    materialization_version: String,
    fingerprint_yaml: Option<String>,
    projections_yaml: String,
    diagnostics_yaml: Option<String>,
    source_document_raw: Vec<u8>,
    source_document_yaml: String,
    semantic_ir_yaml: String,
    operation_metadata_yaml: String,
}

impl From<MaterializationRow> for MaterializationRecord {
    fn from(row: MaterializationRow) -> Self {
        Self {
            materialization_version: row.materialization_version,
            fingerprint_yaml: optional_artifact(row.fingerprint_yaml),
            projections_yaml: row.projections_yaml,
            diagnostics_yaml: optional_artifact(row.diagnostics_yaml),
            source_document_raw: row.source_document_raw,
            source_document_yaml: row.source_document_yaml,
            semantic_ir_yaml: row.semantic_ir_yaml,
            operation_metadata_yaml: row.operation_metadata_yaml,
        }
    }
}

/// Reads an optional artifact column as the loader would read the file.
///
/// A stored empty string is folded into `None` alongside SQL `NULL`: an absent
/// fingerprint and a zero-byte one mean the same thing to the v4 loader, and a
/// writer that has neither must not produce a record that claims to have one.
fn optional_artifact(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
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
        let statement = Query::select()
            .columns([
                Materializations::MaterializationVersion,
                Materializations::FingerprintYaml,
                Materializations::ProjectionsYaml,
                Materializations::DiagnosticsYaml,
                Materializations::SourceDocumentRaw,
                Materializations::SourceDocumentYaml,
                Materializations::SemanticIrYaml,
                Materializations::OperationMetadataYaml,
            ])
            .from(Materializations::Table)
            .and_where(Expr::col(Materializations::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Materializations::SourceName).eq(source_name.as_str()))
            .to_owned();
        let row: Option<MaterializationRow> = self.session.fetch_optional(statement).await?;
        Ok(row.map(Into::into))
    }

    /// Stores one source's materialization, replacing any already held.
    ///
    /// The optional artifacts are normalized to `NULL` on the way in, so an
    /// absent fingerprint is stored the one way the reader will report it back.
    ///
    /// `created_at_unix_nanos` is restated on every write, unlike the catalog
    /// row's: a materialization is replaced wholesale rather than amended, so
    /// the timestamp dates the artifacts that are there.
    pub(crate) async fn upsert(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        record: &MaterializationRecord,
        now_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(Materializations::Table)
            .columns([
                Materializations::WorkspaceId,
                Materializations::SourceName,
                Materializations::MaterializationVersion,
                Materializations::FingerprintYaml,
                Materializations::ProjectionsYaml,
                Materializations::DiagnosticsYaml,
                Materializations::SourceDocumentRaw,
                Materializations::SourceDocumentYaml,
                Materializations::SemanticIrYaml,
                Materializations::OperationMetadataYaml,
                Materializations::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_name.as_str().to_owned()),
                Expr::val(source_name.as_str().to_owned()),
                Expr::val(record.materialization_version.clone()),
                Expr::val(optional_artifact(record.fingerprint_yaml.clone())),
                Expr::val(record.projections_yaml.clone()),
                Expr::val(optional_artifact(record.diagnostics_yaml.clone())),
                Expr::val(record.source_document_raw.clone()),
                Expr::val(record.source_document_yaml.clone()),
                Expr::val(record.semantic_ir_yaml.clone()),
                Expr::val(record.operation_metadata_yaml.clone()),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([Materializations::WorkspaceId, Materializations::SourceName])
                    .update_columns([
                        Materializations::MaterializationVersion,
                        Materializations::FingerprintYaml,
                        Materializations::ProjectionsYaml,
                        Materializations::DiagnosticsYaml,
                        Materializations::SourceDocumentRaw,
                        Materializations::SourceDocumentYaml,
                        Materializations::SemanticIrYaml,
                        Materializations::OperationMetadataYaml,
                        Materializations::CreatedAtUnixNanos,
                    ])
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    /// Drops one source's materialization, reporting whether one was there.
    pub(crate) async fn remove(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<bool, DbError> {
        let statement = Query::delete()
            .from_table(Materializations::Table)
            .and_where(Expr::col(Materializations::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(Materializations::SourceName).eq(source_name.as_str()))
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sea_query::{Alias, Expr, ExprTrait, Query};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::MaterializationRecord;
    use crate::bootstrap;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::AppStateLayout;
    use crate::state::db::session::{DbRepos, DbSession};
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    /// Bytes chosen to break anything that round-trips through a string: an
    /// embedded NUL, a lone 0xFF that is not valid UTF-8, and a trailing NUL
    /// that a C-style truncation would eat.
    const RAW_DOCUMENT: &[u8] = &[0x00, 0x01, 0xFF, 0xFE, b'y', b'a', b'm', b'l', 0x00];

    #[tokio::test]
    async fn materialization_repository_round_trips_against_sqlite() {
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

        assert_materialization_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn materialization_repository_contract_on_postgres() {
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

        assert_materialization_round_trip(&db).await;
    }

    /// Exercises the whole surface against one backend: store, read back with
    /// the raw document intact, replace in place, and drop.
    async fn assert_materialization_round_trip(db: &CoralDb) {
        let (workspace, source_name) = seed_source(db, "materialized-source").await;

        let mut session = db;
        assert_eq!(
            session
                .materializations()
                .get(&workspace, &source_name)
                .await
                .expect("read absent materialization"),
            None
        );

        let stored = materialization();
        let mut tx = db.begin().await.expect("begin store tx");
        tx.materializations()
            .upsert(&workspace, &source_name, &stored, 10)
            .await
            .expect("store materialization");
        tx.commit().await.expect("commit store tx");

        let read_back = session
            .materializations()
            .get(&workspace, &source_name)
            .await
            .expect("read stored materialization")
            .expect("a stored materialization reads back");
        assert_eq!(read_back, stored);
        assert_eq!(
            read_back.source_document_raw, RAW_DOCUMENT,
            "the raw source document must survive the round trip byte for byte"
        );

        assert_optional_artifacts_map_to_none(db, &workspace, &source_name).await;
        assert_remove_reports_what_it_dropped(db, &workspace, &source_name).await;
    }

    /// A materialization without a fingerprint or diagnostics stores SQL NULL
    /// and reads back as `None`, and so does one that carries empty strings —
    /// and the second write replaces the first rather than adding a row.
    async fn assert_optional_artifacts_map_to_none(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let without_optionals = MaterializationRecord {
            fingerprint_yaml: None,
            diagnostics_yaml: None,
            ..materialization()
        };
        let mut tx = db.begin().await.expect("begin replace tx");
        tx.materializations()
            .upsert(workspace, source_name, &without_optionals, 20)
            .await
            .expect("replace materialization");
        tx.commit().await.expect("commit replace tx");

        let mut session = db;
        assert_eq!(
            session
                .materializations()
                .get(workspace, source_name)
                .await
                .expect("read materialization without optionals"),
            Some(without_optionals.clone())
        );
        assert_eq!(
            null_optional_counts(db, workspace, source_name).await,
            (1, 1),
            "an absent artifact must be stored as NULL, not as an empty string"
        );

        let with_empty_optionals = MaterializationRecord {
            fingerprint_yaml: Some(String::new()),
            diagnostics_yaml: Some(String::new()),
            ..materialization()
        };
        let mut tx = db.begin().await.expect("begin empty-optionals tx");
        tx.materializations()
            .upsert(workspace, source_name, &with_empty_optionals, 21)
            .await
            .expect("store empty optionals");
        tx.commit().await.expect("commit empty-optionals tx");

        assert_eq!(
            session
                .materializations()
                .get(workspace, source_name)
                .await
                .expect("read materialization with empty optionals"),
            Some(without_optionals),
            "an empty artifact must read back the same as an absent one"
        );
        assert_eq!(
            null_optional_counts(db, workspace, source_name).await,
            (1, 1),
            "an empty artifact must be normalized to NULL on the way in"
        );
        assert_eq!(
            count_where(db, workspace, source_name, None).await,
            1,
            "a source has one materialization, however many times it is written"
        );
    }

    /// The first drop reports the materialization it removed; a second reports
    /// none.
    async fn assert_remove_reports_what_it_dropped(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let mut tx = db.begin().await.expect("begin drop tx");
        assert!(
            tx.materializations()
                .remove(workspace, source_name)
                .await
                .expect("drop materialization")
        );
        assert!(
            !tx.materializations()
                .remove(workspace, source_name)
                .await
                .expect("drop absent materialization")
        );
        tx.commit().await.expect("commit drop tx");

        let mut session = db;
        assert_eq!(
            session
                .materializations()
                .get(workspace, source_name)
                .await
                .expect("read dropped materialization"),
            None
        );
    }

    /// Counts the `(fingerprint, diagnostics)` columns that are physically
    /// NULL, so the storage form is proven rather than inferred from a read
    /// that folds empty strings into `None` anyway.
    async fn null_optional_counts(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> (i64, i64) {
        let mut counts = [0_i64; 2];
        for (slot, column) in counts
            .iter_mut()
            .zip(["fingerprint_yaml", "diagnostics_yaml"])
        {
            *slot = count_where(db, workspace, source_name, Some(column)).await;
        }
        (counts[0], counts[1])
    }

    /// Counts one source's materialization rows straight from the physical
    /// table, optionally narrowed to rows whose `null_column` is NULL.
    async fn count_where(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        null_column: Option<&str>,
    ) -> i64 {
        let mut statement = Query::select()
            .expr(Expr::col(Alias::new("workspace_id")).count())
            .from(Alias::new("materializations"))
            .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.as_str()))
            .and_where(Expr::col(Alias::new("source_name")).eq(source_name.as_str()))
            .to_owned();
        if let Some(column) = null_column {
            statement = statement
                .and_where(Expr::col(Alias::new(column)).is_null())
                .to_owned();
        }

        let mut session = db;
        let counted: Option<(i64,)> = session
            .fetch_optional(statement)
            .await
            .expect("count materialization rows");
        counted.expect("a count always returns a row").0
    }

    fn materialization() -> MaterializationRecord {
        MaterializationRecord {
            materialization_version: "v4".to_owned(),
            fingerprint_yaml: Some("inputs:\n  - orders.sql\n".to_owned()),
            projections_yaml: "projections:\n  - name: orders\n".to_owned(),
            diagnostics_yaml: Some("warnings: []\n".to_owned()),
            source_document_raw: RAW_DOCUMENT.to_vec(),
            source_document_yaml: "document:\n  kind: source\n".to_owned(),
            semantic_ir_yaml: "ir:\n  version: 4\n".to_owned(),
            operation_metadata_yaml: "operations: []\n".to_owned(),
        }
    }

    /// Installs a workspace and one catalog row for the materialization to hang
    /// off: `materializations` is foreign-keyed to `sources`, so there is no
    /// such thing as a materialization for a source this database does not
    /// have.
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
