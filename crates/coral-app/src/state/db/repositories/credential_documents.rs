use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::sources::SourceName;
use crate::state::db::schema::CredentialDocuments;
use crate::state::db::{DbError, DbSession, DbWriteSession};
use crate::workspaces::WorkspaceName;

#[derive(Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct CredentialDocumentRecord {
    pub(crate) document_version: i64,
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
    pub(crate) wrapped_dek: Vec<u8>,
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    pub(crate) key_id: String,
    pub(crate) algorithm: String,
    pub(crate) aad_version: i64,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) updated_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CredentialDocumentWrite {
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
    pub(crate) wrapped_dek: Vec<u8>,
    pub(crate) wrapped_dek_nonce: Vec<u8>,
    pub(crate) key_id: String,
    pub(crate) algorithm: String,
    pub(crate) aad_version: i64,
}

impl std::fmt::Debug for CredentialDocumentRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialDocumentRecord")
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for CredentialDocumentWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialDocumentWrite")
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

impl CredentialDocumentRecord {
    fn validate(self) -> Result<Self, DbError> {
        if self.document_version < 0
            || self.created_at_unix_nanos < 0
            || self.updated_at_unix_nanos < 0
        {
            return Err(DbError::InvalidData(
                "credential document has negative version or timestamp".to_string(),
            ));
        }
        if self.aad_version != 1 {
            return Err(DbError::InvalidData(format!(
                "credential document has unsupported aad_version {}",
                self.aad_version
            )));
        }
        if self.ciphertext.is_empty()
            || self.nonce.is_empty()
            || self.wrapped_dek.is_empty()
            || self.wrapped_dek_nonce.is_empty()
        {
            return Err(DbError::InvalidData(
                "credential document has an empty encrypted byte field".to_string(),
            ));
        }
        Ok(self)
    }
}

pub(crate) struct CredentialDocumentsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> CredentialDocumentsRepo<'a, S>
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
    ) -> Result<Option<CredentialDocumentRecord>, DbError> {
        let statement = Query::select()
            .columns(record_columns())
            .from(CredentialDocuments::Table)
            .and_where(Expr::col(CredentialDocuments::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(CredentialDocuments::SourceName).eq(source_name.as_str()))
            .to_owned();
        let row: Option<CredentialDocumentRecord> = self.session.fetch_optional(statement).await?;
        row.map(CredentialDocumentRecord::validate).transpose()
    }
}

impl<S> CredentialDocumentsRepo<'_, S>
where
    S: DbWriteSession,
{
    pub(crate) async fn upsert(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        document: &CredentialDocumentWrite,
        now_unix_nanos: i64,
    ) -> Result<CredentialDocumentRecord, DbError> {
        let current_document_version = Expr::col((
            CredentialDocuments::Table,
            CredentialDocuments::DocumentVersion,
        ));
        let statement = Query::insert()
            .into_table(CredentialDocuments::Table)
            .columns([
                CredentialDocuments::WorkspaceId,
                CredentialDocuments::SourceName,
                CredentialDocuments::DocumentVersion,
                CredentialDocuments::Ciphertext,
                CredentialDocuments::Nonce,
                CredentialDocuments::WrappedDek,
                CredentialDocuments::WrappedDekNonce,
                CredentialDocuments::KeyId,
                CredentialDocuments::Algorithm,
                CredentialDocuments::AadVersion,
                CredentialDocuments::CreatedAtUnixNanos,
                CredentialDocuments::UpdatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(source_name.as_str().to_string()),
                Expr::val(1),
                Expr::val(document.ciphertext.clone()),
                Expr::val(document.nonce.clone()),
                Expr::val(document.wrapped_dek.clone()),
                Expr::val(document.wrapped_dek_nonce.clone()),
                Expr::val(document.key_id.clone()),
                Expr::val(document.algorithm.clone()),
                Expr::val(document.aad_version),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::columns([
                    CredentialDocuments::WorkspaceId,
                    CredentialDocuments::SourceName,
                ])
                .value(
                    CredentialDocuments::DocumentVersion,
                    Expr::case(current_document_version.clone().eq(i64::MAX), Expr::null())
                        .finally(current_document_version.add(1)),
                )
                .update_columns([
                    CredentialDocuments::Ciphertext,
                    CredentialDocuments::Nonce,
                    CredentialDocuments::WrappedDek,
                    CredentialDocuments::WrappedDekNonce,
                    CredentialDocuments::KeyId,
                    CredentialDocuments::Algorithm,
                    CredentialDocuments::AadVersion,
                    CredentialDocuments::UpdatedAtUnixNanos,
                ])
                .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await?;
        self.get(workspace_name, source_name).await?.ok_or_else(|| {
            DbError::InvalidData(format!(
                "credential document upsert did not return a row for {workspace_name}:{source_name}"
            ))
        })
    }

    pub(crate) async fn rewrap_if_current(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        expected_document_version: i64,
        document: &CredentialDocumentWrite,
        now_unix_nanos: i64,
    ) -> Result<bool, DbError> {
        let document_version = expected_document_version.checked_add(1).ok_or_else(|| {
            DbError::InvalidData(format!(
                "credential document version overflow for {workspace_name}:{source_name}"
            ))
        })?;
        let statement = Query::update()
            .table(CredentialDocuments::Table)
            .value(
                CredentialDocuments::DocumentVersion,
                Expr::val(document_version),
            )
            .value(
                CredentialDocuments::Ciphertext,
                Expr::val(document.ciphertext.clone()),
            )
            .value(
                CredentialDocuments::Nonce,
                Expr::val(document.nonce.clone()),
            )
            .value(
                CredentialDocuments::WrappedDek,
                Expr::val(document.wrapped_dek.clone()),
            )
            .value(
                CredentialDocuments::WrappedDekNonce,
                Expr::val(document.wrapped_dek_nonce.clone()),
            )
            .value(
                CredentialDocuments::KeyId,
                Expr::val(document.key_id.clone()),
            )
            .value(
                CredentialDocuments::Algorithm,
                Expr::val(document.algorithm.clone()),
            )
            .value(
                CredentialDocuments::AadVersion,
                Expr::val(document.aad_version),
            )
            .value(
                CredentialDocuments::UpdatedAtUnixNanos,
                Expr::val(now_unix_nanos),
            )
            .and_where(Expr::col(CredentialDocuments::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(CredentialDocuments::SourceName).eq(source_name.as_str()))
            .and_where(
                Expr::col(CredentialDocuments::DocumentVersion).eq(expected_document_version),
            )
            .to_owned();
        Ok(self.session.execute_update(statement).await? == 1)
    }

    pub(crate) async fn remove(
        &mut self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<CredentialDocumentRecord>, DbError> {
        let removed = self.get(workspace_name, source_name).await?;
        let statement = Query::delete()
            .from_table(CredentialDocuments::Table)
            .and_where(Expr::col(CredentialDocuments::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(CredentialDocuments::SourceName).eq(source_name.as_str()))
            .to_owned();
        self.session.execute_delete(statement).await?;
        Ok(removed)
    }
}

fn record_columns() -> [CredentialDocuments; 10] {
    [
        CredentialDocuments::DocumentVersion,
        CredentialDocuments::Ciphertext,
        CredentialDocuments::Nonce,
        CredentialDocuments::WrappedDek,
        CredentialDocuments::WrappedDekNonce,
        CredentialDocuments::KeyId,
        CredentialDocuments::Algorithm,
        CredentialDocuments::AadVersion,
        CredentialDocuments::CreatedAtUnixNanos,
        CredentialDocuments::UpdatedAtUnixNanos,
    ]
}

#[cfg(test)]
mod tests {
    use sea_query::{Expr, Query};
    use tempfile::tempdir;

    use super::{CredentialDocumentRecord, CredentialDocumentWrite};
    use crate::bootstrap;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::DbError;
    use crate::state::db::schema::CredentialDocuments;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, CoralTx, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    struct CorruptDocumentRow {
        label: &'static str,
        document_version: i64,
        ciphertext: &'static [u8],
        nonce: &'static [u8],
        wrapped_dek: &'static [u8],
        wrapped_dek_nonce: &'static [u8],
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
        expected_error: &'static str,
    }

    #[tokio::test]
    async fn credential_document_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_credential_document_repository_round_trip(&db).await;
    }

    #[tokio::test]
    async fn credential_document_repository_rejects_corrupt_rows_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        assert_credential_document_repository_rejects_corrupt_rows(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn credential_document_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_credential_document_repository_round_trip(&db).await;
    }

    async fn assert_credential_document_repository_round_trip(db: &CoralDb) {
        let workspace = unique_workspace("credential");
        let source_name = SourceName::parse("github").expect("source name");

        let mut tx = db.begin().await.expect("begin tx");
        seed_source(&mut tx, &workspace, &source_name).await;
        upsert_document(&mut tx, &workspace, &source_name, "key-1", b"first", 10).await;
        tx.commit().await.expect("commit first document");

        let mut tx = db.begin().await.expect("begin replacement tx");
        let replacement = upsert_document(
            &mut tx,
            &workspace,
            &source_name,
            "key-2",
            b"replacement",
            20,
        )
        .await;
        tx.commit().await.expect("commit replacement document");

        assert_document(&replacement, 2, "key-2", b"replacement", 10, 20);
        let debug = format!(
            "{replacement:?} {:?}",
            document_write("key-debug", b"secret")
        );
        assert!(
            debug.contains("ciphertext_len")
                && !debug.contains("replacement")
                && !debug.contains("secret")
        );
        let mut tx = db.begin().await.expect("begin rewrap tx");
        assert!(
            rewrap_document(
                &mut tx,
                &workspace,
                &source_name,
                replacement.document_version,
                "key-3",
                b"rewrapped",
                30,
            )
            .await
        );
        tx.commit().await.expect("commit rewrap document");
        let current = get_document(db, &workspace, &source_name)
            .await
            .expect("credential document");
        assert_document(&current, 3, "key-3", b"rewrapped", 10, 30);

        let mut tx = db.begin().await.expect("begin remove tx");
        let removed = tx
            .credential_documents()
            .remove(&workspace, &source_name)
            .await
            .expect("remove credential document");
        tx.commit().await.expect("commit remove document");

        assert_eq!(removed, Some(current));
        assert_eq!(get_document(db, &workspace, &source_name).await, None);
    }

    async fn assert_credential_document_repository_rejects_corrupt_rows(db: &CoralDb) {
        for row in [
            CorruptDocumentRow {
                label: "negative",
                document_version: -1,
                ciphertext: b"ciphertext",
                nonce: b"nonce",
                wrapped_dek: b"wrapped",
                wrapped_dek_nonce: b"wrapped-nonce",
                created_at_unix_nanos: -1,
                updated_at_unix_nanos: -1,
                expected_error: "negative version or timestamp",
            },
            CorruptDocumentRow {
                label: "emptybytes",
                document_version: 1,
                ciphertext: b"",
                nonce: b"",
                wrapped_dek: b"",
                wrapped_dek_nonce: b"",
                created_at_unix_nanos: 10,
                updated_at_unix_nanos: 10,
                expected_error: "empty encrypted byte field",
            },
        ] {
            let workspace = unique_workspace(row.label);
            let source_name = SourceName::parse("github").expect("source name");
            insert_credential_document_row(db, &workspace, &source_name, &row).await;

            let mut session = db;
            let error = session
                .credential_documents()
                .get(&workspace, &source_name)
                .await
                .expect_err("invalid persisted credential document should fail");
            let DbError::InvalidData(message) = error else {
                panic!("unexpected error: {error}");
            };
            assert!(
                message.contains(row.expected_error),
                "expected {:?} in error: {message}",
                row.expected_error
            );
        }
    }

    fn unique_workspace(prefix: &str) -> WorkspaceName {
        WorkspaceName::parse(&format!("{prefix}{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace")
    }

    async fn get_document(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) -> Option<CredentialDocumentRecord> {
        let mut session = db;
        session
            .credential_documents()
            .get(workspace, source_name)
            .await
            .expect("get credential document")
    }

    async fn upsert_document(
        tx: &mut CoralTx<'_>,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        key_id: &str,
        ciphertext: &[u8],
        now_unix_nanos: i64,
    ) -> CredentialDocumentRecord {
        tx.credential_documents()
            .upsert(
                workspace,
                source_name,
                &document_write(key_id, ciphertext),
                now_unix_nanos,
            )
            .await
            .expect("upsert credential document")
    }

    async fn rewrap_document(
        tx: &mut CoralTx<'_>,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        expected_document_version: i64,
        key_id: &str,
        ciphertext: &[u8],
        now_unix_nanos: i64,
    ) -> bool {
        tx.credential_documents()
            .rewrap_if_current(
                workspace,
                source_name,
                expected_document_version,
                &document_write(key_id, ciphertext),
                now_unix_nanos,
            )
            .await
            .expect("rewrap credential document")
    }

    async fn seed_source(
        tx: &mut CoralTx<'_>,
        workspace: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let source = InstalledSource {
            name: source_name.clone(),
            version: Some("0.1.0".to_string()),
            variables: std::collections::BTreeMap::default(),
            secrets: vec!["GITHUB_TOKEN".to_string()],
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.sources()
            .upsert_source(workspace, &source, 2)
            .await
            .expect("upsert source");
    }

    async fn insert_credential_document_row(
        db: &CoralDb,
        workspace: &WorkspaceName,
        source_name: &SourceName,
        row: &CorruptDocumentRow,
    ) {
        let mut tx = db
            .begin()
            .await
            .expect("begin corrupt credential document tx");
        seed_source(&mut tx, workspace, source_name).await;
        tx.execute(
            Query::insert()
                .into_table(CredentialDocuments::Table)
                .columns([
                    CredentialDocuments::WorkspaceId,
                    CredentialDocuments::SourceName,
                    CredentialDocuments::DocumentVersion,
                    CredentialDocuments::Ciphertext,
                    CredentialDocuments::Nonce,
                    CredentialDocuments::WrappedDek,
                    CredentialDocuments::WrappedDekNonce,
                    CredentialDocuments::KeyId,
                    CredentialDocuments::Algorithm,
                    CredentialDocuments::AadVersion,
                    CredentialDocuments::CreatedAtUnixNanos,
                    CredentialDocuments::UpdatedAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(workspace.as_str().to_string()),
                    Expr::val(source_name.as_str().to_string()),
                    Expr::val(row.document_version),
                    Expr::val(row.ciphertext.to_vec()),
                    Expr::val(row.nonce.to_vec()),
                    Expr::val(row.wrapped_dek.to_vec()),
                    Expr::val(row.wrapped_dek_nonce.to_vec()),
                    Expr::val("key"),
                    Expr::val("AES-256-GCM"),
                    Expr::val(1),
                    Expr::val(row.created_at_unix_nanos),
                    Expr::val(row.updated_at_unix_nanos),
                ])
                .to_owned(),
        )
        .await
        .expect("insert corrupt credential document row");
        tx.commit()
            .await
            .expect("commit corrupt credential document row");
    }

    fn assert_document(
        record: &CredentialDocumentRecord,
        version: i64,
        key_id: &str,
        ciphertext: &[u8],
        created_at_unix_nanos: i64,
        updated_at_unix_nanos: i64,
    ) {
        assert_eq!(record.document_version, version);
        assert_eq!(record.key_id, key_id);
        assert_eq!(record.ciphertext.as_slice(), ciphertext);
        assert_eq!(record.created_at_unix_nanos, created_at_unix_nanos);
        assert_eq!(record.updated_at_unix_nanos, updated_at_unix_nanos);
    }

    fn document_write(key_id: &str, ciphertext: &[u8]) -> CredentialDocumentWrite {
        CredentialDocumentWrite {
            ciphertext: ciphertext.to_vec(),
            nonce: format!("nonce-{key_id}").into_bytes(),
            wrapped_dek: format!("wrapped-{key_id}").into_bytes(),
            wrapped_dek_nonce: format!("wrapped-nonce-{key_id}").into_bytes(),
            key_id: key_id.to_string(),
            algorithm: "AES-256-GCM".to_string(),
            aad_version: 1,
        }
    }
}
