#![cfg_attr(not(test), expect(dead_code, reason = "Used by B4f."))]

use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::bootstrap::AppError;
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::state::db::schema::IdentityDocuments;
use crate::state::db::{CoralTx, DbError, DbSession};

/// Opaque encrypted material persisted for one owner-scoped identity.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentityDocumentRecord {
    pub(crate) owner: IdentityOwner,
    pub(crate) name: IdentityName,
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

/// Validated opaque envelope fields used to insert or replace identity material.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentityDocumentWrite {
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_dek: Vec<u8>,
    wrapped_dek_nonce: Vec<u8>,
    key_id: String,
    algorithm: String,
    aad_version: i64,
}

impl IdentityDocumentWrite {
    /// Validate storage shape without interpreting cryptographic policy.
    pub(crate) fn new(
        ciphertext: Vec<u8>,
        nonce: Vec<u8>,
        wrapped_dek: Vec<u8>,
        wrapped_dek_nonce: Vec<u8>,
        key_id: impl Into<String>,
        algorithm: impl Into<String>,
        aad_version: i64,
    ) -> Result<Self, AppError> {
        let write = Self {
            ciphertext,
            nonce,
            wrapped_dek,
            wrapped_dek_nonce,
            key_id: key_id.into(),
            algorithm: algorithm.into(),
            aad_version,
        };
        validate_document_fields(
            &write.ciphertext,
            &write.nonce,
            &write.wrapped_dek,
            &write.wrapped_dek_nonce,
            &write.key_id,
            &write.algorithm,
            write.aad_version,
        )
        .map_err(AppError::InvalidInput)?;
        Ok(write)
    }
}

impl std::fmt::Debug for IdentityDocumentRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityDocumentRecord")
            .field("owner", &self.owner)
            .field("name", &self.name)
            .field("document_version", &self.document_version)
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for IdentityDocumentWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityDocumentWrite")
            .field("ciphertext_len", &self.ciphertext.len())
            .field("nonce_len", &self.nonce.len())
            .field("wrapped_dek_len", &self.wrapped_dek.len())
            .field("wrapped_dek_nonce_len", &self.wrapped_dek_nonce.len())
            .finish_non_exhaustive()
    }
}

#[derive(sqlx::FromRow)]
struct IdentityDocumentRow {
    owner_kind: String,
    owner_key: String,
    name: String,
    document_version: i64,
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_dek: Vec<u8>,
    wrapped_dek_nonce: Vec<u8>,
    key_id: String,
    algorithm: String,
    aad_version: i64,
    created_at_unix_nanos: i64,
    updated_at_unix_nanos: i64,
}

impl IdentityDocumentRow {
    fn validate(self) -> Result<IdentityDocumentRecord, DbError> {
        if self.document_version < 1
            || self.created_at_unix_nanos < 0
            || self.updated_at_unix_nanos < self.created_at_unix_nanos
        {
            return Err(DbError::CorruptData(
                "identity document row has invalid version or timestamps".to_string(),
            ));
        }
        validate_document_fields(
            &self.ciphertext,
            &self.nonce,
            &self.wrapped_dek,
            &self.wrapped_dek_nonce,
            &self.key_id,
            &self.algorithm,
            self.aad_version,
        )
        .map_err(DbError::CorruptData)?;
        Ok(IdentityDocumentRecord {
            owner: IdentityOwner::from_key_storage_parts(&self.owner_kind, &self.owner_key)?,
            name: IdentityName::from_storage(&self.name)?,
            document_version: self.document_version,
            ciphertext: self.ciphertext,
            nonce: self.nonce,
            wrapped_dek: self.wrapped_dek,
            wrapped_dek_nonce: self.wrapped_dek_nonce,
            key_id: self.key_id,
            algorithm: self.algorithm,
            aad_version: self.aad_version,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Repository shell for encrypted material owned by identities.
pub(crate) struct IdentityDocumentsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> IdentityDocumentsRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    /// Load one encrypted document by its complete owner and name key.
    pub(crate) async fn load_optional(
        &mut self,
        owner: &IdentityOwner,
        name: &IdentityName,
    ) -> Result<Option<IdentityDocumentRecord>, DbError> {
        let row: Option<IdentityDocumentRow> = self
            .session
            .fetch_optional(
                Query::select()
                    .columns(identity_document_columns())
                    .from(IdentityDocuments::Table)
                    .and_where(identity_document_key_where(owner, name))
                    .to_owned(),
            )
            .await?;
        row.map(IdentityDocumentRow::validate).transpose()
    }
}

impl IdentityDocumentsRepo<'_, CoralTx<'_>> {
    /// Insert or atomically replace an encrypted document and increment its version.
    pub(crate) async fn upsert(
        &mut self,
        owner: &IdentityOwner,
        name: &IdentityName,
        document: &IdentityDocumentWrite,
        now_unix_nanos: i64,
    ) -> Result<IdentityDocumentRecord, AppError> {
        validate_write_timestamp(now_unix_nanos)?;
        let current_version =
            Expr::col((IdentityDocuments::Table, IdentityDocuments::DocumentVersion));
        let current_updated_at = Expr::col((
            IdentityDocuments::Table,
            IdentityDocuments::UpdatedAtUnixNanos,
        ));
        let statement = Query::insert()
            .into_table(IdentityDocuments::Table)
            .columns(identity_document_columns())
            .values_panic([
                Expr::val(owner.kind()),
                Expr::val(owner.key()),
                Expr::val(name.as_str()),
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
                    IdentityDocuments::OwnerKind,
                    IdentityDocuments::OwnerKey,
                    IdentityDocuments::Name,
                ])
                .value(
                    IdentityDocuments::DocumentVersion,
                    current_version.clone().add(1),
                )
                .update_columns([
                    IdentityDocuments::Ciphertext,
                    IdentityDocuments::Nonce,
                    IdentityDocuments::WrappedDek,
                    IdentityDocuments::WrappedDekNonce,
                    IdentityDocuments::KeyId,
                    IdentityDocuments::Algorithm,
                    IdentityDocuments::AadVersion,
                ])
                .value(
                    IdentityDocuments::UpdatedAtUnixNanos,
                    Expr::case(
                        current_updated_at.clone().gt(now_unix_nanos),
                        current_updated_at,
                    )
                    .finally(now_unix_nanos),
                )
                .action_and_where(current_version.lt(i64::MAX))
                .to_owned(),
            )
            .to_owned();
        match self.session.execute_affected(statement).await? {
            1 => {}
            0 => {
                return Err(AppError::FailedPrecondition(format!(
                    "identity document version is exhausted for {}:{}:{}",
                    owner.kind(),
                    owner.key(),
                    name,
                )));
            }
            rows_affected => {
                return Err(AppError::Database(format!(
                    "identity document upsert affected {rows_affected} rows"
                )));
            }
        }
        self.load_optional(owner, name).await?.ok_or_else(|| {
            AppError::Database("identity document disappeared after upsert".to_string())
        })
    }
}

fn validate_document_fields(
    ciphertext: &[u8],
    nonce: &[u8],
    wrapped_dek: &[u8],
    wrapped_dek_nonce: &[u8],
    key_id: &str,
    algorithm: &str,
    aad_version: i64,
) -> Result<(), String> {
    if ciphertext.is_empty()
        || nonce.is_empty()
        || wrapped_dek.is_empty()
        || wrapped_dek_nonce.is_empty()
    {
        return Err("identity document has an empty encrypted byte field".to_string());
    }
    if key_id.trim().is_empty() || algorithm.trim().is_empty() || aad_version < 1 {
        return Err("identity document has invalid envelope metadata".to_string());
    }
    Ok(())
}

fn validate_write_timestamp(now_unix_nanos: i64) -> Result<(), AppError> {
    match now_unix_nanos {
        0.. => Ok(()),
        _ => Err(AppError::InvalidInput(
            "identity document timestamp is negative".into(),
        )),
    }
}

fn identity_document_columns() -> [IdentityDocuments; 13] {
    [
        IdentityDocuments::OwnerKind,
        IdentityDocuments::OwnerKey,
        IdentityDocuments::Name,
        IdentityDocuments::DocumentVersion,
        IdentityDocuments::Ciphertext,
        IdentityDocuments::Nonce,
        IdentityDocuments::WrappedDek,
        IdentityDocuments::WrappedDekNonce,
        IdentityDocuments::KeyId,
        IdentityDocuments::Algorithm,
        IdentityDocuments::AadVersion,
        IdentityDocuments::CreatedAtUnixNanos,
        IdentityDocuments::UpdatedAtUnixNanos,
    ]
}

fn identity_document_key_where(
    owner: &IdentityOwner,
    name: &IdentityName,
) -> sea_query::SimpleExpr {
    Expr::col(IdentityDocuments::OwnerKind)
        .eq(owner.kind())
        .and(Expr::col(IdentityDocuments::OwnerKey).eq(owner.key()))
        .and(Expr::col(IdentityDocuments::Name).eq(name.as_str()))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::IdentityDocumentRow;
    use crate::bootstrap::AppError;
    use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
    use crate::state::db::{
        CoralDb, DbError, DbRepos, IdentityDocumentRecord, IdentityDocumentWrite, IdentitySpecKey,
        ResolvedDatabaseConfig,
    };
    use crate::workspaces::WorkspaceName;

    #[test]
    fn persisted_identity_document_key_columns_fail_closed() {
        let row = |owner_key: &str, name: &str| IdentityDocumentRow {
            owner_kind: "user".to_string(),
            owner_key: owner_key.to_string(),
            name: name.to_string(),
            document_version: 1,
            ciphertext: vec![1],
            nonce: vec![2],
            wrapped_dek: vec![3],
            wrapped_dek_nonce: vec![4],
            key_id: "key".to_string(),
            algorithm: "algorithm".to_string(),
            aad_version: 1,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        };
        for corrupt in [row(" member ", "github"), row("member", " github ")] {
            assert!(matches!(corrupt.validate(), Err(DbError::CorruptData(_))));
        }
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines, reason = "Repository contract.")]
    async fn identity_documents_round_trip_replace_and_cascade() {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");

        let workspace = WorkspaceName::parse("local").expect("workspace");
        let owner = IdentityOwner::workspace(workspace.clone());
        let name = IdentityName::parse("github").expect("identity name");
        let spec_key = IdentitySpecKey::global("github").expect("spec key");
        let reference = reference(&owner, spec_key, "fingerprint");
        let initial = document(1, "key-sentinel", "algorithm", 2);

        let mut tx = db.begin().await.expect("begin seed tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("seed workspace");
        tx.identities()
            .upsert(&owner, &name, &reference, 2)
            .await
            .expect("seed identity");
        assert!(
            tx.identity_documents()
                .load_optional(&owner, &name)
                .await
                .expect("load absent document")
                .is_none()
        );
        let first = tx
            .identity_documents()
            .upsert(&owner, &name, &initial, 10)
            .await
            .expect("insert document");
        assert_record(&first, &owner, &name, 1, 1, "key-sentinel", 2);
        assert_eq!(first.algorithm, "algorithm");
        let record_debug = format!("{first:?}");
        let write_debug = format!("{initial:?}");
        for secret in [
            "[1, 1, 1]",
            "[2, 2]",
            "[3, 3, 3, 3]",
            "[4, 4]",
            "key-sentinel",
            "algorithm",
        ] {
            assert!(!record_debug.contains(secret));
            assert!(!write_debug.contains(secret));
        }
        tx.commit().await.expect("commit seed tx");

        let mut session = &db;
        assert_eq!(
            session
                .identity_documents()
                .load_optional(&owner, &name)
                .await
                .expect("reload document"),
            Some(first)
        );

        let replacement = document(40, "replacement-key", "replacement-algorithm", 7);
        let mut tx = db.begin().await.expect("begin replacement tx");
        let replaced = tx
            .identity_documents()
            .upsert(&owner, &name, &replacement, 30)
            .await
            .expect("replace document");
        assert_record(&replaced, &owner, &name, 2, 40, "replacement-key", 7);
        assert_eq!(replaced.algorithm, "replacement-algorithm");
        assert_eq!(
            (
                replaced.created_at_unix_nanos,
                replaced.updated_at_unix_nanos
            ),
            (10, 30)
        );
        let regressed = tx
            .identity_documents()
            .upsert(&owner, &name, &replacement, 5)
            .await
            .expect("replace under regressed clock");
        assert_eq!(
            (
                regressed.document_version,
                regressed.created_at_unix_nanos,
                regressed.updated_at_unix_nanos,
            ),
            (3, 10, 30)
        );
        let rejected = tx
            .identity_documents()
            .upsert(
                &owner,
                &name,
                &document(60, "negative-key", "negative-alg", 8),
                -1,
            )
            .await
            .expect_err("negative timestamp must fail");
        assert!(matches!(rejected, AppError::InvalidInput(_)));
        assert_eq!(
            tx.identity_documents()
                .load_optional(&owner, &name)
                .await
                .expect("reload after rejected write"),
            Some(regressed)
        );
        assert!(
            tx.identities()
                .delete(&owner, &name)
                .await
                .expect("delete identity")
        );
        assert!(
            tx.identity_documents()
                .load_optional(&owner, &name)
                .await
                .expect("load cascaded document")
                .is_none()
        );
        tx.commit().await.expect("commit replacement tx");
    }

    #[test]
    fn identity_document_write_rejects_invalid_envelopes() {
        for (ciphertext, nonce, wrapped_dek, wrapped_dek_nonce) in [
            (vec![], vec![2], vec![3], vec![4]),
            (vec![1], vec![], vec![3], vec![4]),
            (vec![1], vec![2], vec![], vec![4]),
            (vec![1], vec![2], vec![3], vec![]),
        ] {
            let error = IdentityDocumentWrite::new(
                ciphertext,
                nonce,
                wrapped_dek,
                wrapped_dek_nonce,
                "key",
                "algorithm",
                1,
            )
            .expect_err("empty encrypted field must fail");
            assert!(matches!(error, AppError::InvalidInput(_)));
        }
        for (key_id, algorithm, aad_version) in [
            (" ", "algorithm", 1),
            ("key", " ", 1),
            ("key", "algorithm", 0),
        ] {
            let error = IdentityDocumentWrite::new(
                vec![1],
                vec![2],
                vec![3],
                vec![4],
                key_id,
                algorithm,
                aad_version,
            )
            .expect_err("invalid metadata must fail");
            assert!(matches!(error, AppError::InvalidInput(_)));
        }
    }

    fn reference(
        owner: &IdentityOwner,
        key: IdentitySpecKey,
        fingerprint: &str,
    ) -> IdentitySpecReference {
        IdentitySpecReference::new(owner, key, fingerprint, "issuer", "fixed_token")
            .expect("valid reference")
    }

    fn document(
        seed: u8,
        key_id: &str,
        algorithm: &str,
        aad_version: i64,
    ) -> IdentityDocumentWrite {
        IdentityDocumentWrite::new(
            vec![seed; 3],
            vec![seed + 1; 2],
            vec![seed + 2; 4],
            vec![seed + 3; 2],
            key_id,
            algorithm,
            aad_version,
        )
        .expect("valid document")
    }

    fn assert_record(
        record: &IdentityDocumentRecord,
        owner: &IdentityOwner,
        name: &IdentityName,
        version: i64,
        seed: u8,
        key_id: &str,
        aad_version: i64,
    ) {
        assert_eq!(&record.owner, owner);
        assert_eq!(&record.name, name);
        assert_eq!(record.document_version, version);
        assert_eq!(record.ciphertext, vec![seed; 3]);
        assert_eq!(record.nonce, vec![seed + 1; 2]);
        assert_eq!(record.wrapped_dek, vec![seed + 2; 4]);
        assert_eq!(record.wrapped_dek_nonce, vec![seed + 3; 2]);
        assert_eq!(record.key_id, key_id);
        assert_eq!(record.aad_version, aad_version);
    }
}
