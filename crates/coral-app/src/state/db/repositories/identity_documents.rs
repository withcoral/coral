#![expect(
    dead_code,
    reason = "Portable repository contracts and manager consumers land in later stack layers."
)]

use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::bootstrap::AppError;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::state::db::schema::IdentityDocuments;
use crate::state::db::{CoralTx, DbError, DbSession};

/// Opaque encrypted material persisted for one owner-scoped identity.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct IdentityDocumentRecord {
    pub(crate) owner: IdentityOwner,
    pub(crate) name: IdentityName,
    pub(crate) document_version: i64,
    pub(crate) envelope: EncryptedEnvelopeDocument,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) updated_at_unix_nanos: i64,
}

impl std::fmt::Debug for IdentityDocumentRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdentityDocumentRecord")
            .field("owner", &self.owner)
            .field("name", &self.name)
            .field("document_version", &self.document_version)
            .field("envelope", &self.envelope)
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
    binding_version: i64,
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
        let envelope = EncryptedEnvelopeDocument::new(
            self.ciphertext,
            self.nonce,
            self.wrapped_dek,
            self.wrapped_dek_nonce,
            self.key_id,
            self.algorithm,
            self.binding_version,
        )
        .map_err(|error| DbError::CorruptData(error.to_string()))?;
        Ok(IdentityDocumentRecord {
            owner: IdentityOwner::from_key_storage_parts(&self.owner_kind, &self.owner_key)?,
            name: IdentityName::from_storage(&self.name)?,
            document_version: self.document_version,
            envelope,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
        })
    }
}

/// Repository for encrypted material owned by identity instances.
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
    pub(crate) async fn get(
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
        document: &EncryptedEnvelopeDocument,
        now_unix_nanos: i64,
    ) -> Result<IdentityDocumentRecord, AppError> {
        validate_write_timestamp(now_unix_nanos)?;
        document
            .validate()
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
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
                Expr::val(document.binding_version),
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
                    IdentityDocuments::BindingVersion,
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
        match self.session.execute_rows_affected(statement).await? {
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
        self.get(owner, name).await?.ok_or_else(|| {
            AppError::Database("identity document disappeared after upsert".to_string())
        })
    }
}

fn validate_write_timestamp(now_unix_nanos: i64) -> Result<(), AppError> {
    match now_unix_nanos {
        0.. => Ok(()),
        _ => Err(AppError::InvalidInput(
            "identity document timestamp is negative".to_string(),
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
        IdentityDocuments::BindingVersion,
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
    use super::IdentityDocumentRecord;
    use crate::encrypted_document::EncryptedEnvelopeDocument;
    use crate::identities::model::{IdentityName, IdentityOwner};
    use crate::identity::Principal;

    #[test]
    fn identity_document_debug_omits_envelope_material() {
        let envelope = EncryptedEnvelopeDocument::new(
            vec![7; 3],
            vec![8; 2],
            vec![9; 4],
            vec![10; 2],
            "sensitive-key-id",
            "sensitive-algorithm",
            1,
        )
        .expect("valid document");
        assert_eq!(envelope.binding_version, 1);
        let record = IdentityDocumentRecord {
            owner: IdentityOwner::for_user(Principal::local()),
            name: IdentityName::parse("github").expect("identity name"),
            document_version: 1,
            envelope,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        };
        let debug = format!("{record:?}");
        for secret in [
            "[7, 7, 7]",
            "[8, 8]",
            "[9, 9, 9, 9]",
            "[10, 10]",
            "sensitive-key-id",
            "sensitive-algorithm",
        ] {
            assert!(!debug.contains(secret), "debug output leaked {secret}");
        }
    }
}
