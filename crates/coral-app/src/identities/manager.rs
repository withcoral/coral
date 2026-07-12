//! Database-backed identity instance management.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "B4h wires identity managers into public services."
    )
)]

use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::IdentitySpecType;

use crate::bootstrap::AppError;
use crate::credentials::encryption::{CredentialKeyProvider, EncryptedEnvelopeDocument};
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::{UserPrincipal, encrypt_identity_document, run_key_operation};
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::record_to_installed;
use crate::state::db::{
    CoralDb, DbRepos, IdentityDocumentWrite, IdentityRecord, IdentitySpecKey, IdentitySpecRecord,
    now_unix_nanos_i64,
};

const FIXED_TOKEN_KEY: &str = "TOKEN";
const MAX_MUTATION_ATTEMPTS: usize = 8;

/// Database-backed behavior for owner-scoped identity instances.
#[derive(Clone)]
pub(crate) struct IdentityManager {
    db: Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    #[cfg(test)]
    before_write_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_upsert_gate: Option<BeforeUpsertGate>,
}

#[cfg(test)]
#[derive(Clone)]
struct BeforeWriteGate {
    selected: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

#[cfg(test)]
#[derive(Clone)]
struct BeforeUpsertGate {
    barrier: Arc<tokio::sync::Barrier>,
    used: Arc<AtomicBool>,
}

struct SelectedFixedTokenSpec {
    record: IdentitySpecRecord,
    reference: IdentitySpecReference,
}

impl IdentityManager {
    pub(crate) fn new(db: Arc<CoralDb>, key_provider: Arc<dyn CredentialKeyProvider>) -> Self {
        Self {
            db,
            key_provider,
            #[cfg(test)]
            before_write_gate: None,
            #[cfg(test)]
            before_upsert_gate: None,
        }
    }

    #[cfg(test)]
    fn with_before_write_gate(
        mut self,
        selected: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_write_gate = Some(BeforeWriteGate { selected, resume });
        self
    }

    #[cfg(test)]
    fn with_before_upsert_gate(mut self, barrier: Arc<tokio::sync::Barrier>) -> Self {
        self.before_upsert_gate = Some(BeforeUpsertGate {
            barrier,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    /// Create or replace a user-owned fixed-token identity from one exact global spec.
    pub(crate) async fn create_or_replace_user_fixed_token(
        &self,
        principal: &UserPrincipal,
        identity_name: &str,
        identity_spec_name: &str,
        token: String,
    ) -> Result<IdentityRecord, AppError> {
        let owner = IdentityOwner::for_user(principal.clone());
        let name = IdentityName::parse(identity_name)?;
        let spec_key = IdentitySpecKey::global(identity_spec_name)?;
        let token = token.trim().to_string();
        if token.is_empty() {
            return Err(AppError::InvalidInput(
                "fixed-token identity token must not be blank".to_string(),
            ));
        }
        let mut document = None;
        #[cfg(test)]
        let mut before_write_gate = self.before_write_gate.clone();

        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let selected = match self.load_fixed_token_spec(&owner, &spec_key).await {
                Ok(selected) => selected,
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if document.is_none() {
                let document_owner = owner.clone();
                let document_name = name.clone();
                let document_token = token.clone();
                let key_provider = Arc::clone(&self.key_provider);
                document = Some(
                    run_key_operation(move || {
                        prepare_fixed_token_document(
                            &document_owner,
                            &document_name,
                            document_token,
                            key_provider.as_ref(),
                        )
                    })
                    .await?,
                );
            }
            #[cfg(test)]
            if let Some(gate) = before_write_gate.take() {
                gate.selected.wait().await;
                gate.resume.wait().await;
            }
            match self
                .try_write(
                    &owner,
                    &name,
                    &selected,
                    document.as_ref().expect("document prepared above"),
                )
                .await
            {
                Ok(Some(record)) => return Ok(record),
                Ok(None) | Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    /// List safe persisted fields for one exact owner, including orphaned identities.
    pub(crate) async fn list_for_owner(
        &self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        let mut db = self.db.as_ref();
        Ok(db.identities().list_for_owner(owner).await?)
    }

    /// Get safe persisted fields for one identity, including an orphaned identity.
    pub(crate) async fn get(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<IdentityRecord, AppError> {
        let name = IdentityName::parse(identity_name)?;
        let mut db = self.db.as_ref();
        db.identities()
            .load_optional(owner, &name)
            .await?
            .ok_or_else(|| identity_not_found(&name))
    }

    /// Delete one identity and its cascading encrypted document.
    pub(crate) async fn delete(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<(), AppError> {
        let name = IdentityName::parse(identity_name)?;
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            match self.try_delete(owner, &name).await {
                Ok(()) => return Ok(()),
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    async fn try_delete(&self, owner: &IdentityOwner, name: &IdentityName) -> Result<(), AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let deleted = match tx.identities().delete(owner, name).await {
            Ok(deleted) => deleted,
            Err(error) => {
                tx.rollback().await?;
                return Err(error.into());
            }
        };
        if !deleted {
            tx.rollback().await?;
            return Err(identity_not_found(name));
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load_fixed_token_spec(
        &self,
        owner: &IdentityOwner,
        key: &IdentitySpecKey,
    ) -> Result<SelectedFixedTokenSpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        let record = tx.identity_specs().load_optional(key).await?;
        tx.commit().await?;
        let record = record.ok_or_else(|| global_spec_not_found(key))?;
        let installed = record_to_installed(record.clone())?;
        if installed.manifest.identity_type != IdentitySpecType::FixedToken {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{}' has type '{}', not 'fixed_token'",
                key.name(),
                installed.manifest.identity_type.label(),
            )));
        }
        let reference = IdentitySpecReference::new(
            owner,
            installed.key,
            identity_spec_fingerprint(&installed.manifest)?,
            installed.manifest.issuer,
            installed.manifest.identity_type.label(),
        )?;
        Ok(SelectedFixedTokenSpec { record, reference })
    }

    async fn try_write(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        selected: &SelectedFixedTokenSpec,
        document: &IdentityDocumentWrite,
    ) -> Result<Option<IdentityRecord>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let current = tx
            .identity_specs()
            .load_optional(selected.reference.key())
            .await?;
        if current.as_ref() != Some(&selected.record) {
            tx.rollback().await?;
            return Ok(None);
        }
        #[cfg(test)]
        if let Some(gate) = &self.before_upsert_gate
            && !gate.used.swap(true, Ordering::SeqCst)
        {
            gate.barrier.wait().await;
        }
        let now = now_unix_nanos_i64()?;
        let result = async {
            let record = tx
                .identities()
                .upsert(owner, name, &selected.reference, now)
                .await?;
            tx.identity_documents()
                .upsert(owner, name, document, now)
                .await?;
            Ok::<_, AppError>(record)
        }
        .await;
        let record = match result {
            Ok(record) => record,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        tx.commit().await?;
        Ok(Some(record))
    }
}

fn prepare_fixed_token_document(
    owner: &IdentityOwner,
    name: &IdentityName,
    token: String,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<IdentityDocumentWrite, AppError> {
    let values = BTreeMap::from([(FIXED_TOKEN_KEY.to_string(), token)]);
    let EncryptedEnvelopeDocument {
        ciphertext,
        nonce,
        wrapped_dek,
        wrapped_dek_nonce,
        key_id,
        algorithm,
        aad_version,
    } = encrypt_identity_document(
        owner.kind(),
        owner.key(),
        name.as_str(),
        &values,
        key_provider,
    )?;
    IdentityDocumentWrite::new(
        ciphertext,
        nonce,
        wrapped_dek,
        wrapped_dek_nonce,
        key_id,
        algorithm,
        aad_version,
    )
}

fn global_spec_not_found(key: &IdentitySpecKey) -> AppError {
    AppError::IdentitySpecNotFound {
        name: key.name().to_string(),
        scope: "global".to_string(),
    }
}

fn identity_not_found(name: &IdentityName) -> AppError {
    AppError::IdentityNotFound(name.to_string())
}

#[cfg(test)]
pub(crate) mod tests;
