//! DB-backed OAuth identity creation.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use coral_spec::{IdentitySpecConfig, IdentitySpecType, ManifestOAuthCredentialSpec};

use crate::bootstrap::AppError;
use crate::credentials::encryption::CredentialKeyProvider;
use crate::credentials::oauth::{
    OAuthCredentialMaterial, OAuthCredentialService, StartOAuthCredentialRequest,
};
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::{UserPrincipal, encrypt_identity_document, run_key_operation};
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::{
    record_to_installed, resolve_installed_for_use, spec_not_found,
};
use crate::state::db::{
    CoralTx, DbRepos, IdentityDocumentRecord, IdentityDocumentWrite, IdentityRecord,
    IdentitySpecDocumentRecord, IdentitySpecKey, IdentitySpecRecord, now_unix_nanos_i64,
};
use crate::workspaces::WorkspaceName;

use super::{
    IdentityManager, IdentityOAuthCreationEvent, MAX_MUTATION_ATTEMPTS, identity_document_binding,
    identity_document_write, owner_workspace_created_at, owner_workspace_not_found,
};

const OAUTH_ACCESS_TOKEN_KEY: &str = "ACCESS_TOKEN";

#[derive(Clone, PartialEq, Eq)]
struct OAuthCreateSnapshot {
    workspace_created_at_unix_nanos: Option<i64>,
    identity: Option<IdentityRecord>,
    identity_document: Option<IdentityDocumentRecord>,
    identity_spec: Option<IdentitySpecRecord>,
    identity_spec_document: Option<IdentitySpecDocumentRecord>,
}

struct SelectedOAuthSpec {
    requested_key: IdentitySpecKey,
    snapshot: OAuthCreateSnapshot,
    reference: IdentitySpecReference,
    oauth: ManifestOAuthCredentialSpec,
    source_inputs: BTreeMap<String, String>,
    credential_inputs: Vec<(String, String)>,
}

impl IdentityManager {
    /// Authorize and atomically create or replace one user-owned OAuth identity.
    pub(crate) async fn create_or_replace_user_oauth<E, EventFut>(
        &self,
        principal: &UserPrincipal,
        identity_name: &str,
        identity_spec_name: &str,
        events: E,
    ) -> Result<IdentityRecord, AppError>
    where
        E: Fn(IdentityOAuthCreationEvent) -> EventFut + Clone + Send + Sync,
        EventFut: Future<Output = Result<(), AppError>> + Send,
    {
        let owner = IdentityOwner::for_user(principal.clone());
        let name = IdentityName::parse(identity_name)?;
        let requested_key = IdentitySpecKey::global(identity_spec_name)?;
        self.create_or_replace_oauth(owner, name, requested_key, events)
            .await
    }

    /// Authorize and atomically create or replace one workspace-owned OAuth identity.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "mounted by the follow-up service-surface PR")
    )]
    pub(crate) async fn create_or_replace_workspace_oauth<E, EventFut>(
        &self,
        workspace: &WorkspaceName,
        identity_name: &str,
        identity_spec_name: &str,
        events: E,
    ) -> Result<IdentityRecord, AppError>
    where
        E: Fn(IdentityOAuthCreationEvent) -> EventFut + Clone + Send + Sync,
        EventFut: Future<Output = Result<(), AppError>> + Send,
    {
        let owner = IdentityOwner::workspace(workspace.clone());
        let name = IdentityName::parse(identity_name)?;
        let requested_key = IdentitySpecKey::workspace(workspace.clone(), identity_spec_name)?;
        self.create_or_replace_oauth(owner, name, requested_key, events)
            .await
    }

    async fn create_or_replace_oauth<E, EventFut>(
        &self,
        owner: IdentityOwner,
        name: IdentityName,
        requested_key: IdentitySpecKey,
        events: E,
    ) -> Result<IdentityRecord, AppError>
    where
        E: Fn(IdentityOAuthCreationEvent) -> EventFut + Clone + Send + Sync,
        EventFut: Future<Output = Result<(), AppError>> + Send,
    {
        let selected = self.select_oauth_spec(&owner, &name, requested_key).await?;
        let authorization_events = events.clone();
        let mut material = self
            .oauth
            .authorize_with_callback(
                StartOAuthCredentialRequest {
                    input_key: OAUTH_ACCESS_TOKEN_KEY,
                    oauth: &selected.oauth,
                    source_inputs: &selected.source_inputs,
                    credential_inputs: selected.credential_inputs.clone(),
                },
                move |authorization| {
                    authorization_events(IdentityOAuthCreationEvent::Authorization(authorization))
                },
                || async { Ok(()) },
            )
            .await?;

        material.discard_spec_derived_refresh_context();
        let (safe_metadata, values) = oauth_identity_values(material);
        let document_owner = owner.clone();
        let document_name = name.clone();
        let document_reference = selected.reference.clone();
        let key_provider = Arc::clone(&self.key_provider);
        let document = run_key_operation(move || {
            prepare_oauth_document(
                &document_owner,
                &document_name,
                &document_reference,
                &values,
                key_provider.as_ref(),
            )
        })
        .await?;

        events(IdentityOAuthCreationEvent::Completed(safe_metadata.clone())).await?;

        for _ in 0..MAX_MUTATION_ATTEMPTS {
            match self
                .try_commit_oauth(&owner, &name, &selected, &document, &safe_metadata)
                .await
            {
                Ok(Some(record)) => return Ok(record),
                Ok(None) => return Err(AppError::RetryableTransactionConflict),
                Err(AppError::RetryableTransactionConflict) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    async fn select_oauth_spec(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        requested_key: IdentitySpecKey,
    ) -> Result<SelectedOAuthSpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        let snapshot = load_oauth_create_snapshot(&mut tx, owner, name, &requested_key).await?;
        tx.commit().await?;

        let record = snapshot
            .identity_spec
            .clone()
            .ok_or_else(|| spec_not_found(&requested_key))?;
        let installed = record_to_installed(record)?;
        if installed.manifest.identity_type != IdentitySpecType::OAuth {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{}' has type '{}', not 'oauth'",
                requested_key.name(),
                installed.manifest.identity_type.label(),
            )));
        }

        let key_provider = Arc::clone(&self.key_provider);
        let document = snapshot.identity_spec_document.clone();
        let resolved = run_key_operation(move || {
            resolve_installed_for_use(installed, document, key_provider.as_ref())
        })
        .await?;
        let oauth = match &resolved.spec.manifest.config {
            IdentitySpecConfig::OAuth(config) => config.method.oauth.clone(),
            IdentitySpecConfig::FixedToken => {
                return Err(AppError::InvalidInput(format!(
                    "identity spec '{}' is not oauth",
                    requested_key.name(),
                )));
            }
        };
        let source_inputs = resolved.inputs.variables().clone();
        let credential_inputs = oauth_client_inputs(&oauth, &resolved.inputs);
        OAuthCredentialService::validate_credential_inputs(
            &oauth,
            &source_inputs,
            credential_inputs.clone(),
        )?;
        let reference = IdentitySpecReference::new(
            owner,
            resolved.spec.key,
            identity_spec_fingerprint(&resolved.spec.manifest)?,
            resolved.spec.manifest.issuer,
            resolved.spec.manifest.identity_type.label(),
        )?;

        Ok(SelectedOAuthSpec {
            requested_key,
            snapshot,
            reference,
            oauth,
            source_inputs,
            credential_inputs,
        })
    }

    async fn try_commit_oauth(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        selected: &SelectedOAuthSpec,
        document: &IdentityDocumentWrite,
        safe_metadata: &BTreeMap<String, String>,
    ) -> Result<Option<IdentityRecord>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let current =
            load_oauth_create_snapshot(&mut tx, owner, name, &selected.requested_key).await?;
        if current.workspace_created_at_unix_nanos
            != selected.snapshot.workspace_created_at_unix_nanos
        {
            tx.rollback().await?;
            return Err(owner_workspace_not_found(owner));
        }
        if current != selected.snapshot {
            tx.rollback().await?;
            return Ok(None);
        }

        let now = now_unix_nanos_i64()?;
        let result = async {
            let record = tx
                .identities()
                .upsert(owner, name, &selected.reference, safe_metadata, now)
                .await?;
            #[cfg(test)]
            if let Some(gate) = &self.before_write_gate {
                gate.wait_once().await;
            }
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

async fn load_oauth_create_snapshot(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    requested_key: &IdentitySpecKey,
) -> Result<OAuthCreateSnapshot, AppError> {
    let workspace_created_at_unix_nanos = owner_workspace_created_at(tx, owner).await?;
    let identity = tx.identities().load_optional(owner, name).await?;
    let identity_document = tx.identity_documents().load_optional(owner, name).await?;
    let identity_spec = tx.identity_specs().resolve_optional(requested_key).await?;
    let identity_spec_document = match identity_spec.as_ref() {
        Some(record) => {
            tx.identity_spec_documents()
                .load_optional(&record.key)
                .await?
        }
        None => None,
    };
    Ok(OAuthCreateSnapshot {
        workspace_created_at_unix_nanos,
        identity,
        identity_document,
        identity_spec,
        identity_spec_document,
    })
}

fn oauth_client_inputs(
    oauth: &ManifestOAuthCredentialSpec,
    inputs: &crate::identity_specs::manager::ResolvedIdentitySpecInputs,
) -> Vec<(String, String)> {
    let value = |key: &str| {
        inputs
            .variables()
            .get(key)
            .or_else(|| inputs.secrets().get(key))
            .cloned()
    };
    let mut values = Vec::with_capacity(2);
    if let Some(key) = oauth.client.id.input.as_deref()
        && let Some(value) = value(key)
    {
        values.push((key.to_string(), value));
    }
    if let Some(secret) = oauth.client.secret.as_ref()
        && let Some(value) = value(&secret.input)
    {
        values.push((secret.input.clone(), value));
    }
    values
}

fn oauth_identity_values(
    material: OAuthCredentialMaterial,
) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
    let OAuthCredentialMaterial {
        access_token,
        mut internal_metadata,
        safe_metadata,
        ..
    } = material;
    internal_metadata.insert(OAUTH_ACCESS_TOKEN_KEY.to_string(), access_token);
    (safe_metadata, internal_metadata)
}

fn prepare_oauth_document(
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<IdentityDocumentWrite, AppError> {
    let binding = identity_document_binding(owner, name, reference);
    let document = encrypt_identity_document(&binding, values, key_provider)?;
    identity_document_write(document)
}
