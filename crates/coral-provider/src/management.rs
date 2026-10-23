//! Current Workspace authority and exact Provider inspection.
use std::collections::BTreeMap;

use crate::{
    Error, Provider, ProviderCallRoot, ProviderId, ProviderModule, ProviderNamespace, repository,
};
use coral_catalog::{
    AuthenticationConfiguration, CredentialField, OperationKey, OperationSummary,
    ProviderConfiguration, ServerSelection,
};
use coral_workspace::AuthenticatedUser;
use coral_workspace::WorkspaceAction;
use coral_workspace::WorkspaceEvidence;
use coral_workspace::WorkspaceId;

/// Owner-only source provenance, without descriptors or credential material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSource {
    /// Latest successfully published source URL; absent for an uploaded document.
    pub url: Option<String>,
    /// Protocol of the current published input.
    pub kind: crate::ProviderSetupInputKind,
}

/// One Provider and its Provider Configuration, as an Owner reads it. An OAuth
/// application's secret and every Provider Account's credentials are stored
/// apart, so it holds neither. A fixed credential part keeps its value, which
/// a result for a Program must leave out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderWithConfiguration {
    /// The Provider, with its state, display name and Credential Route.
    pub provider: Provider,
    /// Where it was last published from.
    pub source: ProviderSource,
    /// A server per server declaration.
    pub servers: BTreeMap<String, ServerSelection>,
    /// The authentication method, with the OAuth application in use.
    pub authentication: AuthenticationConfiguration,
    /// Non-secret headers sent with every request.
    pub request_headers: BTreeMap<String, String>,
    /// The values a person enters when they sign in.
    pub credential_fields: BTreeMap<String, CredentialField>,
}

impl ProviderWithConfiguration {
    /// The Provider Configuration, with the Provider's display name.
    #[must_use]
    pub fn configuration(&self) -> ProviderConfiguration {
        ProviderConfiguration {
            display_name: self.provider.display_name().to_owned(),
            servers: self.servers.clone(),
            authentication: Some(self.authentication.clone()),
            request_headers: self.request_headers.clone(),
            credential_fields: self.credential_fields.clone(),
        }
    }
}

impl ProviderModule {
    /// Reads the Provider Configuration of the Provider at `namespace`, in any
    /// state, under current Owner authority.
    ///
    /// # Errors
    /// Returns current Owner authority, [`Error::ProviderNotFound`] when no
    /// Provider in the Workspace holds `namespace`, or persistence or
    /// corrupt-evidence failures.
    pub async fn inspect_provider_configuration(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        namespace: &ProviderNamespace,
    ) -> Result<ProviderWithConfiguration, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut tx, actor, workspace)
            .await?;
        let active = repository::provider_by_call_root_in_transaction(
            &mut tx,
            workspace,
            &ProviderCallRoot::Owner(namespace.clone()),
            coral_db::RowLock::None,
        )
        .await?
        .ok_or(Error::ProviderNotFound)?;
        let configuration =
            crate::configuration::current_configuration_in_transaction(&mut tx, &active).await?;
        // Publication requires a confirmed method, so every Generation retains one.
        let Some(ProviderConfiguration {
            authentication: Some(authentication),
            servers,
            request_headers,
            credential_fields,
            ..
        }) = configuration
        else {
            return Err(Error::CorruptData(
                "a published Provider has no confirmed configuration".into(),
            ));
        };
        let kind =
            coral_catalog::setup_input_kind_in_transaction(&mut tx, &active.generation_id).await?;
        tx.rollback().await?;
        Ok(ProviderWithConfiguration {
            provider: active.provider,
            source: ProviderSource {
                url: active.source_url,
                kind,
            },
            servers,
            authentication,
            request_headers,
            credential_fields,
        })
    }

    /// Inspects the saved source for one exact Provider under current Owner authority.
    ///
    /// # Errors
    /// Returns current Owner authority, missing Provider, or persistence failures, and
    /// [`Error::BuiltinProviderManaged`] for the Built-in Provider, whose catalog Coral
    /// defines without a source.
    pub async fn inspect_provider_source(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<ProviderSource, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut tx, actor, workspace)
            .await?;
        let stored = repository::provider_in_transaction(&mut tx, workspace, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        match stored.provider.call_root() {
            ProviderCallRoot::Owner(_) => {}
            ProviderCallRoot::Builtin => return Err(Error::BuiltinProviderManaged),
        }
        let kind =
            coral_catalog::setup_input_kind_in_transaction(&mut tx, &stored.generation_id).await?;
        tx.rollback().await?;
        Ok(ProviderSource {
            url: stored.source_url,
            kind,
        })
    }

    /// Lists exact Workspace Providers under current inspection authority.
    /// Only an Owner sees the Built-in Provider.
    ///
    /// # Errors
    /// Returns current Workspace authority or persistence failures.
    pub async fn inspect_providers(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<Provider>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self.management_authority(&mut tx, actor, workspace).await?;
        let result = repository::management_providers(&mut tx, workspace, None)
            .await?
            .into_iter()
            .map(|stored| stored.provider)
            .filter(|provider| crate::inspects(&evidence, provider))
            .collect();
        tx.rollback().await?;
        Ok(result)
    }

    /// Lists every Workspace Provider, in any state and in creation order, with
    /// what each offers `actor`, from one snapshot. The Built-in Provider,
    /// created with the Workspace, comes first, and only for an Owner.
    ///
    /// # Errors
    /// Returns current Workspace authority, persistence, or corrupt-evidence failures.
    pub async fn published_providers(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<crate::PublishedProvider>, Error> {
        let caller = coral_workspace::AuthenticatedPrincipal::user(actor.clone());
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self.management_authority(&mut tx, actor, workspace).await?;
        let providers = repository::management_providers(&mut tx, workspace, None).await?;
        let mut published = Vec::with_capacity(providers.len());
        for active in providers
            .into_iter()
            .filter(|active| crate::inspects(&evidence, &active.provider))
        {
            published.push(
                crate::program_setup::published_active_provider_in_transaction(
                    &mut tx,
                    active,
                    caller.principal(),
                )
                .await?,
            );
        }
        tx.rollback().await?;
        Ok(published)
    }

    /// Whether a Provider in this Workspace already uses `namespace`, so a
    /// Publication under it would fail with a namespace conflict.
    ///
    /// # Errors
    /// Returns current Workspace authority or persistence failures.
    pub async fn namespace_in_use(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        namespace: &crate::ProviderNamespace,
    ) -> Result<bool, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.management_authority(&mut tx, actor, workspace).await?;
        let in_use = crate::access::call_root_exists(
            &mut tx,
            workspace,
            &crate::ProviderCallRoot::Owner(namespace.clone()),
        )
        .await?;
        tx.rollback().await?;
        Ok(in_use)
    }

    /// Inspects one Provider, concealing other Workspaces, and the Built-in
    /// Provider from a non-Owner, as absent.
    ///
    /// # Errors
    /// Returns current Workspace authority, missing Provider or persistence failures.
    pub async fn inspect_provider(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<Provider, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let stored = self
            .inspected_provider(&mut tx, actor, workspace, provider)
            .await?;
        tx.rollback().await?;
        Ok(stored.provider)
    }
    /// Reads the current catalog without exposing private execution bindings.
    ///
    /// # Errors
    /// Returns current Workspace authority, missing Provider or catalog persistence failures.
    pub async fn inspect_catalog_page(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        after: Option<&OperationKey>,
        page_size: u32,
    ) -> Result<Vec<OperationSummary>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let stored = self
            .inspected_provider(&mut tx, actor, workspace, provider)
            .await?;
        let operations = coral_catalog::operation_summaries_in_transaction(
            &mut tx,
            &stored.generation_id,
            after,
            page_size,
        )
        .await?;
        tx.rollback().await?;
        Ok(operations)
    }
    pub(super) async fn management_authority(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<WorkspaceEvidence, Error> {
        Ok(self
            .workspaces
            .check_in_transaction(tx, actor, workspace, WorkspaceAction::Inspect)
            .await?)
    }

    /// Loads one Provider that `actor` may inspect; anything else is absent.
    async fn inspected_provider(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<repository::ProviderWithActiveGeneration, Error> {
        let evidence = self.management_authority(tx, actor, workspace).await?;
        repository::provider_in_transaction(tx, workspace, provider)
            .await?
            .filter(|stored| crate::inspects(&evidence, &stored.provider))
            .ok_or(Error::ProviderNotFound)
    }
}

impl ProviderModule {
    /// Changes safe display metadata in the caller's transaction, retaining identity,
    /// namespace, route and policy. An unchanged display name writes nothing.
    ///
    /// # Errors
    /// Requires current owner authority, an exact Provider and its current etag. Never commit
    /// the transaction after an error.
    pub async fn rename_provider_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        context: &coral_workspace::ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: crate::ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(tx, context.actor(), workspace)
            .await?;
        let current = crate::change::hold_for_change(
            tx,
            workspace,
            id,
            crate::change::ProviderChange::Rename,
        )
        .await?
        .provider;
        current.check_etag(context)?;
        if current.display_name() == display_name.as_str() {
            return Ok(current);
        }
        repository::update_display_name(tx, &current, &display_name).await?;
        repository::insert_lifecycle_event(
            tx,
            repository::NewLifecycleEvent {
                workspace_id: Some(workspace),
                actor: crate::LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: authority,
                },
                action: crate::LifecycleAction::ProviderUpdated,
                target_id: &id.to_string(),
                outcome: crate::LifecycleOutcome::Accepted,
                changed_field_paths: &["display_name"],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        let updated = repository::provider_in_transaction(tx, workspace, id)
            .await?
            .ok_or(Error::ProviderNotFound)?
            .provider;
        Ok(updated)
    }
}

/// Provider-rule tests rename a Provider without the Provider workflow and its notifications.
#[cfg(any(test, feature = "test-fixtures"))]
impl ProviderModule {
    /// Changes safe display metadata in its own transaction.
    ///
    /// # Errors
    /// Returns the errors of [`Self::rename_provider_in_transaction`].
    pub async fn rename_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: crate::ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let mut tx = self.database.begin().await?;
        let updated = self
            .rename_provider_in_transaction(&mut tx, context, workspace, id, display_name)
            .await?;
        tx.commit().await?;
        Ok(updated)
    }
}
