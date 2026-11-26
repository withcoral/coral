use crate::{
    Agent, AgentCredential, AgentDisplayName, AgentRegistration, AgentRegistrationId, AgentState,
    AuthenticatedPrincipal, AuthenticatedUser, ChangeContext, Error, EventDetail, IfMatch,
    IssuedAgentCredential, LifecycleAction, LifecycleTargetKind, Principal, PrincipalId,
    PrincipalKind, RequestId, ResourceMetadata, WorkspaceId, WorkspaceModule,
    agent_repository as repository, agent_types::credential_verifier, record_event,
};
use coral_db::UnixNanos;

/// Commits an issuance and returns what it shows once. Each metadata is what
/// the transaction left on the row: the caller wrote it, so nothing reads it
/// back.
async fn finish_issuance(
    mut transaction: coral_db::Transaction<'_>,
    (agent, agent_metadata): (Agent, ResourceMetadata),
    (registration, registration_metadata): (AgentRegistration, ResourceMetadata),
    credential: AgentCredential,
) -> Result<IssuedAgentCredential, Error> {
    let creator_profile =
        crate::profile::read_profile(&mut transaction, agent.created_by()).await?;
    transaction.commit().await?;
    Ok(IssuedAgentCredential {
        agent,
        registration,
        agent_metadata,
        registration_metadata,
        credential,
        creator_profile,
    })
}

/// Conceals every Agent from a caller who is not an Owner of the Workspace.
pub(crate) fn require_agent_inspector(evidence: &crate::WorkspaceEvidence) -> Result<(), Error> {
    if evidence.effective_role() == crate::WorkspaceRole::Owner {
        Ok(())
    } else {
        Err(Error::AgentNotFound)
    }
}

impl WorkspaceModule {
    /// Resolves one exact Agent for Owner inspection within an existing snapshot.
    /// Disabled Agents remain inspectable; this does not grant execution authority.
    /// # Errors
    /// Conceals non-Owner access and absent or cross-Workspace Agents.
    pub async fn agent_for_inspection_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        evidence: &crate::WorkspaceEvidence,
        agent_id: &PrincipalId,
    ) -> Result<Agent, Error> {
        require_agent_inspector(evidence)?;
        repository::find(transaction, evidence.workspace().id(), agent_id).await
    }

    /// Loads one exact Agent in a domain-owned transaction without granting authority.
    /// The caller must hold the applicable Workspace authority in that transaction.
    /// # Errors
    /// Conceals absent and cross-Workspace Agents equally.
    pub async fn agent_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
    ) -> Result<Agent, Error> {
        repository::find(transaction, workspace_id, agent_id).await
    }

    /// Creates one Workspace-owned Agent, its Registration, and first credential atomically.
    /// `request` names the creation, so that a repeat of it creates no second Agent.
    ///
    /// # Errors
    ///
    /// Requires an active Workspace and effective Owner. Returns
    /// [`Error::RequestAlreadyApplied`] for a request that already created an
    /// Agent. Failure publishes nothing.
    pub async fn create_agent(
        &self,
        context: &ChangeContext,
        request: &RequestId,
        workspace_id: &WorkspaceId,
        display_name: AgentDisplayName,
        expires_at: Option<UnixNanos>,
    ) -> Result<IssuedAgentCredential, Error> {
        let credential = AgentCredential::generate()?;
        let agent = Agent {
            principal: Principal::from_parts(
                PrincipalId::parse(&format!("agent:{}", uuid::Uuid::new_v4()))?,
                PrincipalKind::Agent,
            ),
            workspace_id: workspace_id.clone(),
            display_name,
            created_by: context.actor().user_id().clone(),
            state: AgentState::Enabled,
        };
        let registration = AgentRegistration {
            id: AgentRegistrationId::generate(),
            agent_id: agent.principal().id().clone(),
            workspace_id: workspace_id.clone(),
            enabled: true,
            expires_at,
        };
        let mut transaction = self.database.begin().await?;
        let evidence = self
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace_id)
            .await?;
        crate::management::claim_request(&mut transaction, context.actor().user_id(), request)
            .await?;
        repository::insert(
            &mut transaction,
            &agent,
            &registration,
            &credential_verifier(credential.expose()),
            context.occurred_at(),
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: LifecycleAction::AgentCreated,
                target_kind: LifecycleTargetKind::Agent,
                target_id: agent.principal().id().as_str(),
                changed_field_paths: &["display_name", "state"],
            },
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: LifecycleAction::AgentRegistrationCreated,
                target_kind: LifecycleTargetKind::AgentRegistration,
                target_id: &registration.id().to_string(),
                changed_field_paths: &["enabled", "expires_at", "credential"],
            },
        )
        .await?;
        let created = ResourceMetadata::created(context.occurred_at());
        finish_issuance(
            transaction,
            (agent, created),
            (registration, created),
            credential,
        )
        .await
    }

    /// Rotates the verifier of the Registration that the caller read at
    /// `if_match`, and shows a fresh credential once, without changing
    /// attribution. `request` names the rotation, so that a repeat of it does
    /// not rotate again.
    ///
    /// # Errors
    ///
    /// Requires an active Workspace, effective Owner, and exact Registration.
    /// Returns [`Error::EtagMismatch`] for a Registration that changed since.
    /// A rotation changes its Registration, so a repeat that sends the etag
    /// it first read gets that error too. A repeat that read the Registration
    /// again gets [`Error::RequestAlreadyApplied`]. Rotation does not enable
    /// a disabled Agent or extend its expiration.
    pub async fn rotate_agent_credential(
        &self,
        context: &ChangeContext,
        request: &RequestId,
        workspace_id: &WorkspaceId,
        registration_id: &AgentRegistrationId,
        if_match: &IfMatch,
    ) -> Result<IssuedAgentCredential, Error> {
        let credential = AgentCredential::generate()?;
        let mut transaction = self.database.begin().await?;
        let evidence = self
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace_id)
            .await?;
        let (registration, read) = repository::registration_by_id_with_metadata(
            &mut transaction,
            workspace_id,
            registration_id,
        )
        .await?;
        let agent =
            repository::find_with_metadata(&mut transaction, workspace_id, registration.agent_id())
                .await?;
        crate::management::check(if_match, read.version)?;
        crate::management::claim_request(&mut transaction, context.actor().user_id(), request)
            .await?;
        repository::update_verifier(
            &mut transaction,
            workspace_id,
            registration_id,
            &credential_verifier(credential.expose()),
            read.version,
            context.occurred_at(),
        )
        .await?;
        let rotated = read.changed(context.occurred_at())?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: LifecycleAction::AgentCredentialRotated,
                target_kind: LifecycleTargetKind::AgentRegistration,
                target_id: &registration_id.to_string(),
                changed_field_paths: &["credential"],
            },
        )
        .await?;
        finish_issuance(transaction, agent, (registration, rotated), credential).await
    }

    /// Changes the lifecycle of the Agent that the caller read at `if_match`,
    /// without removing history or changing identity.
    ///
    /// # Errors
    ///
    /// Requires active effective Owner authority and an Agent in this Workspace.
    /// Returns [`Error::EtagMismatch`] for an Agent that changed since.
    pub async fn set_agent_state(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
        if_match: &IfMatch,
        state: AgentState,
    ) -> Result<Agent, Error> {
        let mut transaction = self.database.begin().await?;
        let evidence = self
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace_id)
            .await?;
        let (mut agent, read) =
            repository::find_with_metadata(&mut transaction, workspace_id, agent_id).await?;
        crate::management::check(if_match, read.version)?;
        if agent.state == state {
            return Ok(agent);
        }
        agent.state = state;
        repository::update_state(
            &mut transaction,
            &agent,
            read.version,
            context.occurred_at(),
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: LifecycleAction::AgentStateUpdated,
                target_kind: LifecycleTargetKind::Agent,
                target_id: agent_id.as_str(),
                changed_field_paths: &["state"],
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(agent)
    }

    /// Replaces the enabled state and optional expiration of the Agent
    /// Registration that the caller read at `if_match`.
    ///
    /// A past deadline expires it immediately. Clearing or extending the deadline
    /// restores Registration authority only if the Agent itself is enabled.
    ///
    /// # Errors
    ///
    /// Requires active effective Owner authority and the exact Registration.
    /// Returns [`Error::EtagMismatch`] for a Registration that changed since.
    pub async fn update_agent_registration(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        registration_id: &AgentRegistrationId,
        if_match: &IfMatch,
        enabled: bool,
        expires_at: Option<UnixNanos>,
    ) -> Result<AgentRegistration, Error> {
        let mut transaction = self.database.begin().await?;
        let evidence = self
            .check_configuration_in_transaction(&mut transaction, context.actor(), workspace_id)
            .await?;
        let (mut registration, read) = repository::registration_by_id_with_metadata(
            &mut transaction,
            workspace_id,
            registration_id,
        )
        .await?;
        crate::management::check(if_match, read.version)?;
        let mut fields = Vec::new();
        if registration.enabled != enabled {
            fields.push("enabled");
        }
        if registration.expires_at != expires_at {
            fields.push("expires_at");
        }
        if fields.is_empty() {
            return Ok(registration);
        }
        registration.enabled = enabled;
        registration.expires_at = expires_at;
        repository::update_registration(
            &mut transaction,
            &registration,
            read.version,
            context.occurred_at(),
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: LifecycleAction::AgentRegistrationUpdated,
                target_kind: LifecycleTargetKind::AgentRegistration,
                target_id: &registration_id.to_string(),
                changed_field_paths: &fields,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(registration)
    }

    /// Lists safe Agent metadata for an effective Owner, including disabled Agents.
    ///
    /// # Errors
    ///
    /// Denies non-Owners; archived Workspace history remains inspectable.
    pub async fn list_agents(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Agent>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut transaction, actor, workspace_id)
            .await?;
        let agents = repository::list(&mut transaction, workspace_id).await?;
        transaction.commit().await?;
        Ok(agents.into_iter().map(|(agent, _metadata)| agent).collect())
    }

    /// Inspects one Agent's single Registration without exposing its verifier.
    ///
    /// # Errors
    ///
    /// Requires effective Owner authority and the exact owning Workspace.
    pub async fn get_agent_registration(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        agent_id: &PrincipalId,
    ) -> Result<AgentRegistration, Error> {
        Ok(self
            .inspect_agent(actor, workspace_id, agent_id)
            .await?
            .registration)
    }

    /// Verifies an opaque bearer credential against the exact Workspace's Registration.
    ///
    /// This is the domain credential check, not HTTP/OAuth/session machinery.
    /// Authority consumers must still recheck lifecycle at their transaction boundary.
    ///
    /// # Errors
    ///
    /// Invalid, rotated, disabled, expired, and cross-Workspace credentials are
    /// indistinguishable. Errors and evidence contain no credential or verifier.
    pub async fn authenticate_agent(
        &self,
        workspace_id: &WorkspaceId,
        credential: &str,
        now: UnixNanos,
    ) -> Result<AuthenticatedPrincipal, Error> {
        self.authenticate_agent_in(Some(workspace_id), credential, now)
            .await
            .map(|(principal, _workspace)| principal)
    }

    /// Verifies an opaque bearer credential and returns the Agent of whichever
    /// Workspace its Registration belongs to, with that Workspace, for the
    /// Organization MCP Endpoint. Every later check must use that Workspace.
    ///
    /// # Errors
    ///
    /// Invalid, rotated, disabled, and expired credentials are
    /// indistinguishable. Errors and evidence contain no credential or verifier.
    pub async fn authenticate_agent_credential(
        &self,
        credential: &str,
        now: UnixNanos,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), Error> {
        self.authenticate_agent_in(None, credential, now).await
    }

    async fn authenticate_agent_in(
        &self,
        workspace_id: Option<&WorkspaceId>,
        credential: &str,
        now: UnixNanos,
    ) -> Result<(AuthenticatedPrincipal, WorkspaceId), Error> {
        if credential.len() != 55 || !credential.starts_with("coral_agent_") {
            return Err(Error::InvalidAgentCredential);
        }
        let verifier = credential_verifier(credential);
        let mut transaction = self.database.begin_read_snapshot().await?;
        let registration = match workspace_id {
            Some(workspace_id) => {
                repository::registration_by_verifier(&mut transaction, workspace_id, &verifier)
                    .await?
            }
            None => repository::registration_by_verifier_alone(&mut transaction, &verifier).await?,
        };
        let agent = repository::find(
            &mut transaction,
            registration.workspace_id(),
            registration.agent_id(),
        )
        .await?;
        registration
            .require_active(&agent, now)
            .map_err(|_lifecycle_loss| Error::InvalidAgentCredential)?;
        transaction.commit().await?;
        let workspace = registration.workspace_id().clone();
        Ok((AuthenticatedPrincipal::agent(registration), workspace))
    }
}
