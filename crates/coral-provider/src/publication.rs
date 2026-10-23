//! Synchronous Provider Publication, behind typed creation and regeneration commands.
//!
//! The Provider module's publication and inspection methods are implemented here.
//! One protocol owns exact replay, pinning, transaction-free materialization, final
//! validation, terminal persistence, Setup consumption, and lifecycle evidence.
//! Preparation runs outside any transaction. The terminal write joins the caller's
//! transaction and returns evidence of what it changed; the caller commits.
//! Commands retain exact creation or regeneration evidence. Workspace owns authority;
//! Catalog owns materialization and immutable Generations. Provider prepares its
//! immutable Call Root and promotes provisional credentials in the final transaction.
//! Every terminal transaction scrubs remaining provisional credentials.
//!
//! Safe structural/domain failures consume the Setup with a failed terminal result.
//! Authority, etag, configuration, and operational failures before commit publish
//! nothing and leave the Setup reusable. Exact replay returns the stored snapshot
//! without materialization or duplicate evidence, including after Provider deletion.
//! Regeneration's etag is a current-state precondition, not a new replay encoding.
//! Distinct terminal snapshots preserve the existing management projections.
use coral_workspace::WorkspaceId;

pub(crate) mod materialization;
mod repository;
#[cfg(test)]
mod tests;
mod types;

/// The terminal Workspace Publication that one management request produced,
/// in the caller's transaction. A request that reached no terminal Publication
/// has none.
///
/// # Errors
///
/// Returns a persistence or corrupt-evidence failure.
pub async fn publication_for_request_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> Result<Option<ProviderPublication>, Error> {
    repository::publication_by_request_in_transaction(transaction, workspace_id, request_id).await
}

/// The terminal regeneration Publication that one management request
/// produced, in the caller's transaction. A request that reached no terminal
/// Publication has none.
///
/// # Errors
///
/// Returns a persistence or corrupt-evidence failure.
pub async fn regeneration_for_request_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> Result<Option<ProviderRegenerationPublication>, Error> {
    repository::regeneration_publication_by_request_in_transaction(
        transaction,
        workspace_id,
        request_id,
    )
    .await
}

/// Publishes one Workspace-owned Provider, or a new Generation of one, or
/// renames one, in its own transaction and notifies Universal Search after the
/// commit. The workflow that owns that transaction implements it, so a Built-in
/// Operation publishes and renames exactly as Reef does.
#[async_trait::async_trait]
pub trait WorkspaceProviderPublisher: Send + Sync {
    /// Publishes the request's Setup. Safe structural and domain failures are
    /// successful calls whose returned outcome is `Failed`.
    ///
    /// # Errors
    ///
    /// Returns authority, etag, configuration and operational failures, which
    /// publish nothing and leave the Setup reusable.
    async fn publish_workspace_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<ProviderPublication, crate::Error>;

    /// Publishes the request's Update Setup as the Provider's new Generation.
    /// The context's etag is the regeneration's precondition. Safe structural
    /// and domain failures are successful calls whose returned outcome is
    /// `Failed`.
    ///
    /// # Errors
    ///
    /// Returns authority, etag, configuration and operational failures, which
    /// publish nothing and leave the Setup reusable.
    async fn regenerate_workspace_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        request: ProviderRegenerationRequest,
    ) -> Result<ProviderRegenerationPublication, crate::Error>;

    /// Renames the Provider `provider` and keeps its Generation. The
    /// context's etag is the rename's precondition.
    ///
    /// # Errors
    ///
    /// Returns authority, etag and operational failures, which rename nothing.
    async fn rename_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        workspace: &WorkspaceId,
        provider: &crate::ProviderId,
        display_name: crate::ProviderDisplayName,
    ) -> Result<crate::Provider, crate::Error>;
}

pub use types::{
    ActivatedGeneration, ProviderPublication, ProviderPublicationId, ProviderPublicationOutcome,
    ProviderRegenerationPublication, ProviderRegenerationRequest, PublicationEvidence, Published,
    WorkspaceProviderPublicationRequest,
};

use coral_catalog::{AuthenticationConfiguration, MaterializationOutcome, ProviderSetupInput};
use coral_db::Transaction;
use coral_workspace::{AuthenticatedUser, AuthoritySource, ChangeContext, WorkspaceAction};

use crate::account::setup as setup_credentials;
use crate::repository as provider_repository;
use crate::types::check_etag;
use crate::{
    Error, LifecycleAction, LifecycleActor, LifecycleOutcome, ManagementRequestId, Provider,
    ProviderDisplayName, ProviderId, ProviderModule, ProviderSetupId, ProviderState,
    PublicationDiagnostic, access, require_owner, require_ready,
};

/// The request carries its target's evidence through both stages. Regeneration
/// advances from an etag precondition to a pinned baseline; creation needs neither.
enum Command<R = RegenerationPrecondition> {
    Workspace(WorkspaceProviderPublicationRequest),
    Regenerate {
        request: ProviderRegenerationRequest,
        regeneration: R,
    },
}

impl<R> Command<R> {
    fn owner(&self) -> WorkspaceId {
        match self {
            Self::Workspace(request) => request.workspace_id().clone(),

            Self::Regenerate { request, .. } => request.owner().clone(),
        }
    }

    fn request_id(&self) -> &ManagementRequestId {
        match self {
            Self::Workspace(request) => request.request_id(),

            Self::Regenerate { request, .. } => request.request_id(),
        }
    }

    fn setup_id(&self) -> &ProviderSetupId {
        match self {
            Self::Workspace(request) => request.setup_id(),

            Self::Regenerate { request, .. } => request.setup_id(),
        }
    }
}

enum Terminal {
    Workspace(ProviderPublication),
    Regeneration(ProviderRegenerationPublication),
}

impl Terminal {
    fn id(&self) -> &ProviderPublicationId {
        match self {
            Self::Workspace(result) => result.id(),

            Self::Regeneration(result) => result.id(),
        }
    }

    fn outcome(&self) -> ProviderPublicationOutcome {
        match self {
            Self::Workspace(result) => result.outcome(),

            Self::Regeneration(result) => result.outcome(),
        }
    }

    fn into_workspace(self) -> Result<ProviderPublication, Error> {
        match self {
            Self::Workspace(publication) => Ok(publication),
            Self::Regeneration(_) => Err(mismatched_command_result()),
        }
    }

    fn into_regeneration(self) -> Result<ProviderRegenerationPublication, Error> {
        match self {
            Self::Regeneration(publication) => Ok(publication),
            Self::Workspace(_) => Err(mismatched_command_result()),
        }
    }
}

fn mismatched_command_result() -> Error {
    Error::CorruptData("Publication returned another command's result".to_string())
}

struct RegenerationPrecondition {
    expected_etag: Option<String>,
}

struct RegenerationBaseline {
    precondition: RegenerationPrecondition,
    generation_id: coral_catalog::ProviderGenerationId,
    fingerprint: coral_catalog::ProviderGenerationFingerprint,
    display_name: ProviderDisplayName,
    operation_paths: Vec<coral_catalog::OperationPathAssignment>,
}

struct PinnedPublication {
    setup_input: ProviderSetupInput,
    command: Command<RegenerationBaseline>,
}

enum Preflight {
    Replay(Terminal),
    Materialize(PinnedPublication),
}

/// What preparation returns: a stored result, or a publication to write.
pub enum PreparedPublication<T> {
    /// The request had already committed. Nothing is left to write.
    Replayed(T),
    /// A materialized publication for [`ProviderModule::publish_in_transaction`].
    Materialized(Box<MaterializedPublication<T>>),
}

/// A materialized publication that holds no transaction.
///
/// It carries the start time: lock waits and materialization count against a
/// Setup grant's lifetime.
pub struct MaterializedPublication<T> {
    pinned: PinnedPublication,
    materialized: MaterializationOutcome,
    started: std::time::Instant,
    result: fn(Terminal) -> Result<T, Error>,
}

/// Final authority checks follow replay for regeneration, so deletion/archive
/// does not invalidate a previously committed response. Creation locks already
/// supply its current authority before replay.
enum TerminalLock {
    Creation(AuthoritySource),
    Regeneration { provider_exists: bool },
}

struct AppliedPublication {
    terminal: Terminal,
    changed_field_paths: &'static [&'static str],
    evidence: PublicationEvidence,
}

const TERMINAL_ONLY_FIELDS: &[&str] = &["setup.state", "publication"];

impl AppliedPublication {
    fn failed_regeneration(
        id: ProviderPublicationId,
        request: ProviderRegenerationRequest,
        diagnostic: PublicationDiagnostic,
        completed_at: i64,
    ) -> Self {
        Self {
            terminal: Terminal::Regeneration(ProviderRegenerationPublication::new(
                id,
                request,
                ProviderPublicationOutcome::Failed,
                Some(diagnostic),
                completed_at,
            )),
            changed_field_paths: TERMINAL_ONLY_FIELDS,
            evidence: PublicationEvidence::Failed,
        }
    }
}

impl ProviderModule {
    async fn prepare_command<T>(
        &self,
        context: &ChangeContext,
        command: Command,
        result: fn(Terminal) -> Result<T, Error>,
    ) -> Result<PreparedPublication<T>, Error> {
        let started = std::time::Instant::now();
        let pinned = match self.publication_preflight(context, command).await? {
            Preflight::Replay(terminal) => {
                return Ok(PreparedPublication::Replayed(result(terminal)?));
            }
            Preflight::Materialize(pinned) => pinned,
        };
        let paths = match &pinned.command {
            Command::Regenerate { regeneration, .. } => regeneration.operation_paths.as_slice(),
            Command::Workspace(_) => &[],
        };
        // No transaction survives preflight. Cancellation here accepts no Publication.
        let materialized = self
            .materializer
            .materialize(&pinned.setup_input, paths)
            .await
            .map_err(|error| Error::OperationalPublication(error.message().to_string()))?;
        Ok(PreparedPublication::Materialized(Box::new(
            MaterializedPublication {
                pinned,
                materialized,
                started,
                result,
            },
        )))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one preflight keeps replay, the row lock, and the regeneration baseline in order"
    )]
    async fn publication_preflight(
        &self,
        context: &ChangeContext,
        command: Command,
    ) -> Result<Preflight, Error> {
        let owner = command.owner();
        let actor = context.actor();

        let mut replay = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut replay, actor, &owner)
            .await?;
        if let Some(terminal) = exact_replay(&mut replay, context.actor(), &command).await? {
            replay.rollback().await?;
            return Ok(Preflight::Replay(terminal));
        }
        replay.rollback().await?;

        match &command {
            Command::Workspace(request) => {
                self.workspaces
                    .check(actor, request.workspace_id(), WorkspaceAction::Configure)
                    .await?;
            }
            Command::Regenerate { .. } => {
                #[cfg(test)]
                self.publication_observer
                    .reached(PublicationPoint::RegenerationReplayMissed)
                    .await;
            }
        }
        let mut transaction = self.database.begin().await?;
        let provider_exists = match &command {
            Command::Regenerate { request, .. } => {
                self.lock_regeneration_in_transaction(&mut transaction, actor, request)
                    .await?
            }
            Command::Workspace(_) => true,
        };
        if let Some(terminal) = exact_replay(&mut transaction, context.actor(), &command).await? {
            transaction.rollback().await?;
            return Ok(Preflight::Replay(terminal));
        }
        // Creation and regeneration share the exact Workspace's request Call Root.
        // Re-read after a key appears: a concurrent exact request may
        // have committed since the first read (Postgres READ COMMITTED).
        if repository::request_key_exists(&mut transaction, &command).await? {
            if let Some(terminal) =
                exact_replay(&mut transaction, context.actor(), &command).await?
            {
                transaction.rollback().await?;
                return Ok(Preflight::Replay(terminal));
            }
            return Err(Error::RequestIdConflict);
        }
        let command = match command {
            Command::Regenerate {
                request,
                regeneration,
            } => {
                self.check_owner_configuration_in_transaction(&mut transaction, actor, &owner)
                    .await?;
                if !provider_exists {
                    return Err(Error::ProviderNotFound);
                }
                let provider = crate::change::load_for_change(
                    &mut transaction,
                    &owner,
                    request.provider_id(),
                    crate::change::ProviderChange::Regenerate,
                )
                .await?;
                if provider.provider.state() != ProviderState::Active {
                    return Err(Error::ProviderNotFound);
                }
                // The terminal transaction checks the etag after settling an
                // irreversibly stale account decision and its provisional material.
                Command::Regenerate {
                    request,
                    regeneration: RegenerationBaseline {
                        precondition: regeneration,
                        fingerprint: coral_catalog::generation_fingerprint_in_transaction(
                            &mut transaction,
                            &provider.generation_id,
                        )
                        .await?,
                        operation_paths: coral_catalog::operation_paths_in_transaction(
                            &mut transaction,
                            &provider.generation_id,
                        )
                        .await?,
                        display_name: provider.provider.display_name_metadata().clone(),
                        generation_id: provider.generation_id,
                    },
                }
            }
            Command::Workspace(request) => Command::Workspace(request),
        };
        // Before anything reads the source on another User's behalf.
        let loaded = crate::started_setup(
            &mut transaction,
            context.actor(),
            &owner,
            command.setup_id(),
        )
        .await?;
        require_ready(&loaded.setup)?;
        if let Command::Regenerate { request, .. } = &command {
            validate_regeneration_setup(request, &loaded)?;
        }
        let setup_input = loaded.pinned_input()?.clone();
        transaction.rollback().await?;
        Ok(Preflight::Materialize(PinnedPublication {
            setup_input,
            command,
        }))
    }

    async fn lock_publication_target(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        command: &Command<RegenerationBaseline>,
    ) -> Result<TerminalLock, Error> {
        match command {
            Command::Workspace(request) => {
                let evidence = self
                    .workspaces
                    .check_configuration_in_transaction(
                        transaction,
                        context.actor(),
                        request.workspace_id(),
                    )
                    .await?;
                Ok(TerminalLock::Creation(evidence.authority_source()))
            }

            Command::Regenerate { request, .. } => Ok(TerminalLock::Regeneration {
                provider_exists: self
                    .lock_regeneration_in_transaction(transaction, context.actor(), request)
                    .await?,
            }),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one owning transaction commits target state, consumption, and evidence"
    )]
    async fn write_terminal(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        pinned: PinnedPublication,
        materialized: MaterializationOutcome,
        started: std::time::Instant,
    ) -> Result<(Terminal, PublicationEvidence), Error> {
        let PinnedPublication {
            setup_input,
            command,
        } = pinned;
        let owner = command.owner();
        let setup_id = command.setup_id().clone();
        let locked = self
            .lock_publication_target(transaction, context, &command)
            .await?;
        // The locks above are same-value holds; the caller may commit a replay.
        if let Some(terminal) = exact_replay(transaction, context.actor(), &command).await? {
            return Ok((terminal, PublicationEvidence::Replayed));
        }
        let authority = match locked {
            TerminalLock::Creation(authority) => authority,
            TerminalLock::Regeneration { provider_exists } => {
                let authority = self
                    .check_owner_configuration_in_transaction(transaction, context.actor(), &owner)
                    .await?;
                if !provider_exists {
                    return Err(Error::ProviderNotFound);
                }
                authority
            }
        };
        lock_setup(transaction, &owner, &setup_id).await?;
        let current_setup =
            crate::started_setup(&mut *transaction, context.actor(), &owner, &setup_id).await?;
        require_ready(&current_setup.setup)?;
        if current_setup.setup_input.as_ref() != Some(&setup_input) {
            return Err(Error::EtagMismatch);
        }
        // A Setup that reached its deadline publishes nothing, ready or not.
        let materialized = if credential_decision_context(context, started).occurred_at_unix_nanos()
            >= setup_credentials::setup_deadline(current_setup.setup.created_at_unix_nanos())?
        {
            MaterializationOutcome::Rejected(PublicationDiagnostic::stable(
                "setup_expired",
                "the Setup expired before Publication",
            ))
        } else {
            materialized
        };
        let terminal_context = TerminalContext {
            context,
            started,
            current_setup: &current_setup,
            setup_input: &setup_input,
            authority,
        };
        let applied = match command {
            Command::Workspace(request) => {
                self.apply_workspace_creation(transaction, &terminal_context, request, materialized)
                    .await?
            }

            Command::Regenerate {
                request,
                regeneration: baseline,
            } => {
                validate_regeneration_setup(&request, &current_setup)?;
                let current_provider = provider_repository::provider_in_transaction(
                    transaction,
                    &owner,
                    request.provider_id(),
                )
                .await?
                .filter(|stored| stored.provider.state() == ProviderState::Active)
                .ok_or(Error::ProviderNotFound)?;
                let access = current_provider.provider.access();
                let destructive = request.account_grant().is_some()
                    || request
                        .account_setting()
                        .is_some_and(|route| route != access.credential_route());
                let materialized = match crate::account::update::check(
                    transaction,
                    request.owner(),
                    request.setup_id(),
                    access,
                    destructive,
                )
                .await
                {
                    Err(Error::ProviderChangedDuringPublication) => {
                        MaterializationOutcome::Rejected(PublicationDiagnostic::stable(
                            "update_account_changed",
                            "the reviewed account setting or authorization changed; start a new Update",
                        ))
                    }
                    Err(error) => return Err(error),
                    Ok(()) => {
                        check_etag(
                            baseline.precondition.expected_etag.as_deref(),
                            &current_provider.provider.etag(),
                        )?;
                        if current_provider.generation_id != baseline.generation_id
                            || current_provider.provider.display_name_metadata()
                                != &baseline.display_name
                        {
                            return Err(Error::ProviderChangedDuringPublication);
                        }
                        materialized
                    }
                };
                self.apply_regeneration(
                    transaction,
                    &terminal_context,
                    request,
                    baseline,
                    materialized,
                )
                .await?
            }
        };
        // Source provenance follows every successful Setup, including a publication
        // that reuses the canonical Generation. Failed publication retains it.
        let source_target = match &applied.terminal {
            Terminal::Workspace(result) => result.published_provider().map(Provider::id),
            Terminal::Regeneration(result)
                if result.outcome() == ProviderPublicationOutcome::Succeeded =>
            {
                Some(result.request().provider_id())
            }
            Terminal::Regeneration(_) => None,
        };
        let mut changed_field_paths = applied.changed_field_paths.to_vec();
        if let Some(provider_id) = source_target {
            let current =
                provider_repository::provider_in_transaction(transaction, &owner, provider_id)
                    .await?
                    .ok_or(Error::ProviderNotFound)?;
            if current.source_url.as_deref() != current_setup.setup.input_url() {
                changed_field_paths.push("provider.source_url");
                // A source-only change still changes the next refresh target.
                // Fence other reviewed updates even when the catalog is identical.
                if matches!(&applied.terminal, Terminal::Regeneration(_)) {
                    provider_repository::advance_provider_version(
                        transaction,
                        &owner,
                        provider_id,
                        current.provider.version(),
                    )
                    .await?;
                }
            }
            provider_repository::update_source_url(
                transaction,
                &owner,
                provider_id,
                current_setup.setup.input_url(),
            )
            .await?;
        }
        repository::insert_terminal(transaction, context.actor().user_id(), &applied.terminal)
            .await
            .map_err(request_conflict)?;
        if !provider_repository::consume_setup(
            transaction,
            &owner,
            &setup_id,
            applied.terminal.id(),
        )
        .await?
        {
            return Err(Error::ProviderSetupConsumed);
        }
        // Every terminal result discards remaining provisional grants after promotion.
        crate::account::setup_repository::scrub(transaction, &owner, &setup_id).await?;
        crate::account::oauth_client_secret_repository::delete(
            transaction,
            &owner,
            &crate::account::oauth_client_secret_repository::Target::Setup(&setup_id),
        )
        .await?;
        let target_id = applied.terminal.id().to_string();
        provider_repository::insert_lifecycle_event(
            transaction,
            provider_repository::NewLifecycleEvent {
                workspace_id: Some(&owner),
                actor: LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: authority,
                },
                action: LifecycleAction::PublicationCompleted,
                target_id: &target_id,
                outcome: match applied.terminal.outcome() {
                    ProviderPublicationOutcome::Succeeded => LifecycleOutcome::Succeeded,
                    ProviderPublicationOutcome::Failed => LifecycleOutcome::Failed,
                },
                changed_field_paths: &changed_field_paths,
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok((applied.terminal, applied.evidence))
    }
}

struct TerminalContext<'a> {
    context: &'a ChangeContext,
    started: std::time::Instant,
    current_setup: &'a provider_repository::LoadedSetup,
    setup_input: &'a ProviderSetupInput,
    authority: AuthoritySource,
}

impl TerminalContext<'_> {
    async fn publish_new_provider(
        &self,
        transaction: &mut Transaction<'_>,
        access: crate::ProviderAccess,
        owner: WorkspaceId,
        prepared: coral_catalog::PreparedProviderCatalog,
        promotion: &setup_credentials::Promotion,
    ) -> Result<(Provider, coral_catalog::ProviderGenerationId), Error> {
        let prepared = with_setup_client(prepared, promotion)?;
        crate::account::oauth_client::require_device_client(&prepared.credential_contract())?;
        crate::account::oauth_client::require_configured(
            transaction,
            &owner,
            self.current_setup.setup.id(),
            &prepared.credential_contract(),
        )
        .await?;
        let provider = Provider::new(
            access,
            ProviderDisplayName::parse(prepared.display_name())?,
            ProviderState::Active,
        );
        let generation = coral_catalog::publish_generation_in_transaction(
            transaction,
            self.setup_input,
            self.current_setup
                .setup
                .input_digest()
                .ok_or(Error::ProviderSetupNotReady)?,
            prepared,
            self.context.occurred_at_unix_nanos(),
        )
        .await?;
        provider_repository::insert_provider(
            transaction,
            &provider,
            &generation,
            self.context.occurred_at_unix_nanos(),
        )
        .await?;
        crate::account::oauth_client_secret_repository::promote(
            transaction,
            provider.owner(),
            self.current_setup.setup.id(),
            provider.id(),
        )
        .await?;
        Ok((provider, generation))
    }
}

// Retain the accepted change's actor, event timestamp and replay evidence separately
// from credential freshness. Materialization and lock waits consume grant lifetime.
fn credential_decision_context(
    context: &ChangeContext,
    started: std::time::Instant,
) -> ChangeContext {
    let elapsed = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);
    ChangeContext::new(
        context.actor().clone(),
        context.occurred_at_unix_nanos().saturating_add(elapsed),
        context.trace_id().clone(),
    )
}

async fn lock_setup(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
) -> Result<(), Error> {
    if provider_repository::lock_setup_for_publication(transaction, owner, setup_id).await? {
        Ok(())
    } else {
        Err(Error::ProviderSetupNotFound)
    }
}

fn validate_regeneration_setup(
    request: &ProviderRegenerationRequest,
    loaded: &provider_repository::LoadedSetup,
) -> Result<(), Error> {
    if loaded.configuration_revision != request.configuration_revision() {
        return Err(Error::EtagMismatch);
    }
    if loaded
        .update_provider_id
        .as_ref()
        .is_some_and(|id| id != request.provider_id())
    {
        return Err(Error::ProviderSetupNotApplicable);
    }
    Ok(())
}

fn request_conflict(error: Error) -> Error {
    match error {
        Error::Database(error) if error.is_unique_violation() => Error::RequestIdConflict,
        other => other,
    }
}

async fn exact_replay<R>(
    transaction: &mut Transaction<'_>,
    actor: &AuthenticatedUser,
    command: &Command<R>,
) -> Result<Option<Terminal>, Error> {
    let stored = repository::terminal_for_command(transaction, command).await?;
    let Some(terminal) = stored else {
        return Ok(None);
    };
    let initiator = repository::publication_request_actor_in_transaction(
        transaction,
        &command.owner(),
        command.request_id(),
    )
    .await?
    .ok_or_else(|| Error::CorruptData("Publication has no request identity".into()))?;
    if &initiator != actor.user_id() {
        return Err(Error::RequestIdConflict);
    }
    let exact = match (command, &terminal) {
        (Command::Workspace(request), Terminal::Workspace(result)) => request == result.request(),
        (Command::Regenerate { request, .. }, Terminal::Regeneration(result)) => {
            request == result.request()
        }
        _ => false,
    };
    if !exact {
        return Err(Error::RequestIdConflict);
    }
    Ok(Some(terminal))
}

/// Where a test may hold a Publication. Holding only synchronizes timing: a
/// test injects a crash or a lost response by canceling the held request,
/// which is the path production takes when a caller goes away.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationPoint {
    /// A regeneration's first replay read found no terminal result.
    RegenerationReplayMissed,
    /// Materialization finished; the terminal transaction has not begun.
    TerminalAttempt,
    /// The terminal writes are staged; the transaction has not committed.
    TerminalCommit,
    /// The terminal transaction committed; the caller has no response yet.
    Committed,
}

#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait PublicationObserver: Send + Sync {
    async fn reached(&self, point: PublicationPoint);
}

#[cfg(test)]
pub(crate) struct NoopPublicationObserver;

#[cfg(test)]
#[async_trait::async_trait]
impl PublicationObserver for NoopPublicationObserver {
    async fn reached(&self, _point: PublicationPoint) {}
}

impl ProviderModule {
    /// Prepares one Workspace-owned Provider creation outside any transaction.
    ///
    /// Runs the preflight and the replay check, then materializes the pinned
    /// Setup. An exact replay of a committed `request_id` returns the stored
    /// result and materializes nothing. Reuse with different request content
    /// fails safely.
    ///
    /// # Errors
    ///
    /// Returns authority, Setup, request-conflict, operational, or persistence
    /// failures. None of them changes state.
    pub async fn prepare_publication(
        &self,
        context: &ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<PreparedPublication<ProviderPublication>, Error> {
        self.prepare_command(
            context,
            Command::Workspace(request),
            Terminal::into_workspace,
        )
        .await
    }

    /// Prepares one replacement Provider Generation outside any transaction.
    ///
    /// The active Generation stays available while the exact pinned Setup is
    /// materialized. The context's etag is the regeneration's precondition.
    /// An exact replay returns the committed result.
    ///
    /// # Errors
    ///
    /// Requires current owner authority and an active exact Provider. A failure
    /// leaves the Setup ready for retry.
    pub async fn prepare_regeneration(
        &self,
        context: &ChangeContext,
        request: ProviderRegenerationRequest,
    ) -> Result<PreparedPublication<ProviderRegenerationPublication>, Error> {
        self.prepare_command(
            context,
            Command::Regenerate {
                request,
                regeneration: RegenerationPrecondition {
                    expected_etag: context.expected_etag().map(str::to_owned),
                },
            },
            Terminal::into_regeneration,
        )
        .await
    }

    /// Writes one prepared publication's terminal result in the caller's transaction.
    ///
    /// Rechecks authority and invariants, checks replay again, then writes the
    /// terminal result, consumes the Setup, and records lifecycle evidence. A
    /// safe structural or domain failure is a terminal result whose outcome is
    /// `Failed`. Commit after every success; the evidence says what the commit
    /// changes.
    ///
    /// # Errors
    ///
    /// Returns authority, Setup, request-conflict, current-state race, or
    /// persistence failures. Some come after earlier writes: never commit the
    /// transaction after an error.
    pub async fn publish_in_transaction<T>(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        publication: MaterializedPublication<T>,
    ) -> Result<Published<T>, Error> {
        let MaterializedPublication {
            pinned,
            materialized,
            started,
            result,
        } = publication;
        let (terminal, evidence) = self
            .write_terminal(transaction, context, pinned, materialized, started)
            .await?;
        Ok(Published {
            publication: result(terminal)?,
            evidence,
        })
    }
}

/// Provider-rule tests publish without the Provider workflow and its notifications.
#[cfg(any(test, feature = "test-fixtures"))]
impl ProviderModule {
    /// Prepares, writes, and commits one Workspace-owned Provider creation.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::prepare_publication`] and
    /// [`Self::publish_in_transaction`].
    pub async fn publish_workspace_provider(
        &self,
        context: &ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<ProviderPublication, Error> {
        match Box::pin(self.prepare_publication(context, request)).await? {
            PreparedPublication::Replayed(publication) => Ok(publication),
            PreparedPublication::Materialized(publication) => {
                Box::pin(self.commit_materialized(context, *publication)).await
            }
        }
    }

    /// Prepares, writes, and commits one Provider regeneration.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::prepare_regeneration`] and
    /// [`Self::publish_in_transaction`].
    pub async fn regenerate_provider(
        &self,
        context: &ChangeContext,
        request: ProviderRegenerationRequest,
    ) -> Result<ProviderRegenerationPublication, Error> {
        match Box::pin(self.prepare_regeneration(context, request)).await? {
            PreparedPublication::Replayed(publication) => Ok(publication),
            PreparedPublication::Materialized(publication) => {
                Box::pin(self.commit_materialized(context, *publication)).await
            }
        }
    }

    async fn commit_materialized<T>(
        &self,
        context: &ChangeContext,
        publication: MaterializedPublication<T>,
    ) -> Result<T, Error> {
        #[cfg(test)]
        self.publication_observer
            .reached(PublicationPoint::TerminalAttempt)
            .await;
        let mut transaction = self.database.begin().await?;
        let published = self
            .publish_in_transaction(&mut transaction, context, publication)
            .await?;
        #[cfg(test)]
        let replayed = published.evidence == PublicationEvidence::Replayed;
        #[cfg(test)]
        if !replayed {
            self.publication_observer
                .reached(PublicationPoint::TerminalCommit)
                .await;
        }
        transaction.commit().await?;
        #[cfg(test)]
        if !replayed {
            self.publication_observer
                .reached(PublicationPoint::Committed)
                .await;
        }
        Ok(published.publication)
    }
}

impl ProviderModule {
    /// Inspects one terminal Provider regeneration Publication.
    ///
    /// # Errors
    ///
    /// Requires current owner inspection authority. An absent Publication or an
    /// owner mismatch returns [`Error::ProviderPublicationNotFound`].
    pub async fn get_regeneration_publication(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        request_id: &ManagementRequestId,
    ) -> Result<ProviderRegenerationPublication, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut transaction, actor, owner)
            .await?;
        let publication = repository::regeneration_publication_by_request_in_transaction(
            &mut transaction,
            owner,
            request_id,
        )
        .await?
        .filter(|publication| publication.request().owner() == owner)
        .ok_or(Error::ProviderPublicationNotFound)?;
        transaction.rollback().await?;
        Ok(publication)
    }

    /// Inspects one terminal Publication by management request identity.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority. An absent Publication
    /// or one from another Workspace returns
    /// [`Error::ProviderPublicationNotFound`].
    pub async fn get_publication(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        request_id: &ManagementRequestId,
    ) -> Result<ProviderPublication, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(
                &mut transaction,
                actor,
                workspace_id,
                WorkspaceAction::Inspect,
            )
            .await?;
        require_owner(&evidence)?;
        let publication = repository::publication_by_request_in_transaction(
            &mut transaction,
            workspace_id,
            request_id,
        )
        .await?
        .ok_or(Error::ProviderPublicationNotFound)?;
        if publication.request().workspace_id() != workspace_id {
            return Err(Error::ProviderPublicationNotFound);
        }
        transaction.rollback().await?;
        Ok(publication)
    }

    // Acquire write locks before any reads so later reads see the state they protect.
    // Administration allows archived Workspaces, and an absent Provider is only
    // rejected after checking replay: neither state invalidates a committed result.
    async fn lock_regeneration_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        request: &ProviderRegenerationRequest,
    ) -> Result<bool, Error> {
        let workspace_id = request.owner();
        {
            self.workspaces
                .check_administration_in_transaction(transaction, actor, workspace_id)
                .await?;
        }
        provider_repository::hold_provider_in_transaction(
            transaction,
            request.owner(),
            request.provider_id(),
        )
        .await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Provider configuration and credential promotion remain one explicit creation decision"
    )]
    async fn apply_workspace_creation(
        &self,
        transaction: &mut Transaction<'_>,
        terminal: &TerminalContext<'_>,
        request: WorkspaceProviderPublicationRequest,
        materialized: MaterializationOutcome,
    ) -> Result<AppliedPublication, Error> {
        let TerminalContext {
            context, authority, ..
        } = *terminal;
        let provider_id = ProviderId::generate();
        let prepared = match access::prepare(
            transaction,
            request.workspace_id(),
            provider_id.clone(),
            request.call_root(),
            request.credential_route(),
        )
        .await?
        {
            access::PreparedProviderAccess::Ready(provider_access) => match materialized {
                MaterializationOutcome::Ready(prepared) => {
                    let contract = prepared.credential_contract();
                    match setup_credentials::decide(
                        transaction,
                        &credential_decision_context(context, terminal.started),
                        &request.workspace_id().clone(),
                        request.setup_id(),
                        request.credential_route(),
                        request.account_grant(),
                        &contract,
                    )
                    .await
                    {
                        Ok(promotion) => Ok((prepared, provider_access, promotion)),
                        Err(diagnostic) => Err(diagnostic),
                    }
                }
                MaterializationOutcome::Rejected(diagnostic) => Err(diagnostic),
            },
            access::PreparedProviderAccess::CallRootConflict => Err(PublicationDiagnostic::stable(
                types::NAMESPACE_CONFLICT,
                "the requested Provider namespace is already in use",
            )),
        };
        let publication_id = ProviderPublicationId::generate();
        let (publication, evidence) = match prepared {
            Ok((prepared, provider_access, promotion)) => {
                let (provider, generation) = terminal
                    .publish_new_provider(
                        transaction,
                        provider_access,
                        request.workspace_id().clone(),
                        prepared,
                        &promotion,
                    )
                    .await?;
                if let setup_credentials::Promotion::Promote { row, .. } = promotion {
                    setup_credentials::promote(
                        transaction,
                        context,
                        setup_credentials::PromotionTarget {
                            owner: &request.workspace_id().clone(),
                            provider: provider.id(),
                            authority,
                            route: request.credential_route(),
                        },
                        &row,
                    )
                    .await?;
                }
                let evidence = PublicationEvidence::Activated(ActivatedGeneration::new(
                    request.workspace_id().clone(),
                    provider.id().clone(),
                    generation,
                    None,
                ));
                (
                    ProviderPublication::new(
                        publication_id,
                        request,
                        ProviderPublicationOutcome::Succeeded,
                        None,
                        Some(provider),
                        context.occurred_at_unix_nanos(),
                    ),
                    evidence,
                )
            }
            Err(diagnostic) => (
                ProviderPublication::new(
                    publication_id,
                    request,
                    ProviderPublicationOutcome::Failed,
                    Some(diagnostic),
                    None,
                    context.occurred_at_unix_nanos(),
                ),
                PublicationEvidence::Failed,
            ),
        };

        let changed_field_paths = if publication.outcome() == ProviderPublicationOutcome::Succeeded
        {
            &[
                "setup.state",
                "provider",
                "active_generation",
                "catalog",
                "provider_access",
            ]
        } else {
            TERMINAL_ONLY_FIELDS
        };
        Ok(AppliedPublication {
            terminal: Terminal::Workspace(publication),
            changed_field_paths,
            evidence,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one locked publication atomically activates the contract and invalidates obsolete authorization"
    )]
    async fn apply_regeneration(
        &self,
        transaction: &mut Transaction<'_>,
        terminal: &TerminalContext<'_>,
        request: ProviderRegenerationRequest,
        baseline: RegenerationBaseline,
        materialized: MaterializationOutcome,
    ) -> Result<AppliedPublication, Error> {
        let TerminalContext {
            context,
            current_setup,
            setup_input,
            ..
        } = *terminal;
        let reuse_active_generation = match &materialized {
            MaterializationOutcome::Ready(prepared) => {
                prepared.display_name() == baseline.display_name.as_str()
                    && prepared.fingerprint() == baseline.fingerprint
            }
            MaterializationOutcome::Rejected(_) => false,
        };
        let publication_id = ProviderPublicationId::generate();
        let (publication, activated) = match materialized {
            MaterializationOutcome::Ready(prepared) => {
                let prepared =
                    with_active_client(transaction, &baseline.generation_id, prepared).await?;
                let active_contract = coral_catalog::credential_contract_in_transaction(
                    transaction,
                    &baseline.generation_id,
                )
                .await?;
                if prepared.credential_contract() != active_contract
                    && !request.allows_incompatible_configuration()
                {
                    return Err(Error::IncompatibleConfigurationConfirmationRequired);
                }
                let replacement_contract = prepared.credential_contract();
                let access = crate::access::exact_in_transaction(
                    transaction,
                    request.owner(),
                    request.provider_id(),
                )
                .await?
                .ok_or(Error::ProviderNotFound)?;
                let route = request
                    .account_setting()
                    .unwrap_or(access.credential_route());
                let setting_changed = route != access.credential_route();
                if setting_changed && !request.allows_incompatible_configuration() {
                    return Err(Error::IncompatibleConfigurationConfirmationRequired);
                }
                if route == crate::CredentialRoute::None
                    && replacement_contract != coral_catalog::ProviderCredentialContract::NONE
                {
                    return Err(Error::AccountRequired);
                }
                let promotion = if request.account_grant().is_some() {
                    if route != crate::CredentialRoute::Fixed {
                        return Err(Error::AccountInput);
                    }
                    match setup_credentials::decide(
                        transaction,
                        &credential_decision_context(context, terminal.started),
                        request.owner(),
                        request.setup_id(),
                        route,
                        request.account_grant(),
                        &replacement_contract,
                    )
                    .await
                    {
                        Ok(promotion) => Some(promotion),
                        Err(diagnostic) => {
                            return Ok(AppliedPublication::failed_regeneration(
                                publication_id,
                                request,
                                diagnostic,
                                context.occurred_at_unix_nanos(),
                            ));
                        }
                    }
                } else {
                    if route == crate::CredentialRoute::Fixed
                        && (setting_changed || replacement_contract != active_contract)
                    {
                        return Err(Error::AccountRequired);
                    }
                    None
                };
                crate::account::oauth_client::require_device_client(&replacement_contract)?;
                crate::account::oauth_client::require_configured(
                    transaction,
                    request.owner(),
                    request.setup_id(),
                    &replacement_contract,
                )
                .await?;
                let display_name = ProviderDisplayName::parse(prepared.display_name())?;
                let activated = if reuse_active_generation {
                    None
                } else {
                    let generation_id = coral_catalog::publish_generation_in_transaction(
                        transaction,
                        setup_input,
                        current_setup
                            .setup
                            .input_digest()
                            .ok_or(Error::ProviderSetupNotReady)?,
                        prepared,
                        context.occurred_at_unix_nanos(),
                    )
                    .await?;
                    if !provider_repository::activate_generation(
                        transaction,
                        request.owner(),
                        request.provider_id(),
                        &baseline.generation_id,
                        &generation_id,
                        &display_name,
                    )
                    .await?
                    {
                        return Err(Error::ProviderChangedDuringPublication);
                    }
                    Some(ActivatedGeneration::new(
                        request.owner().clone(),
                        request.provider_id().clone(),
                        generation_id,
                        Some(baseline.generation_id.clone()),
                    ))
                };
                if setting_changed {
                    crate::account::update::retire_old_setting(
                        transaction,
                        context,
                        &access,
                        terminal.authority,
                    )
                    .await?;
                    access::update_credential_route(transaction, &access, route).await?;
                    let current = provider_repository::provider_in_transaction(
                        transaction,
                        request.owner(),
                        request.provider_id(),
                    )
                    .await?
                    .ok_or(Error::ProviderNotFound)?;
                    provider_repository::advance_provider_version(
                        transaction,
                        request.owner(),
                        request.provider_id(),
                        current.provider.version(),
                    )
                    .await?;
                    provider_repository::insert_lifecycle_event(
                        transaction,
                        provider_repository::NewLifecycleEvent {
                            workspace_id: Some(request.owner()),
                            actor: LifecycleActor::User {
                                user_id: context.actor().user_id().clone(),
                                authority_source: terminal.authority,
                            },
                            action: LifecycleAction::ProviderCredentialRouteChanged,
                            target_id: &request.provider_id().to_string(),
                            outcome: LifecycleOutcome::Accepted,
                            changed_field_paths: &["credential_route", "account_setting_version"],
                            occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                            trace_id: context.trace_id(),
                        },
                    )
                    .await?;
                }
                if let Some(setup_credentials::Promotion::Promote { row, .. }) = promotion {
                    setup_credentials::replace_shared(
                        transaction,
                        context,
                        setup_credentials::PromotionTarget {
                            owner: request.owner(),
                            provider: request.provider_id(),
                            authority: terminal.authority,
                            route,
                        },
                        &row,
                    )
                    .await?;
                }
                if replacement_contract != active_contract {
                    crate::account::cancel_obsolete_authorizations(
                        transaction,
                        context,
                        request.owner(),
                        request.provider_id(),
                        terminal.authority,
                    )
                    .await?;
                }
                if replacement_contract.oauth_client().is_none_or(|policy| {
                    policy.authentication == coral_catalog::OAuthClientAuthenticationMethod::None
                }) {
                    crate::account::oauth_client_secret_repository::delete(
                        transaction,
                        request.owner(),
                        &crate::account::oauth_client_secret_repository::Target::Provider(
                            request.provider_id(),
                        ),
                    )
                    .await?;
                }
                if crate::account::oauth_client_secret_repository::promote(
                    transaction,
                    request.owner(),
                    request.setup_id(),
                    request.provider_id(),
                )
                .await?
                    || replacement_contract != active_contract
                {
                    provider_repository::set_oauth_client_failure(
                        transaction,
                        request.owner(),
                        request.provider_id(),
                        None,
                    )
                    .await?;
                }
                (
                    ProviderRegenerationPublication::new(
                        publication_id,
                        request,
                        ProviderPublicationOutcome::Succeeded,
                        None,
                        context.occurred_at_unix_nanos(),
                    ),
                    activated,
                )
            }
            MaterializationOutcome::Rejected(diagnostic) => {
                return Ok(AppliedPublication::failed_regeneration(
                    publication_id,
                    request,
                    diagnostic,
                    context.occurred_at_unix_nanos(),
                ));
            }
        };

        let changed_field_paths = if activated.is_some() {
            &[
                "setup.state",
                "provider.display_name",
                "provider.active_generation",
                "catalog",
                "search_material",
            ]
        } else {
            TERMINAL_ONLY_FIELDS
        };
        Ok(AppliedPublication {
            terminal: Terminal::Regeneration(publication),
            changed_field_paths,
            evidence: activated.map_or(
                PublicationEvidence::ReusedActiveGeneration,
                PublicationEvidence::Activated,
            ),
        })
    }
}

/// Stamps the discovery application's policy independently of account retention.
/// Discarding an account grant must not discard its Provider application settings.
fn with_setup_client(
    prepared: coral_catalog::PreparedProviderCatalog,
    promotion: &setup_credentials::Promotion,
) -> Result<coral_catalog::PreparedProviderCatalog, Error> {
    match promotion {
        setup_credentials::Promotion::Promote {
            client: Some(policy),
            ..
        }
        | setup_credentials::Promotion::Discard {
            client: Some(policy),
        } => Ok(prepared.with_oauth_client(policy)?),
        setup_credentials::Promotion::Promote { client: None, .. }
        | setup_credentials::Promotion::Discard { client: None }
        | setup_credentials::Promotion::NotRequired => Ok(prepared),
    }
}

/// Regeneration keeps the client ownership the active Generation promoted: an
/// OpenAPI document declares its OAuth flow but never the client, so a
/// replacement whose contract differs only by that client stays compatible.
async fn with_active_client(
    transaction: &mut coral_db::Transaction<'_>,
    active_generation: &coral_catalog::ProviderGenerationId,
    prepared: coral_catalog::PreparedProviderCatalog,
) -> Result<coral_catalog::PreparedProviderCatalog, Error> {
    let replacement = prepared.credential_contract();
    if replacement.oauth_client().is_some()
        || !matches!(
            replacement.authentication(),
            AuthenticationConfiguration::OAuthAuthorizationCode { .. }
        )
    {
        return Ok(prepared);
    }
    let active =
        coral_catalog::credential_contract_in_transaction(transaction, active_generation).await?;
    match active.oauth_client() {
        Some(policy) => Ok(prepared.with_oauth_client(policy)?),
        _ => Ok(prepared),
    }
}
