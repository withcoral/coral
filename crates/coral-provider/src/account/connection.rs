//! Durable User connection requests, independent of Run lifetimes. Invocation
//! owns waiter attachment and detachment in its transaction; an empty request
//! stays open until its host deadline. Every first terminal transition commits
//! User-private lifecycle evidence with its state. Automatic transitions belong
//! to Coral, not the User. None of these changes creates a Run Event.
use coral_workspace::WorkspaceId;

use super::{AccountModule, CredentialEvidence, caller, repository, select_in_transaction};
use crate::access::CredentialRoute;
use crate::{
    Error, LifecycleAction, LifecycleOutcome, ProviderId, repository as provider_repository,
};
use coral_catalog::ProviderCredentialContract;
use coral_db::Transaction;
use coral_workspace::{AuthoritySource, ChangeContext, Principal, PrincipalKind, UserId};
use uuid::Uuid;

/// How long a User has to complete a connection request before it expires.
const CONNECTION_REQUEST_LIFETIME_NANOS: i64 = 900_000_000_000;

account_id!(
    ConnectionRequestId,
    "Opaque, non-authorizing Account Connection Request identifier."
);

/// Durable terminal decision or pending User action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Waiting for the invoking User, independently of any one Run.
    Pending,
    /// The exact requested Account connected.
    Connected,
    /// The User declined.
    Declined,
    /// The host deadline elapsed.
    Expired,
    /// The retained Provider setting changed or disappeared.
    RouteChanged,
    /// Another Account authorization won before connection completed.
    AuthorizationChanged,
}
/// User-private progress. No credentials or account metadata appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionRequest {
    /// Non-authorizing request identifier.
    pub id: ConnectionRequestId,
    /// Exact Provider; route changes never retarget it.
    pub provider_id: ProviderId,
    /// Current durable state.
    pub state: ConnectionState,
    /// Independent host deadline in Unix nanoseconds.
    pub expires_at_unix_nanos: i64,
}

pub(super) struct LoadedConnection {
    pub(super) id: ConnectionRequestId,
    pub(super) provider_account_setting_version: i64,
    pub(super) provider_id: ProviderId,
    pub(super) contract: ProviderCredentialContract,
    pub(super) account_id: super::ProviderAccountId,
    pub(super) account_authorization_version: String,
    pub(super) resolved_account_authorization_version: Option<String>,
    pub(super) state: ConnectionState,
    pub(super) expires_at_unix_nanos: i64,
}
impl LoadedConnection {
    fn view(&self) -> ConnectionRequest {
        ConnectionRequest {
            id: self.id,
            provider_id: self.provider_id.clone(),
            state: self.state,
            expires_at_unix_nanos: self.expires_at_unix_nanos,
        }
    }
}

/// Safe availability of one retained User-private request, independent of live waiters.
/// The caller must first authorize this User's current Workspace access in the same snapshot.
/// Request deadlines end actionability, not retained inspection. Only absent content is unavailable.
/// Does not refresh requests, expose Account evidence, or discover replacements.
/// # Errors
/// Returns persistence/corruption failures; absent and other-User requests return false.
pub async fn retained_connection_available_in_transaction(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    request: ConnectionRequestId,
) -> Result<bool, Error> {
    repository::connection_history_exists(tx, workspace, user, request).await
}

impl AccountModule {
    /// Lists the invoking User's pending connection workflows, including ones with no waiters.
    /// # Errors
    /// Requires current Workspace access; no Owner can list another User's requests.
    pub async fn list_connection_requests(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
    ) -> Result<Vec<ConnectionRequest>, Error> {
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        self.scope_authority(&mut tx, actor, &workspace.clone())
            .await?;
        let requests = pending_for_user(
            &mut tx,
            workspace,
            actor.user_id(),
            context.occurred_at_unix_nanos(),
        )
        .await?;
        tx.commit().await?;
        Ok(requests)
    }

    /// Reads one request as its invoking User and persists expiry/configuration invalidation.
    /// # Errors
    /// Another User's request is indistinguishable from an absent request.
    pub async fn get_connection_request(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        id: ConnectionRequestId,
    ) -> Result<ConnectionRequest, Error> {
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        self.scope_authority(&mut tx, actor, &workspace.clone())
            .await?;
        let mut row = repository::load_connection(&mut tx, workspace, actor.user_id(), id).await?;
        refresh(
            &mut tx,
            workspace,
            actor.user_id(),
            &mut row,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        tx.commit().await?;
        Ok(row.view())
    }

    /// Declines the shared request; a terminal decision cannot be changed.
    /// # Errors
    /// Only the invoking User with current Workspace access may decide.
    pub async fn decline_connection(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        id: ConnectionRequestId,
    ) -> Result<ConnectionRequest, Error> {
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .scope_authority(&mut tx, actor, &workspace.clone())
            .await?;
        let mut row = repository::load_connection(&mut tx, workspace, actor.user_id(), id).await?;
        refresh(
            &mut tx,
            workspace,
            actor.user_id(),
            &mut row,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        if row.state == ConnectionState::Pending {
            end(
                &mut tx,
                workspace,
                actor.user_id(),
                &mut row,
                ConnectionCompletion::Declined,
                context.occurred_at_unix_nanos(),
                Some((context, authority)),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(row.view())
    }
}

/// Settles and returns one User's still-pending requests. The caller must
/// first authorize this User's current Workspace access in the same transaction.
pub(super) async fn pending_for_user(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    now: i64,
) -> Result<Vec<ConnectionRequest>, Error> {
    let mut requests = Vec::new();
    for id in repository::pending_connections(tx, workspace, user).await? {
        let mut row = repository::load_connection(tx, workspace, user, id).await?;
        refresh(tx, workspace, user, &mut row, now).await?;
        if row.state == ConnectionState::Pending {
            requests.push(row.view());
        }
    }
    Ok(requests)
}

// Completion is narrower than the read model: Pending cannot be persisted as
// a terminal decision, and a connected request must carry its exact evidence.
enum ConnectionCompletion {
    Connected {
        account_authorization_version: String,
    },
    Declined,
    Expired,
    RouteChanged,
    AuthorizationChanged,
}

async fn end(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    row: &mut LoadedConnection,
    completion: ConnectionCompletion,
    now: i64,
    decision: Option<(&ChangeContext, AuthoritySource)>,
) -> Result<(), Error> {
    let (state, account_authorization_version) = match completion {
        ConnectionCompletion::Connected {
            account_authorization_version,
        } => (
            ConnectionState::Connected,
            Some(account_authorization_version),
        ),
        ConnectionCompletion::Declined => (ConnectionState::Declined, None),
        ConnectionCompletion::Expired => (ConnectionState::Expired, None),
        ConnectionCompletion::RouteChanged => (ConnectionState::RouteChanged, None),
        ConnectionCompletion::AuthorizationChanged => (ConnectionState::AuthorizationChanged, None),
    };
    let changed = repository::end_connection(
        tx,
        workspace,
        user,
        row,
        state,
        account_authorization_version.as_deref(),
    )
    .await?;
    if changed != 1 {
        return Err(Error::AccountInput);
    }
    let trace = decision.map_or_else(coral_telemetry::TraceId::current_or_new, |(context, _)| {
        context.trace_id().clone()
    });
    provider_repository::insert_scoped_lifecycle_event(
        tx,
        provider_repository::NewLifecycleEvent {
            workspace_id: Some(workspace),
            actor: decision.map_or(
                crate::LifecycleActor::Coral,
                |(context, authority_source)| crate::LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source,
                },
            ),
            action: LifecycleAction::AccountConnectionCompleted,
            target_id: &row.id.to_string(),
            outcome: match state {
                ConnectionState::Connected => LifecycleOutcome::Succeeded,
                ConnectionState::Declined => LifecycleOutcome::Accepted,
                _ => LifecycleOutcome::Failed,
            },
            changed_field_paths: &["state"],
            // Expiry occurs at its deadline, not when a reader or worker notices it.
            occurred_at_unix_nanos: if state == ConnectionState::Expired {
                row.expires_at_unix_nanos
            } else {
                now
            },
            trace_id: &trace,
        },
        Some(user),
    )
    .await?;
    row.state = state;
    row.resolved_account_authorization_version = account_authorization_version;
    Ok(())
}
/// Configuration changes close requests in their owning transaction. Runtime
/// waiters observe terminal evidence without another authorization or replay.
pub(super) async fn end_for_configuration(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    context: &ChangeContext,
    authority: AuthoritySource,
) -> Result<(), Error> {
    for (user, id) in repository::provider_pending_connections(tx, workspace, provider).await? {
        let mut row = repository::load_connection(tx, workspace, &user, id).await?;
        end(
            tx,
            workspace,
            &user,
            &mut row,
            ConnectionCompletion::RouteChanged,
            context.occurred_at_unix_nanos(),
            Some((context, authority)),
        )
        .await?;
    }
    Ok(())
}

async fn refresh(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    row: &mut LoadedConnection,
    now: i64,
) -> Result<(), Error> {
    if let Some(completion) = pending_completion(tx, workspace, user, row, now).await? {
        end(tx, workspace, user, row, completion, now, None).await?;
    }
    Ok(())
}

/// The completion that [`refresh`] would record for a pending request now, if any.
async fn pending_completion(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    row: &LoadedConnection,
    now: i64,
) -> Result<Option<ConnectionCompletion>, Error> {
    if row.state != ConnectionState::Pending {
        return Ok(None);
    }
    let target_provider = &row.provider_id;
    let current = crate::access::exact_in_transaction(tx, workspace, target_provider).await?;
    let completion = if current.is_none_or(|target| {
        target.version() != row.provider_account_setting_version
            || target.credential_route() != CredentialRoute::Caller
    }) {
        Some(ConnectionCompletion::RouteChanged)
    } else if !repository::scoped_account(
        tx,
        workspace,
        target_provider,
        Some(&super::intent::Caller::User(user.clone())),
    )
    .await?
    .is_some_and(|account| {
        account.id == row.account_id
            && account.authorization_version == row.account_authorization_version
            && (account.disconnected || account.requires_authorization_for(&row.contract))
    }) {
        Some(ConnectionCompletion::AuthorizationChanged)
    } else if now >= row.expires_at_unix_nanos {
        Some(ConnectionCompletion::Expired)
    } else {
        None
    };
    Ok(completion)
}

/// Returns, without writing, when the exact connection request expires while it
/// is still pending, so a held [`resolve_connection_in_transaction`] would keep
/// waiting until then. Use it only in a read snapshot; the held resolution stays
/// authoritative.
/// # Errors
/// Returns a persistence, decoding, or identity failure.
pub async fn connection_pending_until_in_snapshot(
    tx: &mut Transaction<'_>,
    target: &crate::ResolvedInvocationTool,
    principal: &Principal,
    request: ConnectionRequestId,
    now: i64,
) -> Result<Option<i64>, Error> {
    let target_provider = target.provider_access();
    if !super::connects_during_invocation(target_provider, principal) {
        return Ok(None);
    }
    let workspace = target_provider.workspace_id();
    let user = UserId::parse(principal.id().as_str())?;
    let contract =
        coral_catalog::credential_contract_in_transaction(tx, target.provider_generation_id())
            .await?;
    let row = repository::load_connection(tx, workspace, &user, request).await?;
    let pending = &row.provider_id == target_provider.id()
        && row.provider_account_setting_version == target_provider.version()
        && row.contract == contract
        && row.state == ConnectionState::Pending
        && pending_completion(tx, workspace, &user, &row, now)
            .await?
            .is_none();
    Ok(pending.then_some(row.expires_at_unix_nanos))
}
/// A completed connection of the User's exact Account satisfies every pending
/// request it is compatible with, wherever the User connected. Incompatible
/// requests stay pending until a read ends them as `AuthorizationChanged`.
pub(super) async fn satisfy_pending(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    user: &UserId,
    access: &crate::access::ProviderAccess,
    account: super::ProviderAccountId,
    contract: &ProviderCredentialContract,
    authority: AuthoritySource,
) -> Result<(), Error> {
    let workspace = access.workspace_id();
    let Some(connected) = repository::scoped_account(
        tx,
        workspace,
        access.id(),
        Some(&super::intent::Caller::User(user.clone())),
    )
    .await?
    .filter(|current| current.id == account && !current.disconnected) else {
        return Ok(());
    };
    let now = context.occurred_at_unix_nanos();
    for id in repository::pending_connections(tx, workspace, user).await? {
        let mut row = repository::load_connection(tx, workspace, user, id).await?;
        if &row.provider_id != access.id() {
            continue;
        }
        if now >= row.expires_at_unix_nanos {
            end(
                tx,
                workspace,
                user,
                &mut row,
                ConnectionCompletion::Expired,
                now,
                None,
            )
            .await?;
        } else if row.account_id == account
            && &row.contract == contract
            && row.provider_account_setting_version == access.version()
        {
            end(
                tx,
                workspace,
                user,
                &mut row,
                ConnectionCompletion::Connected {
                    account_authorization_version: connected.authorization_version.clone(),
                },
                now,
                Some((context, authority)),
            )
            .await?;
        }
    }
    Ok(())
}

/// Reads only a pending request's deadline for the invoking User's Run snapshot,
/// as recorded: a request past its deadline stays pending until a holder
/// records its expiry. The Run owner supplies its exact attached request
/// after checking Run visibility. This facet neither resolves the request
/// nor grants inspection/decision authority.
/// # Errors
/// Returns persistence or invalid canonical Principal failures.
pub async fn pending_connection_deadline_in_transaction(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    principal: &Principal,
    request: ConnectionRequestId,
) -> Result<Option<i64>, Error> {
    if principal.kind() != PrincipalKind::User {
        return Ok(None);
    }
    let user = UserId::parse(principal.id().as_str())?;
    repository::pending_connection_deadline(tx, workspace, &user, request).await
}

/// Reads only a pending request's Provider for the invoking User's own prompt.
/// The Run owner supplies its exact attached request after checking Run visibility.
/// This facet neither resolves the request nor grants inspection/decision authority.
/// # Errors
/// Returns persistence or invalid canonical Principal failures.
pub async fn pending_connection_provider_in_transaction(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    principal: &Principal,
    request: ConnectionRequestId,
) -> Result<Option<ProviderId>, Error> {
    if principal.kind() != PrincipalKind::User {
        return Ok(None);
    }
    let user = UserId::parse(principal.id().as_str())?;
    repository::pending_connection_provider(tx, workspace, &user, request).await
}

/// Transaction result for Invocation-owned selection and durable waiter attachment.
pub enum ConnectionResolution {
    /// Exact credential selection is ready, or the route needs no credentials.
    Ready(Option<CredentialEvidence>),
    /// Keep the original Invocation pending until the request is resolved or
    /// expires at `expires_at_unix_nanos`; its own Run deadline still applies.
    Waiting {
        /// The request the Invocation waits on.
        request: ConnectionRequestId,
        /// When the request expires unless the User resolves it first.
        expires_at_unix_nanos: i64,
    },
}

/// Resolves credentials or creates/joins the exact User connection request.
/// A connected Account whose authorization expired or no longer fits the
/// contract waits on a request like a missing one; the User's explicit reconnect
/// satisfies it. The Invocation owner must have checked current Run, Workspace,
/// Provider, and policy authority and atomically attach the returned request
/// before committing. Its holds are shared, so on Postgres two Invocations
/// of one User may both find no request and create one at once: the unique
/// pending index keeps the first, and the second joins it. SQLite's write
/// transaction serializes them at `BEGIN`, so the second's lookup finds the
/// first's request.
/// # Errors
/// Returns coded gate failures without selecting fallback accounts or replaying work.
pub async fn resolve_connection_in_transaction(
    tx: &mut Transaction<'_>,
    target: &crate::ResolvedInvocationTool,
    principal: &Principal,
    request: Option<ConnectionRequestId>,
    now: i64,
) -> Result<ConnectionResolution, Error> {
    let target_provider = target.provider_access();
    let workspace = target_provider.workspace_id();
    if !super::connects_during_invocation(target_provider, principal) {
        return select_in_transaction(tx, target, principal)
            .await
            .map(ConnectionResolution::Ready);
    }
    let user = UserId::parse(principal.id().as_str())?;
    let contract =
        coral_catalog::credential_contract_in_transaction(tx, target.provider_generation_id())
            .await?;
    let mut row = if let Some(id) = request {
        repository::load_connection(tx, workspace, &user, id).await?
    } else {
        match select_in_transaction(tx, target, principal).await {
            Ok(evidence) => return Ok(ConnectionResolution::Ready(evidence)),
            Err(Error::AccountRequired) => {}
            Err(error) => return Err(error),
        }
        // A missing or disconnected Account connects; a connected one whose grant
        // expired or no longer fits the contract reconnects. The request pins the
        // exact Account and its current authorization version, so only this User's
        // explicit completion satisfies it. Coral never replaces credentials itself.
        let previous =
            repository::pending_connection(tx, target_provider, &user, &contract).await?;
        if let Some(id) = previous {
            let mut previous = repository::load_connection(tx, workspace, &user, id).await?;
            refresh(tx, workspace, &user, &mut previous, now).await?;
            if previous.contract != contract {
                return Err(Error::AccountInput);
            }
            if previous.state == ConnectionState::Pending {
                return Ok(ConnectionResolution::Waiting {
                    request: id,
                    expires_at_unix_nanos: previous.expires_at_unix_nanos,
                });
            }
        }
        let id = ConnectionRequestId(Uuid::new_v4());
        let expires = now
            .checked_add(CONNECTION_REQUEST_LIFETIME_NANOS)
            .ok_or(Error::AccountInput)?;
        let account = repository::ensure_account(
            tx,
            workspace,
            target_provider.id(),
            Some(&super::intent::Caller::User(user.clone())),
            &contract,
        )
        .await?;
        if repository::insert_connection(tx, target_provider, id, &contract, expires, &account)
            .await?
        {
            return Ok(ConnectionResolution::Waiting {
                request: id,
                expires_at_unix_nanos: expires,
            });
        }
        // Another creator committed this User's request between the lookup
        // and the insert: it is pending (the index admits no other) and fresh
        // (committed moments ago with a full lifetime), so it is the one to
        // wait on, read again as it stands now. Gone again already, or
        // resolved meanwhile, the Invocation fails its gate like a missing
        // Account, without inventing a request.
        let Some(id) =
            repository::pending_connection(tx, target_provider, &user, &contract).await?
        else {
            return Err(Error::AccountRequired);
        };
        let mut joined = repository::load_connection(tx, workspace, &user, id).await?;
        refresh(tx, workspace, &user, &mut joined, now).await?;
        if joined.state != ConnectionState::Pending {
            return Err(Error::AccountRequired);
        }
        return Ok(ConnectionResolution::Waiting {
            request: id,
            expires_at_unix_nanos: joined.expires_at_unix_nanos,
        });
    };
    if &row.provider_id != target_provider.id()
        || row.provider_account_setting_version != target_provider.version()
        || row.contract != contract
    {
        return Err(Error::AccountRequired);
    }
    refresh(tx, workspace, &user, &mut row, now).await?;
    match row.state {
        ConnectionState::Pending => Ok(ConnectionResolution::Waiting {
            request: row.id,
            expires_at_unix_nanos: row.expires_at_unix_nanos,
        }),
        ConnectionState::Connected => {
            let evidence = caller::select(tx, target_provider, principal, contract).await?;
            if evidence.account_id != row.account_id
                || Some(evidence.authorization_version.as_str())
                    != row.resolved_account_authorization_version.as_deref()
            {
                return Err(Error::AccountAuthorizationChanged);
            }
            Ok(ConnectionResolution::Ready(Some(evidence)))
        }
        ConnectionState::Declined => Err(Error::AccountConnectionDeclined),
        ConnectionState::Expired => Err(Error::AccountConnectionExpired),
        ConnectionState::AuthorizationChanged => Err(Error::AccountAuthorizationChanged),
        ConnectionState::RouteChanged => Err(Error::CredentialRouteChanged),
    }
}

pub(super) async fn retire_for_access_removal(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    user: &UserId,
    authority: AuthoritySource,
) -> Result<(), Error> {
    for id in repository::pending_connections(tx, workspace, user).await? {
        let mut row = repository::load_connection(tx, workspace, user, id).await?;
        end(
            tx,
            workspace,
            user,
            &mut row,
            ConnectionCompletion::AuthorizationChanged,
            context.occurred_at_unix_nanos(),
            Some((context, authority)),
        )
        .await?;
    }
    Ok(())
}
