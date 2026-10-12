//! Minimal Provider authority snapshots for retained Invocation evidence.
//! Publication creates one Provider; these projections have no independent lifecycle.
use coral_workspace::WorkspaceId;

use coral_db::Transaction;

use crate::{Error, ProviderId};
mod inspection;
mod repository;
pub use inspection::{
    AgentAccountUse, AgentProviderInspection, ProviderBlocker, ProviderReadiness,
    inspect_agent_providers_in_transaction, inspect_provider_readiness_in_transaction,
};
pub(crate) use repository::{
    by_namespace_in_transaction, exact_in_transaction, hold_exact_in_transaction,
    provider_id_in_transaction,
};
#[cfg(test)]
mod tests;

/// Immutable JavaScript namespace of one Provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProviderNamespace(String);

impl ProviderNamespace {
    /// Validates one ASCII JavaScript identifier segment.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for an invalid or unsafe path segment.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let mut bytes = value.bytes();
        let first = bytes.next();
        let valid_first =
            first.is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'));
        let valid_rest =
            bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'));
        if !valid_first
            || !valid_rest
            || value.len() > 128
            || matches!(value, "__proto__" | "prototype" | "constructor" | "then")
        {
            return Err(Error::InvalidInput(
                "Provider namespace must be a non-reserved safe ASCII JavaScript identifier of at most 128 bytes"
                    .to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the immutable namespace segment.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Credential selection configured for one Provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialRoute {
    /// Invocations use no Account.
    None,
    /// The invoking Principal's exact Account.
    Caller,
    /// The Provider's single shared Account.
    Fixed,
}

/// Workspace configuration that exposes one Provider under one namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAccess {
    id: ProviderId,
    workspace_id: WorkspaceId,
    namespace: ProviderNamespace,
    credential_route: CredentialRoute,
    version: i64,
}

impl ProviderAccess {
    pub(crate) const fn new(
        id: ProviderId,
        workspace_id: WorkspaceId,
        namespace: ProviderNamespace,
        credential_route: CredentialRoute,
        version: i64,
    ) -> Self {
        Self {
            id,
            workspace_id,
            namespace,
            credential_route,
            version,
        }
    }

    /// Returns the stable Provider identity.
    #[must_use]
    pub const fn id(&self) -> &ProviderId {
        &self.id
    }

    /// Returns the exact Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the immutable Program namespace.
    #[must_use]
    pub const fn namespace(&self) -> &ProviderNamespace {
        &self.namespace
    }

    /// Returns the exact credential route.
    #[must_use]
    pub const fn credential_route(&self) -> CredentialRoute {
        self.credential_route
    }

    /// Returns authority-bearing account-setting version evidence.
    #[must_use]
    pub const fn version(&self) -> i64 {
        self.version
    }

    fn with_route_and_version(&self, credential_route: CredentialRoute, version: i64) -> Self {
        Self::new(
            self.id.clone(),
            self.workspace_id.clone(),
            self.namespace.clone(),
            credential_route,
            version,
        )
    }
}

/// Exact retained configuration evidence; callers hold the Provider before checking.
pub(crate) enum RetainedProviderVerification {
    Current,
    Removed,
    RouteChanged,
}

pub(crate) async fn verify_retained_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    retained: &ProviderAccess,
) -> Result<RetainedProviderVerification, Error> {
    let Some(current) =
        repository::exact_in_transaction(transaction, workspace_id, retained.id()).await?
    else {
        return Ok(RetainedProviderVerification::Removed);
    };
    if current.credential_route() != retained.credential_route()
        || current.version() != retained.version()
    {
        return Ok(RetainedProviderVerification::RouteChanged);
    }
    if &current != retained {
        return Err(Error::CorruptData(
            "Provider immutable configuration changed".to_string(),
        ));
    }
    Ok(RetainedProviderVerification::Current)
}

pub(super) enum PreparedProviderAccess {
    Ready(ProviderAccess),
    NamespaceConflict,
}

pub(super) async fn prepare(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: ProviderId,
    namespace: ProviderNamespace,
    credential_route: CredentialRoute,
) -> Result<PreparedProviderAccess, Error> {
    if repository::namespace_exists(transaction, workspace_id, &namespace).await? {
        return Ok(PreparedProviderAccess::NamespaceConflict);
    }
    Ok(PreparedProviderAccess::Ready(ProviderAccess::new(
        provider_id,
        workspace_id.clone(),
        namespace,
        credential_route,
        1,
    )))
}

pub(crate) async fn update_credential_route(
    transaction: &mut Transaction<'_>,
    current: &ProviderAccess,
    credential_route: CredentialRoute,
) -> Result<ProviderAccess, Error> {
    let version = current
        .version()
        .checked_add(1)
        .ok_or(Error::ProviderAccountSettingVersionOverflow)?;
    let updated = current.with_route_and_version(credential_route, version);
    repository::update_credential_route(transaction, current, &updated).await?;
    Ok(updated)
}
