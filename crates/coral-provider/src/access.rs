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
pub(crate) use repository::{exact_in_transaction, provider_id_in_transaction};
#[cfg(test)]
mod tests;

/// Immutable, Workspace-unique JavaScript path under which one Provider's
/// Tools are called. No Call Root is a prefix of another, so at most one
/// owns a Call Path.
///
/// Each kind of Provider is one variant. A rule that depends on the kind
/// matches on it, so a new kind must be decided at every such rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProviderCallRoot {
    /// `coral.providers.<namespace>`: a Provider an Owner adds.
    Owner(ProviderNamespace),
    /// `coral.workspace`: the Workspace's Built-in Provider, which Coral defines.
    Builtin,
}

/// The short name an Owner gives a Provider: a non-reserved safe ASCII
/// JavaScript identifier of at most 128 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderNamespace {
    /// The complete Call Root, so [`ProviderCallRoot::as_str`] can borrow it.
    call_root: String,
}

/// The prefix of every Owner-added Provider's Call Root.
const OWNER_ROOT_PREFIX: &str = "coral.providers.";
/// The Call Root of every Workspace's Built-in Provider.
const BUILTIN_ROOT: &str = "coral.workspace";

impl ProviderNamespace {
    /// Validates one Provider Namespace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for an invalid or unsafe namespace.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if !coral_catalog::provider_namespace_is_valid(value) {
            return Err(Error::InvalidInput(
                "Provider Namespace must be a non-reserved safe ASCII JavaScript identifier of at most 128 bytes"
                    .to_string(),
            ));
        }
        Ok(Self {
            call_root: format!("{OWNER_ROOT_PREFIX}{value}"),
        })
    }

    /// Returns the namespace.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.call_root
            .strip_prefix(OWNER_ROOT_PREFIX)
            .unwrap_or(&self.call_root)
    }
}

impl ProviderCallRoot {
    /// Validates one Call Root: `coral.providers.<namespace>` or `coral.workspace`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for any other value.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value == BUILTIN_ROOT {
            return Ok(Self::Builtin);
        }
        value
            .strip_prefix(OWNER_ROOT_PREFIX)
            .and_then(|namespace| ProviderNamespace::parse(namespace).ok())
            .map(Self::Owner)
            .ok_or_else(|| {
                Error::InvalidInput(
                    "Provider Call Root must be coral.providers.<namespace>, with a non-reserved safe ASCII JavaScript identifier of at most 128 bytes, or coral.workspace"
                        .to_string(),
                )
            })
    }

    /// Splits `call_path` into the Call Root that owns it and the
    /// Tool-relative path under that root, if a valid root owns it.
    #[must_use]
    pub fn split_call_path(call_path: &str) -> Option<(Self, &str)> {
        call_path
            .match_indices('.')
            .take(3)
            .filter_map(|(end, _)| call_path.get(..end).and_then(|root| Self::parse(root).ok()))
            .find_map(|root| {
                root.relative_path(call_path)
                    .map(|relative| (root, relative))
            })
    }

    /// Returns the complete Call Root.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Owner(namespace) => &namespace.call_root,
            Self::Builtin => BUILTIN_ROOT,
        }
    }

    /// Returns the namespace an Owner gave the Provider; `None` for the
    /// Built-in Provider, which has none.
    #[must_use]
    pub const fn namespace(&self) -> Option<&ProviderNamespace> {
        match self {
            Self::Owner(namespace) => Some(namespace),
            Self::Builtin => None,
        }
    }

    /// Returns the Call Root's last segment, such as `github` or `workspace`, for
    /// search text and labels. No rule reads it.
    #[must_use]
    pub fn name(&self) -> &str {
        let root = self.as_str();
        root.rsplit('.').next().unwrap_or(root)
    }

    /// Returns the Tool-relative path of a Call Path under this root, or
    /// `None` when the path belongs elsewhere or has an empty or control segment.
    fn relative_path<'a>(&self, call_path: &'a str) -> Option<&'a str> {
        let relative = call_path.strip_prefix(self.as_str())?.strip_prefix('.')?;
        let valid = !relative.chars().any(char::is_control)
            && relative.split('.').all(|segment| !segment.is_empty());
        valid.then_some(relative)
    }
}

/// Credential selection configured for one Provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CredentialRoute {
    /// Invocations use no Account.
    None,
    /// The invoking Principal's exact Account.
    Caller,
    /// The Provider's single shared Account.
    Fixed,
}

/// Whether a Credential Route suits a source's credential contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteFit {
    /// The route sends what the source needs.
    Fits,
    /// The source needs a credential, and the route sends none.
    SourceNeedsCredential,
    /// The source needs no credential, and the route sends one.
    SourceNeedsNoCredential,
}

impl CredentialRoute {
    /// Every route, in the order Reef offers them.
    pub const ALL: [Self; 3] = [Self::None, Self::Caller, Self::Fixed];

    /// Whether this route suits `contract`. Setup publishability, Publication
    /// and Invocation's credential selection all decide by this one rule.
    #[must_use]
    pub fn fits(self, contract: &coral_catalog::ProviderCredentialContract) -> RouteFit {
        let needs_credential = contract != &coral_catalog::ProviderCredentialContract::NONE;
        match (self, needs_credential) {
            (Self::None, true) => RouteFit::SourceNeedsCredential,
            (Self::Fixed | Self::Caller, false) => RouteFit::SourceNeedsNoCredential,
            (Self::None, false) | (Self::Fixed | Self::Caller, true) => RouteFit::Fits,
        }
    }

    /// The routes that suit `contract`, in [`Self::ALL`]'s order.
    #[must_use]
    pub fn fitting(contract: &coral_catalog::ProviderCredentialContract) -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|route| route.fits(contract) == RouteFit::Fits)
            .collect()
    }
}

impl From<coral_catalog::BuiltinCredentialRoute> for CredentialRoute {
    fn from(route: coral_catalog::BuiltinCredentialRoute) -> Self {
        match route {
            coral_catalog::BuiltinCredentialRoute::None => Self::None,
            coral_catalog::BuiltinCredentialRoute::Fixed => Self::Fixed,
            coral_catalog::BuiltinCredentialRoute::Caller => Self::Caller,
        }
    }
}

impl From<CredentialRoute> for coral_catalog::BuiltinCredentialRoute {
    fn from(route: CredentialRoute) -> Self {
        match route {
            CredentialRoute::None => Self::None,
            CredentialRoute::Fixed => Self::Fixed,
            CredentialRoute::Caller => Self::Caller,
        }
    }
}

/// Workspace configuration that exposes one Provider under one Call Root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAccess {
    id: ProviderId,
    workspace_id: WorkspaceId,
    call_root: ProviderCallRoot,
    credential_route: CredentialRoute,
    version: i64,
}

impl ProviderAccess {
    pub(crate) const fn new(
        id: ProviderId,
        workspace_id: WorkspaceId,
        call_root: ProviderCallRoot,
        credential_route: CredentialRoute,
        version: i64,
    ) -> Self {
        Self {
            id,
            workspace_id,
            call_root,
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

    /// Returns the immutable Call Root.
    #[must_use]
    pub const fn call_root(&self) -> &ProviderCallRoot {
        &self.call_root
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
            self.call_root.clone(),
            credential_route,
            version,
        )
    }
}

/// Exact retained configuration evidence; callers hold the Provider before checking.
pub(crate) enum RetainedProviderVerification {
    Current,
    RouteChanged,
}

/// Compares the Provider's current access with the access an Invocation
/// retained at admission.
pub(crate) fn verify_retained(
    current: &ProviderAccess,
    retained: &ProviderAccess,
) -> Result<RetainedProviderVerification, Error> {
    if current.credential_route() != retained.credential_route()
        || current.version() != retained.version()
    {
        return Ok(RetainedProviderVerification::RouteChanged);
    }
    if current != retained {
        return Err(Error::CorruptData(
            "Provider immutable configuration changed".to_string(),
        ));
    }
    Ok(RetainedProviderVerification::Current)
}

/// Whether a Provider in the Workspace already uses `call_root`.
pub(crate) async fn call_root_exists(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    call_root: &ProviderCallRoot,
) -> Result<bool, Error> {
    repository::call_root_exists(transaction, workspace_id, call_root).await
}

pub(super) enum PreparedProviderAccess {
    Ready(ProviderAccess),
    CallRootConflict,
}

pub(super) async fn prepare(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: ProviderId,
    call_root: ProviderCallRoot,
    credential_route: CredentialRoute,
) -> Result<PreparedProviderAccess, Error> {
    if repository::call_root_exists(transaction, workspace_id, &call_root).await? {
        return Ok(PreparedProviderAccess::CallRootConflict);
    }
    Ok(PreparedProviderAccess::Ready(ProviderAccess::new(
        provider_id,
        workspace_id.clone(),
        call_root,
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
