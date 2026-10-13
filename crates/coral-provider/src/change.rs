//! The one step that loads an existing Provider for a change and applies its
//! Call Root's allow-list.
//!
//! Every path that changes an existing Provider names its change here. An
//! Owner-added Provider permits every change except a Coral catalog update.
//! The Built-in Provider, which Coral manages, permits only disabling and
//! restoring it and updating it to the catalog Coral serves, so a new kind of
//! change is denied for it until someone allows it below. Its Operations'
//! policy belongs to `coral-policy` and stays open.
use coral_db::Transaction;
use coral_workspace::WorkspaceId;

use crate::repository::{self, ProviderWithActiveGeneration};
use crate::{Error, ProviderCallRoot, ProviderId};

/// One change to an existing Provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderChange {
    /// Change the Provider's display name.
    Rename,
    /// Prepare or create a Setup that updates the Provider's configuration.
    Reconfigure,
    /// Materialize the Provider's catalog again.
    Regenerate,
    /// Replace the Provider's shared OAuth application secret.
    RotateOAuthClientSecret,
    /// Start or restart a Provider Account authorization.
    AuthorizeAccount,
    /// Disable or restore the Provider.
    SetState,
    /// Update the Built-in Provider to the catalog Coral serves.
    UpdateBuiltinCatalog,
    /// Remove the Provider.
    Delete,
}

impl ProviderChange {
    /// Applies the allow-list of the Provider's Call Root.
    ///
    /// # Errors
    ///
    /// Returns [`Error::BuiltinProviderManaged`] when the Built-in Provider
    /// does not permit this change, or [`Error::NotBuiltinProvider`] for a
    /// Coral catalog update of another Provider.
    pub(crate) fn permit(self, call_root: &ProviderCallRoot) -> Result<(), Error> {
        match (call_root, self) {
            (ProviderCallRoot::Owner(_), Self::UpdateBuiltinCatalog) => {
                Err(Error::NotBuiltinProvider)
            }
            (ProviderCallRoot::Owner(_), _)
            | (ProviderCallRoot::Builtin, Self::SetState | Self::UpdateBuiltinCatalog) => Ok(()),
            (ProviderCallRoot::Builtin, _) => Err(Error::BuiltinProviderManaged),
        }
    }
}

/// Holds the Provider row, loads it, and applies the allow-list for `change`.
///
/// # Errors
///
/// Returns [`Error::ProviderNotFound`] for an absent Provider, the error of
/// [`ProviderChange::permit`], or a persistence failure.
pub(crate) async fn hold_for_change(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
    change: ProviderChange,
) -> Result<ProviderWithActiveGeneration, Error> {
    if !repository::hold_provider_in_transaction(transaction, owner, provider).await? {
        return Err(Error::ProviderNotFound);
    }
    load_for_change(transaction, owner, provider, change).await
}

/// Loads the Provider and applies the allow-list for `change`, in a read
/// snapshot or after the caller has held the row.
///
/// # Errors
///
/// Returns [`Error::ProviderNotFound`] for an absent Provider, the error of
/// [`ProviderChange::permit`], or a persistence failure.
pub(crate) async fn load_for_change(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
    change: ProviderChange,
) -> Result<ProviderWithActiveGeneration, Error> {
    let stored = repository::provider_in_transaction(transaction, owner, provider)
        .await?
        .ok_or(Error::ProviderNotFound)?;
    change.permit(stored.provider.call_root())?;
    Ok(stored)
}
