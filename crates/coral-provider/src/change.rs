//! One Owner's change to an existing Provider, and what it hands Universal
//! Search after it commits.
//!
//! Every path that changes an existing Provider names its change here, and
//! [`ProviderModule::hold_for_change`] runs the steps every such change shares:
//! Owner configuration authority, the exclusive hold, the Call Root's
//! allow-list and the etag. An Owner-added Provider permits every change. The
//! Built-in Provider, which Coral manages, permits only disabling and
//! restoring it, so a new kind of change is denied for it until someone allows
//! it below. Its Tools' policy belongs to `coral-policy` and stays open.
//!
//! A change that commits its own transaction and alters what search indexes
//! returns [`Committed`]: its result beside the [`SearchChange`] the caller
//! hands to search after the commit (ADR 0017). Disabling, restoring and a
//! secret rotation return none: search filters out a disabled Provider when
//! it reads, and indexes no credential. A regeneration checks its etag only
//! after it settles the Account decision it reviewed, so it keeps its own
//! hold in the Publication transaction.
use coral_db::Transaction;
use coral_workspace::{AuthoritySource, ChangeContext, WorkspaceId};

use crate::repository::{self, ProviderHold, ProviderWithActiveGeneration};
use crate::{
    ActivatedGeneration, Error, LifecycleAction, LifecycleActor, LifecycleOutcome,
    ProviderCallRoot, ProviderId, ProviderModule,
};

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
    /// Remove the Provider.
    Delete,
}

impl ProviderChange {
    /// Applies the allow-list of the Provider's Call Root.
    ///
    /// # Errors
    ///
    /// Returns [`Error::BuiltinProviderManaged`] when the Call Root does not
    /// permit this change.
    pub(crate) fn permit(self, call_root: &ProviderCallRoot) -> Result<(), Error> {
        let permitted = match call_root {
            ProviderCallRoot::Owner(_) => true,
            ProviderCallRoot::Builtin => matches!(self, Self::SetState),
        };
        if permitted {
            Ok(())
        } else {
            Err(Error::BuiltinProviderManaged)
        }
    }

    /// Whether the change requires the context's etag to match the
    /// Provider's. A Setup and a secret rotation leave the Provider's own
    /// fields alone, so they take no etag.
    const fn checks_etag(self) -> bool {
        matches!(self, Self::Rename | Self::SetState | Self::Delete)
    }
}

/// A Provider that an Owner holds for one change, in that change's
/// transaction.
pub(crate) struct HeldProvider {
    /// The authority the Owner acts under.
    pub(crate) authority: AuthoritySource,
    /// The Provider as it stands under the hold.
    pub(crate) current: ProviderWithActiveGeneration,
}

impl HeldProvider {
    /// Records the change as an accepted lifecycle event of the acting User.
    pub(crate) async fn record(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        action: LifecycleAction,
        changed_field_paths: &[&str],
    ) -> Result<(), Error> {
        repository::insert_lifecycle_event(
            transaction,
            repository::NewLifecycleEvent {
                workspace_id: Some(self.current.provider.owner()),
                actor: LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: self.authority,
                },
                action,
                target_id: &self.current.provider.id().to_string(),
                outcome: LifecycleOutcome::Accepted,
                changed_field_paths,
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await
    }
}

impl ProviderModule {
    /// Starts the acting Owner's `change` to `provider` in `transaction`:
    /// checks Owner configuration authority, holds the Provider row
    /// exclusively, applies its Call Root's allow-list and, for a change that
    /// takes one, the context's etag.
    ///
    /// # Errors
    ///
    /// Returns an authority failure, [`Error::ProviderNotFound`] for an absent
    /// Provider, the error of [`ProviderChange::permit`],
    /// [`Error::EtagMismatch`], or a persistence failure.
    pub(crate) async fn hold_for_change(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        change: ProviderChange,
    ) -> Result<HeldProvider, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(transaction, context.actor(), workspace)
            .await?;
        let current = repository::current_provider(
            transaction,
            workspace,
            repository::ProviderKey::Id(provider),
            ProviderHold::Exclusive,
        )
        .await?
        .ok_or(Error::ProviderNotFound)?;
        change.permit(current.provider.call_root())?;
        if change.checks_etag() {
            current.provider.check_etag(context)?;
        }
        Ok(HeldProvider { authority, current })
    }

    /// Starts a Setup's change in `transaction`: holds `update_target` for a
    /// Setup that reconfigures it, or checks Owner configuration authority
    /// alone for a new Provider's Setup.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::hold_for_change`].
    pub(crate) async fn hold_for_setup(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        owner: &WorkspaceId,
        update_target: Option<&ProviderId>,
    ) -> Result<(AuthoritySource, Option<ProviderWithActiveGeneration>), Error> {
        let Some(target) = update_target else {
            let authority = self
                .check_owner_configuration_in_transaction(transaction, context.actor(), owner)
                .await?;
            return Ok((authority, None));
        };
        let held = self
            .hold_for_change(
                transaction,
                context,
                owner,
                target,
                ProviderChange::Reconfigure,
            )
            .await?;
        Ok((held.authority, Some(held.current)))
    }
}

/// Loads the Provider and applies the allow-list for `change`, without a
/// hold, as a read snapshot checks a change before it starts one.
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
    let stored = repository::current_provider(
        transaction,
        owner,
        repository::ProviderKey::Id(provider),
        ProviderHold::None,
    )
    .await?
    .ok_or(Error::ProviderNotFound)?;
    change.permit(stored.provider.call_root())?;
    Ok(stored)
}

/// What Universal Search must read again once a Provider change commits.
/// The caller hands it to search after the commit, with ids alone, and
/// search's reconcile pass repairs a notification that never arrives
/// (ADR 0017).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchChange {
    /// The change touched nothing that search indexes.
    Unchanged,
    /// A Generation became active, replacing the one it names, if any.
    Activated(ActivatedGeneration),
    /// The Provider kept its active Generation, but the fields search copies
    /// from the Provider changed, such as its display name.
    Refreshed {
        /// The Provider's Workspace.
        workspace: WorkspaceId,
        /// The Provider.
        provider: ProviderId,
    },
    /// The Provider is gone. Its Generations remain, so no cascade removes
    /// its search rows.
    Removed {
        /// The Provider's Workspace.
        workspace: WorkspaceId,
        /// The Provider.
        provider: ProviderId,
    },
}

/// A Provider change's result, and what search must read again once the
/// change's transaction commits. A change that commits its own transaction
/// returns it after the commit. A change that found nothing to write, such as
/// a replay, commits nothing and names [`SearchChange::Unchanged`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "hand `search` to Universal Search after the commit, or say why the caller need not"]
pub struct Committed<T> {
    /// The change's result.
    pub value: T,
    /// What search must read again, now that the change has committed.
    pub search: SearchChange,
}

impl<T> Committed<T> {
    /// Returns the result without telling search. Search stays stale until
    /// its reconcile pass, so only a test of Provider rules, or of what
    /// search misses, should call this.
    pub fn skip_search(self) -> T {
        self.value
    }

    /// A result that changed nothing search indexes.
    pub(crate) const fn unchanged(value: T) -> Self {
        Self {
            value,
            search: SearchChange::Unchanged,
        }
    }
}
