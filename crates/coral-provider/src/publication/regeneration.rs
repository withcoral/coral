//! What a regeneration changes, decided before any write.
//!
//! [`plan`] holds the rules. The terminal transaction loads the [`Facts`],
//! and applies the [`Plan`]'s effects in its own fixed order.
use coral_catalog::{OAuthClientAuthenticationMethod, ProviderCredentialContract as Contract};

use crate::{CredentialRoute, Error, RouteFit};

/// The request, the Provider as it stands, and the replacement catalog.
#[derive(Debug, Clone, Copy)]
pub(super) struct Facts<'a> {
    /// Whether the owner confirmed a change of contract or of Credential Route.
    pub(super) allows_incompatible_configuration: bool,
    /// The Credential Route the request moves the Provider to, if it names one.
    pub(super) account_setting: Option<CredentialRoute>,
    /// Whether the request names a reviewed Setup grant for the shared Account.
    pub(super) account_grant: bool,
    /// The Credential Route the Provider is on now.
    pub(super) current_route: CredentialRoute,
    /// The active Generation's contract.
    pub(super) active_contract: &'a Contract,
    /// The replacement catalog's contract.
    pub(super) replacement_contract: &'a Contract,
    /// Whether the replacement has the active Generation's name and
    /// fingerprint. The loader compares them: only a materialized catalog
    /// has a fingerprint.
    pub(super) same_catalog: bool,
}

/// One thing a regeneration changes besides its terminal result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Effect {
    /// Publish the replacement as a new Generation and activate it.
    ActivateGeneration,
    /// Move the Provider to the plan's Credential Route, and disconnect the
    /// Accounts connected under the old one.
    ChangeRoute,
    /// Put the reviewed Setup grant in the shared Account's place.
    ReplaceSharedAccount,
    /// Cancel the sign-ins begun under the old contract.
    CancelAuthorizations,
    /// Forget the recorded failure of the old contract's OAuth application.
    ClearClientFailure,
    /// Delete the Provider's application secret: the replacement contract
    /// names no confidential application.
    DropClientSecret,
}

/// A regeneration the rules accept: the route it leaves the Provider on and
/// everything it changes. The effects are a set; the applier owns their order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Plan {
    /// The Credential Route the Provider is on afterwards.
    pub(super) route: CredentialRoute,
    pub(super) effects: Vec<Effect>,
}

impl Plan {
    pub(super) fn includes(&self, effect: Effect) -> bool {
        self.effects.contains(&effect)
    }
}

/// Decides what a materialized regeneration changes.
///
/// # Errors
///
/// Returns [`Error::IncompatibleConfigurationConfirmationRequired`] when the
/// contract or the Credential Route changes and the owner did not confirm
/// it, and [`Error::AccountRequired`] or [`Error::AccountInput`] when the
/// route and the grant do not suit the replacement contract. Each leaves the
/// Setup ready for another request.
pub(super) fn plan(facts: Facts<'_>) -> Result<Plan, Error> {
    let contract_changed = facts.replacement_contract != facts.active_contract;
    if contract_changed && !facts.allows_incompatible_configuration {
        return Err(Error::IncompatibleConfigurationConfirmationRequired);
    }
    let route = facts.account_setting.unwrap_or(facts.current_route);
    let route_changed = route != facts.current_route;
    if route_changed && !facts.allows_incompatible_configuration {
        return Err(Error::IncompatibleConfigurationConfirmationRequired);
    }
    match route.fits(facts.replacement_contract) {
        RouteFit::Fits => {}
        RouteFit::SourceNeedsCredential => return Err(Error::AccountRequired),
        RouteFit::SourceNeedsNoCredential => return Err(Error::AccountInput),
    }
    if facts.account_grant {
        // Only the shared Account takes a Setup grant.
        if route != CredentialRoute::Fixed {
            return Err(Error::AccountInput);
        }
    } else if route == CredentialRoute::Fixed && (route_changed || contract_changed) {
        // The shared Account was authorized for the old route and contract.
        return Err(Error::AccountRequired);
    }
    let confidential_application = facts
        .replacement_contract
        .oauth_client()
        .is_some_and(|policy| policy.authentication != OAuthClientAuthenticationMethod::None);
    let effects = [
        (!facts.same_catalog, Effect::ActivateGeneration),
        (route_changed, Effect::ChangeRoute),
        (facts.account_grant, Effect::ReplaceSharedAccount),
        (contract_changed, Effect::CancelAuthorizations),
        (contract_changed, Effect::ClearClientFailure),
        (!confidential_application, Effect::DropClientSecret),
    ];
    Ok(Plan {
        route,
        effects: effects
            .into_iter()
            .filter_map(|(applies, effect)| applies.then_some(effect))
            .collect(),
    })
}
