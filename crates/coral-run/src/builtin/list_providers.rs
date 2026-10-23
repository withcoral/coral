//! Performs `coral.workspace.listProviders`.

use coral_catalog::{
    ListProvidersResult, ListedProvider, ListedProviderState, ListedSetup, ListedSetupState,
};
use coral_policy::ExactOperationRules;
use coral_provider::access::ProviderCallRoot;
use coral_provider::{ProviderSetup, ProviderSetupState, ProviderState, PublishedProvider};

use super::{BuiltinHost, authority_error, provider_facts, setup_url};
use crate::Error;

/// Lists every Provider in the Workspace that the invoking Principal reaches,
/// with what each offers it, and the Setups its `addProvider` calls left
/// for a person to finish.
pub(super) async fn perform(host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
    let now = host.run.now()?;
    // Setups first: one published between the two reads is then in both
    // lists, never in neither, which a Program would read as ended.
    let setups = host
        .providers
        .open_requested_setups(host.actor, host.workspace_id, now)
        .await
        .map_err(authority_error)?;
    let mut snapshot = host.database.begin_read_snapshot().await?;
    let providers = host
        .providers
        .published_providers_in_snapshot(&mut snapshot, host.actor, host.workspace_id, now)
        .await
        .map_err(authority_error)?;
    let rules =
        coral_policy::exact_operation_rules_in_transaction(&mut snapshot, host.workspace_id)
            .await?;
    snapshot.rollback().await?;
    Ok(ListProvidersResult {
        providers: providers
            .iter()
            .map(|published| listed_provider(published, &rules))
            .collect(),
        setups: setups
            .iter()
            .filter_map(|setup| listed_setup(host, setup))
            .collect(),
    }
    .into_operation_result())
}

/// One entry, under the Call Root a Program calls it by.
fn listed_provider(published: &PublishedProvider, rules: &ExactOperationRules) -> ListedProvider {
    let provider = &published.provider;
    ListedProvider {
        call_root: provider.call_root().as_str().to_owned(),
        state: match provider.state() {
            ProviderState::Active => ListedProviderState::Active,
            ProviderState::Disabled => ListedProviderState::Disabled,
        },
        facts: provider_facts(published, rules),
    }
}

/// One open Setup, by what its Program asked for. Only an open Setup a
/// Program requested is listed, so neither `None` arm is reached.
fn listed_setup(host: &BuiltinHost<'_>, setup: &ProviderSetup) -> Option<ListedSetup> {
    let request = setup.request()?;
    let state = match setup.state() {
        ProviderSetupState::AwaitingConfiguration => ListedSetupState::AwaitingConfiguration,
        ProviderSetupState::AwaitingAuthorization => ListedSetupState::AwaitingSignIn,
        ProviderSetupState::AwaitingDiscovery => ListedSetupState::AwaitingDiscovery,
        ProviderSetupState::Ready => ListedSetupState::Ready,
        ProviderSetupState::Consumed(_)
        | ProviderSetupState::Expired
        | ProviderSetupState::Canceled => return None,
    };
    Some(ListedSetup {
        call_root: ProviderCallRoot::Owner(request.namespace.clone())
            .as_str()
            .to_owned(),
        credential_route: request.route.into(),
        state,
        url: setup_url(host, setup.id()),
    })
}
