//! Performs `coral.workspace.listProviders`.

use coral_catalog::{
    ListProvidersResult, ListedProvider, ListedProviderState, ListedSetup, ListedSetupState,
};
use coral_provider::access::ProviderCallRoot;
use coral_provider::{ProviderSetup, ProviderSetupState, ProviderState, PublishedProvider};

use super::{BuiltinHost, authority_error, provider_facts, setup_url};
use crate::Error;

/// Lists every Provider in the Workspace that the invoking Principal reaches,
/// with what each offers it, and the Setups its `createProvider` calls left
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
    let providers = host
        .providers
        .published_providers(host.actor, host.workspace_id, now)
        .await
        .map_err(authority_error)?;
    Ok(ListProvidersResult {
        providers: providers.iter().map(listed_provider).collect(),
        setups: setups
            .iter()
            .filter_map(|setup| listed_setup(host, setup))
            .collect(),
    }
    .into_operation_result())
}

/// One entry. The Built-in Provider has no namespace: its Operations are
/// under `coral.workspace`.
fn listed_provider(published: &PublishedProvider) -> ListedProvider {
    let provider = &published.provider;
    ListedProvider {
        namespace: match provider.call_root() {
            ProviderCallRoot::Owner(namespace) => Some(namespace.as_str().to_owned()),
            ProviderCallRoot::Builtin => None,
        },
        state: match provider.state() {
            ProviderState::Active => ListedProviderState::Active,
            ProviderState::Disabled => ListedProviderState::Disabled,
        },
        facts: provider_facts(published),
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
        namespace: request.namespace.as_str().to_owned(),
        credential_route: request.route.into(),
        state,
        url: setup_url(host, setup.id()),
    })
}
