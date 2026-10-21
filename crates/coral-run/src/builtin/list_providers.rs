//! Performs `coral.workspace.listProviders`.

use coral_catalog::{ListProvidersResult, ListedProvider};
use coral_provider::PublishedProvider;
use coral_provider::access::ProviderCallRoot;

use super::{BuiltinHost, authority_error, provider_facts};
use crate::Error;

/// Lists every Provider in the Workspace, with what each offers the invoking User.
pub(super) async fn perform(host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
    let providers = host
        .providers
        .published_providers(host.context.actor(), host.workspace_id)
        .await
        .map_err(authority_error)?;
    Ok(ListProvidersResult {
        providers: providers.iter().map(listed_provider).collect(),
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
        state: provider.state().into(),
        facts: provider_facts(published),
    }
}
