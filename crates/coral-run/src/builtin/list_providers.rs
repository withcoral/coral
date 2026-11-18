//! Performs `coral.listProviders`, and makes its read of the Providers for a
//! caller that asks without a Program.

use coral_catalog::{ListProvidersResult, ListedProvider, ListedSetup, ListedSetupState};
use coral_policy::ExactToolRules;
use coral_provider::access::ProviderCallRoot;
use coral_provider::{ProviderModule, ProviderSetup, ProviderSetupState, PublishedProvider};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use super::{BuiltinHost, authority_error, provider_facts, setup_url};
use crate::{Error, RunModule};

/// Lists every Provider in the Workspace that the invoking Principal reaches,
/// with what each offers it, and the Setups its `addProvider` calls left
/// for a person to finish.
pub(super) async fn perform(host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
    let now = coral_db::UnixNanos::new(host.run.now()?);
    // Setups first: one published between the two reads is then in both
    // lists, never in neither, which a Program would read as ended.
    let setups = host
        .providers
        .open_requested_setups(host.actor, host.workspace_id, now)
        .await
        .map_err(authority_error)?;
    let providers = reached_providers(
        host.database,
        host.providers,
        host.actor,
        host.workspace_id,
        now,
        authority_error,
    )
    .await?;
    Ok(ListProvidersResult {
        providers,
        setups: setups
            .iter()
            .filter_map(|setup| listed_setup(host, setup))
            .collect(),
    }
    .into_tool_result())
}

impl RunModule {
    /// Lists every Provider in the Workspace that `actor` reaches, with what
    /// each offers it: the `providers` that `coral.listProviders` returns to
    /// a Program of `actor`'s, read without a Run. Both make the one read.
    ///
    /// # Errors
    ///
    /// Returns current Workspace authority and persistence failures.
    pub async fn listed_providers(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ListedProvider>, Error> {
        reached_providers(
            &self.database,
            &self.providers,
            actor,
            workspace_id,
            coral_db::UnixNanos::new(self.now()?),
            Error::from,
        )
        .await
    }
}

/// Every Provider in the Workspace that `actor` reaches, in one snapshot with
/// the rules that count its Tools. `refused` says what a Provider Module
/// refusal means to the caller: a Program learns that its authority ended.
async fn reached_providers(
    database: &coral_db::Database,
    providers: &ProviderModule,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    now: coral_db::UnixNanos,
    refused: fn(coral_provider::Error) -> Error,
) -> Result<Vec<ListedProvider>, Error> {
    let mut snapshot = database.begin_read_snapshot().await?;
    let published = providers
        .published_providers_in_snapshot(&mut snapshot, actor, workspace_id, now)
        .await
        .map_err(refused)?;
    let rules = coral_policy::exact_tool_rules_in_transaction(&mut snapshot, workspace_id).await?;
    snapshot.rollback().await?;
    Ok(published
        .iter()
        .map(|published| listed_provider(published, &rules))
        .collect())
}

/// One entry, under the Call Root a Program calls it by.
fn listed_provider(published: &PublishedProvider, rules: &ExactToolRules) -> ListedProvider {
    let provider = &published.provider;
    ListedProvider {
        call_root: provider.call_root().as_str().to_owned(),
        namespace: provider
            .call_root()
            .namespace()
            .map(|namespace| namespace.as_str().to_owned()),
        state: provider.state().into(),
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
