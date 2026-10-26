//! Built-in Provider Operations, which Coral performs itself with no upstream
//! dispatch. Their input is validated before any other work, and their results
//! never carry a secret or the OAuth sign-in link Coral prepares for one
//! person. A Provider's OAuth endpoints are configuration, not that link.
//!
//! coral-catalog owns each Operation's contract: its schemas, how its
//! arguments are read, and the result it writes. Each module here performs
//! one Operation, and recovers it when it can, except `setup_operation`: the
//! Setup flow that addProvider and updateProvider share.

use async_trait::async_trait;
use coral_catalog::{
    ArgumentProblem, BuiltinArguments, BuiltinOperation, ExecutionBinding, MaterializedOperation,
    OperationKey, ProviderFacts, ProviderGenerationId,
};
use coral_policy::{ExactOperationRules, OperationPolicyModule};
use coral_provider::account::AccountModule;
use coral_provider::{
    ManagementRequestId, ProviderModule, ProviderSetupId, PublishedProvider,
    ResolvedInvocationOperation, WorkspaceProviderPublisher,
};
use coral_workspace::{AuthenticatedPrincipal, ChangeContext, Principal, WorkspaceId};

use crate::error::InputProblem;
use crate::lifecycle::RunRecord;
use crate::{
    CoralFailure, Error, HumanInterfaceUrl, InvocationId, InvocationResultKind, Rejection,
};

mod add_provider;
mod get_policy;
mod get_provider;
mod list_providers;
mod setup_operation;
mod update_policy;
mod update_provider;

/// One admitted Built-in Operation with its validated input.
#[derive(Clone)]
pub(crate) enum BuiltinCall {
    AddProvider(Box<add_provider::AddProvider>),
    ListProviders,
    GetProvider(get_provider::GetProvider),
    GetPolicy(get_policy::GetPolicy),
    UpdatePolicy(update_policy::UpdatePolicy),
    UpdateProvider(Box<update_provider::UpdateProvider>),
}

impl BuiltinCall {
    /// The Built-in Operation an admitted target names, with its input checked
    /// before any other work; `None` for an Operation sent upstream.
    pub(crate) fn parse(
        target: &ResolvedInvocationOperation,
        arguments: &serde_json::Value,
    ) -> Option<Result<Self, Rejection>> {
        let ExecutionBinding::Builtin(binding) = target.operation().execution_binding() else {
            return None;
        };
        // A newer Coral published an Operation this version does not perform.
        let Some(operation) = binding.operation() else {
            return Some(Err(CoralFailure::OperationUnavailable.into()));
        };
        Some(
            BuiltinArguments::parse(operation, arguments)
                .map_err(|problems| rejection(&problems))
                .and_then(|arguments| match arguments {
                    BuiltinArguments::AddProvider(arguments) => {
                        add_provider::AddProvider::new(*arguments)
                            .map(|call| Self::AddProvider(Box::new(call)))
                    }
                    BuiltinArguments::ListProviders => Ok(Self::ListProviders),
                    BuiltinArguments::GetProvider(arguments) => {
                        get_provider::GetProvider::new(&arguments).map(Self::GetProvider)
                    }
                    BuiltinArguments::GetPolicy(arguments) => {
                        get_policy::GetPolicy::new(&arguments).map(Self::GetPolicy)
                    }
                    BuiltinArguments::UpdatePolicy(arguments) => {
                        update_policy::UpdatePolicy::new(&arguments).map(Self::UpdatePolicy)
                    }
                    BuiltinArguments::UpdateProvider(arguments) => {
                        update_provider::UpdateProvider::new(*arguments)
                            .map(|call| Self::UpdateProvider(Box::new(call)))
                    }
                }),
        )
    }

    /// Performs the Operation and returns its Operation Result.
    pub(crate) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        match self {
            Self::AddProvider(call) => call.perform(host).await,
            Self::ListProviders => list_providers::perform(host).await,
            Self::GetProvider(call) => call.perform(host).await,
            Self::GetPolicy(call) => call.perform(host).await,
            Self::UpdatePolicy(call) => call.perform(host).await,
            Self::UpdateProvider(call) => call.perform(host).await,
        }
    }
}

/// How an Invocation whose worker was lost after the Dispatch Fence ends with
/// its Run, from what its Operation left behind. `None` when the Operation
/// leaves nothing Coral can read, as an upstream Operation does.
pub(crate) async fn recover(
    transaction: &mut coral_db::Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
    generation: &ProviderGenerationId,
    operation: &OperationKey,
) -> Result<Option<(InvocationResultKind, Option<String>)>, Error> {
    let materialized = coral_catalog::operation_in_transaction(transaction, generation, operation)
        .await
        .map_err(coral_provider::Error::from)?;
    let Some(ExecutionBinding::Builtin(binding)) = materialized
        .as_ref()
        .map(MaterializedOperation::execution_binding)
    else {
        return Ok(None);
    };
    match binding.operation() {
        Some(BuiltinOperation::AddProvider) => add_provider::recover(transaction, run, invocation)
            .await
            .map(Some),
        Some(BuiltinOperation::UpdateProvider) => {
            update_provider::recover(transaction, run, invocation)
                .await
                .map(Some)
        }
        // A read leaves nothing behind, a policy change records no call to
        // read, and this version cannot read an Operation it does not perform.
        Some(
            BuiltinOperation::ListProviders
            | BuiltinOperation::GetProvider
            | BuiltinOperation::GetPolicy
            | BuiltinOperation::UpdatePolicy,
        )
        | None => Ok(None),
    }
}

/// The Run that invoked a Built-in Operation: whether it still accepts the
/// result, when it is asked to stop, and its clock.
#[async_trait]
pub(crate) trait InvokingRun: Send + Sync {
    async fn still_current(&self) -> Result<bool, Error>;

    /// Completes when the Run is asked to stop.
    async fn stopped(&self);

    /// The current time on the Run's clock.
    fn now(&self) -> Result<i64, Error>;

    /// Lets a test end the Run once the call's Setup exists.
    #[cfg(test)]
    fn setup_prepared(&self) {}
}

/// What a Built-in Operation acts through: the invoking Principal in the
/// Run's Workspace, the workflow Reef publishes with, and the Reef origin its
/// results link to.
pub(crate) struct BuiltinHost<'a> {
    pub(crate) database: &'a coral_db::Database,
    pub(crate) providers: &'a ProviderModule,
    pub(crate) policies: &'a OperationPolicyModule,
    /// Where people sign in; without it, nobody can.
    pub(crate) accounts: Option<&'a AccountModule>,
    pub(crate) publisher: Option<&'a dyn WorkspaceProviderPublisher>,
    pub(crate) actor: &'a AuthenticatedPrincipal,
    /// The invoking User's change context; `None` for an Agent, which
    /// reaches no Operation that changes the Workspace.
    pub(crate) context: Option<&'a ChangeContext>,
    pub(crate) principal: &'a Principal,
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) human_interface: &'a HumanInterfaceUrl,
    /// The Invocation's identity, which makes a repeated Publication an exact replay.
    pub(crate) request_id: &'a ManagementRequestId,
    pub(crate) run: &'a dyn InvokingRun,
}

/// `invalid_input` naming every argument the contract found to correct.
fn rejection(problems: &[ArgumentProblem]) -> Rejection {
    Rejection::invalid_inputs(
        problems
            .iter()
            .map(|problem| InputProblem::new(problem.field(), problem.reason()))
            .collect(),
    )
}

/// What `addProvider`, `updateProvider` and `listProviders` report about a
/// Provider: how to call it and whether the Program needs a person first. It
/// counts only the Operations that `rules`, read in the same snapshot as
/// `published`, leave callable.
fn provider_facts(published: &PublishedProvider, rules: &ExactOperationRules) -> ProviderFacts {
    let provider = &published.provider;
    ProviderFacts {
        display_name: provider.display_name().to_owned(),
        credential_route: provider.access().credential_route().into(),
        authentication: published.authentication,
        operations: published
            .operations
            .iter()
            .filter(|operation| !rules.disables(provider.id(), operation))
            .count(),
        account_required: published.account_required,
    }
}

/// Reef's new-provider stepper for one Setup. Reef reads the namespace and the
/// Credential Route the Program asked for from the Setup itself, and opens on
/// the Operations, so the person sees what the Provider can do before signing
/// in and publishing. A Setup id needs no URL encoding.
fn setup_url(host: &BuiltinHost<'_>, setup: &ProviderSetupId) -> String {
    host.human_interface.page_url(&format!(
        "/workspaces/{}/providers/new?setup={setup}",
        host.workspace_id
    ))
}

/// A Principal that lost Workspace authority since admission is a failure
/// the Program can catch; anything else is a host defect.
fn authority_error(error: coral_provider::Error) -> Error {
    match error {
        coral_provider::Error::Workspace(error) if crate::fence::loses_authority(&error) => {
            Error::InvocationRejected(CoralFailure::AuthorityLost.into())
        }
        other => other.into(),
    }
}
