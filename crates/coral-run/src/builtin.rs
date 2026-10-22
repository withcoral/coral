//! Built-in Provider Operations, which Coral performs itself with no upstream
//! dispatch. Their input is validated before any other work, and their results
//! never carry a secret or an OAuth URL.
//!
//! coral-catalog owns each Operation's contract: its schemas, how its
//! arguments are read, and the result it writes. Each module here performs
//! one Operation, and recovers it when it can.

use async_trait::async_trait;
use coral_catalog::{
    ArgumentProblem, BuiltinArguments, BuiltinOperation, ExecutionBinding, MaterializedOperation,
    OperationKey, ProviderFacts, ProviderGenerationId,
};
use coral_policy::ExactOperationRules;
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

mod create_provider;
mod list_providers;

/// One admitted Built-in Operation with its validated input.
#[derive(Clone)]
pub(crate) enum BuiltinCall {
    CreateProvider(Box<create_provider::CreateProvider>),
    ListProviders,
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
                    BuiltinArguments::CreateProvider(arguments) => {
                        create_provider::CreateProvider::new(*arguments)
                            .map(|call| Self::CreateProvider(Box::new(call)))
                    }
                    BuiltinArguments::ListProviders => Ok(Self::ListProviders),
                }),
        )
    }

    /// Performs the Operation and returns its Operation Result.
    pub(crate) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        match self {
            Self::CreateProvider(call) => call.perform(host).await,
            Self::ListProviders => list_providers::perform(host).await,
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
        Some(BuiltinOperation::CreateProvider) => {
            create_provider::recover(transaction, run, invocation)
                .await
                .map(Some)
        }
        // A read leaves nothing behind, and this version cannot read an
        // Operation it does not perform.
        Some(BuiltinOperation::ListProviders) | None => Ok(None),
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

/// What both Operations report about a Provider: how to call it and whether
/// the Program needs a person first. It counts only the Operations that
/// `rules`, read in the same snapshot as `published`, leave callable.
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
