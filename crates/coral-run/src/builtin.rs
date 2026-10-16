//! Built-in Provider Operations, which Coral performs itself with no upstream
//! dispatch. Their input is validated before any other work, and their results
//! never carry a secret or an OAuth URL.

use coral_catalog::{BuiltinOperation, ExecutionBinding};
use coral_provider::access::ProviderNamespace;
use coral_provider::{
    OpenApiDocument, ProviderModule, ProviderSetupId, ProviderSetupSource,
    ResolvedInvocationOperation,
};
use coral_workspace::{ChangeContext, WorkspaceId};
use serde::Deserialize;

use crate::{CoralFailure, Error, HumanInterfaceUrl};

/// One admitted Built-in Operation with its validated input.
#[derive(Clone)]
pub(crate) enum BuiltinCall {
    CreateProvider(CreateProvider),
}

impl BuiltinCall {
    /// The Built-in Operation an admitted target names, with its input checked
    /// before any other work; `None` for an Operation sent upstream.
    pub(crate) fn parse(
        target: &ResolvedInvocationOperation,
        arguments: &serde_json::Value,
    ) -> Option<Result<Self, CoralFailure>> {
        match target.operation().execution_binding() {
            ExecutionBinding::Builtin(binding) => Some(match binding.operation() {
                BuiltinOperation::CreateProvider => {
                    CreateProvider::parse(arguments).map(Self::CreateProvider)
                }
            }),
            ExecutionBinding::Http(_) | ExecutionBinding::Mcp(_) => None,
        }
    }

    /// Performs the Operation and returns its Operation Result.
    pub(crate) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        match self {
            Self::CreateProvider(input) => input.perform(host).await,
        }
    }
}

/// What a Built-in Operation acts through: the invoking User in the Run's
/// Workspace, and the Reef origin its results link to.
pub(crate) struct BuiltinHost<'a> {
    pub(crate) providers: &'a ProviderModule,
    pub(crate) context: &'a ChangeContext,
    pub(crate) workspace_id: &'a WorkspaceId,
    pub(crate) human_interface: &'a HumanInterfaceUrl,
}

/// Validated `createProvider` input. The namespace is the Program's suggestion;
/// the person who finishes the Provider in Reef can change it.
#[derive(Clone)]
pub(crate) struct CreateProvider {
    namespace: ProviderNamespace,
    source: ProviderSetupSource,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateProviderArguments {
    namespace: String,
    source: SourceArguments,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SourceArguments {
    OpenapiDocument { document: String },
    OpenapiUrl { url: String },
    McpUrl { url: String },
}

impl CreateProvider {
    /// Validates the namespace syntax first, then the source, without any network call.
    fn parse(arguments: &serde_json::Value) -> Result<Self, CoralFailure> {
        let arguments = CreateProviderArguments::deserialize(arguments)
            .map_err(|_error| CoralFailure::InvalidInput)?;
        let namespace = ProviderNamespace::parse(&arguments.namespace)
            .map_err(|_error| CoralFailure::InvalidInput)?;
        let source = match arguments.source {
            SourceArguments::OpenapiDocument { document } => ProviderSetupSource::OpenApiDocument(
                OpenApiDocument::parse(&document).map_err(|_error| CoralFailure::InvalidInput)?,
            ),
            SourceArguments::OpenapiUrl { url } => ProviderSetupSource::OpenApiUrl(web_url(url)?),
            SourceArguments::McpUrl { url } => ProviderSetupSource::McpUrl {
                url: web_url(url)?,
                request_headers: std::collections::BTreeMap::new(),
            },
        };
        Ok(Self { namespace, source })
    }

    /// Prepares a Setup from the source as the invoking User, then returns the
    /// Reef page where a person finishes and publishes it. The call does not
    /// wait for that person.
    async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let setup = host
            .providers
            .prepare_setup(host.context, host.workspace_id, self.source.clone(), None)
            .await
            .map_err(setup_error)?;
        let url = host.human_interface.page_url(&setup_path(
            host.workspace_id,
            setup.id(),
            &self.namespace,
        ));
        Ok(serde_json::json!({
            "data": {
                "status": "needs_sign_in",
                "url": url,
                "namespace": self.namespace.as_str(),
            },
            "metadata": { "kind": "builtin" },
        }))
    }
}

/// A source URL must be an http or https URL with a host, and no credentials
/// or fragment, as the Setup requires. Whether Coral may fetch it is the
/// destination policy's to decide, when the Setup is prepared.
fn web_url(url: String) -> Result<String, CoralFailure> {
    let parsed = url::Url::parse(&url).map_err(|_error| CoralFailure::InvalidInput)?;
    if matches!(parsed.scheme(), "http" | "https")
        && parsed.host_str().is_some()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.fragment().is_none()
    {
        Ok(url)
    } else {
        Err(CoralFailure::InvalidInput)
    }
}

/// Reef's new-provider page for one Setup, with the requested namespace filled in.
fn setup_path(
    workspace: &WorkspaceId,
    setup: &ProviderSetupId,
    namespace: &ProviderNamespace,
) -> String {
    format!(
        "/workspaces/{workspace}/providers/new?setup={setup}&namespace={}",
        namespace.as_str()
    )
}

/// Maps a Setup preparation failure to the Program-safe vocabulary. Only
/// input problems are the Program's to catch; anything else is a host defect.
fn setup_error(error: coral_provider::Error) -> Error {
    let failure = match error {
        coral_provider::Error::OutboundDestinationRejected => {
            CoralFailure::OutboundDestinationRejected
        }
        coral_provider::Error::InputUnavailable
        | coral_provider::Error::InvalidInput(_)
        | coral_provider::Error::AccountInput => CoralFailure::InvalidInput,
        coral_provider::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
            | coral_workspace::Error::PermissionDenied(_)
            | coral_workspace::Error::WorkspaceArchived,
        ) => CoralFailure::AuthorityLost,
        other => return other.into(),
    };
    Error::InvocationRejected(failure)
}
