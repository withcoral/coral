//! `coral.workspace.getProvider`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. The result spells a Provider's Provider
//! Configuration in the shapes `addProvider` takes, so a Program can read
//! it, change some fields, and send back only those. The facts Coral set
//! itself (an application it registered, and an MCP server's issuer and
//! resource) read beside them. `addProvider` rejects them, so they are
//! never saved as a person's choices.

use std::collections::BTreeMap;

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinTool, Definition, ListedProviderState,
    NAMESPACE_REASON, derived_schema, tool_result,
};
use crate::{
    AuthenticationConfiguration, CredentialField, ServerSelection, ToolHints, ToolPresentation,
};

mod output;
#[cfg(test)]
pub(super) mod tests;

pub use output::ProviderSourceOutput;

/// A Provider's configuration is what only an Owner changes, so only an
/// Owner reads it.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Configure;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::GetProvider.key(),
        summary: "Read a Provider's configuration",
        presentation: ToolPresentation::new(
            Some("Get provider".to_string()),
            "Read the configuration of one Provider in this Workspace, in the shapes \
             addProvider takes."
                .to_string(),
            false,
            vec!["providers".to_string()],
            Some(
                "Pass the namespace of a Provider whose Tools are called under \
                 coral.providers.<namespace>. A namespace that no Provider holds fails with \
                 provider_not_found. The result never holds a secret or a credential value: \
                 a credential part with source literal and no value holds a fixed value that \
                 Coral does not return. Coral sets authentication.issuer, \
                 authentication.resource and authentication.client.registration itself; \
                 remove them before you send authentication to addProvider."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<GetProviderResult>(Contract::Serialize),
    }
}

/// `getProvider`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        description = "The Provider Namespace, such as github. The Provider's Tools are \
                       called under coral.providers.<namespace>."
    )]
    namespace: String,
}

/// `getProvider`'s arguments, checked by every rule that needs no Workspace
/// state.
#[derive(Debug, Clone)]
pub struct GetProviderArguments {
    namespace: String,
}

impl GetProviderArguments {
    /// A valid Provider Namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Reads the arguments: the shape first, then the namespace syntax.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        if !crate::provider_namespace_is_valid(&arguments.namespace) {
            return Err(vec![ArgumentProblem::new("namespace", NAMESPACE_REASON)]);
        }
        Ok(Self {
            namespace: arguments.namespace,
        })
    }
}

/// What `getProvider` returns: one Provider and its Provider Configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(deny_unknown_fields, description = "")]
pub struct GetProviderResult {
    /// The Provider Namespace.
    #[schemars(description = "The Provider Namespace. Its Tools are called under \
                       coral.providers.<namespace>.")]
    pub namespace: String,
    /// Whether the Provider accepts calls.
    #[schemars(
        description = "Whether the Provider accepts calls. A disabled Provider still holds its \
                       namespace."
    )]
    pub state: ListedProviderState,
    /// Where Coral last read the Provider from.
    #[schemars(description = "")]
    pub source: ProviderSourceOutput,
    /// Whose Account Invocations use.
    #[schemars(description = "Whose account Invocations use.")]
    pub credential_route: BuiltinCredentialRoute,
    /// The configuration `addProvider` takes.
    #[serde(flatten)]
    pub configuration: ConfigurationOutput,
}

impl GetProviderResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// A Provider's Provider Configuration in the shapes `addProvider` takes,
/// and the facts Coral set itself. It holds no secret, and no fixed value of
/// a credential part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(deny_unknown_fields, description = "")]
pub struct ConfigurationOutput {
    #[schemars(description = "The Provider display name.")]
    display_name: String,
    #[schemars(
        description = "Server choice per server declaration, keyed by declaration pointer such \
                       as #/servers."
    )]
    servers: BTreeMap<String, output::ServerOutput>,
    authentication: output::AuthenticationOutput,
    #[schemars(description = "Non-secret headers sent with every request.")]
    request_headers: BTreeMap<String, String>,
    #[schemars(
        description = "The values a person enters when they sign in, keyed by field name, such \
                       as token."
    )]
    credential_fields: BTreeMap<String, output::CredentialFieldOutput>,
}

impl ConfigurationOutput {
    /// Reads a Provider Configuration. A fixed value of a credential part is
    /// left out: it can hold a credential, and Run evidence stores the result.
    #[must_use]
    pub fn new(
        display_name: String,
        servers: BTreeMap<String, ServerSelection>,
        authentication: AuthenticationConfiguration,
        request_headers: BTreeMap<String, String>,
        credential_fields: BTreeMap<String, CredentialField>,
    ) -> Self {
        Self {
            display_name,
            servers: servers
                .into_iter()
                .map(|(declaration, server)| (declaration, server.into()))
                .collect(),
            authentication: authentication.into(),
            request_headers,
            credential_fields: credential_fields
                .into_iter()
                .map(|(name, field)| (name, field.into()))
                .collect(),
        }
    }
}
