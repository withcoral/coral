//! `coral.workspace.listProviders`: the schemas a Program is typed from, and
//! the result Coral writes against the output schema. It takes no arguments.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::Serialize;
use serde_json::Value;

use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinTool, Definition, ProviderFacts,
    derived_schema, empty_object_schema, tool_result, words_schema,
};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace lists its Providers.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::ListProviders.key(),
        summary: "List the Providers in this Workspace",
        presentation: ToolPresentation::new(
            Some("List providers".to_string()),
            "List every Provider in this Workspace, including disabled ones and Coral's \
             own, in the order they were created. Also list the Providers your addProvider \
             calls asked for that a person has not finished yet."
                .to_string(),
            false,
            vec!["providers".to_string()],
            Some(
                "Pass an empty object. Call a Provider's Tools under its callRoot. A \
                 namespace is taken when coral.providers.<namespace> is a callRoot under \
                 providers, even when its Provider is disabled. A Provider that addProvider \
                 asked a person to finish is under setups until it is published, and then \
                 under providers. The person can publish it under another namespace. When its \
                 callRoot is in neither list, the person renamed it, did not finish it in \
                 time, or canceled it: ask the person before you call addProvider again."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: empty_object_schema(),
        output: derived_schema::<ListProvidersResult>(Contract::Serialize),
    }
}

/// Accepts only an empty arguments object. A Program that calls with no
/// argument sends one.
pub(super) fn parse(arguments: &Value) -> Result<(), Vec<ArgumentProblem>> {
    let Some(fields) = arguments.as_object() else {
        return Err(vec![ArgumentProblem::new("", "pass an empty object")]);
    };
    if fields.is_empty() {
        return Ok(());
    }
    Err(fields
        .keys()
        .map(|field| ArgumentProblem::new(field.clone(), "unknown field; pass an empty object"))
        .collect())
}

/// What `listProviders` returns: every Provider in the Workspace, in the
/// order they were created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct ListProvidersResult {
    /// Each Provider, with what it offers the caller.
    #[schemars(
        description = "Every Provider in this Workspace, in the order they were created. Coral's \
                       own comes first."
    )]
    pub providers: Vec<ListedProvider>,
    /// The Providers the caller's `addProvider` calls asked a person to
    /// finish, which a person can still finish.
    #[schemars(
        description = "The Providers your addProvider calls asked a person to finish, which \
                       nobody has published yet, oldest first. Each expires an hour after \
                       addProvider started it."
    )]
    pub setups: Vec<ListedSetup>,
}

impl ListProvidersResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// One `listProviders` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "Provider", deny_unknown_fields, description = "")]
pub struct ListedProvider {
    /// The Call Root its Tools are called under, such as
    /// `coral.providers.github`, or `coral.workspace` for the Built-in Provider.
    #[schemars(description = "The path you call the Provider's Tools under: \
                       coral.providers.<namespace> for a Provider a person added, or \
                       coral.workspace for Coral's own.")]
    pub call_root: String,
    /// The Provider Namespace; `None` for the Built-in Provider, which has
    /// none.
    #[schemars(
        description = "The Provider Namespace, which getProvider and getPolicy take. Null for \
                       Coral's own Provider, which has none."
    )]
    pub namespace: Option<String>,
    /// Whether the Provider accepts calls.
    #[schemars(
        description = "Whether the Provider accepts calls. A disabled Provider still holds its \
                       callRoot."
    )]
    pub state: ListedProviderState,
    /// What the caller needs to call it.
    #[serde(flatten)]
    pub facts: ProviderFacts,
}

/// Whether a Provider accepts calls. A disabled one still holds its Call
/// Root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListedProviderState {
    /// It accepts calls.
    Active,
    /// It holds its Call Root but accepts no calls.
    Disabled,
}

impl JsonSchema for ListedProviderState {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ProviderState".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        words_schema(&["active", "disabled"])
    }
}

/// One Provider that an `addProvider` call asked a person to finish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "ProviderSetup", deny_unknown_fields, description = "")]
pub struct ListedSetup {
    /// The Call Root of the namespace the call asked for.
    #[schemars(
        description = "coral.providers.<namespace>, with the namespace addProvider asked \
                       for. The person can change it."
    )]
    pub call_root: String,
    /// The Credential Route the call asked for.
    #[schemars(description = "The credentialRoute addProvider asked for.")]
    pub credential_route: BuiltinCredentialRoute,
    /// What the person does next.
    #[schemars(
        description = "What the person does next: awaiting_configuration to confirm how Coral \
                       reads the source, awaiting_sign_in to sign in so that Coral can read its \
                       Tools, awaiting_discovery to try again on the page when Coral could \
                       not read the Tools after the sign-in (the person does not sign in \
                       again), or ready to add the Provider, signing in first when its route \
                       needs an account."
    )]
    pub state: ListedSetupState,
    /// The Coral UI page where a person finishes it.
    #[schemars(
        description = "The Coral UI page where a person finishes and publishes the Provider. \
                       Show it to the person again if they lost it."
    )]
    pub url: String,
}

/// What the person who finishes a listed Setup does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListedSetupState {
    /// The person confirms how Coral reads the source.
    AwaitingConfiguration,
    /// The person signs in so that Coral can read the source's Tools.
    AwaitingSignIn,
    /// The person signed in, but Coral has not read the source's Tools.
    /// The person tries again, without a new sign-in.
    AwaitingDiscovery,
    /// Coral read the source. The person adds the Provider, after signing in
    /// when its route needs an Account.
    Ready,
}

impl JsonSchema for ListedSetupState {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ProviderSetupState".into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        words_schema(&[
            "awaiting_configuration",
            "awaiting_sign_in",
            "awaiting_discovery",
            "ready",
        ])
    }
}

/// The only arguments `listProviders` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({})]
}
