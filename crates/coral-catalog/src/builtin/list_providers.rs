//! `coral.workspace.listProviders`: the schemas a Program is typed from, and
//! the result Coral writes against the output schema. It takes no arguments.

use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::Serialize;
use serde_json::Value;

use super::{
    ArgumentProblem, BuiltinOperation, Definition, ProviderFacts, derived_schema,
    empty_object_schema, operation_result, words_schema,
};
use crate::{OperationHints, OperationPresentation};

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinOperation::ListProviders.key(),
        summary: "List the Providers in this Workspace",
        presentation: OperationPresentation::new(
            Some("List providers".to_string()),
            "List every Provider in this Workspace, including disabled ones and Coral's \
             own, in the order they were created."
                .to_string(),
            false,
            vec!["providers".to_string()],
            Some(
                "Pass an empty object. A namespace in the list is taken, even when its \
                 Provider is disabled. Call a Provider's Operations under \
                 coral.providers.<namespace>."
                    .to_string(),
            ),
        ),
        // Read-only: the default Operation Access State is enabled.
        hints: OperationHints::new(Some(true), None, Some(true), None),
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
}

impl ListProvidersResult {
    /// The Operation Result.
    #[must_use]
    pub fn into_operation_result(self) -> Value {
        operation_result(&self)
    }
}

/// One `listProviders` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(rename = "Provider", deny_unknown_fields, description = "")]
pub struct ListedProvider {
    /// The Provider Namespace; `None` for the Built-in Provider, whose
    /// Operations are under `coral.workspace`.
    #[schemars(
        description = "The Provider Namespace. Its Operations are called under \
                       coral.providers.<namespace>. Null for Coral's own Provider, whose \
                       Operations are called under coral.workspace."
    )]
    pub namespace: Option<String>,
    /// Whether the Provider accepts calls.
    #[schemars(
        description = "Whether the Provider accepts calls. A disabled Provider still holds its \
                       namespace."
    )]
    pub state: ListedProviderState,
    /// What the caller needs to call it.
    #[serde(flatten)]
    pub facts: ProviderFacts,
}

/// Whether a listed Provider accepts calls. A disabled one still holds its
/// namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListedProviderState {
    /// It accepts calls.
    Active,
    /// It holds its namespace but accepts no calls.
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

/// The only arguments `listProviders` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({})]
}
