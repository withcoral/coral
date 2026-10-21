//! `coral.workspace.listProviders`: the schemas a Program is typed from, and
//! the result Coral writes against the output schema. It takes no arguments.

use serde::Serialize;
use serde_json::{Value, json};

use super::{
    ArgumentProblem, BuiltinCredentialRoute, BuiltinOperation, Definition, ProviderFacts,
    authentication_names, operation_result,
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
        input: json!({ "type": "object", "properties": {}, "additionalProperties": false }),
        output: json!({
            "type": "object",
            "required": ["providers"],
            "properties": {
                "providers": {
                    "type": "array",
                    "description": "Every Provider in this Workspace, in the order they were created. Coral's own comes first.",
                    "items": { "$ref": "#/$defs/Provider" }
                }
            },
            "additionalProperties": false,
            "$defs": {
                "Provider": {
                    "type": "object",
                    "required": [
                        "namespace", "displayName", "state", "credentialRoute",
                        "authentication", "operations", "accountRequired"
                    ],
                    "properties": {
                        "namespace": {
                            "type": ["string", "null"],
                            "description": "The Provider Namespace. Its Operations are called under coral.providers.<namespace>. Null for Coral's own Provider, whose Operations are called under coral.workspace."
                        },
                        "displayName": { "type": "string", "description": "The Provider display name." },
                        "state": {
                            "enum": ["active", "disabled"],
                            "description": "Whether the Provider accepts calls. A disabled Provider still holds its namespace."
                        },
                        "credentialRoute": {
                            "enum": BuiltinCredentialRoute::names(),
                            "description": "Whose account Invocations use."
                        },
                        "authentication": {
                            "enum": authentication_names(),
                            "description": "How Coral sends the Provider's credentials."
                        },
                        "operations": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "How many of the Provider's Operations you can discover and call."
                        },
                        "accountRequired": {
                            "type": "boolean",
                            "description": "True when you must connect your own Account before your calls can use the Provider."
                        }
                    },
                    "additionalProperties": false
                }
            }
        }),
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ListProvidersResult {
    /// Each Provider, with what it offers the caller.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ListedProvider {
    /// The Provider Namespace; `None` for the Built-in Provider, whose
    /// Operations are under `coral.workspace`.
    pub namespace: Option<String>,
    /// Whether the Provider accepts calls.
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

/// The only arguments `listProviders` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![json!({})]
}
