//! `coral.workspace.getPolicy`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. The result holds the exact rules a Workspace
//! Owner set for one Provider, keyed by Call Path, so a Program can find an
//! Operation that discovery hides because a rule disables it.

use std::collections::BTreeMap;

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    ArgumentProblem, BuiltinAccessState, BuiltinOperation, Definition, NAMESPACE_REASON,
    derived_schema, operation_result,
};
use crate::{OperationHints, OperationPresentation};

/// Any Principal that can inspect the Workspace reads its policy. Discovery
/// already shows each Principal what the policy decides.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinOperation::GetPolicy.key(),
        summary: "Read a Provider's Operation policy",
        presentation: OperationPresentation::new(
            Some("Get policy".to_string()),
            "Read the rules a Workspace Owner set for the Operations of one Provider in this \
             Workspace, including the disabled Operations that search does not show."
                .to_string(),
            false,
            vec!["providers".to_string(), "policy".to_string()],
            Some(
                "Pass the namespace of a Provider whose Operations are called under \
                 coral.providers.<namespace>. A namespace that no Provider holds fails with \
                 provider_not_found. An Operation without a rule is left out of rules: it takes \
                 its default, which search shows. No Operation is disabled by default, so \
                 rules names every disabled Operation."
                    .to_string(),
            ),
        ),
        // Read-only: the default Operation Access State is enabled.
        hints: OperationHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<GetPolicyResult>(Contract::Serialize),
    }
}

/// `getPolicy`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        description = "The Provider Namespace, such as github. The Provider's Operations are \
                       called under coral.providers.<namespace>."
    )]
    namespace: String,
}

/// `getPolicy`'s arguments, checked by every rule that needs no Workspace
/// state.
#[derive(Debug, Clone)]
pub struct GetPolicyArguments {
    namespace: String,
}

impl GetPolicyArguments {
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

/// The only arguments `getPolicy` takes, for its conformance test.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({ "namespace": "github" })]
}

/// What `getPolicy` returns: one Provider's exact rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct GetPolicyResult {
    /// The Provider Namespace.
    #[schemars(
        description = "The Provider Namespace. Its Operations are called under \
                       coral.providers.<namespace>."
    )]
    pub namespace: String,
    /// Each exact rule, keyed by its Operation's Call Path relative to the
    /// Provider's Call Root.
    #[schemars(
        description = "The Operation Access State a Workspace Owner set for each Operation, \
                       keyed by its path under coral.providers.<namespace>, such as repos.get. \
                       enabled runs a call at once, approval_required waits for a person to \
                       approve it, and disabled refuses it."
    )]
    pub rules: BTreeMap<String, BuiltinAccessState>,
}

impl GetPolicyResult {
    /// The Operation Result.
    #[must_use]
    pub fn into_operation_result(self) -> Value {
        operation_result(&self)
    }
}
