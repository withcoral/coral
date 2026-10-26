//! `coral.workspace.updatePolicy`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. The call sets the exact Operation Access State
//! of 1–100 Operations of one Provider, each named by its Call Path relative
//! to the Provider's Call Root. It names a Provider under `coral.providers`,
//! so it never reaches a Built-in Operation.

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

/// A Provider's policy is what only an Owner changes.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Configure;

/// The most Operations one call sets.
const MAX_OPERATIONS: usize = 100;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinOperation::UpdatePolicy.key(),
        summary: "Change a Provider's Operation policy",
        presentation: OperationPresentation::new(
            Some("Update policy".to_string()),
            "Enable, require approval for, or disable Operations of one Provider in this \
             Workspace."
                .to_string(),
            false,
            vec!["providers".to_string(), "policy".to_string()],
            Some(
                "Pass the namespace of a Provider whose Operations are called under \
                 coral.providers.<namespace>, and 1 to 100 of its Operations, each by its path \
                 under that root. An Operation you do not name keeps its state. All the states \
                 change, or none does: a namespace that no Provider holds fails with \
                 provider_not_found, and a path that names no Operation fails with \
                 operation_not_found. Read the current rules with getPolicy. This Operation \
                 cannot change the policy of Coral's own Operations."
                    .to_string(),
            ),
        ),
        // Idempotent only: the default Operation Access State is approval_required.
        hints: OperationHints::new(None, None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<UpdatePolicyResult>(Contract::Serialize),
    }
}

/// `updatePolicy`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        description = "The Provider Namespace, such as github. The Provider's Operations are \
                       called under coral.providers.<namespace>."
    )]
    namespace: String,
    #[schemars(
        description = "The Operation Access State to set for each Operation, keyed by its path \
                       under coral.providers.<namespace>, such as repos.get. enabled runs a call \
                       at once, approval_required waits for a person to approve it, and \
                       disabled refuses it.",
        extend("minProperties" = 1, "maxProperties" = MAX_OPERATIONS)
    )]
    operations: BTreeMap<String, BuiltinAccessState>,
}

/// `updatePolicy`'s arguments, checked by every rule that needs no Workspace
/// state.
#[derive(Debug, Clone)]
pub struct UpdatePolicyArguments {
    namespace: String,
    operations: BTreeMap<String, BuiltinAccessState>,
}

impl UpdatePolicyArguments {
    /// A valid Provider Namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// 1–100 states, keyed by Call Path relative to the Provider's Call Root.
    #[must_use]
    pub const fn operations(&self) -> &BTreeMap<String, BuiltinAccessState> {
        &self.operations
    }

    /// Reads the arguments: the shape first, then the namespace syntax and
    /// how many Operations they name.
    ///
    /// # Errors
    ///
    /// Returns every argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        let mut problems = Vec::new();
        if !crate::provider_namespace_is_valid(&arguments.namespace) {
            problems.push(ArgumentProblem::new("namespace", NAMESPACE_REASON));
        }
        if !(1..=MAX_OPERATIONS).contains(&arguments.operations.len()) {
            problems.push(ArgumentProblem::new(
                "operations",
                format!("name 1 to {MAX_OPERATIONS} Operations"),
            ));
        }
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self {
            namespace: arguments.namespace,
            operations: arguments.operations,
        })
    }
}

/// Arguments that use every part of `updatePolicy`'s input schema.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({
        "namespace": "github",
        "operations": {
            "repos.get": "enabled",
            "issues.create": "approval_required",
            "repos.delete": "disabled",
        },
    })]
}

/// What `updatePolicy` returns: the state of each sent path after the call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct UpdatePolicyResult {
    /// The Provider Namespace.
    #[schemars(
        description = "The Provider Namespace. Its Operations are called under \
                       coral.providers.<namespace>."
    )]
    pub namespace: String,
    /// Each sent path's state after the call.
    #[schemars(
        description = "The Operation Access State of each Operation the call named, keyed by its \
                       path under coral.providers.<namespace>."
    )]
    pub operations: BTreeMap<String, BuiltinAccessState>,
}

impl UpdatePolicyResult {
    /// The Operation Result.
    #[must_use]
    pub fn into_operation_result(self) -> Value {
        operation_result(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArgumentProblem, UpdatePolicyArguments};

    // Catches: an empty or oversized batch, or a state no Operation takes,
    // that the call reads as valid.
    #[test]
    fn an_argument_problem_names_what_to_correct() {
        let oversized = (0..=100)
            .map(|index| (format!("operation{index}"), serde_json::json!("enabled")))
            .collect::<serde_json::Map<_, _>>();
        for (operations, field, reason) in [
            (
                serde_json::json!({}),
                "operations",
                "name 1 to 100 Operations",
            ),
            (
                serde_json::Value::Object(oversized),
                "operations",
                "name 1 to 100 Operations",
            ),
            (
                serde_json::json!({ "repos.get": "open" }),
                "operations.repos.get",
                "expected one of `enabled`, `approval_required`, `disabled`",
            ),
        ] {
            let problems = UpdatePolicyArguments::parse(&serde_json::json!({
                "namespace": "github",
                "operations": operations,
            }))
            .expect_err("a problem");
            assert_eq!(problems, [ArgumentProblem::new(field, reason)], "{field}");
        }
    }
}
