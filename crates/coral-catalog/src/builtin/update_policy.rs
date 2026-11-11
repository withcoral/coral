//! `coral.updatePolicy`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. The call sets the exact Tool Access State
//! of 1–100 Tools, each named by its complete Call Path: a Provider's, a
//! Shared Tool or one of the caller's own Private Tools. It never reaches a
//! Built-in Tool.

use std::collections::BTreeMap;

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Tool policy is what only an Owner changes.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Configure;

/// The most Tools one call sets.
const MAX_TOOLS: usize = 100;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::UpdatePolicy.key(),
        summary: "Change the Tool policy of this Workspace",
        presentation: ToolPresentation::new(
            Some("Update policy".to_string()),
            "Enable, require approval for, or disable Tools in this Workspace: a \
             Provider's Tools, Shared Tools and your own Private Tools."
                .to_string(),
            false,
            vec!["policy".to_string()],
            Some(
                "Pass 1 to 100 Tools, each by its complete Call Path, such as \
                 coral.providers.github.repos.delete or coral.shared.mail.remind. A Tool you \
                 do not name keeps its state. All the states change, or none does, and the \
                 failure names each refused path: a path that names no Tool fails with \
                 tool_not_found, and one that names a Built-in Tool, such as coral.search, \
                 fails with invalid_input. Set a Built-in Tool's policy in the Coral UI. Read \
                 the current rules with getPolicy."
                    .to_string(),
            ),
        ),
        // A sent state replaces the previous one; sending it again changes
        // nothing more. It reaches nothing outside Coral. Not read-only, so the
        // default Tool Access State is approval_required.
        hints: ToolHints::new(Some(false), Some(true), Some(true), Some(false)),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<UpdatePolicyResult>(Contract::Serialize),
        declared: Declared::Once,
        enabled_by_default: false,
    }
}

/// `updatePolicy`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[schemars(
        description = "The Tool Access State to set for each Tool, keyed by its complete \
                       Call Path, such as coral.providers.github.repos.get. enabled runs a call \
                       at once, approval_required waits for a person to approve it, and \
                       disabled refuses it.",
        extend("minProperties" = 1, "maxProperties" = MAX_TOOLS)
    )]
    tools: BTreeMap<String, crate::ToolAccessState>,
}

/// `updatePolicy`'s arguments, checked by every rule that needs no Workspace
/// state.
#[derive(Debug, Clone)]
pub struct UpdatePolicyArguments {
    tools: BTreeMap<String, crate::ToolAccessState>,
}

impl UpdatePolicyArguments {
    /// 1–100 states, keyed by complete Call Path.
    #[must_use]
    pub const fn tools(&self) -> &BTreeMap<String, crate::ToolAccessState> {
        &self.tools
    }

    /// Reads the arguments: the shape first, then how many Tools they name.
    ///
    /// # Errors
    ///
    /// Returns every argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        if !(1..=MAX_TOOLS).contains(&arguments.tools.len()) {
            return Err(vec![ArgumentProblem::new(
                "tools",
                format!("name 1 to {MAX_TOOLS} Tools"),
            )]);
        }
        Ok(Self {
            tools: arguments.tools,
        })
    }
}

/// Arguments that use every part of `updatePolicy`'s input schema.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![serde_json::json!({
        "tools": {
            "coral.providers.github.repos.get": "enabled",
            "coral.shared.mail.remind": "approval_required",
            "coral.private.people.greet": "disabled",
        },
    })]
}

/// What `updatePolicy` returns: the state of each sent path after the call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct UpdatePolicyResult {
    /// Each sent path's state after the call.
    #[schemars(
        description = "The Tool Access State of each Tool the call named, keyed by its \
                       complete Call Path."
    )]
    pub tools: BTreeMap<String, crate::ToolAccessState>,
}

impl UpdatePolicyResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArgumentProblem, UpdatePolicyArguments};

    // Catches: an empty or oversized batch, or a state no Tool takes,
    // that the call reads as valid.
    #[test]
    fn an_argument_problem_names_what_to_correct() {
        let oversized = (0..=100)
            .map(|index| (format!("tool{index}"), serde_json::json!("enabled")))
            .collect::<serde_json::Map<_, _>>();
        for (tools, field, reason) in [
            (serde_json::json!({}), "tools", "name 1 to 100 Tools"),
            (
                serde_json::Value::Object(oversized),
                "tools",
                "name 1 to 100 Tools",
            ),
            (
                serde_json::json!({ "coral.search": "open" }),
                "tools.coral.search",
                "expected one of `enabled`, `approval_required`, `disabled`",
            ),
        ] {
            let problems = UpdatePolicyArguments::parse(&serde_json::json!({
                "tools": tools,
            }))
            .expect_err("a problem");
            assert_eq!(problems, [ArgumentProblem::new(field, reason)], "{field}");
        }
    }
}
