//! `coral.getPolicy`: the schemas a Program is typed from, the
//! arguments Coral reads against the input schema, and the result it writes
//! against the output schema. The result holds the exact rules a Workspace
//! Owner set that apply to the Tools the caller can read, keyed by complete
//! Call Path, so a Program can find a Tool that discovery hides because a
//! rule disables it.

use std::collections::BTreeMap;

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace reads its policy. Discovery
/// already shows each Principal what the policy decides.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

pub(super) fn definition() -> Definition {
    Definition {
        key: BuiltinTool::GetPolicy.key(),
        summary: "Read the tool policy of this workspace",
        presentation: ToolPresentation::new(
            Some("Get policy".to_string()),
            "Read the rules a workspace owner set for the tools you can call in this \
             workspace, including the disabled tools that search does not show."
                .to_string(),
            false,
            vec!["Policy".to_string()],
            Some(
                "Rules are keyed by complete call path, such as \
                 coral.providers.github.repos.get, coral.search or coral.shared.mail.remind. \
                 They cover every provider's tools, the built-in tools, the shared tools and \
                 your own private tools. Pass under, such as coral.providers.github, to read \
                 only the tools under that path. A tool without a rule is left out: it takes \
                 its default, which search shows. No tool is disabled by default, so rules \
                 names every disabled tool."
                    .to_string(),
            ),
        ),
        // Read-only: the default Tool Access State is enabled.
        hints: ToolHints::new(Some(true), None, Some(true), None),
        input: derived_schema::<Arguments>(Contract::Deserialize),
        output: derived_schema::<GetPolicyResult>(Contract::Serialize),
        declared: Declared::Once,
        enabled_by_default: false,
    }
}

/// `getPolicy`'s arguments as a Program writes them.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
struct Arguments {
    #[serde(default, deserialize_with = "super::present")]
    #[schemars(
        with = "String",
        description = "Read only the tools under this call path, such as \
                       coral.providers.github or coral.shared. Omit it to read every rule."
    )]
    under: Option<String>,
}

/// Why `under` is not a Call Path prefix.
const UNDER_REASON: &str = "use a call path, such as `coral.providers.github`";

/// `getPolicy`'s arguments, checked by every rule that needs no Workspace
/// state.
#[derive(Debug, Clone)]
pub struct GetPolicyArguments {
    under: Option<String>,
}

impl GetPolicyArguments {
    /// The Call Path the rules are under, or `None` for every rule.
    #[must_use]
    pub fn under(&self) -> Option<&str> {
        self.under.as_deref()
    }

    /// Reads the arguments: the shape first, then the syntax of `under`.
    ///
    /// # Errors
    ///
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        // serde also reads a struct from a sequence, which the schema forbids.
        if !arguments.is_object() {
            return Err(vec![ArgumentProblem::new("", "pass an object")]);
        }
        let arguments: Arguments = super::read(arguments).map_err(|problem| vec![problem])?;
        if arguments
            .under
            .as_deref()
            .is_some_and(|under| !call_path_prefix_is_valid(under))
        {
            return Err(vec![ArgumentProblem::new("under", UNDER_REASON)]);
        }
        Ok(Self {
            under: arguments.under,
        })
    }
}

/// Whether `value` is `coral` or a dotted path under it within the Call
/// Path bound, each segment as a Tool-relative path's: not empty, with no
/// control character.
fn call_path_prefix_is_valid(value: &str) -> bool {
    let mut segments = value.split('.');
    value.len() <= crate::SEARCH_PATH_MAX_BYTES
        && !value.chars().any(char::is_control)
        && segments.next() == Some("coral")
        && segments.all(|segment| !segment.is_empty())
}

/// Arguments that use every part of `getPolicy`'s input schema.
#[cfg(test)]
pub(super) fn valid_arguments() -> Vec<Value> {
    vec![
        serde_json::json!({}),
        serde_json::json!({ "under": "coral.providers.github" }),
    ]
}

/// What `getPolicy` returns: the exact rules the caller reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct GetPolicyResult {
    /// Each exact rule, keyed by its Tool's complete Call Path.
    #[schemars(
        description = "The tool access state a workspace owner set for each tool, \
                       keyed by its complete call path, such as coral.providers.github.repos.get. \
                       enabled runs a call at once, approval_required waits for a person to \
                       approve it, and disabled refuses it."
    )]
    pub rules: BTreeMap<String, crate::ToolAccessState>,
}

impl GetPolicyResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArgumentProblem, GetPolicyArguments};

    // Catches: an `under` that is not a Call Path read as one, or a valid
    // one refused.
    #[test]
    fn under_is_a_call_path() {
        for under in [
            "coral",
            "coral.providers.github",
            "coral.private.people",
            "coral.shared",
            "coral.providers.github.wire-name",
        ] {
            let parsed = GetPolicyArguments::parse(&serde_json::json!({ "under": under }))
                .unwrap_or_else(|problems| panic!("{under}: {problems:?}"));
            assert_eq!(parsed.under(), Some(under));
        }
        for under in [
            "",
            "github",
            "coral.",
            "coral..x",
            "coral.a\u{7}b",
            "Coral.search",
        ] {
            assert_eq!(
                GetPolicyArguments::parse(&serde_json::json!({ "under": under }))
                    .expect_err("a problem"),
                [ArgumentProblem::new(
                    "under",
                    "use a call path, such as `coral.providers.github`"
                )],
                "{under}"
            );
        }
    }
}
