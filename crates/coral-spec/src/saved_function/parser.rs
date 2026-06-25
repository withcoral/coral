use std::collections::HashSet;
use std::fmt;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::{ManifestError, Result};

use super::model::{
    SavedFunctionArgumentSpec, SavedFunctionArgumentType, SavedFunctionImplementationSpec,
    SavedFunctionPublishSpec, SavedFunctionSpec, SavedFunctionValidationSpec,
};
use super::validation::validate_raw_saved_function;

/// One `saved_function` input as authored under the `inputs` map.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedFunctionInputSpec {
    #[serde(rename = "type")]
    data_type: SavedFunctionArgumentType,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    description: String,
}

impl SavedFunctionInputSpec {
    fn into_argument(self, name: String) -> SavedFunctionArgumentSpec {
        SavedFunctionArgumentSpec {
            name,
            data_type: self.data_type,
            required: self.required,
            description: self.description,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RawSavedFunctionInputs(pub(super) Vec<SavedFunctionArgumentSpec>);

impl<'de> Deserialize<'de> for RawSavedFunctionInputs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawSavedFunctionInputsVisitor;

        impl<'de> Visitor<'de> for RawSavedFunctionInputsVisitor {
            type Value = RawSavedFunctionInputs;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a mapping of input names to input specs")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen = HashSet::new();
                let mut arguments = Vec::new();
                while let Some((name, input)) =
                    map.next_entry::<String, SavedFunctionInputSpec>()?
                {
                    if !seen.insert(name.clone()) {
                        return Err(de::Error::custom(format!(
                            "saved_function input '{name}' is declared more than once"
                        )));
                    }
                    arguments.push(input.into_argument(name));
                }
                Ok(RawSavedFunctionInputs(arguments))
            }
        }

        deserializer.deserialize_map(RawSavedFunctionInputsVisitor)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawSavedFunctionSpec {
    pub(super) kind: String,
    pub(super) name: String,
    #[serde(default)]
    pub(super) description: String,
    #[serde(default)]
    pub(super) inputs: RawSavedFunctionInputs,
    pub(super) implementation: SavedFunctionImplementationSpec,
    #[serde(default)]
    pub(super) validation: SavedFunctionValidationSpec,
    pub(super) publish: SavedFunctionPublishSpec,
}

/// Parses and statically validates one `saved_function` YAML document.
///
/// # Errors
///
/// Returns [`ManifestError`] when the YAML cannot be parsed, has an unsupported
/// shape, or violates saved_function-local invariants.
pub fn parse_saved_function_yaml(raw: &str) -> Result<SavedFunctionSpec> {
    let raw: RawSavedFunctionSpec = serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    validate_raw_saved_function(raw)
}

#[cfg(test)]
mod tests {
    use super::super::{SavedFunctionImplementationSpec, SavedFunctionValidationValue};
    use super::parse_saved_function_yaml;

    #[derive(Clone, Copy)]
    enum ExpectedError {
        Exact(&'static str),
        Contains(&'static str),
    }

    fn valid_saved_function() -> &'static str {
        r"
kind: saved_function
name: github_review_queue
description: GitHub PR review queue
inputs:
  owner:
    type: string
    required: true
  repo:
    type: string
    required: true
implementation:
  kind: coral_sql
  query: |
    select *
    from github.pulls(owner => $owner, repo => $repo)
validation:
  args:
    owner: withcoral
    repo: coral
publish:
  table_function:
    schema: saved_functions
    name: github_review_queue
  mcp:
    name: github_review_queue
"
    }

    fn saved_function_with(replacements: &[(&str, &str)]) -> String {
        let mut yaml = valid_saved_function().to_string();
        for (from, to) in replacements {
            assert!(
                yaml.contains(from),
                "saved_function test fixture does not contain replacement target: {from}"
            );
            yaml = yaml.replacen(from, to, 1);
        }
        yaml
    }

    fn saved_function_without(block: &str) -> String {
        saved_function_with(&[(block, "")])
    }

    fn publish_block() -> &'static str {
        r"publish:
  table_function:
    schema: saved_functions
    name: github_review_queue
  mcp:
    name: github_review_queue
"
    }

    fn saved_function_yaml_with_mcp_name(name: &str) -> String {
        saved_function_with(&[(
            "  mcp:\n    name: github_review_queue\n",
            &format!("  mcp:\n    name: {name}\n"),
        )])
    }

    fn assert_saved_function_error(name: &str, yaml: &str, expected: ExpectedError) {
        let error = parse_saved_function_yaml(yaml).unwrap_err();
        match expected {
            ExpectedError::Exact(message) => assert_eq!(error.to_string(), message, "{name}"),
            ExpectedError::Contains(message) => {
                assert!(
                    error.to_string().contains(message),
                    "{name}: expected error to contain {message:?}, got {error}"
                );
            }
        }
    }

    #[test]
    fn parse_saved_function_yaml_accepts_valid_saved_function() {
        let saved_function =
            parse_saved_function_yaml(valid_saved_function()).expect("saved_function should parse");

        assert_eq!(saved_function.name(), "github_review_queue");
        assert_eq!(saved_function.arguments().len(), 2);
        assert!(matches!(
            saved_function.validation().args.get("owner"),
            Some(SavedFunctionValidationValue::String(owner)) if owner == "withcoral"
        ));
        assert!(matches!(
            saved_function.implementation(),
            SavedFunctionImplementationSpec::CoralSql { .. }
        ));
        assert_eq!(
            saved_function.publish().table_function.schema,
            "saved_functions"
        );
        assert_eq!(
            saved_function.publish().table_function.name,
            "github_review_queue"
        );
        assert_eq!(
            saved_function
                .publish()
                .mcp
                .as_ref()
                .map(|mcp| mcp.name.as_str()),
            Some("github_review_queue")
        );
    }

    #[test]
    fn parse_saved_function_yaml_rejects_invalid_artifacts() {
        let cases = [
            (
                "unknown kind",
                saved_function_with(&[("kind: saved_function", "kind: source")]),
                ExpectedError::Exact("saved_function kind must be 'saved_function', got 'source'"),
            ),
            (
                "duplicate inputs",
                saved_function_with(&[(
                    "  repo:\n    type: string\n    required: true\n",
                    "  owner:\n    type: string\n    required: true\n",
                )]),
                ExpectedError::Contains("saved_function input 'owner' is declared more than once"),
            ),
            (
                "mixed-case saved_function name",
                saved_function_with(&[("name: github_review_queue", "name: Demo")]),
                ExpectedError::Exact("saved_function name 'Demo' must be lowercase"),
            ),
            (
                "mixed-case input name",
                saved_function_with(&[("  owner:", "  Owner:")]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' input name 'Owner' must be lowercase",
                ),
            ),
            (
                "unknown field",
                saved_function_with(&[("description:", "presentation: {}\ndescription:")]),
                ExpectedError::Contains("unknown field `presentation`"),
            ),
            (
                "empty coral_sql query",
                saved_function_with(&[(
                    "  query: |\n    select *\n    from github.pulls(owner => $owner, repo => $repo)",
                    "  query: '   '",
                )]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' coral_sql query must not be empty",
                ),
            ),
            (
                "missing required validation arg",
                saved_function_with(&[("    owner: withcoral\n", "")]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' validation.args must include required input 'owner'",
                ),
            ),
            (
                "unknown validation arg",
                saved_function_with(&[("    repo: coral", "    not_declared: coral")]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' validation arg 'not_declared' is not declared as an input",
                ),
            ),
            (
                "validation arg type mismatch",
                saved_function_with(&[("  repo:\n    type: string", "  repo:\n    type: integer")]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' validation arg 'repo' expected integer, got string",
                ),
            ),
            (
                "reserved table-function schema",
                saved_function_with(&[(
                    "schema: saved_functions",
                    "schema: __coral_saved_functions",
                )]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' table_function publish schema '__coral_saved_functions' is reserved",
                ),
            ),
            (
                "missing publish",
                saved_function_without(publish_block()),
                ExpectedError::Contains("missing field `publish`"),
            ),
            (
                "mixed-case table-function schema",
                saved_function_with(&[("schema: saved_functions", "schema: SavedFunctions")]),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' table_function publish schema 'SavedFunctions' must be lowercase",
                ),
            ),
            (
                "mixed-case mcp name",
                saved_function_yaml_with_mcp_name("Demo"),
                ExpectedError::Exact(
                    "saved_function 'github_review_queue' mcp publish name 'Demo' must be lowercase",
                ),
            ),
        ];

        for (name, yaml, expected) in cases {
            assert_saved_function_error(name, &yaml, expected);
        }
    }

    #[test]
    fn parse_saved_function_yaml_allows_builtin_mcp_publish_name() {
        let spec = parse_saved_function_yaml(&saved_function_yaml_with_mcp_name("sql"))
            .expect("built-in MCP names are prefixed by the MCP adapter");

        assert_eq!(spec.publish().mcp.as_ref().expect("mcp").name, "sql");
    }
}
