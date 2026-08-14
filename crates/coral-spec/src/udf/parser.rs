use serde::Deserialize;

use crate::{ManifestError, Result};

use super::model::{
    FunctionCoralSqlImplementationSpec, FunctionDeclaredArgument, FunctionDeclaredResultColumn,
    FunctionImplementationSpec, FunctionLanguage, FunctionSpec,
    FunctionTypeScriptImplementationSpec,
};
use super::validation::validate_raw_function;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawFunctionFrontmatter {
    pub(super) name: String,
    pub(super) schema: String,
    #[serde(default)]
    pub(super) description: String,
    #[serde(default)]
    pub(super) guide: String,
    #[serde(default)]
    pub(super) language: FunctionLanguage,
    pub(super) signature: Option<RawFunctionSignature>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) struct RawFunctionSignature {
    #[serde(default)]
    pub(super) arguments: Vec<FunctionDeclaredArgument>,
    #[serde(default)]
    pub(super) result_columns: Vec<FunctionDeclaredResultColumn>,
}

#[derive(Debug)]
pub(super) struct RawFunctionSpec {
    pub(super) frontmatter: RawFunctionFrontmatter,
    pub(super) implementation: FunctionImplementationSpec,
}

/// Parses and statically validates one function artifact.
///
/// # Errors
///
/// Returns [`ManifestError`] when the frontmatter cannot be parsed, the body is
/// empty, or the artifact violates function-local invariants.
pub fn parse_function_artifact(raw: &str) -> Result<FunctionSpec> {
    let (frontmatter, body) = split_frontmatter(raw)?;
    let frontmatter: RawFunctionFrontmatter =
        serde_yaml::from_str(frontmatter).map_err(|error| {
            ManifestError::validation(format!(
                "function artifact SQL comment frontmatter is invalid: {error}"
            ))
        })?;
    let implementation = match frontmatter.language {
        FunctionLanguage::Sql => {
            FunctionImplementationSpec::CoralSql(FunctionCoralSqlImplementationSpec {
                query: body.trim().to_string(),
            })
        }
        FunctionLanguage::TypeScript => {
            FunctionImplementationSpec::TypeScript(FunctionTypeScriptImplementationSpec {
                source: body.trim().to_string(),
            })
        }
    };
    validate_raw_function(RawFunctionSpec {
        frontmatter,
        implementation,
    })
}

fn split_frontmatter(raw: &str) -> Result<(&str, &str)> {
    let Some(after_open) = raw.strip_prefix("/*") else {
        return Err(ManifestError::validation(
            "function artifact must start with SQL comment frontmatter",
        ));
    };
    let Some((frontmatter, sql)) = after_open.split_once("*/") else {
        return Err(ManifestError::validation(
            "function artifact SQL comment frontmatter must end with '*/'",
        ));
    };
    if frontmatter.trim().is_empty() {
        return Err(ManifestError::validation(
            "function artifact SQL comment frontmatter must not be empty",
        ));
    }
    Ok((frontmatter, sql))
}

#[cfg(test)]
mod tests {
    use crate::{FunctionImplementationSpec, ManifestDataType};

    use super::parse_function_artifact;

    #[derive(Clone, Copy)]
    enum ExpectedError {
        Exact(&'static str),
        Contains(&'static str),
    }

    fn valid_function() -> &'static str {
        r"/*
name: github_review_queue
schema: functions
description: GitHub PR review queue
guide: Use this function for review queue lookups.
*/

select title, html_url
from github.pulls(owner => $owner, repo => $repo)
"
    }

    fn valid_typescript_function() -> &'static str {
        r"/*
name: review_summary
schema: functions
description: Summarize a pull request review queue.
language: typescript
signature:
  arguments:
    - name: owner
      data_type: Utf8
  result_columns:
    - name: title
      data_type: Utf8
      nullable: true
*/

export async function run(owner: string): Promise<string> {
  return `queue for ${owner}`;
}
"
    }

    fn function_with(replacements: &[(&str, &str)]) -> String {
        let mut artifact = valid_function().to_string();
        for (from, to) in replacements {
            assert!(
                artifact.contains(from),
                "function test fixture does not contain replacement target: {from}"
            );
            artifact = artifact.replacen(from, to, 1);
        }
        artifact
    }

    fn assert_function_error(name: &str, artifact: &str, expected: ExpectedError) {
        let error = parse_function_artifact(artifact).unwrap_err();
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
    fn parse_function_artifact_accepts_valid_sql_function() {
        let function = parse_function_artifact(valid_function()).expect("function should parse");

        assert_eq!(function.name(), "github_review_queue");
        assert_eq!(function.group(), "functions");
        assert_eq!(
            function.guide(),
            "Use this function for review queue lookups."
        );
        let FunctionImplementationSpec::CoralSql(implementation) = function.implementation() else {
            panic!("SQL artifact should produce a Coral SQL implementation");
        };
        assert!(implementation.query.contains("github.pulls"));
    }

    #[test]
    fn parse_function_artifact_selects_typescript_and_declared_signature() {
        let function = parse_function_artifact(valid_typescript_function())
            .expect("TypeScript function should parse");

        let FunctionImplementationSpec::TypeScript(implementation) = function.implementation()
        else {
            panic!("TypeScript artifact should produce a TypeScript implementation");
        };
        assert!(implementation.source.contains("export async function run"));
        let signature = function.declared_signature().expect("declared signature");
        let [argument] = signature.arguments.as_slice() else {
            panic!("expected one declared argument");
        };
        assert_eq!(argument.name, "owner");
        assert_eq!(argument.data_type, ManifestDataType::Utf8);
        let [result_column] = signature.result_columns.as_slice() else {
            panic!("expected one declared result column");
        };
        assert_eq!(result_column.name, "title");
        assert!(result_column.nullable);
    }

    #[test]
    fn parse_function_artifact_defaults_omitted_guide() {
        let artifact =
            function_with(&[("guide: Use this function for review queue lookups.\n", "")]);

        let function = parse_function_artifact(&artifact).expect("function should parse");

        assert!(function.guide().is_empty());
    }

    #[test]
    fn parse_function_artifact_rejects_invalid_frontmatter() {
        let cases = [
            (
                "missing frontmatter",
                "select 1".to_string(),
                ExpectedError::Exact("function artifact must start with SQL comment frontmatter"),
            ),
            (
                "unterminated frontmatter",
                "/*\nname: github_review_queue\nschema: functions\nselect 1".to_string(),
                ExpectedError::Exact(
                    "function artifact SQL comment frontmatter must end with '*/'",
                ),
            ),
            (
                "empty frontmatter",
                "/**/\nselect 1".to_string(),
                ExpectedError::Exact("function artifact SQL comment frontmatter must not be empty"),
            ),
            (
                "malformed frontmatter",
                function_with(&[("name: github_review_queue", "name: [")]),
                ExpectedError::Contains("function artifact SQL comment frontmatter is invalid"),
            ),
            (
                "mixed-case function name",
                function_with(&[("name: github_review_queue", "name: Demo")]),
                ExpectedError::Exact("function name 'Demo' must be lowercase"),
            ),
            (
                "unknown field",
                function_with(&[("description:", "presentation: {}\ndescription:")]),
                ExpectedError::Contains("unknown field `presentation`"),
            ),
            (
                "empty sql body",
                function_with(&[(
                    "select title, html_url\nfrom github.pulls(owner => $owner, repo => $repo)",
                    "   ",
                )]),
                ExpectedError::Exact("function 'github_review_queue' SQL body must not be empty"),
            ),
        ];

        for (name, artifact, expected) in cases {
            assert_function_error(name, &artifact, expected);
        }
    }

    #[test]
    fn parse_function_artifact_preserves_body_after_frontmatter_comment() {
        let function = parse_function_artifact(valid_function()).expect("function should parse");
        let FunctionImplementationSpec::CoralSql(implementation) = function.implementation() else {
            panic!("SQL artifact should produce a Coral SQL implementation");
        };
        let query = &implementation.query;

        assert!(
            query.starts_with("select title"),
            "unexpected query: {query}"
        );
        assert!(!query.contains("github_review_queue"));
        assert!(!query.contains("*/"));
    }

    #[test]
    fn parse_function_artifact_rejects_typescript_without_declared_signature() {
        let artifact = function_with(&[(
            "description: GitHub PR review queue",
            "language: typescript\ndescription: GitHub PR review queue",
        )]);

        assert_function_error(
            "missing TypeScript signature",
            &artifact,
            ExpectedError::Exact(
                "function 'github_review_queue' TypeScript implementation requires a declared signature",
            ),
        );
    }

    #[test]
    fn parse_function_artifact_rejects_declared_signature_for_sql() {
        let artifact = function_with(&[(
            "description: GitHub PR review queue",
            "description: GitHub PR review queue\nsignature:\n  result_columns:\n    - name: title\n      data_type: Utf8",
        )]);

        assert_function_error(
            "declared SQL signature",
            &artifact,
            ExpectedError::Exact(
                "function 'github_review_queue' SQL implementation must not declare a signature because Coral infers it from SQL",
            ),
        );
    }

    #[test]
    fn parse_function_artifact_rejects_duplicate_declared_names() {
        let duplicate_arguments = function_with(&[(
            "description: GitHub PR review queue",
            "language: typescript\nsignature:\n  arguments:\n    - name: owner\n      data_type: Utf8\n    - name: owner\n      data_type: Int64\n  result_columns:\n    - name: title\n      data_type: Utf8",
        )]);
        let duplicate_columns = function_with(&[(
            "description: GitHub PR review queue",
            "language: typescript\nsignature:\n  result_columns:\n    - name: title\n      data_type: Utf8\n    - name: title\n      data_type: Int64",
        )]);

        assert_function_error(
            "duplicate argument",
            &duplicate_arguments,
            ExpectedError::Exact(
                "function 'github_review_queue' declares argument 'owner' more than once",
            ),
        );
        assert_function_error(
            "duplicate result column",
            &duplicate_columns,
            ExpectedError::Exact(
                "function 'github_review_queue' declares result column 'title' more than once",
            ),
        );
    }

    #[test]
    fn parse_function_artifact_rejects_empty_typescript_body_and_unknown_language() {
        let empty_body = valid_typescript_function().replace(
            "export async function run(owner: string): Promise<string> {\n  return `queue for ${owner}`;\n}",
            "   ",
        );
        let unknown_language =
            function_with(&[("description: GitHub PR review queue", "language: python")]);

        assert_function_error(
            "empty TypeScript body",
            &empty_body,
            ExpectedError::Exact("function 'review_summary' TypeScript body must not be empty"),
        );
        assert_function_error(
            "unknown language",
            &unknown_language,
            ExpectedError::Contains("function artifact SQL comment frontmatter is invalid"),
        );
    }

    #[test]
    fn parse_function_artifact_rejects_invalid_schema() {
        let cases = [
            (
                "mixed-case table-function schema",
                function_with(&[("schema: functions", "schema: Functions")]),
                ExpectedError::Exact(
                    "function 'github_review_queue' schema 'Functions' must be lowercase",
                ),
            ),
            (
                "reserved table-function schema",
                function_with(&[("schema: functions", "schema: __coral_udfs")]),
                ExpectedError::Exact(
                    "function 'github_review_queue' schema '__coral_udfs' is reserved",
                ),
            ),
            (
                "public table-function schema",
                function_with(&[("schema: functions", "schema: public")]),
                ExpectedError::Exact(
                    "function 'github_review_queue' schema 'public' is reserved and cannot be used by manifests",
                ),
            ),
        ];

        for (name, artifact, expected) in cases {
            assert_function_error(name, &artifact, expected);
        }
    }
}
