use serde::Deserialize;

use crate::{ManifestError, Result};

use super::model::{FunctionCoralSqlImplementationSpec, FunctionImplementationSpec, FunctionSpec};
use super::validation::validate_raw_function;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawFunctionFrontmatter {
    pub(super) name: String,
    pub(super) schema: String,
    #[serde(default)]
    pub(super) description: String,
}

#[derive(Debug)]
pub(super) struct RawFunctionSpec {
    pub(super) frontmatter: RawFunctionFrontmatter,
    pub(super) implementation: FunctionImplementationSpec,
}

/// Parses and statically validates one function SQL artifact.
///
/// # Errors
///
/// Returns [`ManifestError`] when the frontmatter cannot be parsed, the SQL
/// body is empty, or the artifact violates function-local invariants.
pub fn parse_function_sql(raw: &str) -> Result<FunctionSpec> {
    let (frontmatter, sql) = split_frontmatter(raw)?;
    let frontmatter: RawFunctionFrontmatter =
        serde_yaml::from_str(frontmatter).map_err(|error| {
            ManifestError::validation(format!(
                "function artifact SQL comment frontmatter is invalid: {error}"
            ))
        })?;
    validate_raw_function(RawFunctionSpec {
        frontmatter,
        implementation: FunctionImplementationSpec {
            coral_sql: FunctionCoralSqlImplementationSpec {
                query: sql.trim().to_string(),
            },
        },
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
    use super::parse_function_sql;

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
*/

select title, html_url
from github.pulls(owner => $owner, repo => $repo)
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
        let error = parse_function_sql(artifact).unwrap_err();
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
    fn parse_function_sql_accepts_valid_function() {
        let function = parse_function_sql(valid_function()).expect("function should parse");

        assert_eq!(function.name(), "github_review_queue");
        assert_eq!(function.schema(), "functions");
        assert!(
            function
                .implementation()
                .coral_sql
                .query
                .contains("github.pulls")
        );
    }

    #[test]
    fn parse_function_sql_rejects_invalid_frontmatter() {
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
    fn parse_function_sql_preserves_body_after_frontmatter_comment() {
        let function = parse_function_sql(valid_function()).expect("function should parse");
        let query = &function.implementation().coral_sql.query;

        assert!(
            query.starts_with("select title"),
            "unexpected query: {query}"
        );
        assert!(!query.contains("github_review_queue"));
        assert!(!query.contains("*/"));
    }

    #[test]
    fn parse_function_sql_rejects_invalid_schema() {
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
