use std::collections::BTreeMap;
use std::io::{IsTerminal, stdin, stdout};
use std::path::Path;

use coral_api::v1::{
    AvailableSource, CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest,
    ImportSourceRequest, ListSourcesRequest, QueryTestFailure, QueryTestSuccess, Source,
    SourceInputKind, SourceInputSpec, SourceOrigin, SourceSecret, SourceVariable,
    ValidateSourceRequest, ValidateSourceResponse, query_test_result,
};
use coral_client::{AppClient, default_workspace};
use coral_spec::{
    ManifestInputKind, ManifestInputSpec, ValidatedSourceManifest, parse_source_manifest_yaml,
};
use dialoguer::console::style;
use dialoguer::{Input, Password, theme::ColorfulTheme};
use tonic::Request;

const MAX_TABLES_PER_SCHEMA: usize = 9;

/// How many tables to show per schema when pretty-printing validation results.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TableDisplayLimit {
    /// Show every table the source exposes.
    All,
    /// Show at most this many tables per schema, with a summary for the rest.
    Max(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValidationSeverityMode {
    Strict,
    WarnOnly,
}

#[derive(Debug, PartialEq, Eq)]
enum ValidationFollowUp {
    None,
    Warn(String),
    Fail(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryTestCounts {
    declared: usize,
    passed: usize,
    failed: usize,
}

impl TableDisplayLimit {
    /// The default truncation used after `source add` and during onboarding.
    pub(crate) const DEFAULT: Self = Self::Max(MAX_TABLES_PER_SCHEMA);
}

pub(crate) async fn discover_sources(
    app: &AppClient,
) -> Result<Vec<AvailableSource>, anyhow::Error> {
    Ok(app
        .source_client()
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await?
        .into_inner()
        .sources)
}

pub(crate) async fn list_sources(app: &AppClient) -> Result<Vec<Source>, anyhow::Error> {
    Ok(app
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await?
        .into_inner()
        .sources)
}

pub(crate) async fn add_bundled_source(
    app: &AppClient,
    name: &str,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    Ok(app
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
            variables,
            secrets,
        }))
        .await?
        .into_inner())
}

pub(crate) async fn import_source(
    app: &AppClient,
    manifest_yaml: String,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    Ok(app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables,
            secrets,
        }))
        .await?
        .into_inner())
}

pub(crate) async fn validate_source(
    app: &AppClient,
    name: &str,
) -> Result<ValidateSourceResponse, anyhow::Error> {
    Ok(app
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: source_name_arg(Some(name))?,
        }))
        .await?
        .into_inner())
}

pub(crate) fn load_validated_manifest_file(
    file: &Path,
) -> Result<(String, ValidatedSourceManifest), anyhow::Error> {
    let manifest_yaml = std::fs::read_to_string(file)?;
    let manifest = parse_source_manifest_yaml(manifest_yaml.as_str())?;
    Ok((manifest_yaml, manifest))
}

pub(crate) async fn delete_source(app: &AppClient, name: &str) -> Result<(), anyhow::Error> {
    app.source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: source_name_arg(Some(name))?,
        }))
        .await?;
    Ok(())
}

pub(crate) fn require_interactive() -> Result<(), anyhow::Error> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err(anyhow::anyhow!("interactive source install requires a TTY"));
    }
    Ok(())
}

pub(crate) fn source_name_arg(name: Option<&str>) -> Result<String, anyhow::Error> {
    let Some(name) = name else {
        return Err(anyhow::anyhow!("missing source name"));
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(anyhow::anyhow!("missing source name"));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(anyhow::anyhow!(
            "source name must not contain '/' or '\\\\'"
        ));
    }
    if name == "." || name == ".." {
        return Err(anyhow::anyhow!("source name must not be '.' or '..'"));
    }
    Ok(name.to_string())
}

pub(crate) fn prompt_for_inputs(
    inputs: &[ManifestInputSpec],
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    let mut variables = Vec::new();
    let mut secrets = Vec::new();

    for input in inputs {
        match input.kind {
            ManifestInputKind::Variable => {
                if let Some(variable) = prompt_variable(input)? {
                    variables.push(variable);
                }
            }
            ManifestInputKind::Secret => {
                if let Some(secret) = prompt_secret(input)? {
                    secrets.push(secret);
                }
            }
        }
    }

    Ok((variables, secrets))
}

pub(crate) fn collect_inputs_from_env(
    inputs: &[ManifestInputSpec],
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    collect_inputs_with(inputs, |key| read_source_input_env(key).unwrap_or_default())
}

#[allow(
    clippy::disallowed_methods,
    reason = "`coral source add` reads install-time source inputs from matching environment variables."
)]
fn read_source_input_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn collect_inputs_with(
    inputs: &[ManifestInputSpec],
    mut lookup: impl FnMut(&str) -> String,
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    let mut variables = Vec::new();
    let mut secrets = Vec::new();
    let mut missing = Vec::new();

    for input in inputs {
        let raw = lookup(&input.key);
        let value = if raw.is_empty() {
            input.default_value.clone()
        } else {
            raw
        };
        if value.is_empty() {
            if input.required {
                missing.push(input.key.clone());
            }
            continue;
        }
        match input.kind {
            ManifestInputKind::Variable => variables.push(SourceVariable {
                key: input.key.clone(),
                value,
            }),
            ManifestInputKind::Secret => secrets.push(SourceSecret {
                key: input.key.clone(),
                value,
            }),
        }
    }

    if !missing.is_empty() {
        return Err(anyhow::anyhow!(
            "missing required environment variable{}: {}. Set the variable{} or run with --interactive.",
            if missing.len() == 1 { "" } else { "s" },
            missing.join(", "),
            if missing.len() == 1 { "" } else { "s" },
        ));
    }

    Ok((variables, secrets))
}

pub(crate) fn manifest_input_from_proto(
    input: &SourceInputSpec,
) -> Result<ManifestInputSpec, anyhow::Error> {
    let kind = match SourceInputKind::try_from(input.kind) {
        Ok(SourceInputKind::Variable) => ManifestInputKind::Variable,
        Ok(SourceInputKind::Secret) => ManifestInputKind::Secret,
        Ok(SourceInputKind::Unspecified) | Err(_) => {
            return Err(anyhow::anyhow!("unknown input kind for '{}'", input.key));
        }
    };
    Ok(ManifestInputSpec {
        key: input.key.clone(),
        kind,
        required: input.required,
        default_value: input.default_value.clone(),
        hint: (!input.hint.is_empty()).then(|| input.hint.clone()),
    })
}

pub(crate) fn source_origin_label(origin: i32) -> &'static str {
    match SourceOrigin::try_from(origin) {
        Ok(SourceOrigin::Bundled) => "bundled",
        Ok(SourceOrigin::Imported) => "imported",
        Ok(SourceOrigin::Unspecified) | Err(_) => "unknown",
    }
}

pub(crate) async fn validate_and_print(
    app: &AppClient,
    source_name: &str,
    limit: TableDisplayLimit,
    severity_mode: ValidationSeverityMode,
) -> Result<(), anyhow::Error> {
    let response = validate_source(app, source_name).await?;
    print_validation_pretty(&response, limit)?;
    match validation_follow_up(&response, severity_mode) {
        ValidationFollowUp::None => Ok(()),
        ValidationFollowUp::Warn(message) => {
            eprintln!("Warning: {message}");
            Ok(())
        }
        ValidationFollowUp::Fail(message) => Err(anyhow::anyhow!(message)),
    }
}

pub(crate) async fn validate_and_warn(
    app: &AppClient,
    source_name: &str,
    limit: TableDisplayLimit,
) -> Result<(), anyhow::Error> {
    if let Err(err) =
        validate_and_print(app, source_name, limit, ValidationSeverityMode::WarnOnly).await
    {
        eprintln!("Warning: validation failed: {err}");
    }
    Ok(())
}

pub(crate) async fn test_and_print(
    app: &AppClient,
    source_name: &str,
    limit: TableDisplayLimit,
    severity_mode: ValidationSeverityMode,
) -> Result<(), anyhow::Error> {
    let normalized = source_name_arg(Some(source_name))?;
    let err = match validate_and_print(app, &normalized, limit, severity_mode).await {
        Ok(()) => return Ok(()),
        Err(err) => err,
    };

    let is_not_found = err
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() == tonic::Code::NotFound);
    if !is_not_found {
        return Err(err);
    }

    // Discovery failure must not mask the original validation error.
    let Ok(available) = discover_sources(app).await else {
        return Err(err);
    };
    if available
        .iter()
        .any(|source| source.name == normalized && !source.installed)
    {
        return Err(source_not_installed_error(&normalized));
    }

    Err(source_not_found_error(&normalized))
}

fn source_not_installed_error(source_name: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "source '{source_name}' is not installed. Run `coral source add {source_name}` to install it, then retry `coral source test {source_name}`."
    )
}

fn source_not_found_error(source_name: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "source '{source_name}' was not found. Run `coral source list` to see installed sources or `coral source discover` to see bundled sources available to install."
    )
}

pub(crate) fn print_validation_pretty(
    response: &ValidateSourceResponse,
    limit: TableDisplayLimit,
) -> Result<(), anyhow::Error> {
    let source = response
        .source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("validate response missing source metadata"))?;

    println!();
    println!(
        "  {} {}",
        style("✓").green(),
        style(format!("{} connected successfully", source.name)).bold()
    );

    // Group tables by schema, sorted.
    let mut by_schema: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for table in &response.tables {
        by_schema
            .entry(&table.schema_name)
            .or_default()
            .push(&table.name);
    }
    for tables in by_schema.values_mut() {
        tables.sort_unstable();
    }

    for (schema, tables) in &by_schema {
        let count = tables.len();
        println!();
        println!(
            "    {}",
            style(format!(
                "{schema} ({count} {})",
                if count == 1 { "table" } else { "tables" }
            ))
            .bold()
        );

        let show_count = match limit {
            TableDisplayLimit::All => tables.len(),
            TableDisplayLimit::Max(max) => tables.len().min(max),
        };
        let remaining = tables.len() - show_count;

        for (i, table) in tables.iter().take(show_count).enumerate() {
            let is_last = i == show_count - 1 && remaining == 0;
            let branch = if is_last { "└─" } else { "├─" };
            println!("    {} {}", style(branch).dim(), table);
        }

        if remaining > 0 {
            println!(
                "    {} {}",
                style("└─").dim(),
                style(format!("... and {remaining} more")).dim()
            );
        }
    }

    let query_test_counts = query_test_counts(response);
    if query_test_counts.declared > 0 {
        println!("    {}", style("Query tests").bold());
        println!(
            "    {}",
            style(format!(
                "{} declared · {} passed · {} failed",
                query_test_counts.declared, query_test_counts.passed, query_test_counts.failed
            ))
            .dim()
        );
        for test in &response.query_tests {
            println!();
            let status = if matches!(test.outcome, Some(query_test_result::Outcome::Success(_))) {
                style("✓").green()
            } else {
                style("✗").red()
            };
            println!("    {} {}", status, style(test.sql.trim()).bold());
            match &test.outcome {
                Some(query_test_result::Outcome::Success(QueryTestSuccess { row_count })) => {
                    println!(
                        "      {}",
                        style(format!(
                            "{row_count} row{}",
                            if *row_count == 1 { "" } else { "s" }
                        ))
                        .dim()
                    );
                }
                Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
                    if !error_message.is_empty() =>
                {
                    println!("      {}", style(error_message.as_str()).yellow());
                }
                Some(query_test_result::Outcome::Failure(QueryTestFailure { .. })) | None => {}
            }
        }
    }
    println!();

    Ok(())
}

fn validation_follow_up(
    response: &ValidateSourceResponse,
    severity_mode: ValidationSeverityMode,
) -> ValidationFollowUp {
    let query_test_counts = query_test_counts(response);
    if query_test_counts.declared == 0 || query_test_counts.failed == 0 {
        return ValidationFollowUp::None;
    }

    let failure_count = query_test_counts.failed.max(1);
    let message = format!(
        "{} of {} validation quer{} failed",
        failure_count,
        query_test_counts.declared.max(failure_count),
        if query_test_counts.declared == 1 {
            "y"
        } else {
            "ies"
        }
    );
    match severity_mode {
        ValidationSeverityMode::Strict => ValidationFollowUp::Fail(message),
        ValidationSeverityMode::WarnOnly => ValidationFollowUp::Warn(message),
    }
}

fn query_test_counts(response: &ValidateSourceResponse) -> QueryTestCounts {
    let declared = response.query_tests.len();
    let passed = response
        .query_tests
        .iter()
        .filter(|test| matches!(test.outcome, Some(query_test_result::Outcome::Success(_))))
        .count();
    QueryTestCounts {
        declared,
        passed,
        failed: declared.saturating_sub(passed),
    }
}

fn prompt_variable(input: &ManifestInputSpec) -> Result<Option<SourceVariable>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    print_input_hint(input);
    let prompt = if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [{}]", input.key, input.default_value)
    };
    let value = Input::<String>::with_theme(&theme)
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;
    let Some(value) = finalize_input_value(input, value, "source variable")? else {
        return Ok(None);
    };
    Ok(Some(SourceVariable {
        key: input.key.clone(),
        value,
    }))
}

fn prompt_secret(input: &ManifestInputSpec) -> Result<Option<SourceSecret>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    print_input_hint(input);
    let prompt = if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [default hidden]", input.key)
    };
    let value = Password::with_theme(&theme)
        .with_prompt(prompt)
        .allow_empty_password(true)
        .interact()?;
    let Some(value) = finalize_input_value(input, value, "source secret")? else {
        return Ok(None);
    };
    Ok(Some(SourceSecret {
        key: input.key.clone(),
        value,
    }))
}

fn print_input_hint(input: &ManifestInputSpec) {
    if let Some(hint) = input.hint.as_deref()
        && !hint.is_empty()
    {
        println!("  {}", style(hint).dim());
    }
}

pub(crate) fn finalize_input_value(
    input: &ManifestInputSpec,
    value: String,
    kind_label: &str,
) -> Result<Option<String>, anyhow::Error> {
    if !value.is_empty() {
        return Ok(Some(value));
    }
    if input.required {
        return Err(anyhow::anyhow!(
            "missing required {kind_label} '{}'",
            input.key
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use coral_api::v1::ValidateSourceResponse;
    use coral_spec::{ManifestInputKind, ManifestInputSpec};

    use std::collections::HashMap;

    use super::{
        ValidationFollowUp, ValidationSeverityMode, collect_inputs_with, finalize_input_value,
        source_name_arg, validation_follow_up,
    };

    #[test]
    fn collect_inputs_reads_variables_and_secrets_from_lookup() {
        let inputs = vec![
            ManifestInputSpec {
                key: "LINEAR_API_BASE".to_string(),
                kind: ManifestInputKind::Variable,
                required: false,
                default_value: "https://api.linear.app".to_string(),
                hint: None,
            },
            ManifestInputSpec {
                key: "LINEAR_API_KEY".to_string(),
                kind: ManifestInputKind::Secret,
                required: true,
                default_value: String::new(),
                hint: None,
            },
        ];
        let env: HashMap<&str, &str> = [("LINEAR_API_KEY", "lin_token")].into_iter().collect();
        let (variables, secrets) = collect_inputs_with(&inputs, |key| {
            env.get(key).map(|v| (*v).to_string()).unwrap_or_default()
        })
        .expect("should succeed");
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].key, "LINEAR_API_BASE");
        assert_eq!(variables[0].value, "https://api.linear.app");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].key, "LINEAR_API_KEY");
        assert_eq!(secrets[0].value, "lin_token");
    }

    #[test]
    fn collect_inputs_env_value_overrides_default() {
        let inputs = vec![ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
            hint: None,
        }];
        let (variables, _) = collect_inputs_with(&inputs, |_| "https://override.test".to_string())
            .expect("env should override default");
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].value, "https://override.test");
    }

    #[test]
    fn collect_inputs_uses_default_when_env_empty() {
        let inputs = vec![ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: true,
            default_value: "https://example.com".to_string(),
            hint: None,
        }];
        let (variables, secrets) = collect_inputs_with(&inputs, |_| String::new())
            .expect("default should satisfy required");
        assert_eq!(secrets.len(), 0);
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].value, "https://example.com");
    }

    #[test]
    fn collect_inputs_errors_on_missing_required() {
        let inputs = vec![
            ManifestInputSpec {
                key: "LINEAR_API_KEY".to_string(),
                kind: ManifestInputKind::Secret,
                required: true,
                default_value: String::new(),
                hint: None,
            },
            ManifestInputSpec {
                key: "OTHER_KEY".to_string(),
                kind: ManifestInputKind::Variable,
                required: true,
                default_value: String::new(),
                hint: None,
            },
        ];
        let error = collect_inputs_with(&inputs, |_| String::new())
            .expect_err("missing required inputs should fail");
        let message = error.to_string();
        assert!(message.contains("LINEAR_API_KEY"));
        assert!(message.contains("OTHER_KEY"));
        assert!(message.contains("--interactive"));
    }

    #[test]
    fn source_name_arg_rejects_dot_segments() {
        let error = source_name_arg(Some("..")).expect_err("dot segment should fail");
        assert!(error.to_string().contains("must not be '.' or '..'"));

        let error = source_name_arg(Some(" . ")).expect_err("dot segment should fail");
        assert!(error.to_string().contains("must not be '.' or '..'"));
    }

    #[test]
    fn collect_inputs_skips_optional_empty_inputs() {
        let inputs = vec![ManifestInputSpec {
            key: "OPTIONAL".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: String::new(),
            hint: None,
        }];
        let (variables, secrets) =
            collect_inputs_with(&inputs, |_| String::new()).expect("optional should be omitted");
        assert!(variables.is_empty());
        assert!(secrets.is_empty());
    }

    #[test]
    fn empty_optional_input_is_omitted_for_server_side_defaults() {
        let input = ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
            hint: None,
        };
        assert_eq!(
            finalize_input_value(&input, String::new(), "source variable")
                .expect("empty optional input should be omitted"),
            None
        );
    }

    #[test]
    fn empty_required_input_without_default_is_rejected() {
        let input = ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: None,
        };
        let error = finalize_input_value(&input, String::new(), "source secret")
            .expect_err("required empty input should fail");
        assert!(error.to_string().contains("missing required source secret"));
    }

    #[test]
    fn validation_follow_up_is_none_when_all_query_tests_pass() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            query_tests: vec![coral_api::v1::QueryTestResult {
                sql: "SELECT 1".to_string(),
                outcome: Some(coral_api::v1::query_test_result::Outcome::Success(
                    coral_api::v1::QueryTestSuccess { row_count: 1 },
                )),
            }],
        };

        assert_eq!(
            validation_follow_up(&response, ValidationSeverityMode::Strict),
            ValidationFollowUp::None
        );
    }

    #[test]
    fn validation_follow_up_is_error_in_strict_mode() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            query_tests: vec![
                coral_api::v1::QueryTestResult {
                    sql: "SELECT 1".to_string(),
                    outcome: Some(coral_api::v1::query_test_result::Outcome::Success(
                        coral_api::v1::QueryTestSuccess { row_count: 1 },
                    )),
                },
                coral_api::v1::QueryTestResult {
                    sql: "SELECT missing".to_string(),
                    outcome: Some(coral_api::v1::query_test_result::Outcome::Failure(
                        coral_api::v1::QueryTestFailure {
                            error_message: "missing".to_string(),
                        },
                    )),
                },
            ],
        };

        assert_eq!(
            validation_follow_up(&response, ValidationSeverityMode::Strict),
            ValidationFollowUp::Fail("1 of 2 validation queries failed".to_string())
        );
    }

    #[test]
    fn validation_follow_up_is_warning_in_warn_only_mode() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            query_tests: vec![coral_api::v1::QueryTestResult {
                sql: "SELECT missing".to_string(),
                outcome: Some(coral_api::v1::query_test_result::Outcome::Failure(
                    coral_api::v1::QueryTestFailure {
                        error_message: "missing".to_string(),
                    },
                )),
            }],
        };

        assert_eq!(
            validation_follow_up(&response, ValidationSeverityMode::WarnOnly),
            ValidationFollowUp::Warn("1 of 1 validation query failed".to_string())
        );
    }
}
