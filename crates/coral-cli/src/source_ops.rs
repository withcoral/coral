use std::collections::{BTreeMap, btree_map::Entry};
use std::env;
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ExplicitBindings {
    inputs: BTreeMap<String, String>,
}

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
    if !is_interactive() {
        return Err(anyhow::anyhow!("interactive source install requires a TTY"));
    }
    Ok(())
}

pub(crate) fn is_interactive() -> bool {
    stdin().is_terminal() && stdout().is_terminal()
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
    Ok(name.to_string())
}

pub(crate) fn prompt_for_inputs(
    inputs: &[ManifestInputSpec],
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    resolve_inputs(inputs, &ExplicitBindings::default(), true)
}

pub(crate) fn collect_explicit_bindings(
    input_args: &[String],
) -> Result<ExplicitBindings, anyhow::Error> {
    Ok(ExplicitBindings {
        inputs: collect_binding_args(input_args)?,
    })
}

pub(crate) fn interactive_resolution_allowed(
    interactive_terminal: bool,
    explicit_bindings: &ExplicitBindings,
) -> bool {
    interactive_terminal && explicit_bindings.is_empty()
}

pub(crate) fn resolve_inputs(
    inputs: &[ManifestInputSpec],
    explicit_bindings: &ExplicitBindings,
    interactive_allowed: bool,
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    resolve_inputs_with_lookup(
        inputs,
        explicit_bindings,
        interactive_allowed,
        env_input_value,
    )
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
                Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message })) => {
                    if !error_message.is_empty() {
                        println!("      {}", style(error_message.as_str()).yellow());
                    }
                }
                None => {}
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

fn resolve_inputs_with_lookup<F>(
    inputs: &[ManifestInputSpec],
    explicit_bindings: &ExplicitBindings,
    interactive_allowed: bool,
    env_lookup: F,
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error>
where
    F: Fn(&str) -> Option<String>,
{
    let mut variables = Vec::new();
    let mut secrets = Vec::new();
    let declared_keys = inputs
        .iter()
        .map(|input| input.key.as_str())
        .collect::<Vec<_>>();

    if let Some((unknown_key, _)) = explicit_bindings.inputs.iter().find(|(key, _)| {
        !declared_keys
            .iter()
            .any(|declared| declared == &key.as_str())
    }) {
        return Err(anyhow::anyhow!("unknown source input '{unknown_key}'"));
    }

    for input in inputs {
        let explicit_value = explicit_bindings.inputs.get(&input.key);
        let value = if let Some(value) = explicit_value {
            finalize_input_value(input, value.clone(), input_kind_label(input.kind))?
        } else if let Some(value) = env_lookup(&input.key) {
            finalize_input_value(input, value, input_kind_label(input.kind))?
        } else if interactive_allowed {
            match input.kind {
                ManifestInputKind::Variable => {
                    prompt_variable(input)?.map(|variable| variable.value)
                }
                ManifestInputKind::Secret => prompt_secret(input)?.map(|secret| secret.value),
            }
        } else {
            finalize_input_value(input, String::new(), input_kind_label(input.kind))?
        };

        match (input.kind, value) {
            (ManifestInputKind::Variable, Some(value)) => variables.push(SourceVariable {
                key: input.key.clone(),
                value,
            }),
            (ManifestInputKind::Secret, Some(value)) => secrets.push(SourceSecret {
                key: input.key.clone(),
                value,
            }),
            (_, None) => {}
        }
    }

    Ok((variables, secrets))
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

fn collect_binding_args(args: &[String]) -> Result<BTreeMap<String, String>, anyhow::Error> {
    let mut values = BTreeMap::new();
    for raw in args {
        let (key, value) = parse_binding_arg("source input key", raw)?;
        match values.entry(key.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(_) => {
                return Err(anyhow::anyhow!("source input '{key}' is repeated"));
            }
        }
    }
    Ok(values)
}

#[allow(
    clippy::disallowed_methods,
    reason = "source add intentionally resolves manifest-declared inputs from the process environment."
)]
fn env_input_value(key: &str) -> Option<String> {
    env::var_os(key).and_then(|value| value.into_string().ok())
}

fn parse_binding_arg(key_label: &str, raw: &str) -> Result<(String, String), anyhow::Error> {
    let Some((raw_key, value)) = raw.split_once('=') else {
        return Err(anyhow::anyhow!(
            "expected KEY=VALUE for {key_label}, got '{raw}'"
        ));
    };
    let key = normalize_binding_key(key_label, raw_key)?;
    Ok((key, value.to_string()))
}

fn normalize_binding_key(key_label: &str, value: &str) -> Result<String, anyhow::Error> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow::anyhow!("missing {key_label}"));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(anyhow::anyhow!(
            "{key_label} must not contain '/' or '\\\\'"
        ));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(anyhow::anyhow!(
            "{key_label} must not contain '=', '\\n', or '\\r'"
        ));
    }
    if trimmed.starts_with('#') {
        return Err(anyhow::anyhow!("{key_label} must not start with '#'"));
    }
    Ok(trimmed.to_string())
}

fn input_kind_label(kind: ManifestInputKind) -> &'static str {
    match kind {
        ManifestInputKind::Variable => "source variable",
        ManifestInputKind::Secret => "source secret",
    }
}

impl ExplicitBindings {
    fn is_empty(&self) -> bool {
        self.inputs.is_empty()
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

    use super::{
        ExplicitBindings, ValidationFollowUp, ValidationSeverityMode, collect_explicit_bindings,
        finalize_input_value, interactive_resolution_allowed, parse_binding_arg,
        resolve_inputs_with_lookup, validation_follow_up,
    };

    fn variable_input(key: &str, required: bool, default_value: &str) -> ManifestInputSpec {
        ManifestInputSpec {
            key: key.to_string(),
            kind: ManifestInputKind::Variable,
            required,
            default_value: default_value.to_string(),
            hint: None,
        }
    }

    fn secret_input(key: &str, required: bool) -> ManifestInputSpec {
        ManifestInputSpec {
            key: key.to_string(),
            kind: ManifestInputKind::Secret,
            required,
            default_value: String::new(),
            hint: None,
        }
    }

    #[test]
    fn empty_optional_input_is_omitted_for_server_side_defaults() {
        let input = variable_input("API_BASE", false, "https://example.com");
        assert_eq!(
            finalize_input_value(&input, String::new(), "source variable")
                .expect("empty optional input should be omitted"),
            None
        );
    }

    #[test]
    fn empty_required_input_without_default_is_rejected() {
        let input = secret_input("API_TOKEN", true);
        let error = finalize_input_value(&input, String::new(), "source secret")
            .expect_err("required empty input should fail");
        assert!(error.to_string().contains("missing required source secret"));
    }

    #[test]
    fn parse_binding_arg_accepts_key_value_pairs() {
        let (key, value) = parse_binding_arg("source input key", "API_BASE=https://example.com")
            .expect("binding should parse");
        assert_eq!(key, "API_BASE");
        assert_eq!(value, "https://example.com");
    }

    #[test]
    fn parse_binding_arg_rejects_missing_equals() {
        let error = parse_binding_arg("source input key", "API_BASE")
            .expect_err("missing equals should fail");
        assert!(
            error
                .to_string()
                .contains("expected KEY=VALUE for source input key")
        );
    }

    #[test]
    fn duplicate_input_flags_are_rejected() {
        let error = collect_explicit_bindings(&[
            "API_BASE=https://example.com".to_string(),
            "API_BASE=https://override.example.com".to_string(),
        ])
        .expect_err("duplicate variable flags should fail");
        assert!(
            error
                .to_string()
                .contains("source input 'API_BASE' is repeated")
        );
    }

    #[test]
    fn explicit_binding_beats_env_value() {
        let inputs = vec![secret_input("API_TOKEN", true)];
        let explicit = ExplicitBindings {
            inputs: [("API_TOKEN".to_string(), "flag-token".to_string())]
                .into_iter()
                .collect(),
        };

        let (_, secrets) = resolve_inputs_with_lookup(&inputs, &explicit, false, |_| {
            Some("env-token".to_string())
        })
        .expect("explicit binding should resolve");

        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].value, "flag-token");
    }

    #[test]
    fn env_value_is_used_when_no_explicit_binding_exists() {
        let inputs = vec![variable_input("API_BASE", true, "")];
        let explicit = ExplicitBindings::default();

        let (variables, _) = resolve_inputs_with_lookup(&inputs, &explicit, false, |key| {
            (key == "API_BASE").then(|| "https://env.example.com".to_string())
        })
        .expect("env binding should resolve");

        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].value, "https://env.example.com");
    }

    #[test]
    fn optional_input_with_empty_env_value_is_omitted() {
        let inputs = vec![variable_input("API_BASE", false, "https://example.com")];
        let explicit = ExplicitBindings::default();

        let (variables, _) =
            resolve_inputs_with_lookup(&inputs, &explicit, false, |_| Some(String::new()))
                .expect("empty optional env value should be omitted");

        assert!(variables.is_empty());
    }

    #[test]
    fn required_input_with_empty_env_value_is_rejected() {
        let inputs = vec![secret_input("API_TOKEN", true)];
        let explicit = ExplicitBindings::default();

        let error = resolve_inputs_with_lookup(&inputs, &explicit, false, |_| Some(String::new()))
            .expect_err("empty required env value should fail");

        assert!(
            error
                .to_string()
                .contains("missing required source secret 'API_TOKEN'")
        );
    }

    #[test]
    fn unknown_explicit_input_is_rejected() {
        let inputs = vec![secret_input("API_TOKEN", true)];
        let explicit = ExplicitBindings {
            inputs: [("UNUSED".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        };

        let error = resolve_inputs_with_lookup(&inputs, &explicit, false, |_| None)
            .expect_err("unknown explicit input should fail");

        assert!(error.to_string().contains("unknown source input 'UNUSED'"));
    }

    #[test]
    fn interactive_resolution_is_disabled_when_any_input_is_provided() {
        let explicit = ExplicitBindings {
            inputs: [("API_TOKEN".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        };

        assert!(!interactive_resolution_allowed(true, &explicit));
        assert!(!interactive_resolution_allowed(false, &explicit));
        assert!(interactive_resolution_allowed(
            true,
            &ExplicitBindings::default()
        ));
        assert!(!interactive_resolution_allowed(
            false,
            &ExplicitBindings::default()
        ));
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
