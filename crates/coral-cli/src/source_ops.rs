use std::collections::BTreeMap;
use std::io::{IsTerminal, stdin, stdout};
use std::path::Path;

use coral_api::CORAL_ERROR_REASON_SOURCE_NOT_FOUND;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, DeleteSourceRequest, DiscoverSourcesRequest,
    GetSourceInfoRequest, ImportSourceRequest, ImportSourceResponse, ListSourcesRequest,
    OAuthCredentialInput, OAuthCredentialRetrieval, QueryTestFailure, QueryTestSuccess, Source,
    SourceCredentialStorage, SourceInfo, SourceOrigin, SourceSecret, SourceVariable,
    ValidateSourceRequest, ValidateSourceResponse, create_bundled_source_with_o_auth_response,
    import_source_response, query_test_result, source_input_spec::Input as ProtoSourceInput,
};
use coral_client::{AppClient, DecodedStatusError, decode_status_error, default_workspace};
use coral_spec::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
    ManifestInputKind, ManifestInputSpec, ManifestOAuthCredentialSpec, ValidatedSourceManifest,
    parse_source_manifest_yaml,
};
use dialoguer::console::style;
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
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

pub(crate) async fn discover_sources(app: &AppClient) -> Result<Vec<SourceInfo>, anyhow::Error> {
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
    let response = app
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
            variables,
            secrets,
        }))
        .await?
        .into_inner();
    response
        .source
        .ok_or_else(|| anyhow::anyhow!("create bundled source response missing source"))
}

pub(crate) async fn import_source(
    app: &AppClient,
    manifest_yaml: String,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    let mut responses = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables,
            secrets,
            oauth_credential_retrievals: Vec::new(),
        }))
        .await?
        .into_inner();
    while let Some(response) = responses.message().await? {
        if let Some(import_source_response::Event::Source(source)) = response.event {
            return Ok(source);
        }
    }
    Err(anyhow::anyhow!("import source stream ended without source"))
}

pub(crate) struct CollectedSourceInputs {
    pub(crate) variables: Vec<SourceVariable>,
    pub(crate) secrets: Vec<SourceSecret>,
    oauth_credential_retrievals: Vec<OAuthCredentialRetrieval>,
    oauth_labels: BTreeMap<String, String>,
}

impl CollectedSourceInputs {
    fn new() -> Self {
        Self {
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            oauth_labels: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialPromptMode {
    EnvFirst,
    CredentialMethodFirst,
}

impl CredentialPromptMode {
    fn reads_env_before_prompt(self, input: &ManifestInputSpec) -> bool {
        match self {
            Self::EnvFirst => true,
            Self::CredentialMethodFirst => {
                input.kind == ManifestInputKind::Variable || input.credential.is_none()
            }
        }
    }
}

pub(crate) async fn add_bundled_source_with_credentials(
    app: &AppClient,
    name: &str,
    inputs: CollectedSourceInputs,
) -> Result<Source, anyhow::Error> {
    if inputs.oauth_credential_retrievals.is_empty() {
        return add_bundled_source(app, name, inputs.variables, inputs.secrets).await;
    }
    let response = app
        .source_client()
        .create_bundled_source_with_o_auth(Request::new(CreateBundledSourceWithOAuthRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
            variables: inputs.variables,
            secrets: inputs.secrets,
            oauth_credential_retrievals: inputs.oauth_credential_retrievals,
        }))
        .await?;
    source_from_bundled_credential_stream(response.into_inner(), &inputs.oauth_labels).await
}

pub(crate) async fn import_source_with_credentials(
    app: &AppClient,
    manifest_yaml: String,
    inputs: CollectedSourceInputs,
) -> Result<Source, anyhow::Error> {
    if inputs.oauth_credential_retrievals.is_empty() {
        return import_source(app, manifest_yaml, inputs.variables, inputs.secrets).await;
    }
    let response = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: inputs.variables,
            secrets: inputs.secrets,
            oauth_credential_retrievals: inputs.oauth_credential_retrievals,
        }))
        .await?;
    source_from_import_credential_stream(response.into_inner(), &inputs.oauth_labels).await
}

async fn source_from_bundled_credential_stream(
    mut stream: tonic::Streaming<CreateBundledSourceWithOAuthResponse>,
    oauth_labels: &BTreeMap<String, String>,
) -> Result<Source, anyhow::Error> {
    while let Some(response) = stream
        .message()
        .await
        .map_err(|error| oauth_error("retrieve", &error))?
    {
        let event = response.event.map(CredentialStreamEvent::from);
        if let Some(source) = handle_credential_stream_event(event, oauth_labels) {
            return Ok(source);
        }
    }
    Err(anyhow::anyhow!(
        "source credential retrieval stream ended before source installation completed"
    ))
}

async fn source_from_import_credential_stream(
    mut stream: tonic::Streaming<ImportSourceResponse>,
    oauth_labels: &BTreeMap<String, String>,
) -> Result<Source, anyhow::Error> {
    while let Some(response) = stream
        .message()
        .await
        .map_err(|error| oauth_error("retrieve", &error))?
    {
        let event = response.event.map(CredentialStreamEvent::from);
        if let Some(source) = handle_credential_stream_event(event, oauth_labels) {
            return Ok(source);
        }
    }
    Err(anyhow::anyhow!(
        "source credential retrieval stream ended before source import completed"
    ))
}

enum CredentialStreamEvent {
    Source(Source),
    OAuthAuthorization {
        input_key: String,
        authorization_url: String,
        user_code: String,
    },
    OAuthCompleted,
}

impl From<create_bundled_source_with_o_auth_response::Event> for CredentialStreamEvent {
    fn from(event: create_bundled_source_with_o_auth_response::Event) -> Self {
        match event {
            create_bundled_source_with_o_auth_response::Event::Source(source) => {
                Self::Source(source)
            }
            create_bundled_source_with_o_auth_response::Event::OauthAuthorization(
                authorization,
            ) => Self::OAuthAuthorization {
                input_key: authorization.input_key,
                authorization_url: authorization.authorization_url,
                user_code: authorization.user_code,
            },
            create_bundled_source_with_o_auth_response::Event::OauthCompleted(_) => {
                Self::OAuthCompleted
            }
        }
    }
}

impl From<import_source_response::Event> for CredentialStreamEvent {
    fn from(event: import_source_response::Event) -> Self {
        match event {
            import_source_response::Event::Source(source) => Self::Source(source),
            import_source_response::Event::OauthAuthorization(authorization) => {
                Self::OAuthAuthorization {
                    input_key: authorization.input_key,
                    authorization_url: authorization.authorization_url,
                    user_code: authorization.user_code,
                }
            }
            import_source_response::Event::OauthCompleted(_) => Self::OAuthCompleted,
        }
    }
}

fn handle_credential_stream_event(
    event: Option<CredentialStreamEvent>,
    oauth_labels: &BTreeMap<String, String>,
) -> Option<Source> {
    match event {
        Some(CredentialStreamEvent::OAuthAuthorization {
            input_key,
            authorization_url,
            user_code,
        }) => {
            let label = oauth_labels
                .get(&input_key)
                .map_or(input_key.as_str(), String::as_str);
            println!("Open this URL to connect {label}:");
            println!("{authorization_url}");
            if !user_code.is_empty() {
                println!("Enter this code when prompted: {user_code}");
            }
            if let Err(err) = crate::browser::open_url(&authorization_url) {
                println!("{}", style(format!("Could not open browser: {err}")).dim());
            }
            None
        }
        Some(CredentialStreamEvent::Source(source)) => Some(source),
        Some(CredentialStreamEvent::OAuthCompleted) | None => None,
    }
}

pub(crate) async fn validate_source(
    app: &AppClient,
    name: &str,
) -> Result<ValidateSourceResponse, anyhow::Error> {
    Ok(validate_source_request(app, source_name_arg(Some(name))?).await?)
}

async fn validate_source_request(
    app: &AppClient,
    name: String,
) -> Result<ValidateSourceResponse, tonic::Status> {
    Ok(app
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name,
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

pub(crate) async fn print_source_info(
    app: &AppClient,
    name: &str,
    verbose: bool,
) -> Result<(), anyhow::Error> {
    let response = app
        .source_client()
        .get_source_info(Request::new(GetSourceInfoRequest {
            workspace: Some(default_workspace()),
            name: source_name_arg(Some(name))?,
        }))
        .await?
        .into_inner();
    let source = response
        .source_info
        .ok_or_else(|| anyhow::anyhow!("get source info response missing source_info"))?;
    print_source_info_response(&source, verbose);
    Ok(())
}

fn print_source_info_response(source: &SourceInfo, verbose: bool) {
    let status = if source.installed {
        style("installed").green().to_string()
    } else {
        style("not installed").dim().to_string()
    };

    println!("{}", style(&source.name).bold());
    println!("  Status:      {status}");
    println!("  Origin:      {}", source_origin_label(source.origin));
    if source.installed {
        println!(
            "  Secrets:     {}",
            source_credential_storage_label(source.credential_storage)
        );
    }
    println!("  Version:     {}", source.version);
    if !source.description.is_empty() {
        println!("  Description: {}", source.description);
    }

    if source.inputs.is_empty() {
        return;
    }

    println!();
    println!("  {}", style("Inputs").bold());
    for input in &source.inputs {
        let (kind_label, default_value) = match input.input.as_ref() {
            Some(ProtoSourceInput::Variable(variable)) => {
                ("variable", variable.default_value.as_str())
            }
            Some(ProtoSourceInput::Secret(_)) => ("secret", ""),
            None => ("unknown", ""),
        };
        let requirement = if input.required {
            "required"
        } else {
            "optional"
        };
        println!(
            "    {} {}",
            style(&input.key).bold(),
            style(format!("({kind_label}, {requirement})")).dim()
        );
        if !default_value.is_empty() {
            println!("      default: {default_value}");
        }
        if verbose && !input.hint.is_empty() {
            println!("      {}", style(&input.hint).dim());
        }
    }
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

pub(crate) fn prompt_for_inputs_with_credential_methods(
    inputs: &[ManifestInputSpec],
) -> Result<CollectedSourceInputs, anyhow::Error> {
    prompt_for_inputs_with_credential_methods_in_mode(inputs, CredentialPromptMode::EnvFirst)
}

pub(crate) fn prompt_for_inputs_with_credential_methods_in_mode(
    inputs: &[ManifestInputSpec],
    mode: CredentialPromptMode,
) -> Result<CollectedSourceInputs, anyhow::Error> {
    let mut collected = CollectedSourceInputs::new();

    for input in inputs {
        if mode.reads_env_before_prompt(input) {
            let env_value = read_source_input_env(&input.key).unwrap_or_default();
            if !env_value.is_empty() {
                push_collected_input(&mut collected, input, env_value);
                continue;
            }
        }

        match input.kind {
            ManifestInputKind::Variable => {
                if let Some(variable) = prompt_variable(input)? {
                    collected.variables.push(variable);
                }
            }
            ManifestInputKind::Secret => match prompt_secret_with_methods(
                input,
                !collected.secrets.is_empty() || !collected.oauth_credential_retrievals.is_empty(),
            )? {
                SecretInputOutcome::SourceConfig(secret) => {
                    if let Some(secret) = secret {
                        collected.secrets.push(secret);
                    }
                }
                SecretInputOutcome::OAuth { credential, label } => {
                    collected.oauth_labels.insert(input.key.clone(), label);
                    collected.oauth_credential_retrievals.push(credential);
                }
            },
        }
    }

    Ok(collected)
}

fn push_collected_input(
    collected: &mut CollectedSourceInputs,
    input: &ManifestInputSpec,
    value: String,
) {
    match input.kind {
        ManifestInputKind::Variable => collected.variables.push(SourceVariable {
            key: input.key.clone(),
            value,
        }),
        ManifestInputKind::Secret => collected.secrets.push(SourceSecret {
            key: input.key.clone(),
            value,
        }),
    }
}

pub(crate) fn collect_inputs_from_env(
    inputs: &[ManifestInputSpec],
    interactive_command: String,
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    collect_inputs_with_hint(
        inputs,
        |key| read_source_input_env(key).unwrap_or_default(),
        Some(interactive_command),
    )
}

pub(crate) fn shell_quote_arg(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[expect(
    clippy::disallowed_methods,
    reason = "`coral source add` reads install-time source inputs from matching environment variables."
)]
fn read_source_input_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn collect_inputs_with_hint(
    inputs: &[ManifestInputSpec],
    mut lookup: impl FnMut(&str) -> String,
    interactive_command: Option<String>,
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
        let interactive_hint = interactive_command.map_or_else(
            || "--interactive".to_string(),
            |command| format!("`{command}`"),
        );
        return Err(anyhow::anyhow!(
            "missing required environment variable{}: {}. Set the variable{} or run {interactive_hint}.",
            if missing.len() == 1 { "" } else { "s" },
            missing.join(", "),
            if missing.len() == 1 { "" } else { "s" },
        ));
    }

    Ok((variables, secrets))
}

pub(crate) fn source_origin_label(origin: i32) -> &'static str {
    match SourceOrigin::try_from(origin) {
        Ok(SourceOrigin::Bundled) => "bundled",
        Ok(SourceOrigin::Imported) => "imported",
        Ok(SourceOrigin::Unspecified) | Err(_) => "unknown",
    }
}

pub(crate) fn source_credential_storage_label(storage: i32) -> &'static str {
    match SourceCredentialStorage::try_from(storage) {
        Ok(SourceCredentialStorage::Unspecified) => "none",
        Ok(SourceCredentialStorage::File) => "file (plaintext)",
        Ok(SourceCredentialStorage::Keychain) => "keychain",
        Err(_) => "unknown",
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
) -> Result<(), crate::CliError> {
    let normalized = source_name_arg(Some(source_name))?;
    let response = match validate_source_request(app, normalized.clone()).await {
        Ok(response) => response,
        Err(status) if is_source_missing_status(&status) => {
            return source_test_not_found_error(app, &normalized, status).await;
        }
        Err(status) => return Err(anyhow::Error::from(status).into()),
    };

    print_validation_pretty(&response, limit)?;
    match validation_follow_up(&response, severity_mode) {
        ValidationFollowUp::None => Ok(()),
        ValidationFollowUp::Warn(message) => {
            eprintln!("Warning: {message}");
            Ok(())
        }
        ValidationFollowUp::Fail(message) => Err(anyhow::anyhow!(message).into()),
    }
}

async fn source_test_not_found_error(
    app: &AppClient,
    source_name: &str,
    original_status: tonic::Status,
) -> Result<(), crate::CliError> {
    // Discovery failure must not mask the original validation error.
    let Ok(available) = discover_sources(app).await else {
        return Err(anyhow::Error::from(original_status).into());
    };
    if available
        .iter()
        .any(|source| source.name == source_name && !source.installed)
    {
        return Err(crate::CliError::SourceNotInstalled {
            source_name: source_name.to_string(),
        });
    }

    Err(crate::CliError::SourceNotFound {
        source_name: source_name.to_string(),
    })
}

pub(crate) async fn remove_and_print(
    app: &AppClient,
    source_name: &str,
) -> Result<(), crate::CliError> {
    let normalized = source_name_arg(Some(source_name))?;
    match delete_source(app, &normalized).await {
        Ok(()) => {
            println!("Removed source {normalized}");
            Ok(())
        }
        Err(err) => {
            if err
                .downcast_ref::<tonic::Status>()
                .is_some_and(is_source_missing_status)
            {
                Err(crate::CliError::SourceRemoveNotFound {
                    source_name: normalized,
                })
            } else {
                Err(err.into())
            }
        }
    }
}

/// Returns `true` only when the gRPC status carries the server's
/// `SOURCE_NOT_FOUND` AIP-193 reason. Other `Code::NotFound` causes
/// (e.g. a missing manifest file mapped from `io::ErrorKind::NotFound`)
/// have no Coral `ErrorInfo` attached, so they remain diagnosable instead
/// of being rewritten into the friendly "source not found" message.
fn is_source_missing_status(status: &tonic::Status) -> bool {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => {
            error.reason == CORAL_ERROR_REASON_SOURCE_NOT_FOUND
        }
        DecodedStatusError::Plain(_) => false,
    }
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
    println!(
        "  Secrets: {}",
        source_credential_storage_label(source.credential_storage)
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

fn prompt_source_config_secret(
    input: &ManifestInputSpec,
) -> Result<Option<SourceSecret>, anyhow::Error> {
    let env_value = read_source_input_env(&input.key).unwrap_or_default();
    if !env_value.is_empty() {
        return Ok(Some(SourceSecret {
            key: input.key.clone(),
            value: env_value,
        }));
    }
    prompt_secret(input)
}

enum SecretInputOutcome {
    SourceConfig(Option<SourceSecret>),
    OAuth {
        credential: OAuthCredentialRetrieval,
        label: String,
    },
}

fn prompt_secret_with_methods(
    input: &ManifestInputSpec,
    prefer_skip: bool,
) -> Result<SecretInputOutcome, anyhow::Error> {
    let Some(credential) = input.credential.as_ref() else {
        return Ok(SecretInputOutcome::SourceConfig(
            prompt_source_config_secret(input)?,
        ));
    };
    let Some(selected) = select_credential_method(input, credential, prefer_skip)? else {
        return Ok(SecretInputOutcome::SourceConfig(None));
    };
    let method = credential
        .methods
        .get(selected)
        .ok_or_else(|| anyhow::anyhow!("credential method index {selected} is out of range"))?;
    match method.kind {
        ManifestCredentialMethodKind::SourceConfig => Ok(SecretInputOutcome::SourceConfig(
            prompt_source_config_secret(input)?,
        )),
        ManifestCredentialMethodKind::OAuth => Ok(SecretInputOutcome::OAuth {
            credential: collect_oauth_credential_method(input, selected, method)?,
            label: credential_method_label(method),
        }),
    }
}

fn select_credential_method(
    input: &ManifestInputSpec,
    credential: &ManifestCredentialSpec,
    prefer_skip: bool,
) -> Result<Option<usize>, anyhow::Error> {
    if credential.methods.len() == 1 && input.required {
        return Ok(Some(0));
    }
    let theme = ColorfulTheme::default();
    let mut items = credential
        .methods
        .iter()
        .map(credential_method_label)
        .collect::<Vec<_>>();
    if !input.required {
        items.push("Skip".to_string());
    }
    let skip_index = items.len().saturating_sub(1);
    let selected = Select::with_theme(&theme)
        .with_prompt(format!("{} credential", input.key))
        .items(&items)
        .default(if !input.required && prefer_skip {
            skip_index
        } else {
            0
        })
        .interact()?;
    if !input.required && selected == skip_index {
        return Ok(None);
    }
    Ok(Some(selected))
}

fn credential_method_label(method: &ManifestCredentialMethod) -> String {
    method.label.clone().unwrap_or_else(|| match method.kind {
        ManifestCredentialMethodKind::SourceConfig => "Paste token".to_string(),
        ManifestCredentialMethodKind::OAuth => "Connect with OAuth".to_string(),
    })
}

fn collect_oauth_credential_method(
    input: &ManifestInputSpec,
    method_index: usize,
    method: &ManifestCredentialMethod,
) -> Result<OAuthCredentialRetrieval, anyhow::Error> {
    let oauth = method
        .oauth
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("oauth credential method is missing OAuth config"))?;
    Ok(OAuthCredentialRetrieval {
        input_key: input.key.clone(),
        method_index: Some(u32::try_from(method_index)?),
        credential_inputs: prompt_oauth_credential_inputs(oauth)?,
    })
}

fn oauth_error(action: &str, error: &tonic::Status) -> anyhow::Error {
    anyhow::anyhow!(
        "OAuth credential retrieval failed during {action}: {error}. Rerun `coral source add` to try again."
    )
}

fn prompt_oauth_credential_inputs(
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<Vec<OAuthCredentialInput>, anyhow::Error> {
    let mut values = Vec::new();
    if let Some(input_key) = oauth.client.id.input.as_deref()
        && let Some(value) = prompt_oauth_client_id(input_key, oauth.client.id.default.as_deref())?
    {
        values.push(OAuthCredentialInput {
            key: input_key.to_string(),
            value,
        });
    }
    if let Some(secret) = oauth.client.secret.as_ref() {
        let value = prompt_oauth_client_secret(&secret.input)?;
        values.push(OAuthCredentialInput {
            key: secret.input.clone(),
            value,
        });
    }
    Ok(values)
}

fn prompt_oauth_client_id(
    input_key: &str,
    default: Option<&str>,
) -> Result<Option<String>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let prompt = if default.is_some_and(|value| !value.is_empty()) {
        format!("{input_key} [source default]")
    } else {
        input_key.to_string()
    };
    let value = Input::<String>::with_theme(&theme)
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;
    if !value.is_empty() {
        return Ok(Some(value));
    }
    if default.is_some_and(|value| !value.is_empty()) {
        return Ok(None);
    }
    Err(anyhow::anyhow!(
        "missing required OAuth client ID '{input_key}'"
    ))
}

fn prompt_oauth_client_secret(input_key: &str) -> Result<String, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let value = Password::with_theme(&theme)
        .with_prompt(input_key)
        .allow_empty_password(false)
        .interact()?;
    if value.is_empty() {
        return Err(anyhow::anyhow!(
            "missing required OAuth client secret '{input_key}'"
        ));
    }
    Ok(value)
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
    #![expect(
        clippy::indexing_slicing,
        reason = "collected input order assertions intentionally fail loudly in tests"
    )]

    use coral_api::v1::ValidateSourceResponse;
    use coral_spec::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
        ManifestInputKind, ManifestInputSpec,
    };

    use std::collections::HashMap;

    use super::{
        CredentialPromptMode, ValidationFollowUp, ValidationSeverityMode, collect_inputs_with_hint,
        finalize_input_value, shell_quote_arg, source_name_arg, validation_follow_up,
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
                credential: None,
            },
            ManifestInputSpec {
                key: "LINEAR_API_KEY".to_string(),
                kind: ManifestInputKind::Secret,
                required: true,
                default_value: String::new(),
                hint: None,
                credential: None,
            },
        ];
        let env: HashMap<&str, &str> = [("LINEAR_API_KEY", "lin_token")].into_iter().collect();
        let (variables, secrets) = collect_inputs_with_hint(
            &inputs,
            |key| env.get(key).map(|v| (*v).to_string()).unwrap_or_default(),
            None,
        )
        .expect("should succeed");
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].key, "LINEAR_API_BASE");
        assert_eq!(variables[0].value, "https://api.linear.app");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].key, "LINEAR_API_KEY");
        assert_eq!(secrets[0].value, "lin_token");
    }

    #[test]
    fn credential_method_first_defers_env_for_secrets_with_credential_methods() {
        let input = ManifestInputSpec {
            key: "LINEAR_OAUTH_ACCESS_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: Some(ManifestCredentialSpec {
                methods: vec![ManifestCredentialMethod {
                    kind: ManifestCredentialMethodKind::SourceConfig,
                    label: Some("Paste token".to_string()),
                    description: None,
                    oauth: None,
                }],
            }),
        };

        assert!(CredentialPromptMode::EnvFirst.reads_env_before_prompt(&input));
        assert!(!CredentialPromptMode::CredentialMethodFirst.reads_env_before_prompt(&input));
    }

    #[test]
    fn credential_method_first_keeps_env_for_plain_inputs() {
        let variable = ManifestInputSpec {
            key: "LINEAR_API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: None,
        };
        let plain_secret = ManifestInputSpec {
            key: "LINEAR_API_KEY".to_string(),
            kind: ManifestInputKind::Secret,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: None,
        };

        assert!(CredentialPromptMode::CredentialMethodFirst.reads_env_before_prompt(&variable));
        assert!(CredentialPromptMode::CredentialMethodFirst.reads_env_before_prompt(&plain_secret));
    }

    #[test]
    fn collect_inputs_env_value_overrides_default() {
        let inputs = vec![ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
            hint: None,
            credential: None,
        }];
        let (variables, _) =
            collect_inputs_with_hint(&inputs, |_| "https://override.test".to_string(), None)
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
            credential: None,
        }];
        let (variables, secrets) = collect_inputs_with_hint(&inputs, |_| String::new(), None)
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
                credential: None,
            },
            ManifestInputSpec {
                key: "OTHER_KEY".to_string(),
                kind: ManifestInputKind::Variable,
                required: true,
                default_value: String::new(),
                hint: None,
                credential: None,
            },
        ];
        let error = collect_inputs_with_hint(&inputs, |_| String::new(), None)
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
            credential: None,
        }];
        let (variables, secrets) = collect_inputs_with_hint(&inputs, |_| String::new(), None)
            .expect("optional should be omitted");
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
            credential: None,
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
            credential: None,
        };
        let error = finalize_input_value(&input, String::new(), "source secret")
            .expect_err("required empty input should fail");
        assert!(error.to_string().contains("missing required source secret"));
    }

    #[test]
    fn shell_quote_arg_quotes_copyable_commands() {
        assert_eq!(shell_quote_arg("sources/demo.yaml"), "sources/demo.yaml");
        assert_eq!(
            shell_quote_arg("fixtures/my source.yaml"),
            "'fixtures/my source.yaml'"
        );
        assert_eq!(shell_quote_arg("it'demo.yaml"), "'it'\\''demo.yaml'");
    }

    #[test]
    fn validation_follow_up_is_none_when_all_query_tests_pass() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            table_functions: Vec::new(),
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
            table_functions: Vec::new(),
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
            table_functions: Vec::new(),
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
