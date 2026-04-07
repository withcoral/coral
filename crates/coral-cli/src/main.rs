//! `CLI` entrypoint for the local Coral app.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "CLI intentionally renders user-facing output"
)]

use std::io::{IsTerminal, stdin, stdout};
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest, ExecuteSqlRequest,
    ImportSourceRequest, ListSourcesRequest, SourceInputKind, SourceInputSpec, SourceOrigin,
    SourceSecret, SourceVariable, ValidateSourceRequest,
};
use coral_client::{
    ClientBuilder, decode_execute_sql_response, default_workspace, format_batches_json,
    format_batches_table,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec, collect_source_inputs_yaml};
use dialoguer::{Input, Password};
use tonic::Request;

#[derive(Debug, Parser)]
#[command(name = "coral", version)]
/// Query and manage local data sources
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Execute a SQL query
    Sql(SqlArgs),
    /// Manage data sources
    Source(SourceArgs),
    /// Start the MCP server over stdio
    McpStdio,
}

#[derive(Debug, Args)]
/// Execute a SQL query
struct SqlArgs {
    /// Output format for query results
    #[arg(long, value_enum, default_value = "table")]
    format: OutputFormat,
    /// SQL query to execute
    sql: String,
}

#[derive(Debug, Args)]
/// Manage data sources
struct SourceArgs {
    #[command(subcommand)]
    command: SourceCommand,
}

#[derive(Debug, Subcommand)]
enum SourceCommand {
    /// Discover available sources
    Discover,
    /// List configured sources
    List,
    /// Add a new source
    Add {
        /// Name for the new source
        name: String,
    },
    /// Import a source from a manifest file
    Import {
        /// Path to the source manifest file
        path: PathBuf,
    },
    /// Test connectivity for a source
    Test {
        /// Name of the source to test
        name: String,
    },
    /// Remove a source
    Remove {
        /// Name of the source to remove
        name: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

#[tokio::main]
#[allow(
    clippy::too_many_lines,
    reason = "CLI dispatch is intentionally centralized for the current product surface."
)]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let app = ClientBuilder::new().build().await?;

    match cli.command {
        Command::Sql(args) => {
            let response = app
                .query_client()
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(default_workspace()),
                    sql: args.sql,
                }))
                .await?
                .into_inner();
            let result = decode_execute_sql_response(&response)?;
            print_batches(result.batches(), args.format)?;
        }
        Command::Source(args) => match args.command {
            SourceCommand::Discover => {
                let response = app
                    .source_client()
                    .discover_sources(Request::new(DiscoverSourcesRequest {
                        workspace: Some(default_workspace()),
                    }))
                    .await?
                    .into_inner();
                if response.sources.is_empty() {
                    println!("No bundled sources available.");
                } else {
                    for source in response.sources {
                        let status = if source.installed {
                            "installed"
                        } else {
                            "available"
                        };
                        println!("{}\t{}\t{}", source.name, source.version, status);
                    }
                }
            }
            SourceCommand::List => {
                let response = app
                    .source_client()
                    .list_sources(Request::new(ListSourcesRequest {
                        workspace: Some(default_workspace()),
                    }))
                    .await?
                    .into_inner();
                if response.sources.is_empty() {
                    println!("No sources configured.");
                } else {
                    for source in response.sources {
                        let origin = source_origin_label(source.origin);
                        println!("{}\t{}\t{}", source.name, source.version, origin);
                    }
                }
            }
            SourceCommand::Add { name } => {
                require_interactive()?;
                let bundled_name = source_name_arg(Some(&name))?;
                let discover = app
                    .source_client()
                    .discover_sources(Request::new(DiscoverSourcesRequest {
                        workspace: Some(default_workspace()),
                    }))
                    .await?
                    .into_inner();
                let available = discover
                    .sources
                    .into_iter()
                    .find(|source| source.name == bundled_name)
                    .ok_or_else(|| anyhow::anyhow!("unknown bundled source '{bundled_name}'"))?;
                let inputs = available
                    .inputs
                    .iter()
                    .map(manifest_input_from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                let (variables, secrets) = prompt_for_inputs(&inputs)?;
                let response = app
                    .source_client()
                    .create_bundled_source(Request::new(CreateBundledSourceRequest {
                        workspace: Some(default_workspace()),
                        name: available.name.clone(),
                        variables,
                        secrets,
                    }))
                    .await?
                    .into_inner();
                println!("Added source {}", response.name);
            }
            SourceCommand::Import { path } => {
                require_interactive()?;
                let manifest_yaml = std::fs::read_to_string(&path)?;
                let inputs = collect_source_inputs_yaml(&manifest_yaml)?;
                let (variables, secrets) = prompt_for_inputs(&inputs)?;
                let response = app
                    .source_client()
                    .import_source(Request::new(ImportSourceRequest {
                        workspace: Some(default_workspace()),
                        manifest_yaml,
                        variables,
                        secrets,
                    }))
                    .await?
                    .into_inner();
                println!("Imported source {}", response.name);
            }
            SourceCommand::Test { name } => {
                let response = app
                    .source_client()
                    .validate_source(Request::new(ValidateSourceRequest {
                        workspace: Some(default_workspace()),
                        name: source_name_arg(Some(&name))?,
                    }))
                    .await?
                    .into_inner();
                let source = response.source.expect("source");
                println!("Source {} is queryable", source.name);
                for table in response.tables {
                    println!("{}.{}", table.schema_name, table.name);
                }
            }
            SourceCommand::Remove { name } => {
                app.source_client()
                    .delete_source(Request::new(DeleteSourceRequest {
                        workspace: Some(default_workspace()),
                        name: source_name_arg(Some(&name))?,
                    }))
                    .await?;
                println!("Removed source {name}");
            }
        },
        Command::McpStdio => {
            coral_mcp::run_stdio_with_client(app).await?;
        }
    }

    Ok(())
}

fn require_interactive() -> Result<(), anyhow::Error> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err(anyhow::anyhow!("interactive source install requires a TTY"));
    }
    Ok(())
}

fn source_name_arg(name: Option<&str>) -> Result<String, anyhow::Error> {
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

fn prompt_for_inputs(
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

fn manifest_input_from_proto(input: &SourceInputSpec) -> Result<ManifestInputSpec, anyhow::Error> {
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
    })
}

fn prompt_variable(input: &ManifestInputSpec) -> Result<Option<SourceVariable>, anyhow::Error> {
    let prompt = if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [{}]", input.key, input.default_value)
    };
    let value = Input::<String>::new()
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
    let prompt = if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [default hidden]", input.key)
    };
    let value = Password::new()
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

fn finalize_input_value(
    input: &ManifestInputSpec,
    value: String,
    kind_label: &str,
) -> Result<Option<String>, anyhow::Error> {
    if !value.is_empty() {
        return Ok(Some(value));
    }
    if !input.default_value.is_empty() {
        return Ok(Some(input.default_value.clone()));
    }
    if input.required {
        return Err(anyhow::anyhow!(
            "missing required {kind_label} '{}'",
            input.key
        ));
    }
    Ok(None)
}

fn source_origin_label(origin: i32) -> &'static str {
    match SourceOrigin::try_from(origin) {
        Ok(SourceOrigin::Bundled) => "bundled",
        Ok(SourceOrigin::Imported) => "imported",
        Ok(SourceOrigin::Unspecified) | Err(_) => "unknown",
    }
}

fn print_batches(
    batches: &[arrow::record_batch::RecordBatch],
    format: OutputFormat,
) -> Result<(), anyhow::Error> {
    let output = match format {
        OutputFormat::Table => format_batches_table(batches)?,
        OutputFormat::Json => format_batches_json(batches)?,
    };
    println!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use coral_spec::{ManifestInputKind, ManifestInputSpec};

    use super::finalize_input_value;

    #[test]
    fn empty_input_uses_default_value() {
        let input = ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
        };
        assert_eq!(
            finalize_input_value(&input, String::new(), "source variable")
                .expect("default should apply"),
            Some("https://example.com".to_string())
        );
    }

    #[test]
    fn empty_required_input_without_default_is_rejected() {
        let input = ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
        };
        let error = finalize_input_value(&input, String::new(), "source secret")
            .expect_err("required empty input should fail");
        assert!(error.to_string().contains("missing required source secret"));
    }
}
