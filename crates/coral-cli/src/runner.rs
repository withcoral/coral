use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest, ExecuteSqlRequest,
    ImportSourceRequest, ListSourcesRequest, ValidateSourceRequest,
};
use coral_client::{
    decode_execute_sql_response, default_workspace, format_batches_json, format_batches_table,
};
use tonic::Request;

use crate::cli::{Cli, Command, OutputFormat, SourceAddArgs, SourceCommand};
use crate::host::{CliHost, CliPrompter};
use crate::{CliServices, onboard, source_ops};

/// Runs one parsed CLI command against the provided backend and terminal adapters.
///
/// # Errors
/// Returns an error if the command fails.
#[allow(
    clippy::too_many_lines,
    reason = "single match dispatch over CLI commands"
)]
pub async fn run(
    cli: Cli,
    services: &mut CliServices,
    host: &mut dyn CliHost,
    prompts: &mut dyn CliPrompter,
) -> Result<(), anyhow::Error> {
    match cli.command {
        Command::Sql(args) => {
            let response = services
                .query_client
                .execute_sql(Request::new(ExecuteSqlRequest {
                    workspace: Some(default_workspace()),
                    sql: args.sql,
                }))
                .await?
                .into_inner();
            let result = decode_execute_sql_response(&response)?;
            print_batches(host, result.batches(), args.format)?;
        }
        Command::Source(args) => match args.command {
            SourceCommand::Discover => {
                let sources = services
                    .source_client
                    .discover_sources(Request::new(DiscoverSourcesRequest {
                        workspace: Some(default_workspace()),
                    }))
                    .await?
                    .into_inner()
                    .sources;
                if sources.is_empty() {
                    host.println("No bundled sources available.")?;
                } else {
                    for source in sources {
                        let status = if source.installed {
                            "installed"
                        } else {
                            "available"
                        };
                        host.println(&format!("{}\t{}\t{status}", source.name, source.version))?;
                    }
                }
            }
            SourceCommand::List => {
                let sources = services
                    .source_client
                    .list_sources(Request::new(ListSourcesRequest {
                        workspace: Some(default_workspace()),
                    }))
                    .await?
                    .into_inner()
                    .sources;
                if sources.is_empty() {
                    host.println("No sources configured.")?;
                } else {
                    for source in sources {
                        let origin = source_ops::source_origin_label(source.origin);
                        host.println(&format!("{}\t{}\t{origin}", source.name, source.version))?;
                    }
                }
            }
            SourceCommand::Add(SourceAddArgs { name, file }) => {
                source_ops::require_interactive(host)?;
                let response = match (name, file) {
                    (Some(name), None) => {
                        let bundled_name = source_ops::source_name_arg(Some(&name))?;
                        let discover = services
                            .source_client
                            .discover_sources(Request::new(DiscoverSourcesRequest {
                                workspace: Some(default_workspace()),
                            }))
                            .await?
                            .into_inner()
                            .sources;
                        let available = discover
                            .into_iter()
                            .find(|source| source.name == bundled_name)
                            .ok_or_else(|| {
                                anyhow::anyhow!("unknown bundled source '{bundled_name}'")
                            })?;
                        let inputs = available
                            .inputs
                            .iter()
                            .map(source_ops::manifest_input_from_proto)
                            .collect::<Result<Vec<_>, _>>()?;
                        let (variables, secrets) = source_ops::prompt_for_inputs(prompts, &inputs)?;
                        services
                            .source_client
                            .create_bundled_source(Request::new(CreateBundledSourceRequest {
                                workspace: Some(default_workspace()),
                                name: available.name,
                                variables,
                                secrets,
                            }))
                            .await?
                            .into_inner()
                    }
                    (None, Some(file)) => {
                        let (manifest_yaml, inputs) = source_ops::load_manifest_inputs(&file)?;
                        let (variables, secrets) = source_ops::prompt_for_inputs(prompts, &inputs)?;
                        services
                            .source_client
                            .import_source(Request::new(ImportSourceRequest {
                                workspace: Some(default_workspace()),
                                manifest_yaml,
                                variables,
                                secrets,
                            }))
                            .await?
                            .into_inner()
                    }
                    _ => unreachable!("clap enforces exactly one of name or file"),
                };
                host.println(&format!("Added source {}", response.name))?;
            }
            SourceCommand::Test { name } => {
                let response = services
                    .source_client
                    .validate_source(Request::new(ValidateSourceRequest {
                        workspace: Some(default_workspace()),
                        name: source_ops::source_name_arg(Some(&name))?,
                    }))
                    .await?
                    .into_inner();
                source_ops::print_validation_success(host, &response)?;
            }
            SourceCommand::Remove { name } => {
                services
                    .source_client
                    .delete_source(Request::new(DeleteSourceRequest {
                        workspace: Some(default_workspace()),
                        name: source_ops::source_name_arg(Some(&name))?,
                    }))
                    .await?;
                host.println(&format!("Removed source {name}"))?;
            }
        },
        Command::Onboard => {
            onboard::run(services, host, prompts).await?;
        }
        Command::McpStdio => {
            let services = std::mem::replace(
                services,
                CliServices::from_clients(
                    services.source_client.clone(),
                    services.query_client.clone(),
                ),
            );
            services.serve_mcp_stdio().await?;
        }
    }

    Ok(())
}

fn print_batches(
    host: &mut dyn CliHost,
    batches: &[arrow::record_batch::RecordBatch],
    format: OutputFormat,
) -> Result<(), anyhow::Error> {
    let output = match format {
        OutputFormat::Table => format_batches_table(batches)?,
        OutputFormat::Json => format_batches_json(batches)?,
    };
    host.println(&output)?;
    Ok(())
}
