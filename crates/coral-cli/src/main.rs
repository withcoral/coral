//! `CLI` entrypoint for the local Coral app.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "CLI intentionally renders user-facing output"
)]

mod onboard;
mod source_ops;

use clap::Parser;
use coral_api::v1::ExecuteSqlRequest;
use coral_cli::{Cli, Command, OutputFormat, SourceCommand};
use coral_client::{
    ClientBuilder, decode_execute_sql_response, default_workspace, format_batches_json,
    format_batches_table,
};
use tonic::Request;

#[tokio::main]
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
                let sources = source_ops::discover_sources(&app).await?;
                if sources.is_empty() {
                    println!("No bundled sources available.");
                } else {
                    for source in sources {
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
                let sources = source_ops::list_sources(&app).await?;
                if sources.is_empty() {
                    println!("No sources configured.");
                } else {
                    for source in sources {
                        let origin = source_ops::source_origin_label(source.origin);
                        println!("{}\t{}\t{}", source.name, source.version, origin);
                    }
                }
            }
            SourceCommand::Add { name } => {
                source_ops::require_interactive()?;
                let bundled_name = source_ops::source_name_arg(Some(&name))?;
                let discover = source_ops::discover_sources(&app).await?;
                let available = discover
                    .into_iter()
                    .find(|source| source.name == bundled_name)
                    .ok_or_else(|| anyhow::anyhow!("unknown bundled source '{bundled_name}'"))?;
                let inputs = available
                    .inputs
                    .iter()
                    .map(source_ops::manifest_input_from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
                let response =
                    source_ops::add_bundled_source(&app, &available.name, variables, secrets)
                        .await?;
                println!("Added source {}", response.name);
            }
            SourceCommand::Import { path } => {
                source_ops::require_interactive()?;
                let (manifest_yaml, inputs) = source_ops::load_manifest_inputs(&path)?;
                let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
                let response =
                    source_ops::import_source(&app, manifest_yaml, variables, secrets).await?;
                println!("Imported source {}", response.name);
            }
            SourceCommand::Test { name } => {
                let response = source_ops::validate_source(&app, &name).await?;
                source_ops::print_validation_success(&response)?;
            }
            SourceCommand::Remove { name } => {
                source_ops::delete_source(&app, &name).await?;
                println!("Removed source {name}");
            }
        },
        Command::Onboard => {
            onboard::run(&app).await?;
        }
        Command::McpStdio => {
            coral_mcp::run_stdio_with_client(app).await?;
        }
    }

    Ok(())
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
