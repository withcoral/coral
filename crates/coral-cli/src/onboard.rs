use std::collections::BTreeMap;

use coral_api::v1::{
    AvailableSource, CreateBundledSourceRequest, DiscoverSourcesRequest, ExecuteSqlRequest,
    ListSourcesRequest, Source, ValidateSourceRequest, ValidateSourceResponse,
};
use coral_client::{decode_execute_sql_response, default_workspace, format_batches_table};
use dialoguer::console::{measure_text_width, style};
use tonic::Request;

use crate::host::{CliHost, CliPrompter};
use crate::{CliServices, source_ops};

const SOURCE_DESCRIPTION_PREVIEW_LIMIT: usize = 88;

enum TopLevelChoice {
    BundledSource(usize),
    Finish,
    Exit,
}

enum NextStepChoice {
    AddMoreSources,
    Exit,
}

#[derive(Clone, Copy)]
enum NextStepAction {
    RunExampleQuery,
    AddMoreSources,
    OpenDocs,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstalledSourceAction {
    Validate,
    Reconfigure,
    Back,
}

pub(crate) async fn run(
    services: &mut CliServices,
    host: &mut dyn CliHost,
    prompts: &mut dyn CliPrompter,
) -> Result<(), anyhow::Error> {
    source_ops::require_interactive(host)?;

    crate::branding::print_welcome_header(host)?;

    loop {
        let bundled_sources = services
            .source_client
            .discover_sources(Request::new(DiscoverSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await?
            .into_inner()
            .sources;

        if bundled_sources.is_empty() {
            host.println("")?;
            host.println(&format!(
                "No sources available. Visit {} for setup instructions.",
                style("withcoral.com/docs").bold()
            ))?;
            return Ok(());
        }

        host.println("")?;
        host.println(&format!(
            "{}",
            style("To start, connect at least one source:").bold()
        ))?;
        host.println("")?;

        match select_top_level(prompts, &bundled_sources)? {
            TopLevelChoice::BundledSource(idx) => {
                let source = &bundled_sources[idx];
                if source.installed {
                    run_installed_source_menu(services, host, prompts, source).await?;
                } else {
                    run_add_bundled_source(services, host, prompts, source).await?;
                    match run_next_steps(services, host, prompts).await? {
                        NextStepChoice::AddMoreSources => {}
                        NextStepChoice::Exit => return Ok(()),
                    }
                }
            }
            TopLevelChoice::Finish => match run_next_steps(services, host, prompts).await? {
                NextStepChoice::AddMoreSources => {}
                NextStepChoice::Exit => return Ok(()),
            },
            TopLevelChoice::Exit => return Ok(()),
        }
    }
}

fn select_top_level(
    prompts: &mut dyn CliPrompter,
    bundled_sources: &[AvailableSource],
) -> Result<TopLevelChoice, anyhow::Error> {
    let name_width = bundled_sources
        .iter()
        .map(|s| measure_text_width(&s.name))
        .max()
        .unwrap_or(0);

    let mut labels: Vec<String> = bundled_sources
        .iter()
        .map(|source| format_source_list_item(source, name_width))
        .collect();

    let has_installed = bundled_sources.iter().any(|s| s.installed);
    let finish_index = if has_installed {
        labels.push("I have connected enough sources".to_string());
        Some(bundled_sources.len())
    } else {
        None
    };

    let first_uninstalled = bundled_sources
        .iter()
        .position(|s| !s.installed)
        .unwrap_or(bundled_sources.len());

    let selection = prompts.select("Choose a source", &labels, first_uninstalled)?;

    match selection {
        Some(idx) if idx < bundled_sources.len() => Ok(TopLevelChoice::BundledSource(idx)),
        Some(idx) if finish_index == Some(idx) => Ok(TopLevelChoice::Finish),
        _ => Ok(TopLevelChoice::Exit),
    }
}

fn format_source_list_item(source: &AvailableSource, name_width: usize) -> String {
    let check = if source.installed { "✓ " } else { "  " };
    let preview = if source.description.is_empty() {
        String::new()
    } else {
        format!(
            "  {}",
            truncate_description(&source.description, SOURCE_DESCRIPTION_PREVIEW_LIMIT)
        )
    };
    format!("{check}{:<name_width$}{preview}", source.name)
}

async fn run_installed_source_menu(
    services: &mut CliServices,
    host: &mut dyn CliHost,
    prompts: &mut dyn CliPrompter,
    source: &AvailableSource,
) -> Result<(), anyhow::Error> {
    let items = vec![
        "Update credentials".to_string(),
        "Validate".to_string(),
        "Back".to_string(),
    ];
    let actions = [
        InstalledSourceAction::Reconfigure,
        InstalledSourceAction::Validate,
        InstalledSourceAction::Back,
    ];

    let selection = prompts.select(&format!("{} is already installed", source.name), &items, 0)?;

    match selection.map(|i| actions[i]) {
        Some(InstalledSourceAction::Validate) => {
            validate_after_install(services, host, &source.name).await?;
        }
        Some(InstalledSourceAction::Reconfigure) => {
            let inputs = source
                .inputs
                .iter()
                .map(source_ops::manifest_input_from_proto)
                .collect::<Result<Vec<_>, _>>()?;
            let (variables, secrets) = source_ops::prompt_for_inputs(prompts, &inputs)?;
            let result = services
                .source_client
                .create_bundled_source(Request::new(CreateBundledSourceRequest {
                    workspace: Some(default_workspace()),
                    name: source.name.clone(),
                    variables,
                    secrets,
                }))
                .await?;
            let result = result.into_inner();
            host.println(&format!("Reconfigured source {}", result.name))?;
            validate_after_install(services, host, &result.name).await?;
        }
        Some(InstalledSourceAction::Back) | None => {}
    }

    Ok(())
}

async fn run_add_bundled_source(
    services: &mut CliServices,
    host: &mut dyn CliHost,
    prompts: &mut dyn CliPrompter,
    source: &AvailableSource,
) -> Result<(), anyhow::Error> {
    let inputs = source
        .inputs
        .iter()
        .map(source_ops::manifest_input_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let (variables, secrets) = source_ops::prompt_for_inputs(prompts, &inputs)?;
    let result = services
        .source_client
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: source.name.clone(),
            variables,
            secrets,
        }))
        .await?
        .into_inner();
    host.println(&format!("Added source {}", result.name))?;
    validate_after_install(services, host, &result.name).await
}

async fn validate_after_install(
    services: &mut CliServices,
    host: &mut dyn CliHost,
    source_name: &str,
) -> Result<(), anyhow::Error> {
    let response = services
        .source_client
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: source_ops::source_name_arg(Some(source_name))?,
        }))
        .await?
        .into_inner();
    print_validation_pretty(host, &response)
}

const MAX_TABLES_PER_SCHEMA: usize = 9;

fn print_validation_pretty(
    host: &mut dyn CliHost,
    response: &ValidateSourceResponse,
) -> Result<(), anyhow::Error> {
    let source = response
        .source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("validate response missing source metadata"))?;

    host.println("")?;
    host.println(&format!(
        "  {} {}",
        style("✓").green(),
        style(format!("{} connected successfully", source.name)).bold()
    ))?;

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
        host.println("")?;
        host.println(&format!(
            "    {}",
            style(format!(
                "{schema} ({count} {})",
                if count == 1 { "table" } else { "tables" }
            ))
            .bold()
        ))?;

        let show_count = tables.len().min(MAX_TABLES_PER_SCHEMA);
        let remaining = tables.len() - show_count;

        for (i, table) in tables.iter().take(show_count).enumerate() {
            let is_last = i == show_count - 1 && remaining == 0;
            let branch = if is_last { "└─" } else { "├─" };
            host.println(&format!("    {} {}", style(branch).dim(), table))?;
        }

        if remaining > 0 {
            host.println(&format!(
                "    {} {}",
                style("└─").dim(),
                style(format!("... and {remaining} more")).dim()
            ))?;
        }
    }
    host.println("")?;

    Ok(())
}

async fn run_next_steps(
    services: &mut CliServices,
    host: &mut dyn CliHost,
    prompts: &mut dyn CliPrompter,
) -> Result<NextStepChoice, anyhow::Error> {
    let installed_sources = services
        .source_client
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await?
        .into_inner()
        .sources;
    show_next_steps_screen(services, host, prompts, &installed_sources).await
}

async fn show_next_steps_screen(
    services: &mut CliServices,
    host: &mut dyn CliHost,
    prompts: &mut dyn CliPrompter,
    installed_sources: &[Source],
) -> Result<NextStepChoice, anyhow::Error> {
    // --- Static summary ---
    host.println("")?;
    if installed_sources.is_empty() {
        host.println(&format!(
            "No sources connected yet — you can add them anytime with {}.",
            style("coral source add").bold()
        ))?;
    } else {
        let n = installed_sources.len();
        host.println(&format!(
            "{}",
            style(format!(
                "You've connected {} {}.",
                n,
                if n == 1 { "source" } else { "sources" }
            ))
            .bold()
        ))?;
        host.println("")?;
        for s in installed_sources {
            host.println(&format!("  {} {}", style("✓").green(), s.name))?;
        }
    }

    host.println("")?;
    host.println(&format!("{}", style("What's next:").bold()))?;
    if !installed_sources.is_empty() {
        host.println(&format!(
            "  {} {}",
            style("•").dim(),
            style("coral sql \"SELECT ...\"            Run a one-off query").dim()
        ))?;
    }
    host.println(&format!(
        "  {} {}",
        style("•").dim(),
        style("npx skills add withcoral/skills     Add Coral skills to your agent").dim()
    ))?;
    host.println(&format!(
        "  {} {}",
        style("•").dim(),
        style("Set up MCP for your agent       withcoral.com/docs/guides/use-coral-over-mcp").dim()
    ))?;
    host.println("")?;
    host.println(&format!(
        "{}",
        style("Learn more about Coral at withcoral.com/docs").dim()
    ))?;

    // --- Interactive menu ---
    let has_sources = !installed_sources.is_empty();

    loop {
        host.println("")?;
        let mut items: Vec<(&str, NextStepAction)> = Vec::new();
        if has_sources {
            items.push(("Run an example query", NextStepAction::RunExampleQuery));
        }
        items.push(("Add more sources", NextStepAction::AddMoreSources));
        items.push(("Open docs in browser", NextStepAction::OpenDocs));
        items.push(("Exit", NextStepAction::Exit));

        let labels = items
            .iter()
            .map(|(label, _)| (*label).to_string())
            .collect::<Vec<_>>();
        let selection = prompts.select("What would you like to do?", &labels, 0)?;

        let action = selection.map(|i| items[i].1);
        match action {
            Some(NextStepAction::RunExampleQuery) => {
                let sql = "SELECT schema_name, COUNT(*) AS table_count FROM coral.tables GROUP BY schema_name ORDER BY 1";
                match run_first_query(services, sql).await {
                    Ok(output) => {
                        host.println("")?;
                        host.println(&format!("{}", style(sql).dim()))?;
                        host.println(&output)?;
                    }
                    Err(err) => {
                        host.println("")?;
                        host.println(&format!(
                            "{}",
                            style(format!("Could not run query: {err}")).red()
                        ))?;
                    }
                }
            }
            Some(NextStepAction::AddMoreSources) => return Ok(NextStepChoice::AddMoreSources),
            Some(NextStepAction::OpenDocs) => {
                host.open_url("https://withcoral.com/docs");
            }
            Some(NextStepAction::Exit) | None => return Ok(NextStepChoice::Exit),
        }
    }
}

async fn run_first_query(services: &mut CliServices, sql: &str) -> Result<String, anyhow::Error> {
    let response = services
        .query_client
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: sql.to_string(),
        }))
        .await?
        .into_inner();
    let result = decode_execute_sql_response(&response)?;
    Ok(format_batches_table(result.batches())?)
}

fn truncate_description(description: &str, max_len: usize) -> String {
    let description = description.trim();
    if description.chars().count() <= max_len {
        return description.to_string();
    }

    let preview = description
        .chars()
        .take(max_len.saturating_sub(3))
        .collect::<String>();
    format!("{preview}...")
}

#[cfg(test)]
mod tests {
    use coral_api::v1::AvailableSource;

    use super::{format_source_list_item, truncate_description};

    #[test]
    fn source_list_item_shows_checkmark_for_installed() {
        let source = AvailableSource {
            name: "github".to_string(),
            description: "Query repositories and issues".to_string(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: true,
            origin: 1,
        };
        let item = format_source_list_item(&source, 10);
        assert!(item.starts_with("✓ "));
        assert!(item.contains("github"));
        assert!(item.contains("Query repositories and issues"));
    }

    #[test]
    fn source_list_item_shows_space_for_uninstalled() {
        let source = AvailableSource {
            name: "slack".to_string(),
            description: "Send and receive messages".to_string(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: 1,
        };
        let item = format_source_list_item(&source, 10);
        assert!(item.starts_with("  "));
        assert!(item.contains("slack"));
    }

    #[test]
    fn source_list_item_aligns_names() {
        let short = AvailableSource {
            name: "gh".to_string(),
            description: "GitHub".to_string(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: 1,
        };
        let long = AvailableSource {
            name: "statusgator".to_string(),
            description: "Status pages".to_string(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: 1,
        };
        let width = 11; // len of "statusgator"
        let short_item = format_source_list_item(&short, width);
        let long_item = format_source_list_item(&long, width);
        // Description columns should start at the same position
        let short_desc_pos = short_item.find("GitHub").unwrap();
        let long_desc_pos = long_item.find("Status pages").unwrap();
        assert_eq!(short_desc_pos, long_desc_pos);
    }

    #[test]
    fn truncate_description_adds_ascii_ellipsis_when_needed() {
        let description = "abcdefghijklmnopqrstuvwxyz";
        assert_eq!(truncate_description(description, 10), "abcdefg...");
    }
}
