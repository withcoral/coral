use std::collections::BTreeMap;

use coral_api::v1::{AvailableSource, ExecuteSqlRequest, Source, ValidateSourceResponse};
use coral_client::{
    AppClient, decode_execute_sql_response, default_workspace, format_batches_table,
};
use dialoguer::console::{measure_text_width, style};
use dialoguer::{Select, theme::ColorfulTheme};
use tonic::Request;

use crate::source_ops;

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

pub(crate) async fn run(app: &AppClient) -> Result<(), anyhow::Error> {
    source_ops::require_interactive()?;
    let theme = ColorfulTheme::default();

    crate::branding::print_welcome_header();

    loop {
        let bundled_sources = source_ops::discover_sources(app).await?;

        println!();
        println!("{}", style("To start, connect at least one source:").bold());
        println!();

        match select_top_level(&theme, &bundled_sources)? {
            TopLevelChoice::BundledSource(idx) => {
                let source = &bundled_sources[idx];
                if source.installed {
                    run_installed_source_menu(app, &theme, source).await?;
                } else {
                    run_add_bundled_source(app, source).await?;
                    match run_next_steps(app, &theme).await? {
                        NextStepChoice::AddMoreSources => {}
                        NextStepChoice::Exit => return Ok(()),
                    }
                }
            }
            TopLevelChoice::Finish => match run_next_steps(app, &theme).await? {
                NextStepChoice::AddMoreSources => {}
                NextStepChoice::Exit => return Ok(()),
            },
            TopLevelChoice::Exit => return Ok(()),
        }
    }
}

fn select_top_level(
    theme: &ColorfulTheme,
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

    labels.push("I have connected enough sources".to_string());

    let first_uninstalled = bundled_sources
        .iter()
        .position(|s| !s.installed)
        .unwrap_or(bundled_sources.len());

    let selection = Select::with_theme(theme)
        .with_prompt("Choose a source")
        .items(&labels)
        .default(first_uninstalled)
        .interact_opt()?;

    match selection {
        Some(idx) if idx < bundled_sources.len() => Ok(TopLevelChoice::BundledSource(idx)),
        Some(idx) if idx == bundled_sources.len() => Ok(TopLevelChoice::Finish),
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
    app: &AppClient,
    theme: &ColorfulTheme,
    source: &AvailableSource,
) -> Result<(), anyhow::Error> {
    let items = ["Update credentials", "Validate", "Back"];
    let actions = [
        InstalledSourceAction::Reconfigure,
        InstalledSourceAction::Validate,
        InstalledSourceAction::Back,
    ];

    let selection = Select::with_theme(theme)
        .with_prompt(format!("{} is already installed", source.name))
        .items(items)
        .default(0)
        .interact_opt()?;

    match selection.map(|i| actions[i]) {
        Some(InstalledSourceAction::Validate) => {
            validate_after_install(app, &source.name).await?;
        }
        Some(InstalledSourceAction::Reconfigure) => {
            let inputs = source
                .inputs
                .iter()
                .map(source_ops::input_from_proto)
                .collect::<Result<Vec<_>, _>>()?;
            let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
            let result =
                source_ops::add_bundled_source(app, &source.name, variables, secrets).await?;
            println!("Reconfigured source {}", result.name);
            validate_after_install(app, &result.name).await?;
        }
        Some(InstalledSourceAction::Back) | None => {}
    }

    Ok(())
}

async fn run_add_bundled_source(
    app: &AppClient,
    source: &AvailableSource,
) -> Result<(), anyhow::Error> {
    let inputs = source
        .inputs
        .iter()
        .map(source_ops::input_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
    let result = source_ops::add_bundled_source(app, &source.name, variables, secrets).await?;
    println!("Added source {}", result.name);
    validate_after_install(app, &result.name).await
}

async fn validate_after_install(app: &AppClient, source_name: &str) -> Result<(), anyhow::Error> {
    let response = source_ops::validate_source(app, source_name).await?;
    print_validation_pretty(&response)
}

const MAX_TABLES_PER_SCHEMA: usize = 9;

fn print_validation_pretty(response: &ValidateSourceResponse) -> Result<(), anyhow::Error> {
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

        let show_count = tables.len().min(MAX_TABLES_PER_SCHEMA);
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
    println!();

    Ok(())
}

async fn run_next_steps(
    app: &AppClient,
    theme: &ColorfulTheme,
) -> Result<NextStepChoice, anyhow::Error> {
    let installed_sources = source_ops::list_sources(app).await?;
    show_next_steps_screen(app, theme, &installed_sources).await
}

async fn show_next_steps_screen(
    app: &AppClient,
    theme: &ColorfulTheme,
    installed_sources: &[Source],
) -> Result<NextStepChoice, anyhow::Error> {
    // --- Static summary ---
    println!();
    if installed_sources.is_empty() {
        println!(
            "No sources connected yet — you can add them anytime with {}.",
            style("coral source add").bold()
        );
    } else {
        let n = installed_sources.len();
        println!(
            "{}",
            style(format!(
                "You've connected {} {}.",
                n,
                if n == 1 { "source" } else { "sources" }
            ))
            .bold()
        );
        println!();
        for s in installed_sources {
            println!("  {} {}", style("✓").green(), s.name);
        }
    }

    println!();
    println!("{}", style("What's next:").bold());
    if !installed_sources.is_empty() {
        println!(
            "  {} {}",
            style("•").dim(),
            style("coral sql \"SELECT ...\"            Run a one-off query").dim()
        );
    }
    println!(
        "  {} {}",
        style("•").dim(),
        style("npx skills add withcoral/skills     Add Coral skills to your agent").dim()
    );
    println!(
        "  {} {}",
        style("•").dim(),
        style("Set up MCP for your agent       withcoral.com/docs/guides/use-coral-over-mcp").dim()
    );
    println!();
    println!(
        "{}",
        style("Learn more about Coral at withcoral.com/docs").dim()
    );

    // --- Interactive menu ---
    let has_sources = !installed_sources.is_empty();

    loop {
        println!();
        let mut items: Vec<(&str, NextStepAction)> = Vec::new();
        if has_sources {
            items.push(("Run an example query", NextStepAction::RunExampleQuery));
        }
        items.push(("Add more sources", NextStepAction::AddMoreSources));
        items.push(("Open docs in browser", NextStepAction::OpenDocs));
        items.push(("Exit", NextStepAction::Exit));

        let labels: Vec<&str> = items.iter().map(|(label, _)| *label).collect();

        let selection = Select::with_theme(theme)
            .with_prompt("What would you like to do?")
            .items(&labels)
            .default(0)
            .interact_opt()?;

        let action = selection.map(|i| items[i].1);
        match action {
            Some(NextStepAction::RunExampleQuery) => {
                let sql = "SELECT schema_name, COUNT(*) AS table_count FROM coral.tables GROUP BY schema_name ORDER BY 1";
                match run_first_query(app, sql).await {
                    Ok(output) => {
                        println!();
                        println!("{}", style(sql).dim());
                        println!("{output}");
                    }
                    Err(err) => {
                        println!();
                        println!("{}", style(format!("Could not run query: {err}")).red());
                    }
                }
            }
            Some(NextStepAction::AddMoreSources) => return Ok(NextStepChoice::AddMoreSources),
            Some(NextStepAction::OpenDocs) => {
                open_url("https://withcoral.com/docs");
            }
            Some(NextStepAction::Exit) | None => return Ok(NextStepChoice::Exit),
        }
    }
}

async fn run_first_query(app: &AppClient, sql: &str) -> Result<String, anyhow::Error> {
    let response = app
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(default_workspace()),
            sql: sql.to_string(),
        }))
        .await?
        .into_inner();
    let result = decode_execute_sql_response(&response)?;
    Ok(format_batches_table(result.batches())?)
}

fn open_url(url: &str) {
    let result = if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).status()
    } else if cfg!(target_os = "linux") {
        std::process::Command::new("xdg-open").arg(url).status()
    } else if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(["/c", "start", url])
            .status()
    } else {
        return;
    };
    match result {
        Ok(status) if status.success() => {}
        Ok(status) => println!("{}", style(format!("Browser exited with {status}")).dim()),
        Err(err) => println!("{}", style(format!("Could not open browser: {err}")).dim()),
    }
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
