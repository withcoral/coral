use coral_api::v1::{AvailableSource, Source};
use coral_client::AppClient;
use dialoguer::console::{measure_text_width, style};
use dialoguer::{Confirm, Select, theme::ColorfulTheme};

use crate::source_ops;

const SOURCE_DESCRIPTION_PREVIEW_LIMIT: usize = 88;

enum TopLevelChoice {
    BundledSource(usize),
    Finish,
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
        let installed_sources = source_ops::list_sources(app).await?;
        let bundled_sources = source_ops::discover_sources(app).await?;

        println!();
        println!(
            "{}",
            style("To start, we recommend connecting as many sources as possible:").bold()
        );
        println!();

        match select_top_level(&theme, &bundled_sources)? {
            TopLevelChoice::BundledSource(idx) => {
                let source = &bundled_sources[idx];
                if source.installed {
                    run_installed_source_menu(app, &theme, source).await?;
                } else {
                    run_add_bundled_source(app, &theme, source).await?;
                }
            }
            TopLevelChoice::Finish => {
                print_next_steps(&installed_sources);
                return Ok(());
            }
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

    labels.push("Finish onboarding".to_string());

    let first_uninstalled = bundled_sources
        .iter()
        .position(|s| !s.installed)
        .unwrap_or(0);

    let selection = Select::with_theme(theme)
        .with_prompt("Choose a source")
        .items(&labels)
        .default(first_uninstalled)
        .interact_opt()?;

    match selection {
        Some(idx) if idx < bundled_sources.len() => Ok(TopLevelChoice::BundledSource(idx)),
        Some(idx) if idx == bundled_sources.len() => Ok(TopLevelChoice::Finish),
        _ => Ok(TopLevelChoice::Finish),
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
    let items = ["Validate", "Reconfigure", "Back"];
    let actions = [
        InstalledSourceAction::Validate,
        InstalledSourceAction::Reconfigure,
        InstalledSourceAction::Back,
    ];

    let selection = Select::with_theme(theme)
        .with_prompt(format!("{} is already installed", source.name))
        .items(items)
        .default(0)
        .interact_opt()?;

    match selection.map(|i| actions[i]) {
        Some(InstalledSourceAction::Validate) => {
            let response = source_ops::validate_source(app, &source.name).await?;
            source_ops::print_validation_success(&response)?;
        }
        Some(InstalledSourceAction::Reconfigure) => {
            let inputs = source
                .inputs
                .iter()
                .map(source_ops::manifest_input_from_proto)
                .collect::<Result<Vec<_>, _>>()?;
            let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
            let result =
                source_ops::add_bundled_source(app, &source.name, variables, secrets).await?;
            println!("Reconfigured source {}", result.name);
            maybe_validate_after_install(app, theme, &result.name).await?;
        }
        Some(InstalledSourceAction::Back) | None => {}
    }

    Ok(())
}

async fn run_add_bundled_source(
    app: &AppClient,
    theme: &ColorfulTheme,
    source: &AvailableSource,
) -> Result<(), anyhow::Error> {
    let inputs = source
        .inputs
        .iter()
        .map(source_ops::manifest_input_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
    let result = source_ops::add_bundled_source(app, &source.name, variables, secrets).await?;
    println!("Added source {}", result.name);
    maybe_validate_after_install(app, theme, &result.name).await
}

async fn maybe_validate_after_install(
    app: &AppClient,
    theme: &ColorfulTheme,
    source_name: &str,
) -> Result<(), anyhow::Error> {
    let should_validate = Confirm::with_theme(theme)
        .with_prompt(format!("Validate {source_name} now?"))
        .default(true)
        .interact()?;
    if should_validate {
        let response = source_ops::validate_source(app, source_name).await?;
        source_ops::print_validation_success(&response)?;
    }
    Ok(())
}

fn print_next_steps(installed_sources: &[Source]) {
    println!();
    println!("Next steps:");
    if installed_sources.is_empty() {
        println!("  coral source discover");
        println!("  coral source list");
    } else {
        println!("  coral source list");
        println!("  coral sql \"SELECT schema_name, table_name FROM coral.tables ORDER BY 1, 2\"");
        println!("  npx skills add withcoral/skills");
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
