use std::path::PathBuf;

use coral_api::v1::{AvailableSource, Source};
use coral_client::AppClient;
use dialoguer::console::{measure_text_width, style};
use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};

use crate::source_ops;

const SOURCE_DESCRIPTION_PREVIEW_LIMIT: usize = 88;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnboardAction {
    AddBundledSource,
    ImportSourceManifest,
    ValidateInstalledSource,
    Finish,
}

pub(crate) async fn run(app: &AppClient) -> Result<(), anyhow::Error> {
    source_ops::require_interactive()?;
    let theme = ColorfulTheme::default();

    println!("{}", style("Coral onboarding").bold().cyan());
    println!();
    println!(
        "{}",
        style("Add a source to this workspace, validate it, then start querying.").dim()
    );

    loop {
        let installed_sources = source_ops::list_sources(app).await?;
        let bundled_sources = source_ops::discover_sources(app).await?;

        print_workspace_summary(&installed_sources, &bundled_sources);

        match select_onboard_action(&theme, !installed_sources.is_empty())? {
            OnboardAction::AddBundledSource => {
                run_add_bundled_source(app, &theme, &bundled_sources).await?;
            }
            OnboardAction::ImportSourceManifest => {
                run_import_source(app, &theme).await?;
            }
            OnboardAction::ValidateInstalledSource => {
                run_validate_source(app, &theme, &installed_sources).await?;
            }
            OnboardAction::Finish => {
                print_next_steps(&installed_sources);
                return Ok(());
            }
        }
    }
}

fn print_workspace_summary(installed_sources: &[Source], bundled_sources: &[AvailableSource]) {
    println!();
    println!("{}", style("Workspace").bold());
    for (label, value) in workspace_summary_lines(installed_sources, bundled_sources) {
        println!("  {:<10} {}", style(label).dim(), value);
    }
    println!();
}

fn select_onboard_action(
    theme: &ColorfulTheme,
    has_installed_sources: bool,
) -> Result<OnboardAction, anyhow::Error> {
    let mut items = vec![
        ("Add a bundled source", OnboardAction::AddBundledSource),
        (
            "Import a source manifest",
            OnboardAction::ImportSourceManifest,
        ),
    ];
    if has_installed_sources {
        items.push(("Validate a source", OnboardAction::ValidateInstalledSource));
    }
    items.push(("Finish onboarding", OnboardAction::Finish));
    let labels = items.iter().map(|(label, _)| *label).collect::<Vec<_>>();

    let selection = Select::with_theme(theme)
        .with_prompt("Choose an action")
        .items(&labels)
        .default(0)
        .interact_opt()?;

    Ok(selection.map_or(OnboardAction::Finish, |index| items[index].1))
}

async fn run_add_bundled_source(
    app: &AppClient,
    theme: &ColorfulTheme,
    bundled_sources: &[AvailableSource],
) -> Result<(), anyhow::Error> {
    let addable_sources = addable_bundled_sources(bundled_sources);
    if addable_sources.is_empty() {
        println!("No bundled sources are available in this build.");
        return Ok(());
    }

    let name_width = addable_sources
        .iter()
        .map(|source| measure_text_width(&source.name))
        .max()
        .unwrap_or(0);
    let items = addable_sources
        .iter()
        .map(|source| format_bundled_source_item(source, name_width))
        .collect::<Vec<_>>();
    let selection = Select::with_theme(theme)
        .with_prompt("Choose a bundled source")
        .items(&items)
        .default(0)
        .interact_opt()?;
    let Some(selection) = selection else {
        return Ok(());
    };
    let selected = addable_sources[selection];

    let inputs = selected
        .inputs
        .iter()
        .map(source_ops::manifest_input_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
    let source = source_ops::add_bundled_source(app, &selected.name, variables, secrets).await?;
    println!("Added source {}", source.name);
    maybe_validate_after_install(app, theme, &source.name).await
}

async fn run_import_source(app: &AppClient, theme: &ColorfulTheme) -> Result<(), anyhow::Error> {
    let path = Input::<String>::with_theme(theme)
        .with_prompt("Path to a source manifest YAML file")
        .interact_text()?;
    let path = PathBuf::from(path);
    let (manifest_yaml, inputs) = source_ops::load_manifest_inputs(&path)?;
    let (variables, secrets) = source_ops::prompt_for_inputs(&inputs)?;
    let source = source_ops::import_source(app, manifest_yaml, variables, secrets).await?;
    println!("Imported source {}", source.name);
    maybe_validate_after_install(app, theme, &source.name).await
}

async fn run_validate_source(
    app: &AppClient,
    theme: &ColorfulTheme,
    installed_sources: &[Source],
) -> Result<(), anyhow::Error> {
    if installed_sources.is_empty() {
        println!("No installed sources are available to validate.");
        return Ok(());
    }

    let items = installed_sources
        .iter()
        .map(|source| {
            format!(
                "{} ({})",
                source.name,
                source_ops::source_origin_label(source.origin)
            )
        })
        .collect::<Vec<_>>();
    let selection = Select::with_theme(theme)
        .with_prompt("Choose a source to validate")
        .items(&items)
        .default(0)
        .interact_opt()?;
    let Some(selection) = selection else {
        return Ok(());
    };
    let response = source_ops::validate_source(app, &installed_sources[selection].name).await?;
    source_ops::print_validation_success(&response)?;
    Ok(())
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

fn addable_bundled_sources(sources: &[AvailableSource]) -> Vec<&AvailableSource> {
    sources.iter().filter(|source| !source.installed).collect()
}

fn workspace_summary_lines(
    installed_sources: &[Source],
    bundled_sources: &[AvailableSource],
) -> Vec<(&'static str, String)> {
    let installed_value = if installed_sources.is_empty() {
        "none".to_string()
    } else {
        format!(
            "{} source{}: {}",
            installed_sources.len(),
            if installed_sources.len() == 1 {
                ""
            } else {
                "s"
            },
            installed_sources
                .iter()
                .map(|source| source.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let available_count = bundled_sources
        .iter()
        .filter(|source| !source.installed)
        .count();
    vec![
        ("Installed", installed_value),
        (
            "Available",
            format!(
                "{available_count} bundled source{}",
                if available_count == 1 { "" } else { "s" }
            ),
        ),
        ("Tip", "Esc goes back".to_string()),
    ]
}

fn format_bundled_source_item(source: &AvailableSource, name_width: usize) -> String {
    if source.description.is_empty() {
        source.name.clone()
    } else {
        let preview = truncate_description(&source.description, SOURCE_DESCRIPTION_PREVIEW_LIMIT);
        format!("{:<name_width$}  {}", source.name, preview)
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
    use coral_api::v1::{AvailableSource, Source};

    use super::{
        addable_bundled_sources, format_bundled_source_item, truncate_description,
        workspace_summary_lines,
    };

    #[test]
    fn bundled_source_items_align_name_and_include_description_preview() {
        let source = AvailableSource {
            name: "github".to_string(),
            description: "Query repositories and issues".to_string(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: 1,
        };
        let item = format_bundled_source_item(&source, 10);
        assert_eq!(item, "github      Query repositories and issues");
    }

    #[test]
    fn addable_bundled_sources_omit_installed_sources() {
        let installed = AvailableSource {
            name: "github".to_string(),
            description: String::new(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: true,
            origin: 1,
        };
        let available = AvailableSource {
            name: "slack".to_string(),
            description: String::new(),
            version: "1.0.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: 1,
        };

        let sources = [installed, available];
        let addable = addable_bundled_sources(&sources);
        assert_eq!(addable.len(), 1);
        assert_eq!(addable[0].name, "slack");
    }

    #[test]
    fn workspace_summary_includes_counts_and_names() {
        let installed_sources = vec![
            Source {
                workspace: None,
                name: "github".to_string(),
                version: "1.0.0".to_string(),
                secrets: Vec::new(),
                variables: Vec::new(),
                origin: 1,
            },
            Source {
                workspace: None,
                name: "slack".to_string(),
                version: "1.0.0".to_string(),
                secrets: Vec::new(),
                variables: Vec::new(),
                origin: 1,
            },
        ];
        let bundled_sources = vec![
            AvailableSource {
                name: "github".to_string(),
                description: String::new(),
                version: "1.0.0".to_string(),
                inputs: Vec::new(),
                installed: true,
                origin: 1,
            },
            AvailableSource {
                name: "linear".to_string(),
                description: String::new(),
                version: "1.0.0".to_string(),
                inputs: Vec::new(),
                installed: false,
                origin: 1,
            },
        ];

        let lines = workspace_summary_lines(&installed_sources, &bundled_sources);
        assert_eq!(
            lines[0],
            ("Installed", "2 sources: github, slack".to_string())
        );
        assert_eq!(lines[1], ("Available", "1 bundled source".to_string()));
        assert_eq!(lines[2], ("Tip", "Esc goes back".to_string()));
    }

    #[test]
    fn truncate_description_adds_ascii_ellipsis_when_needed() {
        let description = "abcdefghijklmnopqrstuvwxyz";
        assert_eq!(truncate_description(description, 10), "abcdefg...");
    }
}
