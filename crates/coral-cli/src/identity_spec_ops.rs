//! Terminal adapter for exact-scope identity-spec lifecycle operations.

use std::io::{IsTerminal as _, stdin, stdout};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::{Args, Subcommand};
use coral_api::v1::{
    AddIdentitySpecRequest, DeleteIdentitySpecRequest, GetIdentitySpecRequest,
    GlobalIdentitySpecScope, IdentitySpec, IdentitySpecInputValue, IdentitySpecScope,
    IdentitySpecType, ListIdentitySpecsRequest, Workspace, identity_spec_scope,
};
use coral_client::AppClient;
use coral_spec::{IdentityManifest, ManifestInputKind, ManifestInputSpec};
use dialoguer::{Input, Password, theme::ColorfulTheme};
use tonic::Request;

use crate::source_ops::display_version;

#[derive(Debug, Args)]
/// Manage installed identity specifications
pub(crate) struct IdentitySpecArgs {
    /// Use global identity-spec scope instead of the selected workspace
    #[arg(long, global = true)]
    global: bool,
    #[command(subcommand)]
    command: IdentitySpecCommand,
}

impl IdentitySpecArgs {
    pub(crate) fn validate_explicit_workspace(
        &self,
        workspace: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        if self.global && workspace.is_some() {
            anyhow::bail!("--global cannot be used together with --workspace");
        }
        if self.global
            && matches!(
                self.command,
                IdentitySpecCommand::List {
                    include_global: true
                }
            )
        {
            anyhow::bail!("--include-global cannot be used together with --global");
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum IdentitySpecCommand {
    /// Install or replace an identity spec from a manifest file
    Add {
        /// Path to an identity-spec YAML file
        #[arg(long)]
        file: PathBuf,
        /// Prompt for missing setup inputs instead of using only environment variables
        #[arg(long)]
        interactive: bool,
    },
    /// List installed identity specs in the requested exact scope
    List {
        /// Include global specs alongside the selected workspace's specs
        #[arg(long)]
        include_global: bool,
    },
    /// Show one installed identity spec in the requested exact scope
    Info {
        /// Identity-spec name
        name: String,
    },
    /// Remove one installed identity spec from the requested exact scope
    Remove {
        /// Identity-spec name
        name: String,
    },
}

pub(crate) async fn run(
    app: &AppClient,
    selected_workspace: &Workspace,
    args: IdentitySpecArgs,
) -> Result<(), anyhow::Error> {
    let IdentitySpecArgs { global, command } = args;
    let scope = requested_scope(global, selected_workspace);

    match command {
        IdentitySpecCommand::Add { file, interactive } => {
            let (manifest_yaml, manifest) = load_manifest(&file)?;
            let input_values = input_values_for_add(&manifest, interactive)?;
            let response = app
                .identity_spec_client()
                .add_identity_spec(Request::new(AddIdentitySpecRequest {
                    manifest_yaml,
                    input_values,
                    scope: Some(scope),
                }))
                .await?
                .into_inner();
            let identity_spec = response.identity_spec.ok_or_else(|| {
                anyhow::anyhow!("add identity spec response missing identity_spec")
            })?;
            let action = if response.replaced {
                "Replaced"
            } else {
                "Added"
            };
            println!(
                "{action} identity spec '{}' ({}) in {}.",
                identity_spec.name,
                display_version(&identity_spec.version),
                scope_description(identity_spec.scope.as_ref())?
            );
        }
        IdentitySpecCommand::List { include_global } => {
            let requested = requested_scope_description(&scope, include_global)?;
            let response = app
                .identity_spec_client()
                .list_identity_specs(Request::new(ListIdentitySpecsRequest {
                    scope: Some(scope),
                    include_global,
                }))
                .await?
                .into_inner();
            if response.identity_specs.is_empty() {
                println!("No identity specs installed for {requested}.");
            } else {
                let rows = response
                    .identity_specs
                    .into_iter()
                    .map(|identity_spec| {
                        Ok([
                            identity_spec.name,
                            display_version(&identity_spec.version),
                            identity_spec.issuer,
                            identity_type_label(identity_spec.identity_type).to_string(),
                            scope_column(identity_spec.scope.as_ref())?,
                        ])
                    })
                    .collect::<Result<Vec<_>, anyhow::Error>>()?;
                super::print_text_table(
                    ["Identity Spec", "Version", "Issuer", "Type", "Scope"],
                    rows,
                );
            }
        }
        IdentitySpecCommand::Info { name } => {
            let response = app
                .identity_spec_client()
                .get_identity_spec(Request::new(GetIdentitySpecRequest {
                    name,
                    scope: Some(scope),
                }))
                .await?
                .into_inner();
            let identity_spec = response.identity_spec.ok_or_else(|| {
                anyhow::anyhow!("get identity spec response missing identity_spec")
            })?;
            print_info(&identity_spec)?;
        }
        IdentitySpecCommand::Remove { name } => {
            let scope_description = scope_description(Some(&scope))?;
            app.identity_spec_client()
                .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
                    name: name.clone(),
                    scope: Some(scope),
                }))
                .await?;
            println!("Removed identity spec '{name}' from {scope_description}.");
        }
    }
    Ok(())
}

fn requested_scope(global: bool, workspace: &Workspace) -> IdentitySpecScope {
    let value = if global {
        identity_spec_scope::Value::Global(GlobalIdentitySpecScope {})
    } else {
        identity_spec_scope::Value::Workspace(workspace.clone())
    };
    IdentitySpecScope { value: Some(value) }
}

fn load_manifest(file: &Path) -> Result<(String, IdentityManifest), anyhow::Error> {
    let manifest_yaml = std::fs::read_to_string(file)
        .with_context(|| format!("failed to read identity spec '{}'", file.display()))?;
    let manifest = coral_spec::parse_identity_manifest_yaml(&manifest_yaml)
        .with_context(|| format!("invalid identity spec '{}'", file.display()))?;
    Ok((manifest_yaml, manifest))
}

fn input_values_for_add(
    manifest: &IdentityManifest,
    interactive: bool,
) -> Result<Vec<IdentitySpecInputValue>, anyhow::Error> {
    if interactive && (!stdin().is_terminal() || !stdout().is_terminal()) {
        anyhow::bail!("interactive identity-spec add requires a TTY");
    }
    if manifest.inputs.is_empty() {
        return Ok(Vec::new());
    }
    if interactive {
        prompt_input_values(&manifest.inputs)
    } else {
        Ok(collect_input_values(&manifest.inputs, read_input_env))
    }
}

fn collect_input_values(
    inputs: &[ManifestInputSpec],
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Vec<IdentitySpecInputValue> {
    inputs
        .iter()
        .filter_map(|input| {
            lookup(&input.key)
                .filter(|value| !value.trim().is_empty())
                .map(|value| IdentitySpecInputValue {
                    key: input.key.clone(),
                    value,
                })
        })
        .collect()
}

#[expect(
    clippy::disallowed_methods,
    reason = "identity-spec add reads declared setup inputs from matching environment variables outside a TTY"
)]
fn read_input_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn prompt_input_values(
    inputs: &[ManifestInputSpec],
) -> Result<Vec<IdentitySpecInputValue>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let mut values = Vec::new();
    for input in inputs {
        if let Some(value) = read_input_env(&input.key).filter(|value| !value.trim().is_empty()) {
            values.push(IdentitySpecInputValue {
                key: input.key.clone(),
                value,
            });
            continue;
        }
        if let Some(hint) = input.hint.as_deref().filter(|hint| !hint.trim().is_empty()) {
            println!("  Hint: {hint}");
        }
        let value = match input.kind {
            ManifestInputKind::Variable => Input::<String>::with_theme(&theme)
                .with_prompt(variable_prompt(input))
                .allow_empty(true)
                .interact_text()?,
            ManifestInputKind::Secret => Password::with_theme(&theme)
                .with_prompt(&input.key)
                .allow_empty_password(true)
                .interact()?,
        };
        if !value.trim().is_empty() {
            values.push(IdentitySpecInputValue {
                key: input.key.clone(),
                value,
            });
        }
    }
    Ok(values)
}

fn variable_prompt(input: &ManifestInputSpec) -> String {
    if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [{}]", input.key, input.default_value)
    }
}

fn scope_value(
    scope: Option<&IdentitySpecScope>,
) -> Result<&identity_spec_scope::Value, anyhow::Error> {
    scope
        .and_then(|scope| scope.value.as_ref())
        .ok_or_else(|| anyhow::anyhow!("identity spec response missing exact scope"))
}

fn scope_column(scope: Option<&IdentitySpecScope>) -> Result<String, anyhow::Error> {
    match scope_value(scope)? {
        identity_spec_scope::Value::Global(_) => Ok("global".to_string()),
        identity_spec_scope::Value::Workspace(workspace) => {
            Ok(format!("workspace:{}", workspace.name))
        }
    }
}

fn scope_description(scope: Option<&IdentitySpecScope>) -> Result<String, anyhow::Error> {
    match scope_value(scope)? {
        identity_spec_scope::Value::Global(_) => Ok("global scope".to_string()),
        identity_spec_scope::Value::Workspace(workspace) => {
            Ok(format!("workspace '{}'", workspace.name))
        }
    }
}

fn requested_scope_description(
    scope: &IdentitySpecScope,
    include_global: bool,
) -> Result<String, anyhow::Error> {
    match (scope_value(Some(scope))?, include_global) {
        (identity_spec_scope::Value::Workspace(workspace), true) => {
            Ok(format!("workspace '{}' or global scope", workspace.name))
        }
        _ => scope_description(Some(scope)),
    }
}

fn identity_type_label(value: i32) -> &'static str {
    match IdentitySpecType::try_from(value) {
        Ok(IdentitySpecType::Oauth) => "oauth",
        Ok(IdentitySpecType::FixedToken) => "fixed_token",
        Ok(IdentitySpecType::Unspecified) | Err(_) => "unknown",
    }
}

fn print_info(identity_spec: &IdentitySpec) -> Result<(), anyhow::Error> {
    println!("{}", identity_spec.name);
    println!(
        "  Scope:       {}",
        scope_column(identity_spec.scope.as_ref())?
    );
    println!("  Version:     {}", display_version(&identity_spec.version));
    println!("  Description: {}", identity_spec.description);
    println!("  Issuer:      {}", identity_spec.issuer);
    println!(
        "  Type:        {}",
        identity_type_label(identity_spec.identity_type)
    );
    println!("  Manifest:");
    print!("{}", identity_spec.manifest_yaml);
    if !identity_spec.manifest_yaml.ends_with('\n') {
        println!();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use coral_api::v1::IdentitySpecInputValue;

    use super::collect_input_values;
    use crate::{Cli, Command, RequiredRuntime};

    fn identity_spec_args(args: &[&str]) -> super::IdentitySpecArgs {
        let cli = Cli::try_parse_from(args).expect("identity-spec args parse");
        let Command::IdentitySpec(args) = cli.command else {
            panic!("expected identity-spec command");
        };
        args
    }

    #[test]
    fn command_requires_app_client_and_rejects_unsafe_or_incomplete_args() {
        assert_eq!(
            Cli::try_parse_from(["coral", "identity-spec", "list"])
                .expect("list parses")
                .command
                .required_runtime(),
            RequiredRuntime::AppClient
        );
        Cli::try_parse_from(["coral", "identity-spec", "add"])
            .expect_err("add requires a manifest file");
        Cli::try_parse_from(["coral", "identity-spec", "remove", "demo", "--force"])
            .expect_err("force deletion must not be exposed");
    }

    #[test]
    fn global_scope_rejects_explicit_workspace_and_include_global() {
        let args = identity_spec_args(&[
            "coral",
            "--workspace",
            "work",
            "identity-spec",
            "--global",
            "list",
        ]);
        assert!(args.validate_explicit_workspace(Some("work")).is_err());
        let args = identity_spec_args(&[
            "coral",
            "identity-spec",
            "--global",
            "list",
            "--include-global",
        ]);
        assert!(args.validate_explicit_workspace(None).is_err());
    }

    #[test]
    fn environment_inputs_preserve_order_and_omit_blank_values() {
        let inputs = vec![
            coral_spec::ManifestInputSpec {
                key: "TENANT".to_string(),
                kind: coral_spec::ManifestInputKind::Variable,
                default_value: String::new(),
                required: false,
                hint: None,
                credential: None,
            },
            coral_spec::ManifestInputSpec {
                key: "CLIENT_SECRET".to_string(),
                kind: coral_spec::ManifestInputKind::Secret,
                default_value: String::new(),
                required: true,
                hint: None,
                credential: None,
            },
        ];
        let values = collect_input_values(&inputs, |key| match key {
            "TENANT" => Some("  ".to_string()),
            "CLIENT_SECRET" => Some("secret-value".to_string()),
            _ => None,
        });
        assert_eq!(
            values,
            vec![IdentitySpecInputValue {
                key: "CLIENT_SECRET".to_string(),
                value: "secret-value".to_string(),
            }]
        );
    }
}
