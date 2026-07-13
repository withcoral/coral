//! Terminal adapter for exact-scope identity-spec lifecycle operations.

use std::io::{IsTerminal as _, stdin, stdout};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::{Args, Subcommand};
use coral_api::v1::{
    AddIdentitySpecRequest, DeleteIdentitySpecRequest, GetIdentitySpecRequest, IdentitySpec,
    IdentitySpecInputValue, ListIdentitySpecsRequest, Workspace,
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
            return Err(anyhow::anyhow!(
                "--global cannot be used together with --workspace"
            ));
        }
        if self.global
            && matches!(
                self.command,
                IdentitySpecCommand::List {
                    include_global: true
                }
            )
        {
            return Err(anyhow::anyhow!(
                "--include-global cannot be used together with --global"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum IdentitySpecCommand {
    /// Install an identity spec from a manifest file
    Add {
        /// Path to an identity-spec YAML file
        #[arg(long)]
        file: PathBuf,
    },
    /// List installed identity specs in the exact requested scope
    List {
        /// Include global specs alongside the selected workspace's specs
        #[arg(long)]
        include_global: bool,
    },
    /// Show one installed identity spec in the exact requested scope
    Info {
        /// Identity-spec name
        name: String,
    },
    /// Remove one installed identity spec from the exact requested scope
    Remove {
        /// Identity-spec name
        name: String,
        /// Allow stored identities to become orphaned
        #[arg(long)]
        force: bool,
    },
}

pub(crate) async fn run(
    app: &AppClient,
    selected_workspace: &Workspace,
    args: IdentitySpecArgs,
) -> Result<(), anyhow::Error> {
    let IdentitySpecArgs { global, command } = args;
    let workspace = (!global).then(|| selected_workspace.clone());

    match command {
        IdentitySpecCommand::Add { file } => {
            let (manifest_yaml, manifest) = load_manifest(&file)?;
            let input_values = input_values_for_add(&manifest)?;
            let response = app
                .identity_spec_client()
                .add_identity_spec(Request::new(AddIdentitySpecRequest {
                    manifest_yaml,
                    input_values,
                    workspace,
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
                scope_description(identity_spec.workspace.as_ref())
            );
        }
        IdentitySpecCommand::List { include_global } => {
            let response = app
                .identity_spec_client()
                .list_identity_specs(Request::new(ListIdentitySpecsRequest {
                    workspace: workspace.clone(),
                    include_global,
                }))
                .await?
                .into_inner();
            if response.identity_specs.is_empty() {
                println!(
                    "No identity specs installed for {}.",
                    requested_scope_description(workspace.as_ref(), include_global)
                );
            } else {
                let rows = response.identity_specs.into_iter().map(|identity_spec| {
                    [
                        identity_spec.name,
                        display_version(&identity_spec.version),
                        identity_spec.issuer,
                        identity_spec.identity_type,
                        scope_column(identity_spec.workspace.as_ref()),
                    ]
                });
                super::print_text_table(
                    ["Identity Spec", "Version", "Issuer", "Type", "Scope"],
                    rows,
                );
            }
        }
        IdentitySpecCommand::Info { name } => {
            let response = app
                .identity_spec_client()
                .get_identity_spec(Request::new(GetIdentitySpecRequest { name, workspace }))
                .await?
                .into_inner();
            let identity_spec = response.identity_spec.ok_or_else(|| {
                anyhow::anyhow!("get identity spec response missing identity_spec")
            })?;
            print_info(&identity_spec);
        }
        IdentitySpecCommand::Remove { name, force } => {
            let scope = scope_description(workspace.as_ref());
            let response = app
                .identity_spec_client()
                .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
                    name: name.clone(),
                    workspace,
                    force,
                }))
                .await?
                .into_inner();
            let noun = if response.orphaned_identities == 1 {
                "identity"
            } else {
                "identities"
            };
            println!(
                "Removed identity spec '{name}' from {scope} ({} orphaned {noun}).",
                response.orphaned_identities
            );
        }
    }
    Ok(())
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
) -> Result<Vec<IdentitySpecInputValue>, anyhow::Error> {
    if manifest.inputs.is_empty() {
        return Ok(Vec::new());
    }
    if interactive_mode(stdin().is_terminal(), stdout().is_terminal()) {
        prompt_input_values(&manifest.inputs)
    } else {
        Ok(collect_input_values(&manifest.inputs, read_input_env))
    }
}

fn interactive_mode(stdin_terminal: bool, stdout_terminal: bool) -> bool {
    stdin_terminal && stdout_terminal
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
        if let Some(hint) = input.hint.as_deref() {
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

fn scope_column(workspace: Option<&Workspace>) -> String {
    workspace.map_or_else(
        || "global".to_string(),
        |workspace| format!("workspace:{}", workspace.name),
    )
}

fn scope_description(workspace: Option<&Workspace>) -> String {
    workspace.map_or_else(
        || "global scope".to_string(),
        |workspace| format!("workspace '{}'", workspace.name),
    )
}

fn requested_scope_description(workspace: Option<&Workspace>, include_global: bool) -> String {
    match (workspace, include_global) {
        (Some(workspace), true) => format!("workspace '{}' or global scope", workspace.name),
        _ => scope_description(workspace),
    }
}

fn print_info(identity_spec: &IdentitySpec) {
    println!("{}", identity_spec.name);
    println!(
        "  Scope:       {}",
        scope_column(identity_spec.workspace.as_ref())
    );
    println!("  Version:     {}", display_version(&identity_spec.version));
    println!("  Description: {}", identity_spec.description);
    println!("  Issuer:      {}", identity_spec.issuer);
    println!("  Type:        {}", identity_spec.identity_type);
    println!("  Manifest:");
    print!("{}", identity_spec.manifest_yaml);
    if !identity_spec.manifest_yaml.ends_with('\n') {
        println!();
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use coral_api::v1::{IdentitySpecInputValue, Workspace};
    use coral_spec::parse_identity_manifest_yaml;

    use super::{IdentitySpecCommand, collect_input_values, interactive_mode, scope_column};
    use crate::{Cli, Command, RequiredRuntime};

    const IDENTITY_WITH_INPUTS: &str = r"kind: identity
spec_version: 1
name: demo
version: 1.0.0
issuer: demo
type: oauth
inputs:
  TENANT:
    kind: variable
    default: public
  CLIENT_SECRET:
    kind: secret
    required: true
oauth:
  method:
    flow:
      type: authorization_code
      pkce: disabled
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    endpoints:
      authorization_url: https://provider.example.com/authorize
      token_url: https://provider.example.com/token
    client:
      id:
        input: TENANT
      secret:
        input: CLIENT_SECRET
        transport: basic_auth
";

    fn identity_spec_args(args: &[&str]) -> super::IdentitySpecArgs {
        let cli = Cli::try_parse_from(args).expect("identity-spec args parse");
        let Command::IdentitySpec(args) = cli.command else {
            panic!("expected identity-spec command");
        };
        args
    }

    #[test]
    fn identity_spec_commands_parse_and_require_app_client() {
        let add = identity_spec_args(&["coral", "identity-spec", "add", "--file", "demo.yaml"]);
        assert!(matches!(add.command, IdentitySpecCommand::Add { .. }));
        let list = identity_spec_args(&["coral", "identity-spec", "list", "--include-global"]);
        assert!(matches!(
            list.command,
            IdentitySpecCommand::List {
                include_global: true
            }
        ));
        let info = identity_spec_args(&["coral", "identity-spec", "info", "demo"]);
        assert!(matches!(info.command, IdentitySpecCommand::Info { .. }));
        let remove = identity_spec_args(&["coral", "identity-spec", "remove", "demo", "--force"]);
        assert!(matches!(
            remove.command,
            IdentitySpecCommand::Remove { force: true, .. }
        ));

        let cli = Cli::try_parse_from(["coral", "identity-spec", "list"]).expect("list parses");
        assert_eq!(cli.command.required_runtime(), RequiredRuntime::AppClient);
    }

    #[test]
    fn global_scope_parses_before_or_after_leaf_command() {
        let before = identity_spec_args(&["coral", "identity-spec", "--global", "info", "demo"]);
        let after = identity_spec_args(&["coral", "identity-spec", "info", "demo", "--global"]);
        assert!(before.global);
        assert!(after.global);
    }

    #[test]
    fn global_scope_rejects_workspace_and_include_global() {
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
    fn add_requires_file_and_remove_force_defaults_false() {
        Cli::try_parse_from(["coral", "identity-spec", "add"])
            .expect_err("identity-spec add without --file must fail");
        let remove = identity_spec_args(&["coral", "identity-spec", "remove", "demo"]);
        assert!(matches!(
            remove.command,
            IdentitySpecCommand::Remove { force: false, .. }
        ));
    }

    #[test]
    fn environment_inputs_preserve_order_and_omit_unset_or_whitespace_values() {
        let manifest = parse_identity_manifest_yaml(IDENTITY_WITH_INPUTS).expect("manifest");
        let values = collect_input_values(&manifest.inputs, |key| match key {
            "CLIENT_SECRET" => Some("secret-value".to_string()),
            "TENANT" => Some("  ".to_string()),
            _ => None,
        });
        assert_eq!(
            values,
            vec![IdentitySpecInputValue {
                key: "CLIENT_SECRET".to_string(),
                value: "secret-value".to_string(),
            }]
        );

        let values = collect_input_values(&manifest.inputs, |key| Some(format!("value-{key}")));
        let keys = values
            .iter()
            .map(|value| value.key.as_str())
            .collect::<Vec<_>>();
        let expected = manifest
            .inputs
            .iter()
            .map(|input| input.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, expected);
    }

    #[test]
    fn interactive_mode_requires_both_terminals() {
        assert!(interactive_mode(true, true));
        assert!(!interactive_mode(true, false));
        assert!(!interactive_mode(false, true));
        assert!(!interactive_mode(false, false));
    }

    #[test]
    fn scope_column_distinguishes_global_and_workspace() {
        assert_eq!(scope_column(None), "global");
        assert_eq!(
            scope_column(Some(&Workspace {
                name: "work".to_string()
            })),
            "workspace:work"
        );
    }
}
