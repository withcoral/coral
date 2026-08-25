//! Terminal adapter for fixed-token identity lifecycle operations.

use std::io::{IsTerminal as _, Read as _, stdin, stdout};

use clap::{Args, Subcommand};
use coral_api::v1::{
    CreateUserOwnedFixedTokenIdentityRequest, CreateWorkspaceOwnedFixedTokenIdentityRequest,
    DeleteUserOwnedIdentityRequest, DeleteWorkspaceOwnedIdentityRequest, FixedTokenIdentitySetup,
    GetUserOwnedIdentityRequest, GetWorkspaceOwnedIdentityRequest, Identity, IdentityAudience,
    IdentitySpecReference, IdentitySpecType, ListUserOwnedIdentitiesRequest,
    ListWorkspaceOwnedIdentitiesRequest, Workspace, identity_owner, identity_spec_scope,
};
use coral_client::AppClient;
use dialoguer::{Password, theme::ColorfulTheme};
use tonic::Request;

#[derive(Debug, Args)]
/// Manage current-user or explicitly selected workspace identities
pub(crate) struct IdentityArgs {
    /// Manage identities owned by the explicitly selected workspace
    #[arg(long)]
    workspace_owned: bool,
    #[command(subcommand)]
    command: IdentityCommand,
}

impl IdentityArgs {
    pub(crate) fn validate_workspace_selection(
        &self,
        workspace: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        match (self.workspace_owned, workspace) {
            (false, Some(_)) => {
                anyhow::bail!(
                    "--workspace cannot be used with current-user identity commands; add --workspace-owned to manage workspace identities"
                );
            }
            (true, None) => {
                anyhow::bail!("--workspace-owned requires an explicit --workspace NAME");
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) const fn uses_selected_workspace(&self) -> bool {
        self.workspace_owned
    }
}

#[derive(Debug, Subcommand)]
enum IdentityCommand {
    /// Create or replace a fixed-token identity for the selected owner
    Add {
        /// Identity name used by source identity bindings
        name: String,
        /// Fixed-token identity-spec name resolved for the selected owner
        #[arg(long = "identity-spec", value_name = "NAME")]
        identity_spec: String,
        /// Read the fixed token from stdin instead of prompting
        #[arg(long)]
        token_stdin: bool,
    },
    /// List identities owned by the selected owner
    List,
    /// Show one identity owned by the selected owner
    Info {
        /// Identity name
        name: String,
    },
    /// Remove one identity owned by the selected owner
    Remove {
        /// Identity name
        name: String,
    },
}

pub(crate) async fn run(
    app: &AppClient,
    selected_workspace: &Workspace,
    args: IdentityArgs,
) -> Result<(), anyhow::Error> {
    let target = if args.workspace_owned {
        IdentityTarget::Workspace(selected_workspace)
    } else {
        IdentityTarget::CurrentUser
    };
    match args.command {
        IdentityCommand::Add {
            name,
            identity_spec,
            token_stdin,
        } => {
            let token = fixed_token(token_stdin)?;
            let identity = create_identity(app, target, name, identity_spec, token).await?;
            let view = identity_view(&identity, target)?;
            print_stored(&identity, &view, target);
        }
        IdentityCommand::List => {
            let identities = list_identities(app, target).await?;
            if identities.is_empty() {
                match target {
                    IdentityTarget::CurrentUser => {
                        println!("No current-user identities configured.");
                    }
                    IdentityTarget::Workspace(workspace) => println!(
                        "No identities configured for workspace '{}'.",
                        workspace.name
                    ),
                }
            } else {
                let rows = identities
                    .iter()
                    .map(|identity| identity_row(identity, target))
                    .collect::<Result<Vec<_>, _>>()?;
                super::print_text_table(
                    [
                        "Identity",
                        "Identity Spec",
                        "Issuer",
                        "Type",
                        "Audience",
                        "Spec Scope",
                    ],
                    rows,
                );
            }
        }
        IdentityCommand::Info { name } => {
            let identity = get_identity(app, target, name).await?;
            print_info(&identity, target)?;
        }
        IdentityCommand::Remove { name } => {
            delete_identity(app, target, &name).await?;
            print_removed(&name, target);
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum IdentityTarget<'a> {
    CurrentUser,
    Workspace(&'a Workspace),
}

async fn create_identity(
    app: &AppClient,
    target: IdentityTarget<'_>,
    name: String,
    identity_spec_name: String,
    token: String,
) -> Result<Identity, anyhow::Error> {
    let setup = Some(FixedTokenIdentitySetup { token });
    let identity = match target {
        IdentityTarget::CurrentUser => {
            app.identity_client()
                .create_user_owned_fixed_token_identity(Request::new(
                    CreateUserOwnedFixedTokenIdentityRequest {
                        name,
                        identity_spec_name,
                        setup,
                    },
                ))
                .await?
                .into_inner()
                .identity
        }
        IdentityTarget::Workspace(workspace) => {
            app.workspace_identity_client()
                .create_workspace_owned_fixed_token_identity(Request::new(
                    CreateWorkspaceOwnedFixedTokenIdentityRequest {
                        workspace: Some(workspace.clone()),
                        name,
                        identity_spec_name,
                        setup,
                    },
                ))
                .await?
                .into_inner()
                .identity
        }
    };
    identity.ok_or_else(|| anyhow::anyhow!("create identity response missing identity"))
}

async fn list_identities(
    app: &AppClient,
    target: IdentityTarget<'_>,
) -> Result<Vec<Identity>, anyhow::Error> {
    match target {
        IdentityTarget::CurrentUser => Ok(app
            .identity_client()
            .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
            .await?
            .into_inner()
            .identities),
        IdentityTarget::Workspace(workspace) => Ok(app
            .workspace_identity_client()
            .list_workspace_owned_identities(Request::new(ListWorkspaceOwnedIdentitiesRequest {
                workspace: Some(workspace.clone()),
            }))
            .await?
            .into_inner()
            .identities),
    }
}

async fn get_identity(
    app: &AppClient,
    target: IdentityTarget<'_>,
    name: String,
) -> Result<Identity, anyhow::Error> {
    let identity = match target {
        IdentityTarget::CurrentUser => {
            app.identity_client()
                .get_user_owned_identity(Request::new(GetUserOwnedIdentityRequest { name }))
                .await?
                .into_inner()
                .identity
        }
        IdentityTarget::Workspace(workspace) => {
            app.workspace_identity_client()
                .get_workspace_owned_identity(Request::new(GetWorkspaceOwnedIdentityRequest {
                    workspace: Some(workspace.clone()),
                    name,
                }))
                .await?
                .into_inner()
                .identity
        }
    };
    identity.ok_or_else(|| anyhow::anyhow!("get identity response missing identity"))
}

async fn delete_identity(
    app: &AppClient,
    target: IdentityTarget<'_>,
    name: &str,
) -> Result<(), anyhow::Error> {
    match target {
        IdentityTarget::CurrentUser => {
            app.identity_client()
                .delete_user_owned_identity(Request::new(DeleteUserOwnedIdentityRequest {
                    name: name.to_string(),
                }))
                .await?;
        }
        IdentityTarget::Workspace(workspace) => {
            app.workspace_identity_client()
                .delete_workspace_owned_identity(Request::new(
                    DeleteWorkspaceOwnedIdentityRequest {
                        workspace: Some(workspace.clone()),
                        name: name.to_string(),
                    },
                ))
                .await?;
        }
    }
    Ok(())
}

fn fixed_token(token_stdin: bool) -> Result<String, anyhow::Error> {
    if token_stdin {
        read_fixed_token_from_stdin()
    } else {
        prompt_fixed_token()
    }
}

fn read_fixed_token_from_stdin() -> Result<String, anyhow::Error> {
    let mut token = String::new();
    stdin().read_to_string(&mut token)?;
    normalize_fixed_token(&token)
}

fn prompt_fixed_token() -> Result<String, anyhow::Error> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        anyhow::bail!("fixed-token identity setup requires a TTY or an explicit --token-stdin");
    }
    let token = Password::with_theme(&ColorfulTheme::default())
        .with_prompt("Fixed token")
        .allow_empty_password(false)
        .interact()?;
    normalize_fixed_token(&token)
}

fn normalize_fixed_token(token: &str) -> Result<String, anyhow::Error> {
    let token = token.trim_end_matches(['\r', '\n']);
    if token.trim().is_empty() {
        anyhow::bail!("fixed-token identity token must not be blank");
    }
    Ok(token.to_string())
}

#[derive(Debug)]
struct IdentityView<'a> {
    identity_spec: &'a IdentitySpecReference,
    owner: String,
    spec_scope: String,
}

fn identity_view<'a>(
    identity: &'a Identity,
    target: IdentityTarget<'_>,
) -> Result<IdentityView<'a>, anyhow::Error> {
    let owner = identity
        .owner
        .as_ref()
        .and_then(|owner| owner.value.as_ref());
    let owner = match (target, owner) {
        (IdentityTarget::CurrentUser, Some(identity_owner::Value::CurrentUser(_))) => {
            "current_user".to_string()
        }
        (IdentityTarget::CurrentUser, Some(identity_owner::Value::Workspace(_))) => {
            anyhow::bail!("current-user identity response has a workspace owner");
        }
        (IdentityTarget::CurrentUser, None) => {
            anyhow::bail!("current-user identity response missing exact owner");
        }
        (IdentityTarget::Workspace(expected), Some(identity_owner::Value::Workspace(actual)))
            if actual.name == expected.name =>
        {
            format!("workspace:{}", actual.name)
        }
        (IdentityTarget::Workspace(_), Some(identity_owner::Value::CurrentUser(_))) => {
            anyhow::bail!("workspace identity response has a current-user owner");
        }
        (IdentityTarget::Workspace(expected), Some(identity_owner::Value::Workspace(actual))) => {
            anyhow::bail!(
                "workspace identity response owner '{}' does not match selected workspace '{}'",
                actual.name,
                expected.name
            )
        }
        (IdentityTarget::Workspace(_), None) => {
            anyhow::bail!("workspace identity response missing exact owner");
        }
    };
    let identity_spec = identity
        .identity_spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("identity response missing identity spec reference"))?;
    let scope = identity_spec
        .scope
        .as_ref()
        .and_then(|scope| scope.value.as_ref())
        .ok_or_else(|| anyhow::anyhow!("identity response missing exact identity-spec scope"))?;
    let spec_scope = match (target, scope) {
        (_, identity_spec_scope::Value::Global(_)) => "global".to_string(),
        (IdentityTarget::Workspace(expected), identity_spec_scope::Value::Workspace(actual))
            if actual.name == expected.name =>
        {
            format!("workspace:{}", actual.name)
        }
        (IdentityTarget::CurrentUser, identity_spec_scope::Value::Workspace(_)) => {
            anyhow::bail!("current-user identity response has a workspace identity-spec scope");
        }
        (IdentityTarget::Workspace(expected), identity_spec_scope::Value::Workspace(actual)) => {
            anyhow::bail!(
                "workspace identity response spec scope '{}' does not match selected workspace '{}'",
                actual.name,
                expected.name
            )
        }
    };
    Ok(IdentityView {
        identity_spec,
        owner,
        spec_scope,
    })
}

#[cfg(test)]
fn current_user_identity(identity: &Identity) -> Result<IdentityView<'_>, anyhow::Error> {
    identity_view(identity, IdentityTarget::CurrentUser)
}

fn identity_row(
    identity: &Identity,
    target: IdentityTarget<'_>,
) -> Result<[String; 6], anyhow::Error> {
    let view = identity_view(identity, target)?;
    Ok([
        identity.name.clone(),
        view.identity_spec.name.clone(),
        view.identity_spec.issuer.clone(),
        identity_type_label(view.identity_spec.identity_type).to_string(),
        audience_label(view.identity_spec.audience.as_ref())?,
        view.spec_scope,
    ])
}

fn print_info(identity: &Identity, target: IdentityTarget<'_>) -> Result<(), anyhow::Error> {
    let view = identity_view(identity, target)?;
    println!("{}", identity.name);
    println!("  Owner:         {}", view.owner);
    println!("  Identity spec: {}", view.identity_spec.name);
    println!("  Spec scope:    {}", view.spec_scope);
    println!("  Fingerprint:   {}", view.identity_spec.fingerprint);
    println!("  Issuer:        {}", view.identity_spec.issuer);
    println!(
        "  Type:          {}",
        identity_type_label(view.identity_spec.identity_type)
    );
    println!(
        "  Audience:      {}",
        audience_label(view.identity_spec.audience.as_ref())?
    );
    Ok(())
}

fn print_stored(identity: &Identity, view: &IdentityView<'_>, target: IdentityTarget<'_>) {
    match target {
        IdentityTarget::CurrentUser => println!(
            "Stored current-user identity '{}' using global identity spec '{}'.",
            identity.name, view.identity_spec.name
        ),
        IdentityTarget::Workspace(workspace) => println!(
            "Stored workspace identity '{}' in '{}' using identity spec '{}' from {}.",
            identity.name, workspace.name, view.identity_spec.name, view.spec_scope
        ),
    }
}

fn print_removed(name: &str, target: IdentityTarget<'_>) {
    match target {
        IdentityTarget::CurrentUser => println!("Removed current-user identity '{name}'."),
        IdentityTarget::Workspace(workspace) => {
            println!(
                "Removed workspace identity '{name}' from '{}'.",
                workspace.name
            );
        }
    }
}

fn identity_type_label(value: i32) -> &'static str {
    match IdentitySpecType::try_from(value) {
        Ok(IdentitySpecType::Oauth) => "oauth",
        Ok(IdentitySpecType::FixedToken) => "fixed_token",
        Ok(IdentitySpecType::Unspecified) | Err(_) => "unknown",
    }
}

fn audience_label(audience: Option<&IdentityAudience>) -> Result<String, anyhow::Error> {
    let Some(audience) = audience else {
        return Ok("-".to_string());
    };
    if audience.host.trim().is_empty() {
        anyhow::bail!("identity response has an invalid empty audience host");
    }
    let Some(port) = audience.port else {
        return Ok(audience.host.clone());
    };
    if port == 0 || port > u32::from(u16::MAX) {
        anyhow::bail!("identity response has an invalid audience port");
    }
    let host = if audience.host.contains(':') {
        audience
            .host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(&audience.host)
    } else {
        &audience.host
    };
    if audience.host.contains(':') {
        Ok(format!("[{host}]:{port}"))
    } else {
        Ok(format!("{host}:{port}"))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use coral_api::v1::{
        CurrentUserIdentityOwner, GlobalIdentitySpecScope, IdentityOwner, IdentitySpecScope,
        Workspace,
    };

    use super::{audience_label, normalize_fixed_token};
    use crate::{Cli, Command, RequiredRuntime};

    fn identity_args(args: &[&str]) -> super::IdentityArgs {
        let cli = Cli::try_parse_from(args).expect("identity command parses");
        let Command::Identity(args) = cli.command else {
            panic!("expected identity command");
        };
        args
    }

    #[test]
    fn command_requires_app_client_and_rejects_unsafe_or_incomplete_args() {
        assert_eq!(
            Cli::try_parse_from(["coral", "identity", "list"])
                .expect("list parses")
                .command
                .required_runtime(),
            RequiredRuntime::AppClient
        );
        Cli::try_parse_from(["coral", "identity", "add", "demo"])
            .expect_err("add requires an identity spec");
        Cli::try_parse_from(["coral", "identity", "remove", "demo", "--force"])
            .expect_err("force deletion must not be exposed");
        Cli::try_parse_from(["coral", "identity", "remove", "demo", "--orphan"])
            .expect_err("orphan deletion must not be exposed");
    }

    #[test]
    fn identity_owner_selection_is_explicit_and_unambiguous() {
        let current_user = identity_args(&["coral", "identity", "list"]);
        current_user
            .validate_workspace_selection(None)
            .expect("implicit workspace environment is ignored");
        current_user
            .validate_workspace_selection(Some("work"))
            .expect_err("explicit workspace must be rejected");

        let workspace = identity_args(&["coral", "identity", "--workspace-owned", "list"]);
        workspace
            .validate_workspace_selection(None)
            .expect_err("workspace ownership requires an explicit workspace");
        workspace
            .validate_workspace_selection(Some("work"))
            .expect("both workspace selectors are accepted");
    }

    #[test]
    fn token_normalization_strips_only_line_endings_and_rejects_blank_values() {
        assert_eq!(
            normalize_fixed_token(" token \r\n").expect("token with line ending"),
            " token "
        );
        normalize_fixed_token(" \r\n").expect_err("blank token must be rejected");
    }

    #[test]
    fn audience_rendering_preserves_optional_ports_and_brackets_ipv6() {
        assert_eq!(audience_label(None).expect("legacy audience"), "-");
        assert_eq!(
            audience_label(Some(&super::IdentityAudience {
                host: "api.example.com".to_string(),
                port: None,
            }))
            .expect("host-only audience"),
            "api.example.com"
        );
        assert_eq!(
            audience_label(Some(&super::IdentityAudience {
                host: "2001:db8::1".to_string(),
                port: Some(443),
            }))
            .expect("IPv6 audience"),
            "[2001:db8::1]:443"
        );
    }

    #[test]
    fn current_user_identity_requires_exact_owner_spec_and_global_scope() {
        let mut identity = super::Identity {
            owner: Some(IdentityOwner {
                value: Some(super::identity_owner::Value::CurrentUser(
                    CurrentUserIdentityOwner {},
                )),
            }),
            identity_spec: Some(super::IdentitySpecReference {
                scope: Some(IdentitySpecScope {
                    value: Some(super::identity_spec_scope::Value::Global(
                        GlobalIdentitySpecScope {},
                    )),
                }),
                ..super::IdentitySpecReference::default()
            }),
            ..super::Identity::default()
        };
        super::current_user_identity(&identity).expect("exact current-user identity");
        identity.owner = None;
        super::current_user_identity(&identity).expect_err("missing owner must be rejected");
        identity.owner = Some(IdentityOwner {
            value: Some(super::identity_owner::Value::CurrentUser(
                CurrentUserIdentityOwner {},
            )),
        });
        identity
            .identity_spec
            .as_mut()
            .expect("identity spec")
            .scope = None;
        super::current_user_identity(&identity)
            .expect_err("missing identity-spec scope must be rejected");
    }

    #[test]
    fn workspace_identity_requires_matching_owner_and_resolved_scope() {
        let selected = Workspace {
            name: "work".to_string(),
        };
        let mut identity = super::Identity {
            owner: Some(IdentityOwner {
                value: Some(super::identity_owner::Value::Workspace(selected.clone())),
            }),
            identity_spec: Some(super::IdentitySpecReference {
                scope: Some(IdentitySpecScope {
                    value: Some(super::identity_spec_scope::Value::Global(
                        GlobalIdentitySpecScope {},
                    )),
                }),
                ..super::IdentitySpecReference::default()
            }),
            ..super::Identity::default()
        };
        let global = super::identity_view(&identity, super::IdentityTarget::Workspace(&selected))
            .expect("global fallback is valid");
        assert_eq!(global.spec_scope, "global");

        identity
            .identity_spec
            .as_mut()
            .expect("identity spec")
            .scope = Some(IdentitySpecScope {
            value: Some(super::identity_spec_scope::Value::Workspace(
                selected.clone(),
            )),
        });
        let local = super::identity_view(&identity, super::IdentityTarget::Workspace(&selected))
            .expect("matching workspace scope is valid");
        assert_eq!(local.spec_scope, "workspace:work");

        identity.owner = Some(IdentityOwner {
            value: Some(super::identity_owner::Value::Workspace(Workspace {
                name: "other".to_string(),
            })),
        });
        super::identity_view(&identity, super::IdentityTarget::Workspace(&selected))
            .expect_err("wrong workspace owner must be rejected");

        identity.owner = Some(IdentityOwner {
            value: Some(super::identity_owner::Value::Workspace(selected.clone())),
        });
        identity
            .identity_spec
            .as_mut()
            .expect("identity spec")
            .scope = Some(IdentitySpecScope {
            value: Some(super::identity_spec_scope::Value::Workspace(Workspace {
                name: "other".to_string(),
            })),
        });
        super::identity_view(&identity, super::IdentityTarget::Workspace(&selected))
            .expect_err("wrong workspace spec scope must be rejected");
    }
}
