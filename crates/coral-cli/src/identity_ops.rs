//! Terminal adapter for current-user fixed-token identity lifecycle operations.

use std::io::{IsTerminal as _, Read as _, stdin, stdout};

use clap::{Args, Subcommand};
use coral_api::v1::{
    CreateUserOwnedFixedTokenIdentityRequest, DeleteUserOwnedIdentityRequest,
    FixedTokenIdentitySetup, GetUserOwnedIdentityRequest, Identity, IdentityAudience,
    IdentitySpecReference, IdentitySpecType, ListUserOwnedIdentitiesRequest, identity_owner,
    identity_spec_scope,
};
use coral_client::AppClient;
use dialoguer::{Password, theme::ColorfulTheme};
use tonic::Request;

#[derive(Debug, Args)]
/// Manage identities owned by the current Coral user
pub(crate) struct IdentityArgs {
    #[command(subcommand)]
    command: IdentityCommand,
}

impl IdentityArgs {
    pub(crate) fn validate_explicit_workspace(
        workspace: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        if workspace.is_some() {
            anyhow::bail!("--workspace cannot be used with current-user identity commands");
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum IdentityCommand {
    /// Create or replace a current-user fixed-token identity
    Add {
        /// Identity name used by source identity bindings
        name: String,
        /// Globally installed fixed-token identity-spec name
        #[arg(long = "identity-spec", value_name = "NAME")]
        identity_spec: String,
        /// Read the fixed token from stdin instead of prompting
        #[arg(long)]
        token_stdin: bool,
    },
    /// List identities owned by the current user
    List,
    /// Show one identity owned by the current user
    Info {
        /// Identity name
        name: String,
    },
    /// Remove one identity owned by the current user
    Remove {
        /// Identity name
        name: String,
    },
}

pub(crate) async fn run(app: &AppClient, args: IdentityArgs) -> Result<(), anyhow::Error> {
    match args.command {
        IdentityCommand::Add {
            name,
            identity_spec,
            token_stdin,
        } => {
            let token = fixed_token(token_stdin)?;
            let response = app
                .identity_client()
                .create_user_owned_fixed_token_identity(Request::new(
                    CreateUserOwnedFixedTokenIdentityRequest {
                        name,
                        identity_spec_name: identity_spec,
                        setup: Some(FixedTokenIdentitySetup { token }),
                    },
                ))
                .await?
                .into_inner();
            let identity = response.identity.ok_or_else(|| {
                anyhow::anyhow!("create current-user identity response missing identity")
            })?;
            let view = current_user_identity(&identity)?;
            println!(
                "Stored current-user identity '{}' using global identity spec '{}'.",
                identity.name, view.identity_spec.name
            );
        }
        IdentityCommand::List => {
            let response = app
                .identity_client()
                .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
                .await?
                .into_inner();
            if response.identities.is_empty() {
                println!("No current-user identities configured.");
            } else {
                let rows = response
                    .identities
                    .iter()
                    .map(identity_row)
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
            let response = app
                .identity_client()
                .get_user_owned_identity(Request::new(GetUserOwnedIdentityRequest { name }))
                .await?
                .into_inner();
            let identity = response.identity.ok_or_else(|| {
                anyhow::anyhow!("get current-user identity response missing identity")
            })?;
            print_info(&identity)?;
        }
        IdentityCommand::Remove { name } => {
            app.identity_client()
                .delete_user_owned_identity(Request::new(DeleteUserOwnedIdentityRequest {
                    name: name.clone(),
                }))
                .await?;
            println!("Removed current-user identity '{name}'.");
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
struct CurrentUserIdentity<'a> {
    identity_spec: &'a IdentitySpecReference,
}

fn current_user_identity(identity: &Identity) -> Result<CurrentUserIdentity<'_>, anyhow::Error> {
    match identity
        .owner
        .as_ref()
        .and_then(|owner| owner.value.as_ref())
    {
        Some(identity_owner::Value::CurrentUser(_)) => {}
        Some(identity_owner::Value::Workspace(_)) => {
            anyhow::bail!("current-user identity response has a workspace owner");
        }
        None => anyhow::bail!("current-user identity response missing exact owner"),
    }
    let identity_spec = identity.identity_spec.as_ref().ok_or_else(|| {
        anyhow::anyhow!("current-user identity response missing identity spec reference")
    })?;
    match identity_spec
        .scope
        .as_ref()
        .and_then(|scope| scope.value.as_ref())
    {
        Some(identity_spec_scope::Value::Global(_)) => {}
        Some(identity_spec_scope::Value::Workspace(_)) => {
            anyhow::bail!("current-user identity response has a workspace identity-spec scope");
        }
        None => anyhow::bail!("current-user identity response missing exact identity-spec scope"),
    }
    Ok(CurrentUserIdentity { identity_spec })
}

fn identity_row(identity: &Identity) -> Result<[String; 6], anyhow::Error> {
    let view = current_user_identity(identity)?;
    Ok([
        identity.name.clone(),
        view.identity_spec.name.clone(),
        view.identity_spec.issuer.clone(),
        identity_type_label(view.identity_spec.identity_type).to_string(),
        audience_label(view.identity_spec.audience.as_ref())?,
        "global".to_string(),
    ])
}

fn print_info(identity: &Identity) -> Result<(), anyhow::Error> {
    let view = current_user_identity(identity)?;
    println!("{}", identity.name);
    println!("  Owner:         current_user");
    println!("  Identity spec: {}", view.identity_spec.name);
    println!("  Spec scope:    global");
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
        anyhow::bail!("current-user identity response has an invalid empty audience host");
    }
    let Some(port) = audience.port else {
        return Ok(audience.host.clone());
    };
    if port == 0 || port > u32::from(u16::MAX) {
        anyhow::bail!("current-user identity response has an invalid audience port");
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
    };

    use super::{audience_label, normalize_fixed_token};
    use crate::{Cli, RequiredRuntime};

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
    fn current_user_commands_reject_an_explicit_workspace() {
        super::IdentityArgs::validate_explicit_workspace(None)
            .expect("implicit workspace environment is ignored");
        super::IdentityArgs::validate_explicit_workspace(Some("work"))
            .expect_err("explicit workspace must be rejected");
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
}
