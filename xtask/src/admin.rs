//! Repository-only workspace ownership and issuer recovery commands.

use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use coral_app::admin::{AdminDb, SetOwnerOutcome, UserSummary, WorkspaceSummary, reject_local};

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// State directory to recover. Defaults to `CORAL_CONFIG_DIR`, then Coral's local state directory.
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List every workspace and its shared-mode ownership state.
    ListWorkspaces,
    /// List users who have authenticated at least once.
    ListUsers {
        /// Include provider subjects, which may identify a person.
        #[arg(long)]
        show_subjects: bool,
    },
    /// Add or promote one existing user to workspace Owner.
    SetOwner {
        #[arg(long)]
        workspace: String,
        /// Internal ID printed by list-users.
        #[arg(long)]
        user: String,
    },
    /// Rebind identities after an issuer rename while the server is stopped.
    RebindIssuer {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    validate_before_open(&args.command)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build the workspace recovery runtime")?
        .block_on(execute(args))
}

fn validate_before_open(command: &Command) -> Result<()> {
    match command {
        Command::SetOwner { user, .. } => reject_local(user)?,
        Command::RebindIssuer { from, to } => {
            reject_local(from)?;
            reject_local(to)?;
        }
        Command::ListWorkspaces | Command::ListUsers { .. } => {}
    }
    Ok(())
}

async fn execute(args: &Args) -> Result<bool> {
    let db = AdminDb::open(args.state_dir.clone())
        .await
        .context("open the existing Coral state database")?;
    match &args.command {
        Command::ListWorkspaces => {
            print!("{}", render_workspaces(&db.list_workspaces().await?));
            Ok(true)
        }
        Command::ListUsers { show_subjects } => {
            print!("{}", render_users(&db.list_users().await?, *show_subjects));
            Ok(true)
        }
        Command::SetOwner { workspace, user } => {
            match set_owner_report(workspace, user, db.set_owner(workspace, user).await?) {
                Ok(report) => {
                    println!("{report}");
                    Ok(true)
                }
                Err(report) => {
                    eprintln!("{report}");
                    Ok(false)
                }
            }
        }
        Command::RebindIssuer { from, to } => {
            let rows = db.rebind_issuer(from, to).await?;
            let (from, to) = (display(from), display(to));
            println!(
                "rebound {rows} {} from {from} to {to}",
                if rows == 1 { "user" } else { "users" }
            );
            Ok(true)
        }
    }
}

fn render_workspaces(workspaces: &[WorkspaceSummary]) -> String {
    if workspaces.is_empty() {
        return "no workspaces\n".to_string();
    }
    let width = workspaces
        .iter()
        .map(|workspace| display(&workspace.name).len())
        .max()
        .unwrap_or(0)
        .max("WORKSPACE".len());
    let mut output = format!("{:width$}  SHARED OWNERS  MEMBERS\n", "WORKSPACE");
    let mut inaccessible = 0;
    for workspace in workspaces {
        let warning = if workspace.owner_count == 0 {
            inaccessible += 1;
            "  <- RECOVERY REQUIRED"
        } else {
            ""
        };
        writeln!(
            output,
            "{:width$}  {:>13}  {:>7}{warning}",
            display(&workspace.name),
            workspace.owner_count,
            workspace.member_count
        )
        .expect("String writes cannot fail");
    }
    let noun = match workspaces.len() {
        1 => "workspace",
        _ => "workspaces",
    };
    writeln!(
        output,
        "\n{} {noun}, {inaccessible} require shared-mode recovery",
        workspaces.len()
    )
    .expect("String writes cannot fail");
    output
}

fn render_users(users: &[UserSummary], show_subjects: bool) -> String {
    if users.is_empty() {
        return "no users; a person appears after their first successful login\n".to_string();
    }
    let mut output = "USER ID  DISPLAY NAME  ISSUER".to_string();
    if show_subjects {
        output.push_str("  SUBJECT");
    }
    output.push('\n');
    for user in users {
        write!(
            output,
            "{}  {}  {}",
            display(&user.user_id),
            display(user.display_name.as_deref().unwrap_or("-")),
            display(&user.issuer)
        )
        .expect("String writes cannot fail");
        if show_subjects {
            write!(output, "  {}", display(&user.subject)).expect("String writes cannot fail");
        }
        output.push('\n');
    }
    if !show_subjects {
        output.push_str("subjects withheld; pass --show-subjects only when needed\n");
    }
    output
}

fn display(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

fn set_owner_report(
    workspace: &str,
    user: &str,
    outcome: SetOwnerOutcome,
) -> Result<String, String> {
    let (workspace, user) = (display(workspace), display(user));
    match outcome {
        SetOwnerOutcome::Added => Ok(format!("{user} is now an owner of {workspace}")),
        SetOwnerOutcome::Promoted => Ok(format!(
            "{user} was a member of {workspace} and is now an owner"
        )),
        SetOwnerOutcome::Unchanged => Ok(format!(
            "{user} was already an owner of {workspace}; unchanged"
        )),
        SetOwnerOutcome::WorkspaceNotFound => Err(format!(
            "no workspace named {workspace}; run list-workspaces for exact names"
        )),
        SetOwnerOutcome::UserNotFound => Err(format!(
            "no user {user}; the intended owner must sign in once before recovery"
        )),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use coral_app::admin::{SetOwnerOutcome, UserSummary, WorkspaceSummary};

    use super::{Command, render_users, render_workspaces, set_owner_report};
    use crate::{Cli, Command as TopCommand};

    fn parse(command: &str) -> super::Args {
        let cli = Cli::try_parse_from(std::iter::once("xtask").chain(command.split_whitespace()))
            .expect("valid workspace-admin command");
        let TopCommand::WorkspaceAdmin(args) = cli.command else {
            panic!("expected workspace-admin");
        };
        args
    }

    #[test]
    fn parser_accepts_global_state_and_rejects_local_before_database_open() {
        let args = parse(
            "workspace-admin --state-dir /missing set-owner --workspace shared --user coral:local",
        );
        assert!(args.state_dir.is_some());
        assert!(super::validate_before_open(&args.command).is_err());
        assert!(matches!(args.command, Command::SetOwner { .. }));
        for command in [
            "workspace-admin rebind-issuer --from coral:local --to new",
            "workspace-admin rebind-issuer --from old --to coral:local",
        ] {
            assert!(super::validate_before_open(&parse(command).command).is_err());
        }
    }

    #[test]
    fn renderers_flag_recovery_and_hide_subjects_by_default() {
        let workspaces = [WorkspaceSummary {
            name: "legacy\n\x1b[2J".to_string(),
            owner_count: 0,
            member_count: 1,
        }];
        assert!(render_workspaces(&workspaces).contains("legacy\\n\\u{1b}[2J"));
        let users = [UserSummary {
            user_id: "u-1".to_string(),
            display_name: Some("Ada".to_string()),
            issuer: "https://issuer".to_string(),
            subject: "private-subject".to_string(),
        }];
        assert!(!render_users(&users, false).contains("private-subject"));
        assert!(render_users(&users, true).contains("private-subject"));
        assert!(
            render_users(
                &[UserSummary {
                    subject: "bad\n\x1b[2J".into(),
                    ..users[0].clone()
                }],
                true
            )
            .contains("bad\\n\\u{1b}[2J")
        );
    }

    #[test]
    fn owner_reports_distinguish_success_from_missing_state() {
        set_owner_report("w", "u", SetOwnerOutcome::Added).unwrap();
        set_owner_report("w", "u", SetOwnerOutcome::Promoted).unwrap();
        set_owner_report("w", "u", SetOwnerOutcome::Unchanged).unwrap();
        set_owner_report("w", "u", SetOwnerOutcome::UserNotFound).unwrap_err();
        set_owner_report("w", "u", SetOwnerOutcome::WorkspaceNotFound).unwrap_err();
    }
}
