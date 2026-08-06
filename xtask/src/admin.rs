//! Operator recovery commands over a Coral deployment's state database.
//!
//! Behind the `admin` feature and deliberately not part of the shipped product.
//! [`coral_app::admin`] documents why this exists, what it can and cannot
//! confine to one machine, and why the server need not be stopped.

use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;
use coral_app::admin::{AdminDb, SetOwnerOutcome, UserSummary, WorkspaceSummary};

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// State directory to repair. Defaults to `CORAL_CONFIG_DIR`, then the
    /// platform app-state directory the server uses.
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List every workspace with its owner and member counts.
    ///
    /// Ownerless workspaces are flagged: nothing else reports them, because an
    /// ownerless workspace is invisible to every RPC caller.
    ListWorkspaces,
    /// List every provisioned user, to map a person to the internal user ID
    /// `set-owner` takes.
    ListUsers(ListUsersArgs),
    /// Make a user an owner of a workspace, repairing an ownerless one.
    SetOwner(SetOwnerArgs),
    /// Move every user bound to one issuer onto another issuer.
    RebindIssuer(RebindIssuerArgs),
}

#[derive(Debug, clap::Args)]
struct ListUsersArgs {
    /// Also print each provider subject.
    ///
    /// Subjects identify people and are frequently email addresses, so they are
    /// withheld by default: this output gets pasted into tickets and chat, and
    /// the user ID, display name, and issuer are normally enough to pick the
    /// right person. Ask for them when two identities are otherwise
    /// indistinguishable.
    #[arg(long)]
    show_subjects: bool,
}

#[derive(Debug, clap::Args)]
struct SetOwnerArgs {
    /// Workspace to repair, by name.
    #[arg(long)]
    workspace: String,

    /// Internal user ID to make an owner, as reported by `list-users`.
    #[arg(long)]
    user: String,
}

#[derive(Debug, clap::Args)]
struct RebindIssuerArgs {
    /// Issuer the users are bound to today.
    #[arg(long)]
    from: String,

    /// Issuer to bind them to instead.
    #[arg(long)]
    to: String,
}

/// Runs one recovery command, returning `Ok(false)` when the deployment simply
/// does not contain what the operator named.
pub(crate) fn run(args: &Args) -> Result<bool> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build the recovery command runtime")?
        .block_on(execute(args))
}

async fn execute(args: &Args) -> Result<bool> {
    let db = AdminDb::open(args.state_dir.clone())
        .await
        .context("open the Coral state database")?;
    match &args.command {
        Command::ListWorkspaces => {
            print!("{}", render_workspaces(&db.list_workspaces().await?));
            Ok(true)
        }
        Command::ListUsers(list) => {
            let users = db.list_users().await?;
            print!("{}", render_users(&users, list.show_subjects));
            Ok(true)
        }
        Command::SetOwner(set) => {
            let outcome = db.set_owner(&set.workspace, &set.user).await?;
            match report_set_owner(&set.workspace, &set.user, outcome) {
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
        Command::RebindIssuer(rebind) => {
            let rows = db.rebind_issuer(&rebind.from, &rebind.to).await?;
            println!(
                "rebound {rows} {} from {} to {}",
                if rows == 1 { "user" } else { "users" },
                rebind.from,
                rebind.to
            );
            Ok(true)
        }
    }
}

fn render_workspaces(workspaces: &[WorkspaceSummary]) -> String {
    if workspaces.is_empty() {
        return "no workspaces\n".to_string();
    }

    let name_width = workspaces
        .iter()
        .map(|workspace| workspace.name.chars().count())
        .max()
        .unwrap_or(0)
        .max("WORKSPACE".len());
    let mut out = format!("{:name_width$}  OWNERS  MEMBERS\n", "WORKSPACE");
    let mut ownerless = 0_usize;
    for workspace in workspaces {
        let flag = if workspace.owner_count == 0 {
            ownerless += 1;
            "  <- OWNERLESS: unreachable until an owner is appointed"
        } else {
            ""
        };
        writeln!(
            out,
            "{:name_width$}  {:>6}  {:>7}{flag}",
            workspace.name, workspace.owner_count, workspace.member_count
        )
        .expect("writing into a String cannot fail");
    }
    let total = workspaces.len();
    let noun = if total == 1 {
        "workspace"
    } else {
        "workspaces"
    };
    writeln!(out, "\n{total} {noun}, {ownerless} ownerless")
        .expect("writing into a String cannot fail");
    out
}

fn render_users(users: &[UserSummary], show_subjects: bool) -> String {
    if users.is_empty() {
        return "no users; users are created at their first login\n".to_string();
    }

    let id_width = users
        .iter()
        .map(|user| user.user_id.chars().count())
        .max()
        .unwrap_or(0)
        .max("USER ID".len());
    let name_width = users
        .iter()
        .map(|user| display_name(user).chars().count())
        .max()
        .unwrap_or(0)
        .max("DISPLAY NAME".len());
    let issuer_width = users
        .iter()
        .map(|user| user.issuer.chars().count())
        .max()
        .unwrap_or(0)
        .max("ISSUER".len());
    let mut out = format!(
        "{:id_width$}  {:name_width$}  {:issuer_width$}",
        "USER ID", "DISPLAY NAME", "ISSUER"
    );
    if show_subjects {
        out.push_str("  SUBJECT");
    }
    out.push('\n');
    for user in users {
        write!(
            out,
            "{:id_width$}  {:name_width$}  {:issuer_width$}",
            user.user_id,
            display_name(user),
            user.issuer
        )
        .expect("writing into a String cannot fail");
        if show_subjects {
            write!(out, "  {}", user.subject).expect("writing into a String cannot fail");
        }
        out.push('\n');
    }
    if !show_subjects {
        out.push_str("\nsubjects withheld; pass --show-subjects when two identities look alike\n");
    }
    out
}

fn display_name(user: &UserSummary) -> &str {
    user.display_name.as_deref().unwrap_or("-")
}

/// Phrases one `set-owner` outcome, as `Err` when nothing was repaired.
fn report_set_owner(
    workspace: &str,
    user: &str,
    outcome: SetOwnerOutcome,
) -> Result<String, String> {
    match outcome {
        SetOwnerOutcome::Added => Ok(format!("{user} is now an owner of {workspace}")),
        SetOwnerOutcome::Promoted => Ok(format!(
            "{user} was a member of {workspace} and is now an owner"
        )),
        SetOwnerOutcome::Unchanged => Ok(format!(
            "{user} was already an owner of {workspace}; unchanged"
        )),
        SetOwnerOutcome::WorkspaceNotFound => Err(format!(
            "xtask: no workspace named {workspace}; run `list-workspaces` to see the exact names"
        )),
        SetOwnerOutcome::UserNotFound => Err(format!(
            "xtask: no user {user}; run `list-users` for internal user IDs, and note that a person only appears there after their first login"
        )),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::{
        Command, SetOwnerOutcome, UserSummary, WorkspaceSummary, render_users, render_workspaces,
        report_set_owner,
    };
    use crate::{Cli, Command as TopLevelCommand};

    fn workspace(name: &str, owner_count: usize, member_count: usize) -> WorkspaceSummary {
        WorkspaceSummary {
            name: name.to_string(),
            owner_count,
            member_count,
        }
    }

    fn user(user_id: &str, display_name: Option<&str>) -> UserSummary {
        UserSummary {
            user_id: user_id.to_string(),
            display_name: display_name.map(str::to_string),
            issuer: "https://issuer.example".to_string(),
            subject: "ada@example.com".to_string(),
        }
    }

    fn parse(command_line: &[&str]) -> super::Args {
        let cli =
            Cli::try_parse_from(command_line).expect("xtask should accept the recovery invocation");
        match cli.command {
            TopLevelCommand::WorkspaceAdmin(recovery) => recovery,
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn every_recovery_subcommand_takes_the_state_directory() {
        let path = Some(std::path::Path::new("/srv/coral"));
        let before = parse(&[
            "xtask",
            "workspace-admin",
            "--state-dir",
            "/srv/coral",
            "list-workspaces",
        ]);
        assert_eq!(before.state_dir.as_deref(), path);
        assert!(matches!(before.command, Command::ListWorkspaces));

        // The flag is global, so it is accepted after the subcommand too.
        let after = parse(&[
            "xtask",
            "workspace-admin",
            "list-users",
            "--state-dir",
            "/srv/coral",
        ]);
        assert_eq!(after.state_dir.as_deref(), path);
        assert!(matches!(after.command, Command::ListUsers(_)));

        // Omitting it falls through to CORAL_CONFIG_DIR at open time.
        let set_owner = parse(&[
            "xtask",
            "workspace-admin",
            "set-owner",
            "--workspace",
            "shared",
            "--user",
            "u-1",
        ]);
        assert!(set_owner.state_dir.is_none());
        assert!(
            matches!(set_owner.command, Command::SetOwner(args) if args.workspace == "shared" && args.user == "u-1")
        );

        let rebind = parse(&[
            "xtask",
            "workspace-admin",
            "rebind-issuer",
            "--from",
            "a",
            "--to",
            "b",
        ]);
        assert!(
            matches!(rebind.command, Command::RebindIssuer(args) if args.from == "a" && args.to == "b")
        );
    }

    #[test]
    fn rendered_workspaces_call_out_the_ownerless_ones() {
        let rendered =
            render_workspaces(&[workspace("healthy", 1, 2), workspace("stranded", 0, 3)]);

        let stranded = rendered
            .lines()
            .find(|line| line.starts_with("stranded"))
            .expect("stranded row");
        assert!(stranded.contains("OWNERLESS"), "unexpected row: {stranded}");
        let healthy = rendered
            .lines()
            .find(|line| line.starts_with("healthy"))
            .expect("healthy row");
        assert!(!healthy.contains("OWNERLESS"), "unexpected row: {healthy}");
        assert!(
            rendered.contains("2 workspaces, 1 ownerless"),
            "missing summary: {rendered}"
        );

        assert_eq!(render_workspaces(&[]), "no workspaces\n");
    }

    #[test]
    fn rendered_users_withhold_subjects_unless_asked() {
        let users = [user("u-1", Some("Ada")), user("u-2", None)];

        let default = render_users(&users, false);
        assert!(default.contains("u-1"), "missing user ID: {default}");
        assert!(default.contains("Ada"), "missing display name: {default}");
        assert!(
            default.contains("https://issuer.example"),
            "missing issuer: {default}"
        );
        assert!(
            !default.contains("ada@example.com"),
            "subject must be withheld by default: {default}"
        );

        let revealed = render_users(&users, true);
        assert!(
            revealed.contains("ada@example.com"),
            "--show-subjects must print subjects: {revealed}"
        );
    }

    #[test]
    fn set_owner_reports_repairs_and_fails_on_unknown_names() {
        for outcome in [
            SetOwnerOutcome::Added,
            SetOwnerOutcome::Promoted,
            SetOwnerOutcome::Unchanged,
        ] {
            let report = report_set_owner("shared", "u-1", outcome)
                .expect("a repaired workspace must be reported as success");
            assert!(
                report.contains("shared") && report.contains("u-1"),
                "unhelpful message for {outcome:?}: {report}"
            );
        }

        let unknown_user = report_set_owner("shared", "u-9", SetOwnerOutcome::UserNotFound)
            .expect_err("an unknown user must fail");
        assert!(
            unknown_user.contains("first login"),
            "unhelpful message: {unknown_user}"
        );
        let unknown_workspace =
            report_set_owner("absent", "u-1", SetOwnerOutcome::WorkspaceNotFound)
                .expect_err("an unknown workspace must fail");
        assert!(
            unknown_workspace.contains("list-workspaces"),
            "unhelpful message: {unknown_workspace}"
        );
    }
}
