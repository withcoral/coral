//! CLI reference documentation generator.
//!
//! Generates `docs/reference/cli-reference.mdx` from the `clap` command tree
//! defined in [`coral_cli::Cli`].
//!
//! Run without arguments to regenerate the file:
//! ```sh
//! cargo run -p xtask
//! ```
//!
//! Run with `--check` in CI to verify the file is up to date:
//! ```sh
//! cargo run -p xtask -- --check
//! ```

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "xtask intentionally prints status to the terminal"
)]

use std::fmt::Write;
use std::path::Path;
use std::{fs, process};

use clap::CommandFactory;
use coral_cli::Cli;

fn main() {
    let check = std::env::args().any(|a| a == "--check");

    let cmd = Cli::command();
    let expected = generate_cli_reference(&cmd);

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("cannot resolve workspace root");
    let path = workspace.join("docs/reference/cli-reference.mdx");

    if check {
        let actual = fs::read_to_string(&path).unwrap_or_default();
        if actual != expected {
            eprintln!(
                "error: {} is out of date. Run `cargo run -p xtask` to regenerate.",
                path.display()
            );
            process::exit(1);
        }
        println!("cli-reference.mdx is up to date.");
    } else {
        fs::write(&path, &expected).unwrap_or_else(|e| {
            eprintln!("error: failed to write {}: {e}", path.display());
            process::exit(1);
        });
        println!("Wrote {}", path.display());
    }
}

// ---------------------------------------------------------------------------
// MDX generation
// ---------------------------------------------------------------------------

fn generate_cli_reference(root: &clap::Command) -> String {
    let mut out = String::new();

    // Frontmatter
    writeln!(out, "---").unwrap();
    writeln!(out, "title: \"CLI reference\"").unwrap();
    writeln!(
        out,
        "description: \"Reference for the current Coral CLI commands and options.\""
    )
    .unwrap();
    writeln!(out, "---").unwrap();
    writeln!(out).unwrap();

    // Intro
    writeln!(
        out,
        "This reference is generated from the current clap command tree."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "```text").unwrap();
    writeln!(out, "{}", render_help(root, None)).unwrap();
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();

    let subs: Vec<_> = visible_subcommands(root).collect();
    for sub in &subs {
        render_command(&mut out, &[root.get_name()], sub, 2);
    }

    out
}

fn render_command(out: &mut String, parents: &[&str], cmd: &clap::Command, depth: usize) {
    let cmd_path = format!("{} {}", parents.join(" "), cmd.get_name());

    let hashes = "#".repeat(depth);

    writeln!(out).unwrap();
    writeln!(out, "{hashes} `{cmd_path}`").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "```text").unwrap();
    writeln!(out, "{}", render_help(cmd, Some(&cmd_path))).unwrap();
    writeln!(out, "```").unwrap();

    let children: Vec<_> = visible_subcommands(cmd).collect();
    let mut parents = parents.to_vec();
    parents.push(cmd.get_name());
    for sub in &children {
        render_command(out, &parents, sub, depth + 1);
    }
}

fn render_help(cmd: &clap::Command, cmd_path: Option<&str>) -> String {
    let mut render = cmd.clone().term_width(100);
    if let Some(cmd_path) = cmd_path {
        render = render.bin_name(cmd_path).display_name(cmd_path);
    }
    render.render_long_help().to_string().trim_end().to_string()
}

fn visible_subcommands(cmd: &clap::Command) -> impl Iterator<Item = &clap::Command> {
    cmd.get_subcommands()
        .filter(|s| !s.is_hide_set() && s.get_name() != "help")
}
