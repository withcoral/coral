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
        "Coral is currently a CLI-first product. The main command surface is:"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "```shellscript").unwrap();
    writeln!(out, "{} <COMMAND>", root.get_name()).unwrap();
    writeln!(out, "```").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Top-level commands:").unwrap();
    writeln!(out).unwrap();

    let subs: Vec<_> = visible_subcommands(root).collect();
    for sub in &subs {
        writeln!(out, "- `{}`", sub.get_name()).unwrap();
    }

    for sub in &subs {
        render_command(&mut out, &[root.get_name()], sub, 2);
    }

    out
}

fn render_command(out: &mut String, parents: &[&str], cmd: &clap::Command, depth: usize) {
    let cmd_path = format!("{} {}", parents.join(" "), cmd.get_name());

    // Build heading: include positional args in display name
    let mut display = cmd_path.clone();
    for arg in cmd.get_arguments() {
        if arg.is_positional() {
            write!(display, " <{}>", arg.get_id().as_str().to_uppercase()).unwrap();
        }
    }

    let hashes = "#".repeat(depth);

    // Heading
    writeln!(out).unwrap();
    writeln!(out, "{hashes} `{display}`").unwrap();
    writeln!(out).unwrap();

    // Description
    if let Some(about) = cmd.get_about() {
        writeln!(out, "{}", as_sentence(&about.to_string())).unwrap();
    }

    // Usage block — only for leaf commands (no visible subcommands)
    let children: Vec<_> = visible_subcommands(cmd).collect();
    if children.is_empty() {
        writeln!(out).unwrap();
        let usage = build_usage(&cmd_path, cmd);
        writeln!(out, "```shellscript").unwrap();
        writeln!(out, "{usage}").unwrap();
        writeln!(out, "```").unwrap();
    }

    // Options table
    let options: Vec<_> = cmd
        .get_arguments()
        .filter(|a| !a.is_positional() && !is_builtin_arg(a))
        .collect();
    if !options.is_empty() {
        writeln!(out).unwrap();
        let sub_hashes = "#".repeat(depth + 1);
        writeln!(out, "{sub_hashes} Options").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "| Option | Type | Default | Description |").unwrap();
        writeln!(out, "| --- | --- | --- | --- |").unwrap();
        for opt in &options {
            let name = opt
                .get_long()
                .map(|l| format!("`--{l}`"))
                .or_else(|| opt.get_short().map(|s| format!("`-{s}`")))
                .unwrap_or_else(|| format!("`{}`", opt.get_id()));

            let vals = possible_values(opt);
            let type_str = if vals.is_empty() {
                "`string`".to_string()
            } else {
                vals.iter()
                    .map(|v| format!("`{v}`"))
                    .collect::<Vec<_>>()
                    .join(" \\| ")
            };

            let default = {
                let defs = opt.get_default_values();
                if defs.is_empty() {
                    "\u{2014}".to_string()
                } else {
                    format!("`{}`", defs[0].to_str().unwrap_or(""))
                }
            };

            let desc = opt
                .get_help()
                .map(std::string::ToString::to_string)
                .unwrap_or_default();

            writeln!(out, "| {name} | {type_str} | {default} | {desc} |").unwrap();
        }
    }

    // After-long-help (prose notes + examples)
    if let Some(after) = cmd.get_after_long_help() {
        let text = after.to_string();
        let (prose, examples) = parse_after_help(&text);

        if !prose.is_empty() {
            writeln!(out).unwrap();
            writeln!(out, "{prose}").unwrap();
        }

        if !examples.is_empty() {
            writeln!(out).unwrap();
            let sub_hashes = "#".repeat(depth + 1);
            writeln!(out, "{sub_hashes} Examples").unwrap();
            writeln!(out).unwrap();
            writeln!(out, "```shellscript").unwrap();
            for ex in &examples {
                writeln!(out, "{ex}").unwrap();
            }
            writeln!(out, "```").unwrap();
        }
    }

    // Recurse into subcommands
    let mut parents = parents.to_vec();
    parents.push(cmd.get_name());
    for sub in &children {
        render_command(out, &parents, sub, depth + 1);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_usage(cmd_path: &str, cmd: &clap::Command) -> String {
    let mut parts = vec![cmd_path.to_string()];

    for arg in cmd.get_arguments() {
        if arg.is_positional() || is_builtin_arg(arg) {
            continue;
        }
        let flag = arg.get_long().map_or_else(
            || format!("-{}", arg.get_short().unwrap()),
            |l| format!("--{l}"),
        );
        let vals = possible_values(arg);
        let value_hint = if vals.is_empty() {
            format!("<{}>", arg.get_id().as_str().to_uppercase())
        } else {
            vals.join("|")
        };
        parts.push(format!("[{flag} {value_hint}]"));
    }

    for arg in cmd.get_arguments() {
        if !arg.is_positional() {
            continue;
        }
        let name = arg.get_id().as_str().to_uppercase();
        if arg.is_required_set() {
            parts.push(format!("<{name}>"));
        } else {
            parts.push(format!("[{name}]"));
        }
    }

    parts.join(" ")
}

fn parse_after_help(text: &str) -> (String, Vec<String>) {
    let marker = "Examples:\n";
    if let Some(idx) = text.find(marker) {
        let prose = text[..idx].trim_end().to_string();
        let examples = text[idx + marker.len()..]
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        (prose, examples)
    } else {
        (text.to_string(), vec![])
    }
}

fn as_sentence(text: &str) -> String {
    let text = text.trim();
    if text.ends_with('.') || text.ends_with('!') || text.ends_with('?') {
        text.to_string()
    } else {
        format!("{text}.")
    }
}

fn is_builtin_arg(arg: &clap::Arg) -> bool {
    matches!(arg.get_id().as_str(), "help" | "version")
}

fn visible_subcommands(cmd: &clap::Command) -> impl Iterator<Item = &clap::Command> {
    cmd.get_subcommands()
        .filter(|s| !s.is_hide_set() && s.get_name() != "help")
}

fn possible_values(arg: &clap::Arg) -> Vec<String> {
    arg.get_value_parser()
        .possible_values()
        .map(|iter| {
            iter.filter(|v| !v.is_hide_set())
                .map(|v| v.get_name().to_string())
                .collect()
        })
        .unwrap_or_default()
}
