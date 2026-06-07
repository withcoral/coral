//! Architecture and removed-contract regression gates.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use walkdir::WalkDir;

struct CargoDependencyRule {
    crate_name: &'static str,
    forbidden: &'static [&'static str],
}

const DEPENDENCY_RULES: &[CargoDependencyRule] = &[
    CargoDependencyRule {
        crate_name: "coral-capabilities",
        forbidden: &[
            "coral-api",
            "coral-app",
            "coral-client",
            "coral-code-mode",
            "coral-exports",
            "coral-importers",
            "coral-mcp",
            "coral-sql",
            "coral-spec",
            "coral-upstream",
            "datafusion",
        ],
    },
    CargoDependencyRule {
        crate_name: "coral-exports",
        forbidden: &[
            "coral-api",
            "coral-app",
            "coral-client",
            "coral-code-mode",
            "coral-importers",
            "coral-mcp",
            "coral-sql",
            "coral-spec",
            "coral-upstream",
            "datafusion",
        ],
    },
    CargoDependencyRule {
        crate_name: "coral-importers",
        forbidden: &[
            "coral-api",
            "coral-app",
            "coral-client",
            "coral-code-mode",
            "coral-exports",
            "coral-mcp",
            "coral-sql",
            "coral-upstream",
            "datafusion",
        ],
    },
    CargoDependencyRule {
        crate_name: "coral-client",
        forbidden: &[
            "coral-app",
            "coral-capabilities",
            "coral-code-mode",
            "coral-exports",
            "coral-importers",
            "coral-mcp",
            "coral-spec",
            "coral-sql",
            "coral-upstream",
            "datafusion",
        ],
    },
];

/// Validate capability/export dependency direction.
pub(crate) fn architecture_check() -> Result<bool> {
    let mut ok = true;
    for rule in DEPENDENCY_RULES {
        let cargo_toml = PathBuf::from("crates")
            .join(rule.crate_name)
            .join("Cargo.toml");
        let raw = fs::read_to_string(&cargo_toml)
            .with_context(|| format!("reading {}", cargo_toml.display()))?;
        for forbidden in rule.forbidden {
            if manifest_declares_dependency(&raw, forbidden) {
                eprintln!(
                    "architecture-check: {} must not depend on {}",
                    rule.crate_name, forbidden
                );
                ok = false;
            }
        }
    }
    ok &= reject_source_export_composition_in_sql()?;
    ok &= reject_engine_coupling_in_target_crates()?;
    ok &= reject_engine_workspace_membership()?;
    ok &= reject_removed_spec_modules()?;
    if ok {
        println!("architecture-check: ok");
    }
    Ok(ok)
}

/// Scan for removed source-contract names.
pub(crate) fn removed_contract_check() -> Result<bool> {
    let forbidden = [
        concat!("Validated", "Source", "Manifest"),
        concat!("Http", "Source", "Manifest"),
        concat!("Mcp", "Source", "Manifest"),
        concat!("File", "Source", "Manifest"),
        concat!("Runtime", "Source", "Component"),
        concat!("Projection", "Visibility", "::", "Hidden"),
        concat!("projections", ".yaml"),
        concat!("semantic", "-ir", ".yaml"),
    ];
    let mut ok = true;
    for path in rust_and_docs_files(Path::new(".")) {
        if is_removed_contract_gate_path(&path) {
            continue;
        }
        let raw =
            fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        for needle in forbidden {
            if raw.lines().any(|line| line.contains(needle)) {
                eprintln!(
                    "removed-contract-check: {} contains deleted contract name '{}'",
                    path.display(),
                    needle
                );
                ok = false;
            }
        }
        if raw.lines().any(|line| {
            line.contains(concat!("dsl", "_version: 3"))
                || line.contains(concat!("backend", ": http"))
                || line.contains(concat!("backend", ": file"))
                || line.contains(concat!("backend", ": mcp"))
        }) {
            eprintln!(
                "removed-contract-check: {} contains deleted authored source contract syntax",
                path.display()
            );
            ok = false;
        }
    }
    if ok {
        println!("removed-contract-check: ok");
    }
    Ok(ok)
}

fn manifest_declares_dependency(raw: &str, dependency: &str) -> bool {
    raw.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with(&format!("{dependency} "))
            || trimmed.starts_with(&format!("{dependency}="))
            || trimmed.starts_with(&format!("{dependency}.workspace"))
    })
}

fn reject_source_export_composition_in_sql() -> Result<bool> {
    let mut ok = true;
    for path in rust_files(Path::new("crates/coral-sql/src")) {
        let raw =
            fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        for forbidden in ["SourceExports", "WorkspaceExports"] {
            if raw.contains(forbidden) {
                eprintln!(
                    "architecture-check: coral-sql must not load or compose {}; found in {}",
                    forbidden,
                    path.display()
                );
                ok = false;
            }
        }
    }
    Ok(ok)
}

fn reject_engine_coupling_in_target_crates() -> Result<bool> {
    let mut ok = true;
    for root in [
        Path::new("crates/coral-app"),
        Path::new("crates/coral-cli"),
        Path::new("crates/coral-mcp"),
        Path::new("crates/coral-sql"),
    ] {
        for path in rust_and_toml_files(root) {
            let raw =
                fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
            for forbidden in [
                concat!("coral", "_engine"),
                concat!("coral", "-engine"),
                concat!("Engine", "Extensions"),
                concat!("Engine", "Extensions", "Provider"),
                concat!("Query", "Source"),
                concat!("Runtime", "Source", "Component"),
            ] {
                if raw.contains(forbidden) {
                    eprintln!(
                        "architecture-check: target crate file {} must not reference removed engine coupling '{}'",
                        path.display(),
                        forbidden
                    );
                    ok = false;
                }
            }
        }
    }
    Ok(ok)
}

fn reject_engine_workspace_membership() -> Result<bool> {
    let raw = fs::read_to_string("Cargo.toml").context("reading Cargo.toml")?;
    let mut ok = true;
    for forbidden in [
        concat!("coral", "-engine = "),
        concat!("crates/coral", "-engine"),
    ] {
        if raw.lines().any(|line| line.contains(forbidden)) {
            eprintln!(
                "architecture-check: root workspace must not include removed {}",
                forbidden.trim()
            );
            ok = false;
        }
    }
    Ok(ok)
}

fn reject_removed_spec_modules() -> Result<bool> {
    let path = Path::new("crates/coral-spec/src/lib.rs");
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut ok = true;
    for forbidden in [concat!("pub mod ", "backends"), concat!("pub mod ", "v4")] {
        if raw.contains(forbidden) {
            eprintln!(
                "architecture-check: coral-spec must not expose removed module '{forbidden}'"
            );
            ok = false;
        }
    }
    Ok(ok)
}

fn rust_and_docs_files(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(walkdir::DirEntry::into_path)
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("rs" | "md" | "mdx" | "yaml" | "yml" | "json" | "toml")
            )
        })
        .filter(|path| {
            !path
                .components()
                .any(|component| matches!(component.as_os_str().to_str(), Some(".git" | "target")))
        })
        .collect()
}

fn rust_and_toml_files(root: &Path) -> Vec<PathBuf> {
    rust_and_docs_files(root)
        .into_iter()
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("rs" | "toml")
            )
        })
        .collect()
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    rust_and_docs_files(root)
        .into_iter()
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("rs"))
        .collect()
}

fn is_removed_contract_gate_path(path: &Path) -> bool {
    let raw = path.to_string_lossy();
    raw.starts_with("./sources/")
        || raw.contains("/snapshots/")
        || raw.ends_with("xtask/src/gates.rs")
        || raw.ends_with("docs/project/capability-projection-plan.mdx")
}
