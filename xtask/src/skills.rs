//! Export installable agent skills from the canonical plugin tree.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

const SOURCE_DIR: &str = "plugins/coral/skills";

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillMetadata {
    name: String,
    description: String,
}

#[derive(Debug)]
struct Skill {
    dir: PathBuf,
    metadata: SkillMetadata,
}

pub(crate) fn export(dest: &Path) -> Result<bool> {
    let repo_root = std::env::current_dir().context("resolving repo root")?;
    let source_dir = repo_root.join(SOURCE_DIR);
    if !source_dir.is_dir() {
        bail!("missing source directory: {}", source_dir.display());
    }

    fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;
    let repo_root = repo_root
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", repo_root.display()))?;
    let source_dir = source_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", source_dir.display()))?;
    let dest = dest
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", dest.display()))?;
    reject_unsafe_dest(&repo_root, &source_dir, &dest)?;

    let skills = discover_skills(&source_dir)?;
    remove_stale_skill_dirs(&dest, &skills)?;
    for skill in &skills {
        let target = dest.join(&skill.metadata.name);
        if target.exists() {
            fs::remove_dir_all(&target)
                .with_context(|| format!("removing {}", target.display()))?;
        }
        copy_dir_all(&skill.dir, &target)?;
    }

    let license = dest.join("LICENSE");
    fs::copy(repo_root.join("LICENSE"), &license)
        .with_context(|| format!("copying {}", license.display()))?;

    fs::write(dest.join("README.md"), render_readme(&skills))
        .with_context(|| format!("writing {}", dest.join("README.md").display()))?;
    println!(
        "xtask: exported {} skills to {}",
        skills.len(),
        dest.display()
    );
    Ok(true)
}

fn reject_unsafe_dest(repo_root: &Path, source_dir: &Path, dest: &Path) -> Result<()> {
    if dest == Path::new("/")
        || dest == repo_root
        || dest == source_dir
        || dest.starts_with(source_dir)
    {
        bail!("refusing unsafe destination: {}", dest.display());
    }
    Ok(())
}

fn discover_skills(source_dir: &Path) -> Result<Vec<Skill>> {
    let mut skills = Vec::new();
    for entry in
        fs::read_dir(source_dir).with_context(|| format!("reading {}", source_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let dir = entry.path();
        let skill_file = dir.join("SKILL.md");
        if !skill_file.is_file() {
            continue;
        }
        let metadata = parse_skill_metadata(&skill_file)?;
        let dir_name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .context("skill directory name is not valid UTF-8")?;
        if metadata.name != dir_name {
            bail!(
                "skill directory '{}' does not match frontmatter name '{}'",
                dir_name,
                metadata.name
            );
        }
        skills.push(Skill { dir, metadata });
    }
    skills.sort_by(|left, right| left.metadata.name.cmp(&right.metadata.name));
    if skills.is_empty() {
        bail!("no skills found in {}", source_dir.display());
    }
    Ok(skills)
}

fn parse_skill_metadata(path: &Path) -> Result<SkillMetadata> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse_skill_metadata_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

fn parse_skill_metadata_str(raw: &str) -> Result<SkillMetadata> {
    let mut lines = raw.lines();
    if lines.next() != Some("---") {
        bail!("missing frontmatter fence");
    }

    let mut name = None;
    let mut description = None;
    for line in lines {
        if line == "---" {
            break;
        }
        if let Some(value) = line.strip_prefix("name:") {
            name = Some(unquote(value.trim()).to_owned());
        } else if let Some(value) = line.strip_prefix("description:") {
            description = Some(unquote(value.trim()).to_owned());
        }
    }

    let name = name
        .filter(|value| !value.is_empty())
        .context("missing name")?;
    let description = description
        .filter(|value| !value.is_empty())
        .context("missing description")?;
    Ok(SkillMetadata { name, description })
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
}

fn remove_stale_skill_dirs(dest: &Path, skills: &[Skill]) -> Result<()> {
    let skill_names: BTreeSet<&str> = skills
        .iter()
        .map(|skill| skill.metadata.name.as_str())
        .collect();
    for entry in fs::read_dir(dest).with_context(|| format!("reading {}", dest.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let dir = entry.path();
        if !dir.join("SKILL.md").is_file() {
            continue;
        }
        let Some(name) = dir.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !skill_names.contains(name) {
            fs::remove_dir_all(&dir).with_context(|| format!("removing {}", dir.display()))?;
        }
    }
    Ok(())
}

fn copy_dir_all(source: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;
    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry.with_context(|| format!("walking {}", source.display()))?;
        let path = entry.path();
        let relative = path
            .strip_prefix(source)
            .with_context(|| format!("stripping prefix {}", source.display()))?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = dest.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)
                .with_context(|| format!("creating {}", target.display()))?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            fs::copy(path, &target)
                .with_context(|| format!("copying {} to {}", path.display(), target.display()))?;
        }
    }
    Ok(())
}

fn render_readme(skills: &[Skill]) -> String {
    let mut out = String::new();
    out.push_str("# Coral Skills\n\n");
    out.push_str(
        "<!-- AUTO-GENERATED from withcoral/coral plugins/coral/skills. Do not edit directly. -->\n\n",
    );
    out.push_str("Agent skills for [Coral](https://withcoral.com) - one SQL interface over APIs, files, and live sources, built for agents.\n\n");
    out.push_str("## Installation\n\n");
    out.push_str("```bash\nnpx skills add withcoral/skills\n```\n\n");
    out.push_str("## Available Skills\n\n");
    out.push_str("| Skill | Description |\n");
    out.push_str("|-------|-------------|\n");
    for skill in skills {
        writeln!(
            out,
            "| [`{0}`]({0}/SKILL.md) | {1} |",
            skill.metadata.name,
            skill.metadata.description.replace('|', "\\|"),
        )
        .expect("writing to String is infallible");
    }
    out.push_str("\n## License\n\n");
    out.push_str("Apache 2.0 - see [LICENSE](LICENSE).\n");
    out
}

#[cfg(test)]
mod tests {
    use super::{Skill, SkillMetadata, parse_skill_metadata_str, render_readme, unquote};

    #[test]
    fn parses_quoted_frontmatter() {
        let raw = r#"---
name: coral
description: "Query live sources through Coral MCP."
---

# Coral
"#;
        assert_eq!(
            parse_skill_metadata_str(raw).expect("metadata"),
            SkillMetadata {
                name: "coral".to_string(),
                description: "Query live sources through Coral MCP.".to_string(),
            }
        );
    }

    #[test]
    fn strips_single_and_double_quotes() {
        assert_eq!(unquote("\"hello\""), "hello");
        assert_eq!(unquote("'hello'"), "hello");
        assert_eq!(unquote("hello"), "hello");
    }

    #[test]
    fn renders_readme_table_and_escapes_pipes() {
        let skills = vec![Skill {
            dir: "coral".into(),
            metadata: SkillMetadata {
                name: "coral".to_string(),
                description: "Query A | B".to_string(),
            },
        }];
        let readme = render_readme(&skills);
        assert!(readme.contains("| [`coral`](coral/SKILL.md) | Query A \\| B |"));
        assert!(readme.ends_with("Apache 2.0 - see [LICENSE](LICENSE).\n"));
    }
}
