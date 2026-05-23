//! Export installable agent skills from the canonical plugin tree.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use walkdir::WalkDir;

const SOURCE_DIR: &str = "plugins/coral/skills";

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillMetadata {
    name: String,
    description: String,
    title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct AgentMetadata {
    interface: AgentInterface,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct AgentInterface {
    display_name: String,
    short_description: String,
    default_prompt: String,
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
        let agent_metadata = parse_agent_metadata(&dir.join("agents/openai.yaml"))?;
        validate_skill_definition(&metadata, &agent_metadata)?;
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
    let title = raw
        .lines()
        .find_map(|line| line.strip_prefix("# "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("missing top-level heading")?
        .to_owned();
    Ok(SkillMetadata {
        name,
        description,
        title,
    })
}

fn parse_agent_metadata(path: &Path) -> Result<AgentMetadata> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let metadata: AgentMetadata =
        serde_yaml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
    validate_agent_metadata(path, &metadata)?;
    Ok(metadata)
}

fn validate_agent_metadata(path: &Path, metadata: &AgentMetadata) -> Result<()> {
    if metadata.interface.display_name.trim().is_empty() {
        bail!("{} has empty interface.display_name", path.display());
    }
    if metadata.interface.short_description.trim().is_empty() {
        bail!("{} has empty interface.short_description", path.display());
    }
    if metadata.interface.default_prompt.trim().is_empty() {
        bail!("{} has empty interface.default_prompt", path.display());
    }
    Ok(())
}

fn validate_skill_definition(
    metadata: &SkillMetadata,
    agent_metadata: &AgentMetadata,
) -> Result<()> {
    let expected_display_name = expected_display_name(&metadata.name)?;
    if agent_metadata.interface.display_name != expected_display_name {
        bail!(
            "skill '{}' display_name must be '{}', got '{}'",
            metadata.name,
            expected_display_name,
            agent_metadata.interface.display_name
        );
    }
    if metadata.title != agent_metadata.interface.display_name {
        bail!(
            "skill '{}' heading must match display_name '{}', got '{}'",
            metadata.name,
            agent_metadata.interface.display_name,
            metadata.title
        );
    }
    let expected_prompt_token = format!("${}", metadata.name);
    if !mentions_skill_token(
        &agent_metadata.interface.default_prompt,
        &expected_prompt_token,
    ) {
        bail!(
            "skill '{}' default_prompt must mention '{}'",
            metadata.name,
            expected_prompt_token
        );
    }
    Ok(())
}

fn mentions_skill_token(prompt: &str, token: &str) -> bool {
    prompt.match_indices(token).any(|(start, _)| {
        let before = prompt
            .get(..start)
            .and_then(|value| value.chars().next_back());
        let after = prompt
            .get(start + token.len()..)
            .and_then(|value| value.chars().next());
        before.is_none_or(|ch| !is_skill_token_char(ch) && ch != '$')
            && after.is_none_or(|ch| !is_skill_token_char(ch))
    })
}

fn is_skill_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'
}

fn expected_display_name(name: &str) -> Result<String> {
    let suffix = name
        .strip_prefix("coral")
        .context("Coral skill names must start with 'coral'")?;
    if suffix.is_empty() {
        return Ok("Coral".to_string());
    }
    let suffix = suffix
        .strip_prefix('-')
        .filter(|value| !value.is_empty())
        .context("Coral skill names must be 'coral' or start with 'coral-'")?;
    let mut display_name = String::from("Coral");
    for part in suffix.split('-') {
        if part.is_empty() {
            bail!("Coral skill name '{name}' contains an empty segment");
        }
        display_name.push(' ');
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            display_name.extend(first.to_uppercase());
            display_name.push_str(chars.as_str());
        }
    }
    Ok(display_name)
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
    use super::{
        AgentInterface, AgentMetadata, Skill, SkillMetadata, expected_display_name,
        mentions_skill_token, parse_skill_metadata_str, render_readme, unquote,
        validate_skill_definition,
    };

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
                title: "Coral".to_string(),
            }
        );
    }

    #[test]
    fn derives_coral_display_names_from_skill_names() {
        assert_eq!(expected_display_name("coral").unwrap(), "Coral");
        assert_eq!(
            expected_display_name("coral-create-source-spec").unwrap(),
            "Coral Create Source Spec"
        );
        assert_eq!(
            expected_display_name("coral-review-source-spec").unwrap(),
            "Coral Review Source Spec"
        );
    }

    #[test]
    fn rejects_skill_heading_that_does_not_match_display_name() {
        let metadata = SkillMetadata {
            name: "coral-create-source-spec".to_string(),
            description: "Create source specs.".to_string(),
            title: "Create Source Spec".to_string(),
        };
        let agent_metadata = AgentMetadata {
            interface: AgentInterface {
                display_name: "Coral Create Source Spec".to_string(),
                short_description: "Author source specs".to_string(),
                default_prompt: "Use $coral-create-source-spec.".to_string(),
            },
        };
        let error = validate_skill_definition(&metadata, &agent_metadata)
            .expect_err("heading drift should be rejected")
            .to_string();
        assert!(error.contains("heading must match display_name"));
    }

    #[test]
    fn matches_skill_prompt_token_as_standalone_token() {
        assert!(mentions_skill_token("Use $coral to query data.", "$coral"));
        assert!(mentions_skill_token("Use ($coral).", "$coral"));
        assert!(mentions_skill_token(
            "Use $coral-create-source-spec to create specs.",
            "$coral-create-source-spec"
        ));
        assert!(!mentions_skill_token(
            "Use $coral-create-source-spec to create specs.",
            "$coral"
        ));
        assert!(!mentions_skill_token("Use $coral_review.", "$coral"));
        assert!(!mentions_skill_token("Use $$coral.", "$coral"));
    }

    #[test]
    fn rejects_prompt_that_only_mentions_longer_skill_token() {
        let metadata = SkillMetadata {
            name: "coral".to_string(),
            description: "Query live sources.".to_string(),
            title: "Coral".to_string(),
        };
        let agent_metadata = AgentMetadata {
            interface: AgentInterface {
                display_name: "Coral".to_string(),
                short_description: "Query live sources".to_string(),
                default_prompt: "Use $coral-create-source-spec to create specs.".to_string(),
            },
        };
        let error = validate_skill_definition(&metadata, &agent_metadata)
            .expect_err("longer token should not satisfy the base skill")
            .to_string();
        assert!(error.contains("default_prompt must mention '$coral'"));
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
                title: "Coral".to_string(),
            },
        }];
        let readme = render_readme(&skills);
        assert!(readme.contains("| [`coral`](coral/SKILL.md) | Query A \\| B |"));
        assert!(readme.ends_with("Apache 2.0 - see [LICENSE](LICENSE).\n"));
    }
}
