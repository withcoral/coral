//! Maintain the generator-owned entries in the Mintlify `docs.json`
//! navigation.
//!
//! The generator owns two nav entries, each in a different group:
//!
//! - `reference/bundled-sources` — last entry of the `Reference` group.
//!   Stale `reference/sources/*` entries from an earlier per-source-page
//!   design are also stripped here.
//! - `project/changelog` — last entry of the `Project` group,
//!   generated alongside the changelog mdx page.
//!
//! Every other navigation entry is hand-authored and left in place.
//! Each owned entry is appended (not inserted) so it sits at the bottom
//! of its group.
//!
//! Reconciliation is idempotent: calling [`update_docs_json`] on its own
//! output must produce the same output, since both entries are added
//! only when absent.

use anyhow::{Context, Result};
use serde_json::Value;

const BUNDLED_SOURCES_ENTRY: &str = "reference/bundled-sources";
const CHANGELOG_ENTRY: &str = "project/changelog";

/// Returns an updated `docs.json` body with the generator-owned
/// navigation entries reconciled:
///
/// - `Reference` group: stale `reference/sources/*` entries are stripped,
///   and `reference/bundled-sources` is appended when absent.
/// - `Project` group: `project/changelog` is appended when absent.
///
/// All other navigation entries are preserved in their authored order.
pub(crate) fn update_docs_json(existing: &str) -> Result<String> {
    let mut root: Value = serde_json::from_str(existing).context("parsing docs.json as JSON")?;

    let groups = root
        .get_mut("navigation")
        .and_then(|n| n.get_mut("groups"))
        .and_then(Value::as_array_mut)
        .context("docs.json is missing navigation.groups array")?;

    reconcile_reference_group(groups)?;
    reconcile_project_group(groups)?;

    let mut serialized =
        serde_json::to_string_pretty(&root).context("serializing updated docs.json")?;
    if !serialized.ends_with('\n') {
        serialized.push('\n');
    }
    Ok(serialized)
}

fn reconcile_reference_group(groups: &mut [Value]) -> Result<()> {
    let pages = group_pages_mut(groups, "Reference")?;

    pages.retain(|entry| match entry.as_str() {
        Some(s) => !s.starts_with("reference/sources/"),
        None => true,
    });

    // Without this restoration, a hand edit that drops the entry would
    // silently pass `docs-check` while leaving the generated page
    // orphaned from nav.
    if !pages
        .iter()
        .any(|e| e.as_str() == Some(BUNDLED_SOURCES_ENTRY))
    {
        pages.push(Value::String(BUNDLED_SOURCES_ENTRY.to_string()));
    }
    Ok(())
}

fn reconcile_project_group(groups: &mut [Value]) -> Result<()> {
    let pages = group_pages_mut(groups, "Project")?;
    if !pages.iter().any(|e| e.as_str() == Some(CHANGELOG_ENTRY)) {
        pages.push(Value::String(CHANGELOG_ENTRY.to_string()));
    }
    Ok(())
}

fn group_pages_mut<'a>(groups: &'a mut [Value], name: &str) -> Result<&'a mut Vec<Value>> {
    let group = groups
        .iter_mut()
        .find(|group| {
            group
                .get("group")
                .and_then(Value::as_str)
                .is_some_and(|n| n == name)
        })
        .with_context(|| format!("docs.json navigation has no '{name}' group"))?;

    group
        .get_mut("pages")
        .and_then(Value::as_array_mut)
        .with_context(|| format!("'{name}' group is missing a 'pages' array"))
}

#[cfg(test)]
mod tests {
    use super::update_docs_json;

    const FIXTURE_DOCS_JSON: &str = r#"{
  "name": "Coral Docs",
  "navigation": {
    "groups": [
      {
        "group": "Get started",
        "pages": [
          "index",
          "getting-started/installation"
        ]
      },
      {
        "group": "Reference",
        "pages": [
          "reference/cli-reference",
          "reference/bundled-sources",
          "reference/sources/stale_manifest",
          "reference/source-spec-reference"
        ]
      },
      {
        "group": "Project",
        "pages": [
          "project/roadmap"
        ]
      }
    ]
  }
}
"#;

    const FIXTURE_WITHOUT_BUNDLED_SOURCES: &str = r#"{
  "name": "Coral Docs",
  "navigation": {
    "groups": [
      {
        "group": "Get started",
        "pages": [
          "index"
        ]
      },
      {
        "group": "Reference",
        "pages": [
          "reference/cli-reference",
          "reference/source-spec-reference"
        ]
      },
      {
        "group": "Project",
        "pages": [
          "project/roadmap"
        ]
      }
    ]
  }
}
"#;

    #[test]
    fn update_docs_json_strips_generator_entries_and_preserves_others() {
        let updated = update_docs_json(FIXTURE_DOCS_JSON).expect("update nav");
        insta::assert_snapshot!("docs_json_nav_update", updated);
    }

    #[test]
    fn update_docs_json_restores_missing_bundled_sources_entry() {
        let updated =
            update_docs_json(FIXTURE_WITHOUT_BUNDLED_SOURCES).expect("restore bundled-sources");
        assert!(
            updated.contains("\"reference/bundled-sources\""),
            "expected bundled-sources to be restored: {updated}",
        );
    }

    #[test]
    fn update_docs_json_appends_changelog_entry_to_project() {
        let updated = update_docs_json(FIXTURE_DOCS_JSON).expect("append changelog");
        assert!(
            updated.contains("\"project/changelog\""),
            "expected changelog entry in Project group: {updated}",
        );
    }

    #[test]
    fn update_docs_json_is_idempotent_for_changelog_entry() {
        // Running the reconciliation on its own output must not duplicate
        // the changelog entry — protects against a future bug where we
        // push unconditionally instead of only when absent.
        let once = update_docs_json(FIXTURE_DOCS_JSON).expect("first pass");
        let twice = update_docs_json(&once).expect("second pass");
        assert_eq!(once, twice, "second pass changed the output");
        let occurrences = twice.matches("\"project/changelog\"").count();
        assert_eq!(occurrences, 1, "changelog entry duplicated: {twice}");
    }
}
