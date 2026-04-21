//! Maintain the generator-owned entries in the Mintlify `docs.json`
//! navigation.
//!
//! The generator owns exactly one nav entry — `reference/bundled-sources` —
//! plus any stale `reference/sources/*` entries left over from an earlier
//! per-source-page design. Every other nav entry is hand-authored and left
//! in place.

use anyhow::{Context, Result};
use serde_json::Value;

const BUNDLED_SOURCES_ENTRY: &str = "reference/bundled-sources";

/// Returns an updated `docs.json` body with the generator-owned Reference
/// entries reconciled: stale `reference/sources/*` entries are stripped,
/// and the required `reference/bundled-sources` entry is appended when
/// absent. All other navigation entries are preserved in their authored
/// order.
pub(crate) fn update_docs_json(existing: &str) -> Result<String> {
    let mut root: Value = serde_json::from_str(existing).context("parsing docs.json as JSON")?;

    let groups = root
        .get_mut("navigation")
        .and_then(|n| n.get_mut("groups"))
        .and_then(Value::as_array_mut)
        .context("docs.json is missing navigation.groups array")?;

    let reference = groups
        .iter_mut()
        .find(|group| {
            group
                .get("group")
                .and_then(Value::as_str)
                .is_some_and(|name| name == "Reference")
        })
        .context("docs.json navigation has no 'Reference' group")?;

    let pages = reference
        .get_mut("pages")
        .and_then(Value::as_array_mut)
        .context("Reference group is missing a 'pages' array")?;

    pages.retain(|entry| match entry.as_str() {
        Some(s) => !s.starts_with("reference/sources/"),
        None => true,
    });

    // Ensure the generator-owned `reference/bundled-sources` entry is
    // present. Without this, a hand edit that drops it would silently pass
    // `docs-check` while leaving the generated page orphaned from nav.
    let has_bundled_sources = pages
        .iter()
        .any(|entry| entry.as_str() == Some(BUNDLED_SOURCES_ENTRY));
    if !has_bundled_sources {
        pages.push(Value::String(BUNDLED_SOURCES_ENTRY.to_string()));
    }

    let mut serialized =
        serde_json::to_string_pretty(&root).context("serializing updated docs.json")?;
    if !serialized.ends_with('\n') {
        serialized.push('\n');
    }
    Ok(serialized)
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
        "group": "Reference",
        "pages": [
          "reference/cli-reference",
          "reference/source-spec-reference"
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
}
