//! Prune any generator-owned `reference/sources/*` entries from the
//! Mintlify `docs.json` navigation.
//!
//! Earlier revisions of this generator added one page per source. The
//! current design consolidates everything onto `bundled-sources.mdx`, so
//! this module now only removes stale generator entries and leaves every
//! hand-authored nav entry in place.

use anyhow::{Context, Result};
use serde_json::Value;

/// Returns an updated `docs.json` body with any `reference/sources/*`
/// entries stripped from the Reference group. All other navigation entries
/// are preserved in-place.
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

    #[test]
    fn update_docs_json_strips_generator_entries_and_preserves_others() {
        let updated = update_docs_json(FIXTURE_DOCS_JSON).expect("update nav");
        insta::assert_snapshot!("docs_json_nav_update", updated);
    }
}
