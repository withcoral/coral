//! Maintain the generator-owned entries in the Mintlify `docs.json`
//! navigation.
//!
//! The generator owns nav entries in the Reference and Project groups:
//!
//! - `reference/bundled-sources` and `reference/community-sources` in the
//!   `Reference` group. Stale `reference/sources/*` entries from an earlier
//!   per-source-page design are also stripped here.
//! - `project/changelog` — last entry of the `Project` group,
//!   generated alongside the changelog mdx page.
//!
//! Every other navigation entry is hand-authored and left in place.
//! Reference source catalog entries are inserted before
//! `reference/source-spec-reference` when missing, keeping source catalogs
//! together while preserving other authored entries.
//!
//! Reconciliation is idempotent: calling [`update_docs_json`] on its own
//! output must produce the same output, since both entries are added
//! only when absent.

use anyhow::{Context, Result};
use serde_json::Value;

const BUNDLED_SOURCES_ENTRY: &str = "reference/bundled-sources";
const COMMUNITY_SOURCES_ENTRY: &str = "reference/community-sources";
const CHANGELOG_ENTRY: &str = "project/changelog";
const SOURCE_SPEC_REFERENCE_ENTRY: &str = "reference/source-spec-reference";

/// Returns an updated `docs.json` body with the generator-owned
/// navigation entries reconciled:
///
/// - `Reference` group: stale `reference/sources/*` entries are stripped,
///   and source catalog entries are restored when absent.
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

    restore_reference_entry(pages, BUNDLED_SOURCES_ENTRY);
    restore_reference_entry(pages, COMMUNITY_SOURCES_ENTRY);
    Ok(())
}

fn restore_reference_entry(pages: &mut Vec<Value>, entry: &str) {
    pages.retain(|e| e.as_str() != Some(entry));

    let insert_at = pages
        .iter()
        .position(|e| e.as_str() == Some(SOURCE_SPEC_REFERENCE_ENTRY))
        .unwrap_or(pages.len());
    pages.insert(insert_at, Value::String(entry.to_string()));
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
    use super::{
        BUNDLED_SOURCES_ENTRY, CHANGELOG_ENTRY, COMMUNITY_SOURCES_ENTRY, update_docs_json,
    };
    use serde_json::json;

    fn fixture_docs_json(get_started_pages: &[&str], reference_pages: &[&str]) -> String {
        serde_json::to_string_pretty(&json!({
            "name": "Coral Docs",
            "navigation": {
                "groups": [
                    {
                        "group": "Get started",
                        "pages": get_started_pages,
                    },
                    {
                        "group": "Reference",
                        "pages": reference_pages,
                    },
                    {
                        "group": "Project",
                        "pages": ["project/roadmap"],
                    },
                ],
            },
        }))
        .expect("fixture docs json should serialize")
    }

    fn fixture_docs_json_with_stale_source() -> String {
        fixture_docs_json(
            &["index", "getting-started/installation"],
            &[
                "reference/cli-reference",
                "reference/bundled-sources",
                "reference/sources/stale_manifest",
                "reference/source-spec-reference",
            ],
        )
    }

    fn fixture_docs_json_without_source_catalogs() -> String {
        fixture_docs_json(
            &["index"],
            &["reference/cli-reference", "reference/source-spec-reference"],
        )
    }

    fn assert_contains_entry(updated: &str, entry: &str) {
        let needle = format!("\"{entry}\"");
        assert!(
            updated.contains(&needle),
            "expected {entry} entry in docs nav: {updated}",
        );
    }

    #[test]
    fn update_docs_json_strips_generator_entries_and_preserves_others() {
        let updated = update_docs_json(&fixture_docs_json_with_stale_source()).expect("update nav");
        insta::assert_snapshot!("docs_json_nav_update", updated);
    }

    #[test]
    fn update_docs_json_restores_missing_source_catalog_entries() {
        let updated = update_docs_json(&fixture_docs_json_without_source_catalogs())
            .expect("restore source catalog entries");
        for entry in [BUNDLED_SOURCES_ENTRY, COMMUNITY_SOURCES_ENTRY] {
            assert_contains_entry(&updated, entry);
        }
    }

    #[test]
    fn update_docs_json_normalizes_duplicate_or_misordered_source_catalog_entries() {
        let input = fixture_docs_json(
            &[],
            &[
                "reference/community-sources",
                "reference/cli-reference",
                "reference/source-spec-reference",
                "reference/bundled-sources",
                "reference/community-sources",
            ],
        );

        let updated = update_docs_json(&input).expect("normalize source catalog entries");
        let root: serde_json::Value = serde_json::from_str(&updated).expect("updated docs json");
        let groups = root
            .get("navigation")
            .and_then(|navigation| navigation.get("groups"))
            .and_then(serde_json::Value::as_array)
            .expect("navigation groups");
        let reference_pages = groups
            .iter()
            .find(|group| {
                group.get("group").and_then(serde_json::Value::as_str) == Some("Reference")
            })
            .and_then(|group| group.get("pages"))
            .and_then(serde_json::Value::as_array)
            .expect("reference pages")
            .iter()
            .map(|page| page.as_str().expect("page string"))
            .collect::<Vec<_>>();

        assert_eq!(
            reference_pages,
            vec![
                "reference/cli-reference",
                "reference/bundled-sources",
                "reference/community-sources",
                "reference/source-spec-reference",
            ]
        );
    }

    #[test]
    fn update_docs_json_appends_changelog_entry_to_project() {
        let updated =
            update_docs_json(&fixture_docs_json_with_stale_source()).expect("append changelog");
        assert_contains_entry(&updated, CHANGELOG_ENTRY);
    }

    #[test]
    fn update_docs_json_is_idempotent_for_changelog_entry() {
        // Running the reconciliation on its own output must not duplicate
        // the changelog entry — protects against a future bug where we
        // push unconditionally instead of only when absent.
        let once = update_docs_json(&fixture_docs_json_with_stale_source()).expect("first pass");
        let twice = update_docs_json(&once).expect("second pass");
        assert_eq!(once, twice, "second pass changed the output");
        let occurrences = twice.matches(&format!("\"{CHANGELOG_ENTRY}\"")).count();
        assert_eq!(occurrences, 1, "changelog entry duplicated: {twice}");
    }
}
