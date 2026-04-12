//! Reads and groups a YAML needles file into per-table row collections.

use std::collections::HashMap;
use std::path::Path;

use super::{NeedleSpec, error::NeedleError};

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NeedleEntry {
    schema: String,
    table: String,
    data: serde_json::Map<String, serde_json::Value>,
}

/// Key identifying a table within a source schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TableKey {
    pub schema: String,
    pub table: String,
}

/// Grouped needle rows keyed by source schema and table name.
#[derive(Debug, Default)]
pub(crate) struct NeedleGroups {
    inner: HashMap<TableKey, Vec<serde_json::Value>>,
}

#[derive(Debug)]
pub(crate) struct LoadedNeedles {
    pub(crate) groups: NeedleGroups,
    pub(crate) specs: Vec<NeedleSpec>,
}

impl NeedleGroups {
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn get(&self, schema: &str, table: &str) -> Option<&[serde_json::Value]> {
        let key = TableKey {
            schema: schema.to_string(),
            table: table.to_string(),
        };
        self.inner.get(&key).map(Vec::as_slice)
    }

    pub(crate) fn take(&mut self, schema: &str, table: &str) -> Option<Vec<serde_json::Value>> {
        let key = TableKey {
            schema: schema.to_string(),
            table: table.to_string(),
        };
        self.inner.remove(&key)
    }

    pub(crate) fn ensure_all_consumed(&self) -> Result<(), NeedleError> {
        if self.inner.is_empty() {
            return Ok(());
        }

        let mut tables = self
            .inner
            .keys()
            .map(|key| format!("{}.{}", key.schema, key.table))
            .collect::<Vec<_>>();
        tables.sort();
        Err(NeedleError::UnusedEntries {
            tables: tables.join(", "),
        })
    }
}

/// Reads a YAML needles file and groups entries by `(schema, table)`.
///
/// # Blocking I/O
///
/// This function performs synchronous file I/O. It is called once at source
/// registration time with a small config file, so blocking a Tokio worker
/// thread briefly is acceptable.
///
/// # Errors
///
/// Returns [`NeedleError`] if the file cannot be read or parsed.
pub(crate) fn load_needles(path: &Path) -> Result<LoadedNeedles, NeedleError> {
    let contents = std::fs::read_to_string(path).map_err(|e| NeedleError::io(path, e))?;
    let entries: Vec<NeedleEntry> =
        serde_yaml::from_str(&contents).map_err(|e| NeedleError::Yaml(e.to_string()))?;

    let mut inner: HashMap<TableKey, Vec<serde_json::Value>> = HashMap::new();
    let mut specs = Vec::with_capacity(entries.len());
    for entry in entries {
        specs.push(NeedleSpec {
            schema: entry.schema.clone(),
            table: entry.table.clone(),
            column_values: entry.data.clone(),
        });
        inner
            .entry(TableKey {
                schema: entry.schema,
                table: entry.table,
            })
            .or_default()
            .push(serde_json::Value::Object(entry.data));
    }

    Ok(LoadedNeedles {
        groups: NeedleGroups { inner },
        specs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_returns_empty_groups() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(&path, "[]").unwrap();
        let groups = load_needles(&path).unwrap().groups;
        assert!(groups.is_empty());
    }

    #[test]
    fn valid_yaml_groups_by_schema_and_table() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  table: issues
  data:
    id: "needle-1"
    title: "test issue"
- schema: github
  table: issues
  data:
    id: "needle-2"
    title: "another issue"
- schema: slack
  table: messages
  data:
    text: "hello"
"#,
        )
        .unwrap();

        let groups = load_needles(&path).unwrap().groups;
        assert!(!groups.is_empty());
        assert_eq!(groups.get("github", "issues").unwrap().len(), 2);
        assert_eq!(groups.get("slack", "messages").unwrap().len(), 1);
        assert!(groups.get("github", "prs").is_none());
    }

    #[test]
    fn malformed_yaml_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(&path, "not: valid: yaml: [").unwrap();
        let result = load_needles(&path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("YAML"), "error should mention YAML: {err}");
    }

    #[test]
    fn missing_schema_field_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- table: issues
  data:
    id: "needle-1"
"#,
        )
        .unwrap();
        let result = load_needles(&path);
        assert!(result.is_err());
    }

    #[test]
    fn missing_table_field_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  data:
    id: "needle-1"
"#,
        )
        .unwrap();
        let result = load_needles(&path);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_field_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  tabel: issues
  data:
    id: "needle-1"
"#,
        )
        .unwrap();
        let result = load_needles(&path);
        assert!(result.is_err(), "typo'd field should be rejected");
    }

    #[test]
    fn missing_file_returns_io_error() {
        let result = load_needles(Path::new("/nonexistent/needles.yaml"));
        assert!(result.is_err());
    }

    #[test]
    fn take_removes_consumed_group() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  table: issues
  data:
    id: "needle-1"
"#,
        )
        .unwrap();

        let mut groups = load_needles(&path).unwrap().groups;
        assert_eq!(groups.take("github", "issues").unwrap().len(), 1);
        assert!(groups.get("github", "issues").is_none());
    }

    #[test]
    fn ensure_all_consumed_rejects_unused_entries() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  table: issues
  data:
    id: "needle-1"
"#,
        )
        .unwrap();

        let groups = load_needles(&path).unwrap().groups;
        let error = groups.ensure_all_consumed().unwrap_err();
        assert!(error.to_string().contains("github.issues"));
    }

    #[test]
    fn table_names_for_schema_returns_sorted_matches() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  table: pull_requests
  data:
    id: "needle-1"
- schema: github
  table: issues
  data:
    id: "needle-2"
- schema: slack
  table: messages
  data:
    id: "needle-3"
"#,
        )
        .unwrap();

        let groups = load_needles(&path).unwrap().groups;
        let mut github_tables = groups
            .inner
            .keys()
            .filter(|key| key.schema == "github")
            .map(|key| key.table.clone())
            .collect::<Vec<_>>();
        github_tables.sort();
        assert_eq!(
            github_tables,
            vec!["issues".to_string(), "pull_requests".to_string()]
        );
        assert!(
            groups.inner.keys().all(|key| key.schema != "linear"),
            "unexpected linear entries"
        );
    }
}
