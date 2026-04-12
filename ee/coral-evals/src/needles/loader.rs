//! Reads and groups a YAML needles file into per-table row collections.

use std::collections::HashMap;
use std::path::Path;

use super::error::NeedleError;

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NeedleEntry {
    schema: String,
    table: String,
    data: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TableKey {
    pub(crate) schema: String,
    pub(crate) table: String,
}

#[derive(Debug, Default)]
pub(crate) struct NeedleGroups {
    inner: HashMap<TableKey, Vec<serde_json::Value>>,
}

impl NeedleGroups {
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn take(&mut self, schema: &str, table: &str) -> Option<Vec<serde_json::Value>> {
        let key = TableKey {
            schema: schema.to_string(),
            table: table.to_string(),
        };
        self.inner.remove(&key)
    }

    pub(crate) fn ensure_all_consumed(self) -> Result<(), NeedleError> {
        if self.inner.is_empty() {
            return Ok(());
        }

        let mut tables = self
            .inner
            .into_keys()
            .map(|key| format!("{}.{}", key.schema, key.table))
            .collect::<Vec<_>>();
        tables.sort();
        Err(NeedleError::UnusedEntries {
            tables: tables.join(", "),
        })
    }
}

pub(crate) fn load_needle_groups(path: &Path) -> Result<NeedleGroups, NeedleError> {
    let contents = std::fs::read_to_string(path).map_err(|error| NeedleError::io(path, error))?;
    let entries: Vec<NeedleEntry> =
        serde_yaml::from_str(&contents).map_err(|error| NeedleError::Yaml(error.to_string()))?;

    let mut inner = HashMap::new();
    for entry in entries {
        inner
            .entry(TableKey {
                schema: entry.schema,
                table: entry.table,
            })
            .or_insert_with(Vec::new)
            .push(serde_json::Value::Object(entry.data));
    }

    Ok(NeedleGroups { inner })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_returns_empty_groups() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("needles.yaml");
        std::fs::write(&path, "[]").expect("write empty file");
        let groups = load_needle_groups(&path).expect("load groups");
        assert!(groups.is_empty());
    }

    #[test]
    fn valid_yaml_groups_by_schema_and_table() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("needles.yaml");
        std::fs::write(
            &path,
            r#"
- schema: github
  table: issues
  data:
    id: "needle-1"
- schema: github
  table: issues
  data:
    id: "needle-2"
"#,
        )
        .expect("write valid yaml");

        let mut groups = load_needle_groups(&path).expect("load groups");
        assert_eq!(
            groups
                .take("github", "issues")
                .expect("github issues")
                .len(),
            2
        );
        assert!(groups.take("github", "issues").is_none());
    }

    #[test]
    fn malformed_yaml_returns_error() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("needles.yaml");
        std::fs::write(&path, "not: valid: yaml: [").expect("write malformed yaml");
        let error = load_needle_groups(&path).expect_err("malformed yaml should fail");
        assert!(error.to_string().contains("failed to parse needles YAML"));
    }
}
