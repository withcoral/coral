use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::diagnostic::Diagnostic;
use crate::{CatalogInfo, CoreError, TableInfo};

/// Versioned virtual graph declaration.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    /// Declaration schema version.
    pub version: u16,
    /// Stable graph name.
    pub name: String,
    /// Optional human-facing graph description.
    #[serde(default)]
    pub description: Option<String>,
    /// Node label mappings.
    #[serde(default)]
    pub nodes: Vec<Node>,
    /// Relationship type mappings.
    #[serde(default)]
    pub relationships: Vec<Relationship>,
}

/// SQL table reference used by a graph mapping.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    /// `DataFusion` schema name.
    pub schema: String,
    /// `DataFusion` table name.
    pub name: String,
}

/// Node label mapping.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// Graph node label.
    pub label: String,
    /// Source table backing the label.
    pub table: TableRef,
    /// Source table column used as the stable node key.
    pub key: String,
    /// Exposed graph property to source column mappings.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// Relationship type mapping.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Relationship {
    /// Graph relationship type.
    #[serde(rename = "type")]
    pub relationship_type: String,
    /// Source table backing the relationship.
    pub table: TableRef,
    /// Optional source table column used as a stable relationship key.
    #[serde(default)]
    pub key: Option<String>,
    /// Relationship endpoint mapped to the source table's from-key column.
    pub from: Endpoint,
    /// Relationship endpoint mapped to the source table's to-key column.
    pub to: Endpoint,
    /// Exposed graph property to source column mappings.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// Relationship endpoint mapping.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    /// Node label at this endpoint.
    pub label: String,
    /// Relationship table column that joins to the endpoint node key.
    pub key: String,
}

impl Declaration {
    /// Parses and validates one virtual graph declaration from YAML.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] when the YAML cannot be parsed or
    /// the declaration violates the v1 mapping contract.
    pub fn from_yaml(raw: &str) -> Result<Self, CoreError> {
        let declaration = serde_yaml::from_str::<Self>(raw).map_err(|error| {
            CoreError::InvalidInput(format!("virtual graph YAML could not be parsed: {error}"))
        })?;
        declaration.validate()?;
        Ok(declaration)
    }

    /// Validates the declaration against the v1 mapping contract.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] with a path-qualified diagnostic
    /// when the declaration is invalid.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.version != 1 {
            return Err(Diagnostic::new(
                "UNSUPPORTED_VERSION",
                "version",
                "only virtual graph declaration version 1 is supported",
            )
            .into_core_error());
        }
        require_non_empty("name", &self.name)?;
        if self.nodes.is_empty() {
            return Err(Diagnostic::new(
                "EMPTY_NODES",
                "nodes",
                "at least one node mapping is required",
            )
            .into_core_error());
        }

        let mut labels = BTreeSet::new();
        for (index, node) in self.nodes.iter().enumerate() {
            let path = format!("nodes[{index}]");
            node.validate(&path)?;
            if !labels.insert(node.label.as_str()) {
                return Err(Diagnostic::new(
                    "DUPLICATE_NODE_LABEL",
                    format!("{path}.label"),
                    format!("node label '{}' is declared more than once", node.label),
                )
                .into_core_error());
            }
        }

        let mut relationship_types = BTreeSet::new();
        for (index, relationship) in self.relationships.iter().enumerate() {
            let path = format!("relationships[{index}]");
            relationship.validate(&path, &labels)?;
            if !relationship_types.insert(relationship.relationship_type.as_str()) {
                return Err(Diagnostic::new(
                    "DUPLICATE_RELATIONSHIP_TYPE",
                    format!("{path}.type"),
                    format!(
                        "relationship type '{}' is declared more than once",
                        relationship.relationship_type
                    ),
                )
                .into_core_error());
            }
        }

        Ok(())
    }

    /// Validates this declaration against a query runtime catalog snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] when a mapped table or column is
    /// missing, or when a mapped table requires filters that virtual graph
    /// scans cannot currently supply.
    pub fn validate_against_catalog(&self, catalog: &CatalogInfo) -> Result<(), CoreError> {
        self.validate()?;
        for (index, node) in self.nodes.iter().enumerate() {
            let path = format!("nodes[{index}]");
            let table = find_table(catalog, &node.table).ok_or_else(|| {
                Diagnostic::new(
                    "MAPPED_TABLE_NOT_FOUND",
                    format!("{path}.table"),
                    format!(
                        "node label '{}' maps to missing table {}.{}",
                        node.label, node.table.schema, node.table.name
                    ),
                )
                .into_core_error()
            })?;
            validate_table_scan_supported(table, &format!("{path}.table"))?;
            validate_column(table, &node.key, &format!("{path}.key"))?;
            for (property, column) in &node.properties {
                validate_column(table, column, &format!("{path}.properties.{property}"))?;
            }
        }

        for (index, relationship) in self.relationships.iter().enumerate() {
            let path = format!("relationships[{index}]");
            let table = find_table(catalog, &relationship.table).ok_or_else(|| {
                Diagnostic::new(
                    "MAPPED_TABLE_NOT_FOUND",
                    format!("{path}.table"),
                    format!(
                        "relationship type '{}' maps to missing table {}.{}",
                        relationship.relationship_type,
                        relationship.table.schema,
                        relationship.table.name
                    ),
                )
                .into_core_error()
            })?;
            validate_table_scan_supported(table, &format!("{path}.table"))?;
            if let Some(key) = &relationship.key {
                validate_column(table, key, &format!("{path}.key"))?;
            }
            validate_column(table, &relationship.from.key, &format!("{path}.from.key"))?;
            validate_column(table, &relationship.to.key, &format!("{path}.to.key"))?;
            for (property, column) in &relationship.properties {
                validate_column(table, column, &format!("{path}.properties.{property}"))?;
            }
        }

        Ok(())
    }

    /// Returns the node mapping for a label.
    #[must_use]
    pub fn node(&self, label: &str) -> Option<&Node> {
        self.nodes.iter().find(|node| node.label == label)
    }

    /// Returns the relationship mapping for a type.
    #[must_use]
    pub fn relationship(&self, relationship_type: &str) -> Option<&Relationship> {
        self.relationships
            .iter()
            .find(|relationship| relationship.relationship_type == relationship_type)
    }
}

impl Node {
    fn validate(&self, path: &str) -> Result<(), CoreError> {
        require_non_empty(format!("{path}.label"), &self.label)?;
        self.table.validate(&format!("{path}.table"))?;
        require_non_empty(format!("{path}.key"), &self.key)?;
        validate_properties(&format!("{path}.properties"), &self.properties)
    }

    pub(crate) fn column_for_property(&self, property: &str) -> Option<&str> {
        self.properties
            .get(property)
            .map(String::as_str)
            .or_else(|| (property == self.key).then_some(self.key.as_str()))
    }
}

impl Relationship {
    fn validate(&self, path: &str, labels: &BTreeSet<&str>) -> Result<(), CoreError> {
        require_non_empty(format!("{path}.type"), &self.relationship_type)?;
        self.table.validate(&format!("{path}.table"))?;
        if let Some(key) = &self.key {
            require_non_empty(format!("{path}.key"), key)?;
        }
        self.from.validate(&format!("{path}.from"), labels)?;
        self.to.validate(&format!("{path}.to"), labels)?;
        validate_properties(&format!("{path}.properties"), &self.properties)
    }

    pub(crate) fn column_for_property(&self, property: &str) -> Option<&str> {
        if self.key.as_deref() == Some(property) {
            return self.key.as_deref();
        }
        self.properties.get(property).map(String::as_str)
    }
}

impl TableRef {
    fn validate(&self, path: &str) -> Result<(), CoreError> {
        require_non_empty(format!("{path}.schema"), &self.schema)?;
        require_non_empty(format!("{path}.name"), &self.name)
    }
}

impl Endpoint {
    fn validate(&self, path: &str, labels: &BTreeSet<&str>) -> Result<(), CoreError> {
        require_non_empty(format!("{path}.label"), &self.label)?;
        require_non_empty(format!("{path}.key"), &self.key)?;
        if !labels.contains(self.label.as_str()) {
            return Err(Diagnostic::new(
                "UNKNOWN_ENDPOINT_LABEL",
                format!("{path}.label"),
                format!(
                    "relationship endpoint references unknown node label '{}'",
                    self.label
                ),
            )
            .into_core_error());
        }
        Ok(())
    }
}

fn validate_properties(path: &str, properties: &BTreeMap<String, String>) -> Result<(), CoreError> {
    for (property, column) in properties {
        require_non_empty(format!("{path}.{property}"), property)?;
        require_non_empty(format!("{path}.{property}"), column)?;
    }
    Ok(())
}

fn require_non_empty(path: impl Into<String>, value: &str) -> Result<(), CoreError> {
    let path = path.into();
    if value.trim().is_empty() {
        return Err(
            Diagnostic::new("EMPTY_FIELD", path, "field must not be empty").into_core_error(),
        );
    }
    Ok(())
}

fn find_table<'a>(catalog: &'a CatalogInfo, table_ref: &TableRef) -> Option<&'a TableInfo> {
    catalog
        .tables
        .iter()
        .find(|table| table.schema_name == table_ref.schema && table.table_name == table_ref.name)
}

fn validate_table_scan_supported(table: &TableInfo, path: &str) -> Result<(), CoreError> {
    if !table.required_filters.is_empty() {
        return Err(Diagnostic::new(
            "MAPPED_TABLE_REQUIRES_FILTERS",
            path,
            format!(
                "table {}.{} requires filters [{}], which virtual graph scans do not support yet",
                table.schema_name,
                table.table_name,
                table.required_filters.join(", ")
            ),
        )
        .into_core_error());
    }
    Ok(())
}

fn validate_column(table: &TableInfo, column: &str, path: &str) -> Result<(), CoreError> {
    if table
        .columns
        .iter()
        .any(|candidate| candidate.name == column)
    {
        return Ok(());
    }
    Err(Diagnostic::new(
        "MAPPED_COLUMN_NOT_FOUND",
        path,
        format!(
            "mapped column '{}' was not found on table {}.{}",
            column, table.schema_name, table.table_name
        ),
    )
    .into_core_error())
}

#[cfg(test)]
mod tests {
    use super::Declaration;
    use crate::{CatalogInfo, ColumnInfo, CoreError, StatusCode, TableInfo};

    const VALID_GRAPH: &str = r"
version: 1
name: ownership
description: Service ownership graph
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      team: team
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
";

    #[test]
    fn declaration_from_yaml_accepts_valid_v1_mapping() {
        let graph = Declaration::from_yaml(VALID_GRAPH).expect("graph should parse");

        assert_eq!(graph.name, "ownership");
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.relationships.len(), 1);
        assert_eq!(
            graph
                .relationships
                .first()
                .and_then(|rel| rel.key.as_deref()),
            Some("ownership_id")
        );
        assert_eq!(
            graph
                .node("Person")
                .and_then(|node| node.column_for_property("name")),
            Some("full_name")
        );
    }

    #[test]
    fn declaration_rejects_duplicate_node_labels() {
        let raw = VALID_GRAPH.replace("label: Service", "label: Person");
        let error = Declaration::from_yaml(&raw).expect_err("duplicate label should fail");

        assert_invalid_graph_error(error, "DUPLICATE_NODE_LABEL");
    }

    #[test]
    fn declaration_rejects_unknown_relationship_endpoint_label() {
        let raw = VALID_GRAPH.replace(
            "label: Service, key: service_id",
            "label: System, key: service_id",
        );
        let error = Declaration::from_yaml(&raw).expect_err("unknown endpoint should fail");

        assert_invalid_graph_error(error, "UNKNOWN_ENDPOINT_LABEL");
    }

    #[test]
    fn declaration_rejects_empty_relationship_keys() {
        let raw = VALID_GRAPH.replace("key: ownership_id", "key: ''");
        let error = Declaration::from_yaml(&raw).expect_err("empty relationship key should fail");

        assert_invalid_graph_error(error, "EMPTY_FIELD");
    }

    #[test]
    fn declaration_rejects_unsupported_versions() {
        let raw = VALID_GRAPH.replace("version: 1", "version: 2");
        let error = Declaration::from_yaml(&raw).expect_err("unsupported version should fail");

        assert_invalid_graph_error(error, "UNSUPPORTED_VERSION");
    }

    #[test]
    fn declaration_rejects_unknown_yaml_fields() {
        let raw = VALID_GRAPH.replace("name: ownership", "name: ownership\nunknown: true");
        let error = Declaration::from_yaml(&raw).expect_err("unknown field should fail");

        assert_invalid_graph_error(error, "unknown field");
    }

    #[test]
    fn node_explicit_property_mapping_takes_precedence_over_key_fallback() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: explicit-key-property
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      id: external_id
relationships: []
",
        )
        .expect("graph should parse");

        assert_eq!(
            graph
                .node("Person")
                .and_then(|node| node.column_for_property("id")),
            Some("external_id")
        );
    }

    #[test]
    fn declaration_catalog_validation_accepts_mapped_tables_and_columns() {
        let graph = Declaration::from_yaml(VALID_GRAPH).expect("graph should parse");

        graph
            .validate_against_catalog(&ownership_catalog())
            .expect("catalog should satisfy graph declaration");
    }

    #[test]
    fn declaration_catalog_validation_rejects_missing_columns() {
        let graph = Declaration::from_yaml(VALID_GRAPH).expect("graph should parse");
        let mut catalog = ownership_catalog();
        let people = catalog
            .tables
            .iter_mut()
            .find(|table| table.table_name == "people")
            .expect("people table should exist");
        people.columns.retain(|column| column.name != "full_name");

        let error = graph
            .validate_against_catalog(&catalog)
            .expect_err("missing property column should fail");

        assert_invalid_graph_error(error, "MAPPED_COLUMN_NOT_FOUND");
    }

    #[test]
    fn declaration_catalog_validation_rejects_missing_relationship_key_columns() {
        let graph = Declaration::from_yaml(VALID_GRAPH).expect("graph should parse");
        let mut catalog = ownership_catalog();
        let ownerships = catalog
            .tables
            .iter_mut()
            .find(|table| table.table_name == "ownerships")
            .expect("ownerships table should exist");
        ownerships
            .columns
            .retain(|column| column.name != "ownership_id");

        let error = graph
            .validate_against_catalog(&catalog)
            .expect_err("missing relationship key column should fail");

        assert_invalid_graph_error(error, "MAPPED_COLUMN_NOT_FOUND");
    }

    #[test]
    fn declaration_catalog_validation_rejects_required_filter_tables() {
        let graph = Declaration::from_yaml(VALID_GRAPH).expect("graph should parse");
        let mut catalog = ownership_catalog();
        let people = catalog
            .tables
            .iter_mut()
            .find(|table| table.table_name == "people")
            .expect("people table should exist");
        people.required_filters.push("tenant_id".to_string());

        let error = graph
            .validate_against_catalog(&catalog)
            .expect_err("required filter table should fail");

        assert_invalid_graph_error(error, "MAPPED_TABLE_REQUIRES_FILTERS");
    }

    fn assert_invalid_graph_error(error: CoreError, expected_code: &str) {
        assert_eq!(error.status_code(), StatusCode::InvalidArgument);
        match error {
            CoreError::InvalidInput(detail) => {
                assert!(
                    detail.contains(expected_code),
                    "expected {expected_code} in {detail}"
                );
            }
            other => panic!("expected invalid input, got {other:?}"),
        }
    }

    fn ownership_catalog() -> CatalogInfo {
        CatalogInfo {
            tables: vec![
                table("ops", "people", &["id", "full_name", "team"]),
                table("ops", "services", &["id", "service_name"]),
                table(
                    "ops",
                    "ownerships",
                    &["ownership_id", "person_id", "service_id", "since"],
                ),
            ],
            table_functions: Vec::new(),
        }
    }

    fn table(schema: &str, name: &str, columns: &[&str]) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            table_name: name.to_string(),
            description: String::new(),
            guide: String::new(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(position, column)| ColumnInfo {
                    name: (*column).to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                    description: String::new(),
                    ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
                })
                .collect(),
            required_filters: Vec::new(),
        }
    }
}
