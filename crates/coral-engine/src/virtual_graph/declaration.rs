//! Virtual graph declaration schema: the YAML-deserialized `Declaration`
//! (version, name, description, nodes, relationships) and its `Node` /
//! `Relationship` / `Endpoint` / `TableRef` mappings from graph labels and
//! relationship types onto `DataFusion` schema tables and key columns. Owns
//! `from_yaml` parsing, structural `validate`, `validate_against_catalog`, and
//! node-lookup accessors. This central type is extended by sibling modules — the
//! SQL `SqlRenderer` adds `lower_graph_*` and the validator adds `validate_graph_plan`.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::diagnostic::Diagnostic;
use super::diagnostic_codes;
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
                diagnostic_codes::UNSUPPORTED_VERSION,
                "version",
                "only virtual graph declaration version 1 is supported",
            )
            .into_core_error());
        }
        require_non_empty("name", &self.name)?;
        if self.nodes.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::EMPTY_NODES,
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
                    diagnostic_codes::DUPLICATE_NODE_LABEL,
                    format!("{path}.label"),
                    format!("node label '{}' is declared more than once", node.label),
                )
                .into_core_error());
            }
        }

        let mut relationship_mappings = BTreeSet::new();
        for (index, relationship) in self.relationships.iter().enumerate() {
            let path = format!("relationships[{index}]");
            relationship.validate(&path, &labels)?;
            if !relationship_mappings.insert((
                relationship.relationship_type.as_str(),
                relationship.from.label.as_str(),
                relationship.to.label.as_str(),
            )) {
                return Err(Diagnostic::new(
                    diagnostic_codes::DUPLICATE_RELATIONSHIP_MAPPING,
                    path,
                    format!(
                        "relationship mapping '{}: {} -> {}' is declared more than once",
                        relationship.relationship_type,
                        relationship.from.label,
                        relationship.to.label
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
                    diagnostic_codes::MAPPED_TABLE_NOT_FOUND,
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
                    diagnostic_codes::MAPPED_TABLE_NOT_FOUND,
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

    /// Returns relationship mappings for a type.
    pub(crate) fn relationships_for_type(
        &self,
        relationship_type: &str,
    ) -> impl Iterator<Item = &Relationship> {
        self.relationships
            .iter()
            .filter(move |relationship| relationship.relationship_type == relationship_type)
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
                diagnostic_codes::UNKNOWN_ENDPOINT_LABEL,
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
        return Err(Diagnostic::new(
            diagnostic_codes::EMPTY_FIELD,
            path,
            "field must not be empty",
        )
        .into_core_error());
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
            diagnostic_codes::MAPPED_TABLE_REQUIRES_FILTERS,
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
        diagnostic_codes::MAPPED_COLUMN_NOT_FOUND,
        path,
        format!(
            "mapped column '{}' was not found on table {}.{}",
            column, table.schema_name, table.table_name
        ),
    )
    .into_core_error())
}

#[path = "declaration_tests.rs"]
#[cfg(test)]
mod tests;
