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
usage_notes:
  - Service ownership notes apply to every graph slice.
  - text: Use ownership rows when a question asks who owns a service.
    labels: [Person, Service]
    relationships: [OWNS]
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
    assert_eq!(graph.usage_notes.len(), 2);
    assert_eq!(
        graph.usage_notes[1].text(),
        "Use ownership rows when a question asks who owns a service."
    );
}

#[test]
fn declaration_accepts_relationship_type_overloads_for_distinct_endpoints() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: ownership
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
relationships:
  - type: OWNS
    table: { schema: ops, name: person_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: team_ownerships }
    from: { label: Team, key: team_id }
    to: { label: Service, key: service_id }
",
    )
    .expect("relationship type overloads should parse");

    assert_eq!(graph.relationships_for_type("OWNS").count(), 2);
}

#[test]
fn declaration_rejects_duplicate_relationship_mapping_signatures() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: ownership
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships_a }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: ownerships_b }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
",
    )
    .expect_err("duplicate relationship mapping should fail");

    assert_invalid_graph_error(graph, "DUPLICATE_RELATIONSHIP_MAPPING");
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
fn declaration_rejects_usage_note_unknown_label() {
    let raw = VALID_GRAPH.replace("labels: [Person, Service]", "labels: [System]");
    let error = Declaration::from_yaml(&raw).expect_err("unknown usage-note label should fail");

    assert_invalid_graph_error(error, "UNKNOWN_NODE_LABEL");
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
