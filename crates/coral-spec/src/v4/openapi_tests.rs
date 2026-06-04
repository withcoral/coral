use std::collections::BTreeMap;

use super::*;
use crate::parse_source_manifest_yaml;

#[test]
fn extracts_openapi_document_metadata() {
    let metadata = openapi_document_metadata(
        r"
openapi: 3.0.3
info:
  title: Demo
  description: Query demo data.
servers:
  - url: https://api.example.com/v1
paths: {}
"
        .as_bytes(),
    )
    .expect("metadata");
    assert_eq!(metadata.description.as_deref(), Some("Query demo data."));
    assert_eq!(
        metadata.server_url.as_deref(),
        Some("https://api.example.com/v1")
    );
}

#[test]
fn extracts_openapi_server_url_with_variable_defaults() {
    let metadata = openapi_document_metadata(
        r"
openapi: 3.0.1
info:
  title: StatusGator
  version: v3
servers:
  - url: https://{defaultHost}/api/v3
    variables:
      defaultHost:
        default: statusgator.com
paths: {}
"
        .as_bytes(),
    )
    .expect("metadata");
    assert_eq!(
        metadata.server_url.as_deref(),
        Some("https://statusgator.com/api/v3")
    );
}
#[test]
fn importer_recognizes_common_wrapped_list_response_fields() {
    let manifest = parse_source_manifest_yaml(
        r"
name: statusgator
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /boards/{board_id}/incidents:
    get:
      operationId: listIncidents
      parameters:
        - {name: board_id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  success: {type: boolean}
                  data:
                    type: array
                    items: {$ref: '#/components/schemas/Incident'}
                  pagination:
                    type: object
components:
  schemas:
    Incident:
      type: object
      properties:
        id: {type: string}
        name: {type: string}
"
        .as_bytes(),
    )
    .expect("import");
    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::WrappedList);
    assert_eq!(operation.output.row_path, vec!["data".to_string()]);

    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "listincidents")
        .expect("projection");
    assert_eq!(projection.name, "incidents");
    assert!(matches!(projection.kind, ProjectionKind::Table));
}

#[test]
fn importer_recognizes_single_array_payload_wrappers() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /orgs/{org}/actions/permissions/repositories:
    get:
      operationId: actions/list-selected-repositories-enabled-github-actions-organization
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  total_count: {type: integer}
                  repositories:
                    type: array
                    items: {$ref: '#/components/schemas/Repository'}
components:
  schemas:
    Repository:
      type: object
      properties:
        id: {type: integer}
        name: {type: string}
"
        .as_bytes(),
    )
    .expect("import");
    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::WrappedList);
    assert_eq!(operation.output.row_path, vec!["repositories".to_string()]);

    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
    let projection = catalog.projections.first().expect("projection");
    assert_eq!(projection.name, "repositories");
    assert!(matches!(projection.kind, ProjectionKind::Table));
}

#[test]
fn importer_handles_recursive_schema_refs() {
    let manifest = parse_source_manifest_yaml(
        r"
name: trees
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /trees/{id}:
    get:
      operationId: trees/get
      parameters:
        - {name: id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/tree'}
components:
  schemas:
    tree:
      type: object
      properties:
        id: {type: string}
        children:
          type: array
          items: {$ref: '#/components/schemas/tree'}
"
        .as_bytes(),
    )
    .expect("recursive schema imports");
    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.type_ref, "tree");

    let types = ir
        .types
        .iter()
        .map(|ty| (ty.id.as_str(), ty))
        .collect::<BTreeMap<_, _>>();
    let tree = types.get("tree").expect("tree type");
    let IrTypeShape::Object { fields } = &tree.shape else {
        panic!("tree should import as an object: {:?}", tree.shape);
    };
    let fields = fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect::<BTreeMap<_, _>>();

    let id = fields.get("id").expect("id field");
    assert_eq!(id.type_ref, "tree_id");
    assert!(matches!(
        types.get(id.type_ref.as_str()).expect("id type").shape,
        IrTypeShape::Scalar(IrScalarType::String)
    ));

    let children = fields.get("children").expect("children field");
    let children_type = types
        .get(children.type_ref.as_str())
        .expect("children type");
    let IrTypeShape::List { item_type_ref } = &children_type.shape else {
        panic!(
            "children should import as a list type: {:?}",
            children_type.shape
        );
    };
    assert_eq!(item_type_ref, "tree");
}

#[test]
fn importer_preserves_ref_backed_property_descriptions() {
    let manifest = parse_source_manifest_yaml(
        r"
name: property_descriptions
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /items/{id}:
    get:
      operationId: items/get
      parameters:
        - {name: id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  status: {$ref: '#/components/schemas/status'}
components:
  schemas:
    status:
      type: string
      description: Current lifecycle status.
"
        .as_bytes(),
    )
    .expect("property descriptions import");
    let operation = ir.operations.first().expect("operation");
    let row_type = ir
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type");
    let IrTypeShape::Object { fields } = &row_type.shape else {
        panic!("row type imported as {:?}", row_type.shape);
    };
    let status = fields
        .iter()
        .find(|field| field.name == "status")
        .expect("status field");
    assert_eq!(status.description, "Current lifecycle status.");
}

#[test]
fn importer_resolves_referenced_response_objects() {
    let manifest = parse_source_manifest_yaml(
        r"
name: referenced_responses
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          $ref: '#/components/responses/IssueList'
components:
  responses:
    IssueList:
      content:
        application/json:
          schema:
            type: array
            items: {$ref: '#/components/schemas/Issue'}
  schemas:
    Issue:
      type: object
      properties:
        id: {type: integer}
        title: {type: string}
"
        .as_bytes(),
    )
    .expect("response ref imports");
    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::List);
    assert!(
        operation.diagnostics.is_empty(),
        "{:?}",
        operation.diagnostics
    );

    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "issues_list_for_repo")
        .expect("projection");
    assert_eq!(projection.name, "issues");
    assert_eq!(projection.visibility, ProjectionVisibility::Published);
    assert!(matches!(projection.kind, ProjectionKind::Table));
}

#[test]
fn importer_handles_2xx_response_range_success_codes() {
    let manifest = parse_source_manifest_yaml(
        r"
name: response_ranges
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /range-items:
    get:
      operationId: range/list
      responses:
        '2XX':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: string}
  /numeric-items:
    get:
      operationId: numeric/list
      responses:
        '201':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: string}
        '2XX':
          content:
            application/json:
              schema:
                type: object
                properties:
                  id: {type: string}
"
        .as_bytes(),
    )
    .expect("response range imports");
    let operations = ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();

    let range = operations.get("range_list").expect("range operation");
    assert_eq!(range.output.cardinality, OutputCardinality::List);
    let IrExecutionAttachment::Rest(range_rest) = &range.execution;
    assert_eq!(range_rest.response.status_code, 200);

    let numeric = operations.get("numeric_list").expect("numeric operation");
    assert_eq!(numeric.output.cardinality, OutputCardinality::List);
    let IrExecutionAttachment::Rest(numeric_rest) = &numeric.execution;
    assert_eq!(numeric_rest.response.status_code, 201);
}

#[test]
fn importer_preserves_non_string_schema_enum_values() {
    let manifest = parse_source_manifest_yaml(
        r"
name: enum_values
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /status:
    get:
      operationId: enum/get
      responses:
        '200':
          content:
            application/json:
              schema:
                enum:
                  - active
                  - 0
                  - true
                  - null
"
        .as_bytes(),
    )
    .expect("enum import");
    let operation = ir.operations.first().expect("operation");
    let ty = ir
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("enum type");
    let IrTypeShape::Enum { values } = &ty.shape else {
        panic!("enum imported as {:?}", ty.shape);
    };
    assert_eq!(values, &["active", "0", "true", "null"]);
}

#[test]
fn importer_respects_additional_properties_false() {
    let manifest = parse_source_manifest_yaml(
        r"
name: additional_properties
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /closed:
    get:
      operationId: schemas/closed
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/closed_object'}
  /map:
    get:
      operationId: schemas/map
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/string_map'}
components:
  schemas:
    closed_object:
      type: object
      additionalProperties: false
    string_map:
      type: object
      additionalProperties:
        type: string
"
        .as_bytes(),
    )
    .expect("additionalProperties import");

    let operations = ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();

    let closed_operation = operations
        .get("schemas_closed")
        .expect("closed object operation");
    let closed = ir
        .types
        .iter()
        .find(|ty| ty.id == closed_operation.output.type_ref)
        .expect("closed object row type");
    let IrTypeShape::Object { fields } = &closed.shape else {
        panic!("closed object imported as {:?}", closed.shape);
    };
    assert!(fields.is_empty());

    let map_operation = operations.get("schemas_map").expect("map operation");
    let map = ir
        .types
        .iter()
        .find(|ty| ty.id == map_operation.output.type_ref)
        .expect("map row type");
    let IrTypeShape::Map { value_type_ref } = &map.shape else {
        panic!(
            "schema-valued additionalProperties imported as {:?}",
            map.shape
        );
    };
    let value = ir
        .types
        .iter()
        .find(|ty| ty.id == value_type_ref.as_str())
        .expect("map value type");
    assert!(matches!(
        &value.shape,
        IrTypeShape::Scalar(IrScalarType::String)
    ));
}

#[test]
fn importer_warns_for_unresolved_response_object_refs() {
    let manifest = parse_source_manifest_yaml(
        r"
name: broken_responses
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /missing:
    get:
      operationId: missing/list
      responses:
        '200':
          $ref: '#/components/responses/Missing'
  /external:
    get:
      operationId: external/list
      responses:
        '200':
          $ref: 'https://example.com/openapi.yaml#/components/responses/Items'
"
        .as_bytes(),
    )
    .expect("broken response refs import with diagnostics");
    let codes = ir
        .operations
        .iter()
        .flat_map(|operation| operation.diagnostics.iter())
        .map(|diagnostic| diagnostic.code.as_str())
        .collect::<Vec<_>>();
    assert!(codes.contains(&"OPENAPI_REF_NOT_FOUND"), "{codes:?}");
    assert!(
        codes.contains(&"OPENAPI_EXTERNAL_REF_UNSUPPORTED"),
        "{codes:?}"
    );
    for operation in &ir.operations {
        assert_eq!(operation.output.cardinality, OutputCardinality::None);
    }
}

#[test]
fn importer_preserves_non_string_parameter_defaults() {
    let manifest = parse_source_manifest_yaml(
        r"
name: defaults
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
        - {name: archived, in: query, schema: {type: boolean, default: false}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: string}
"
        .as_bytes(),
    )
    .expect("defaults import");
    let operation = ir.operations.first().expect("operation");
    let defaults = operation
        .inputs
        .iter()
        .map(|input| (input.name.as_str(), input.default_value.as_deref()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(defaults.get("per_page"), Some(&Some("30")));
    assert_eq!(defaults.get("archived"), Some(&Some("false")));
}

#[test]
fn importer_warns_for_invalid_parameters_and_unresolved_responses() {
    let manifest = parse_source_manifest_yaml(
        r"
name: broken
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/missing'}
"
        .as_bytes(),
    )
    .expect("broken schema imports with diagnostics");
    let operation = ir.operations.first().expect("operation");
    let codes = operation
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.as_str())
        .collect::<Vec<_>>();
    assert!(codes.contains(&"OPENAPI_PARAMETER_INVALID"), "{codes:?}");
    assert!(
        codes.contains(&"OPENAPI_RESPONSE_SCHEMA_UNRESOLVED"),
        "{codes:?}"
    );
    assert_eq!(operation.output.cardinality, OutputCardinality::Unknown);
}
