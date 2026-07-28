use std::collections::BTreeMap;

use super::*;
use crate::{
    ManifestDataType, PaginationMode, PaginationSpec, SourceTableFunctionKind,
    parse_source_manifest_yaml,
};

fn imported_rest_pagination<'a>(
    surface: &'a ImportedSurface,
    operation_id: &str,
) -> &'a PaginationSpec {
    match surface
        .operation_metadata
        .operations
        .get(operation_id)
        .expect("operation metadata")
    {
        OperationMetadata::Rest { pagination, .. } => pagination,
        OperationMetadata::Mcp { .. } => panic!("expected REST metadata"),
    }
}

fn imported_row_path<'a>(surface: &'a ImportedSurface, operation_id: &str) -> &'a [String] {
    surface
        .operation_metadata
        .operations
        .get(operation_id)
        .expect("operation metadata")
        .row_path()
}

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
fn importer_resolves_local_openapi_operation_refs() {
    let manifest = parse_source_manifest_yaml(
        r"
name: local_ref_test
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /items:
    get:
      $ref: '#/x-operations/listItems'
x-operations:
  listItems:
    operationId: items/list
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
    .expect("local operation ref import");

    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.id, "items_list");
    assert_eq!(operation.output.cardinality, OutputCardinality::List);
    assert!(
        ir.diagnostics.is_empty(),
        "local operation ref should not produce diagnostics: {:?}",
        ir.diagnostics
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The OpenAPI fixture keeps related naming metadata cases together."
)]
fn importer_preserves_openapi_operation_naming_metadata() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /orgs/{org}/settings/billing/ai-credit-usage:
    get:
      tags: ['', 'billing', 'ignored']
      operationId: billing/get-github-billing-ai-credit-usage-report-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Usage'}
  /quotes:
    get:
      tags: ['forex', 'finance', 'quotes']
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Quote'}
  /items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Item'}
  /fallback:
    get:
      tags: ['misc']
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Item'}
components:
  schemas:
    Usage:
      type: object
      properties:
        total: {type: integer}
    Quote:
      type: object
      properties:
        symbol: {type: string}
    Item:
      type: object
      properties:
        id: {type: string}
"
        .as_bytes(),
    )
    .expect("import");

    let operations = ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation.naming.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let billing = operations
        .get("billing_get_github_billing_ai_credit_usage_report_org")
        .and_then(|naming| *naming)
        .expect("billing naming metadata");
    assert_eq!(billing.group.as_deref(), Some("billing"));
    assert_eq!(
        billing.operation.as_deref(),
        Some("get_github_billing_ai_credit_usage_report_org")
    );

    let quotes = operations
        .get("get_quotes")
        .and_then(|naming| *naming)
        .expect("quotes naming metadata");
    assert_eq!(quotes.group.as_deref(), Some("forex"));
    assert_eq!(quotes.operation.as_deref(), Some("get_quotes"));

    let items = operations
        .get("items_list")
        .and_then(|naming| *naming)
        .expect("items naming metadata");
    assert_eq!(items.group.as_deref(), None);
    assert_eq!(items.operation.as_deref(), Some("list"));

    let fallback = operations
        .get("get_fallback")
        .and_then(|naming| *naming)
        .expect("fallback naming metadata");
    assert_eq!(fallback.group.as_deref(), Some("misc"));
    assert_eq!(fallback.operation.as_deref(), Some("get_fallback"));

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let quotes_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "get_quotes")
        .expect("quotes projection");
    assert_eq!(quotes_projection.name, "forex_get_quotes");
    assert!(matches!(quotes_projection.kind, ProjectionKind::Table));
}

#[test]
fn importer_infers_row_paths_for_common_envelope_objects() {
    let manifest = parse_source_manifest_yaml(
        r"
name: statusgator
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
    // The IR keeps the declared envelope; only the metadata says where its rows
    // are.
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    assert_eq!(imported_row_path(&ir, "listincidents"), ["data"]);

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "listincidents")
        .expect("projection");
    let columns = projection
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.data_type))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(columns.get("id"), Some(&ManifestDataType::Utf8));
    assert_eq!(columns.get("name"), Some(&ManifestDataType::Utf8));
    assert_eq!(columns.len(), 2);
    assert!(matches!(
        projection.kind,
        ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Table
        }
    ));
}

#[test]
fn importer_infers_row_path_for_a_sole_array_payload_beside_a_total() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    assert_eq!(
        imported_row_path(
            &ir,
            "actions_list_selected_repositories_enabled_github_actions_organization"
        ),
        ["repositories"]
    );

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog.projections.first().expect("projection");
    let columns = projection
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.data_type))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(columns.get("id"), Some(&ManifestDataType::Int64));
    assert_eq!(columns.get("name"), Some(&ManifestDataType::Utf8));
    assert_eq!(columns.len(), 2);
    assert!(matches!(
        projection.kind,
        ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Table
        }
    ));
}

#[test]
fn importer_prefers_a_named_row_property_over_other_arrays() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /search/issues:
    get:
      operationId: search/issues-and-pull-requests
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                required: [total_count, incomplete_results, items, search_type]
                properties:
                  total_count: {type: integer}
                  incomplete_results: {type: boolean}
                  items:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: integer}
                        number: {type: integer}
                        title: {type: string}
                        state: {type: string}
                  search_type:
                    type: string
                    enum: [lexical, semantic, hybrid]
                  lexical_fallback_reason:
                    type: array
                    items:
                      type: string
                      enum: [no_text_terms, quoted_text]
"
        .as_bytes(),
    )
    .expect("import");

    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    // `lexical_fallback_reason` is an array too; the conventional row name wins.
    assert_eq!(
        imported_row_path(&ir, "search_issues_and_pull_requests"),
        ["items"]
    );

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog.projections.first().expect("projection");
    let columns = projection
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.data_type))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(columns.get("id"), Some(&ManifestDataType::Int64));
    assert_eq!(columns.get("number"), Some(&ManifestDataType::Int64));
    assert_eq!(columns.get("title"), Some(&ManifestDataType::Utf8));
    assert_eq!(columns.get("state"), Some(&ManifestDataType::Utf8));
    assert_eq!(columns.len(), 4);
    assert!(!columns.contains_key("total_count"));
    assert!(!columns.contains_key("lexical_fallback_reason"));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The fixture mirrors the resource object shape that regressed."
)]
fn importer_keeps_resource_objects_with_array_fields_as_singletons() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /orgs/{org}/projectsV2/{project_number}/items/{item_id}:
    get:
      operationId: projects/get-org-item
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
        - {name: project_number, in: path, required: true, schema: {type: integer}}
        - {name: item_id, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/ProjectItemWithContent'}
components:
  schemas:
    ProjectItemWithContent:
      type: object
      properties:
        id:
          type: number
          description: The unique identifier of the project item.
        node_id:
          type: string
          description: The node ID of the project item.
        project_url:
          type: string
          format: uri
          description: The API URL of the project that contains this item.
        content_type:
          type: string
          enum: [Issue, PullRequest, DraftIssue, Redacted]
          description: The type of content tracked in a project item.
        content:
          type: object
          additionalProperties: true
          nullable: true
          description: The content of the item, which varies by content type.
        creator:
          type: object
          properties:
            login: {type: string}
          description: A GitHub user.
        created_at:
          type: string
          format: date-time
          description: The time when the item was created.
        updated_at:
          type: string
          format: date-time
          description: The time when the item was last updated.
        archived_at:
          type: string
          format: date-time
          nullable: true
          description: The time when the item was archived.
        item_url:
          type: string
          format: uri
          nullable: true
          description: The API URL of this item.
        fields:
          type: array
          items:
            type: object
            additionalProperties: true
          description: The fields and values associated with this item.
      required: [id, content_type, created_at, updated_at, archived_at]
"
        .as_bytes(),
    )
    .expect("import");

    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    // A project item is a resource, not a page of its `fields`: nothing in the
    // response or the request says otherwise.
    assert!(imported_row_path(&ir, "projects_get_org_item").is_empty());

    let row_type = ir
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type");
    let IrTypeShape::Object { fields } = &row_type.shape else {
        panic!("row type imported as {:?}", row_type.shape);
    };
    assert_eq!(fields.len(), 11);
    assert!(fields.iter().any(|field| field.name == "fields"));

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog.projections.first().expect("projection");
    let column_types = projection
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.data_type))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(projection.columns.len(), 11);
    assert_eq!(column_types.get("id"), Some(&ManifestDataType::Float64));
    assert_eq!(
        column_types.get("content_type"),
        Some(&ManifestDataType::Utf8)
    );
    assert_eq!(
        column_types.get("created_at"),
        Some(&ManifestDataType::Timestamp)
    );
    assert_eq!(column_types.get("fields"), Some(&ManifestDataType::Json));
}

#[test]
fn importer_keeps_named_array_fields_on_resource_objects_as_singletons() {
    let manifest = parse_source_manifest_yaml(
        r"
name: resources
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /bundles/{bundle_id}:
    get:
      operationId: bundles/get
      parameters:
        - {name: bundle_id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  id: {type: string}
                  items:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
"
        .as_bytes(),
    )
    .expect("import");

    let operation = ir.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    // A bundle's `items` is a conventional row name, but a bundle is still a
    // resource: no metadata sibling, no pagination parameter.
    assert!(imported_row_path(&ir, "bundles_get").is_empty());
}

#[test]
fn importer_handles_recursive_schema_refs() {
    let manifest = parse_source_manifest_yaml(
        r"
name: trees
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);

    let types = ir
        .types
        .iter()
        .map(|ty| (ty.id.as_str(), ty))
        .collect::<BTreeMap<_, _>>();
    let tree = types
        .get(operation.output.type_ref.as_str())
        .expect("tree row type");
    let IrTypeShape::Object { fields } = &tree.shape else {
        panic!("tree should import as an object: {:?}", tree.shape);
    };
    let fields = fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect::<BTreeMap<_, _>>();

    let id = fields.get("id").expect("id field");
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "issues_list_for_repo")
        .expect("projection");
    assert_eq!(projection.name, "issue");
    assert_eq!(projection.visibility, ProjectionVisibility::Published);
    assert!(matches!(
        projection.kind,
        ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Table
        }
    ));
}

#[test]
fn importer_handles_2xx_response_range_success_codes() {
    let manifest = parse_source_manifest_yaml(
        r"
name: response_ranges
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
    let IrExecutionAttachment::Rest(range_rest) = &range.execution else {
        panic!("range operation should be REST");
    };
    assert_eq!(range_rest.response.status_code, 200);

    let numeric = operations.get("numeric_list").expect("numeric operation");
    assert_eq!(numeric.output.cardinality, OutputCardinality::List);
    let IrExecutionAttachment::Rest(numeric_rest) = &numeric.execution else {
        panic!("numeric operation should be REST");
    };
    assert_eq!(numeric_rest.response.status_code, 201);
}

#[test]
fn importer_preserves_non_string_schema_enum_values() {
    let manifest = parse_source_manifest_yaml(
        r"
name: enum_values
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
    let messages = ir
        .operations
        .iter()
        .flat_map(|operation| operation.diagnostics.iter())
        .map(|diagnostic| diagnostic.message.as_str())
        .collect::<Vec<_>>();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("was not found")),
        "{messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("external reference")),
        "{messages:?}"
    );
    for operation in &ir.operations {
        assert_eq!(operation.output.cardinality, OutputCardinality::None);
    }
}

#[test]
fn importer_warns_for_openapi_all_of_property_conflicts() {
    let manifest = parse_source_manifest_yaml(
        r"
name: conflicting_all_of
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Combined'}
components:
  schemas:
    Combined:
      allOf:
        - type: object
          properties:
            id: {type: string}
        - type: object
          properties:
            id: {type: integer}
"
        .as_bytes(),
    )
    .expect("conflicting allOf imports with diagnostics");

    let operation = ir.operations.first().expect("operation");
    assert!(
        operation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("allOf property")),
        "{:?}",
        operation.diagnostics
    );
    assert_eq!(operation.output.type_ref, "json");
}

#[test]
fn importer_preserves_non_string_parameter_defaults() {
    let manifest = parse_source_manifest_yaml(
        r"
name: defaults
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
#[expect(
    clippy::too_many_lines,
    reason = "The OpenAPI fixture keeps common pagination aliases together."
)]
fn importer_infers_common_query_pagination_modes() {
    let manifest = parse_source_manifest_yaml(
        r"
name: pagination
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /paged:
    get:
      operationId: paged/list
      parameters:
        - {name: pageNumber, in: query, schema: {type: integer, default: 2}}
        - {name: pageSize, in: query, schema: {type: integer, default: 25}}
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
  /offset:
    get:
      operationId: offset/list
      parameters:
        - {name: offset, in: query, schema: {type: integer, default: 5}}
        - {name: limit, in: query, schema: {type: integer, default: 50}}
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
  /skip-take:
    get:
      operationId: skip-take/list
      parameters:
        - {name: skip, in: query, schema: {type: integer, default: 5}}
        - {name: take, in: query, schema: {type: integer, default: 50}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /odata:
    get:
      operationId: odata/list
      parameters:
        - {name: $skip, in: query, schema: {type: integer, default: 10}}
        - {name: $top, in: query, schema: {type: integer, default: 25}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /current-page:
    get:
      operationId: current-page/list
      parameters:
        - {name: current_page, in: query, schema: {type: integer, default: 2}}
        - {name: items_per_page, in: query, schema: {type: integer, default: 30}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /page-index:
    get:
      operationId: page-index/list
      parameters:
        - {name: pageIndex, in: query, schema: {type: integer, default: 3}}
        - {name: size, in: query, schema: {type: integer, default: 40}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /dotted-offset:
    get:
      operationId: dotted-offset/list
      parameters:
        - {name: page.offset, in: query, schema: {type: integer, default: 15}}
        - {name: page.limit, in: query, schema: {type: integer, default: 60}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /offset-count:
    get:
      operationId: offset-count/list
      parameters:
        - {name: offsetIndex, in: query, schema: {type: integer, default: 20}}
        - {name: count, in: query, schema: {type: integer, default: 70}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
"
        .as_bytes(),
    )
    .expect("pagination import");
    let page = imported_rest_pagination(&ir, "paged_list");
    assert_eq!(page.mode, PaginationMode::Page);
    assert_eq!(page.page_param.as_deref(), Some("pageNumber"));
    assert_eq!(page.page_start, 2);
    let page_size = page.page_size.as_ref().expect("page size");
    assert_eq!(page_size.default, 25);
    assert_eq!(page_size.max, 100);
    assert_eq!(page_size.query_param.as_deref(), Some("pageSize"));

    let offset = imported_rest_pagination(&ir, "offset_list");
    assert_eq!(offset.mode, PaginationMode::Offset);
    assert_eq!(offset.offset_param.as_deref(), Some("offset"));
    assert_eq!(offset.offset_start, 5);
    assert_eq!(offset.offset_step, None);
    let page_size = offset.page_size.as_ref().expect("limit page size");
    assert_eq!(page_size.default, 50);
    assert_eq!(page_size.max, 100);
    assert_eq!(page_size.query_param.as_deref(), Some("limit"));

    for (operation_id, offset_param, size_param, start) in [
        ("skip_take_list", "skip", "take", 5),
        ("odata_list", "$skip", "$top", 10),
        ("dotted_offset_list", "page.offset", "page.limit", 15),
        ("offset_count_list", "offsetIndex", "count", 20),
    ] {
        let pagination = imported_rest_pagination(&ir, operation_id);
        assert_eq!(pagination.mode, PaginationMode::Offset);
        assert_eq!(pagination.offset_param.as_deref(), Some(offset_param));
        assert_eq!(pagination.offset_start, start);
        assert_eq!(
            pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
            Some(size_param)
        );
    }
    for (operation_id, page_param, size_param, start) in [
        ("current_page_list", "current_page", "items_per_page", 2),
        ("page_index_list", "pageIndex", "size", 3),
    ] {
        let pagination = imported_rest_pagination(&ir, operation_id);
        assert_eq!(pagination.mode, PaginationMode::Page);
        assert_eq!(pagination.page_param.as_deref(), Some(page_param));
        assert_eq!(pagination.page_start, start);
        assert_eq!(
            pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
            Some(size_param)
        );
    }
}

#[test]
fn importer_skips_pagination_params_with_non_numeric_types() {
    let manifest = parse_source_manifest_yaml(
        r"
name: string_pagination
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /string-page:
    get:
      operationId: string-page/list
      parameters:
        - {name: page, in: query, schema: {type: string, default: '1'}}
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /string-offset:
    get:
      operationId: string-offset/list
      parameters:
        - {name: offset, in: query, schema: {type: string}}
        - {name: limit, in: query, schema: {type: integer, default: 50}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /string-limit:
    get:
      operationId: string-limit/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: limit, in: query, schema: {type: string}}
      responses:
        '200': {content: {application/json: {schema: {type: array, items: {type: object}}}}}
  /string-page-link:
    get:
      operationId: string-page-link/list
      parameters:
        - {name: page, in: query, schema: {type: string, default: '1'}}
      responses:
        '200':
          headers:
            Link:
              schema: {type: string}
          content:
            application/json:
              schema: {type: array, items: {type: object}}
"
        .as_bytes(),
    )
    .expect("string pagination import");
    for operation_id in [
        "string_page_list",
        "string_offset_list",
        "string_limit_list",
    ] {
        assert_eq!(
            imported_rest_pagination(&ir, operation_id).mode,
            PaginationMode::None,
            "operation {operation_id} must not infer pagination from non-numeric inputs"
        );
    }
    let link = imported_rest_pagination(&ir, "string_page_link_list");
    assert_eq!(link.mode, PaginationMode::LinkHeader);
    assert_eq!(link.page_param, None);
    ir.validated_plan()
        .expect("inferred pagination must satisfy operation metadata validation");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The OpenAPI fixture keeps related pagination cardinality cases together."
)]
fn importer_infers_response_pagination_only_for_list_responses() {
    let manifest = parse_source_manifest_yaml(
        r"
name: response_pagination
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /cursor:
    get:
      operationId: cursor/list
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
        - {name: limit, in: query, schema: {type: integer, default: 20}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                  meta:
                    type: object
                    properties:
                      nextCursor: {type: string}
  /pagination-token:
    get:
      operationId: pagination-token/list
      parameters:
        - {name: max_results, in: query, schema: {type: integer, default: 50}}
        - {name: pagination_token, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                  meta:
                    type: object
                    properties:
                      next_token: {type: string}
  /link:
    get:
      operationId: link/list
      parameters:
        - {name: page, in: query, schema: {type: integer}}
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
      responses:
        '200':
          headers:
            Link:
              schema: {type: string}
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: string}
  /iterator:
    get:
      operationId: iterator/list
      parameters:
        - {name: after, in: query, schema: {type: string, format: date-time}}
        - {name: iterator, in: query, schema: {type: string}}
        - {name: limit, in: query, schema: {type: integer, default: 20}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  data: {type: array, items: {type: object}}
                  paging:
                    type: object
                    properties:
                      iterator: {type: string}
  /start-cursor:
    get:
      operationId: start-cursor/list
      parameters:
        - {name: start_cursor, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  results: {type: array, items: {type: object}}
                  continuationToken: {type: string}
  /nested-next:
    get:
      operationId: nested-next/list
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  results: {type: array, items: {type: object}}
                  meta:
                    type: object
                    properties:
                      cursor:
                        type: object
                        properties:
                          next: {type: string}
  /singleton:
    get:
      operationId: singleton/get
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  id: {type: string}
                  nextCursor: {type: string}
  /cursor-header:
    get:
      operationId: cursor-header/list
      parameters:
        - {name: pageToken, in: query, schema: {type: string}}
      responses:
        '200':
          headers:
            X-Next-Cursor:
              schema: {type: string}
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
  /cursor-page:
    get:
      operationId: cursor-page/list
      parameters:
        - {name: page, in: query, schema: {type: string}}
        - {name: limit, in: query, schema: {type: integer, default: 10}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                  next_page:
                    type: string
                    nullable: true
  /numeric-page:
    get:
      operationId: numeric-page/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: limit, in: query, schema: {type: integer, default: 10}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                  next_page:
                    type: string
                    nullable: true
  /next-url-header:
    get:
      operationId: next-url-header/list
      parameters:
        - {name: limit, in: query, schema: {type: integer, default: 25}}
      responses:
        '200':
          headers:
            X-Next-Page-Url:
              schema: {type: string}
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
    .expect("response pagination import");
    let operations = ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();

    // Each of these declares an envelope object, so cardinality stays
    // Singleton; the inferred row path is what makes them paginate.
    for (operation_id, mode) in [
        ("cursor_list", PaginationMode::CursorQuery),
        ("pagination_token_list", PaginationMode::CursorQuery),
        ("iterator_list", PaginationMode::CursorQuery),
        ("start_cursor_list", PaginationMode::CursorQuery),
        ("nested_next_list", PaginationMode::CursorQuery),
        ("cursor_header_list", PaginationMode::CursorQuery),
        ("cursor_page_list", PaginationMode::CursorQuery),
        ("numeric_page_list", PaginationMode::Page),
    ] {
        let operation = operations.get(operation_id).expect("operation");
        assert_eq!(
            operation.output.cardinality,
            OutputCardinality::Singleton,
            "{operation_id}"
        );
        assert!(
            !imported_row_path(&ir, operation_id).is_empty(),
            "{operation_id}"
        );
        assert_eq!(
            imported_rest_pagination(&ir, operation_id).mode,
            mode,
            "{operation_id}"
        );
    }

    // A singleton with a cursor query parameter has no row collection to page
    // through, so the parameter stays an ordinary input.
    let singleton = operations.get("singleton_get").expect("singleton");
    assert_eq!(singleton.output.cardinality, OutputCardinality::Singleton);
    assert!(imported_row_path(&ir, "singleton_get").is_empty());
    assert_eq!(
        imported_rest_pagination(&ir, "singleton_get").mode,
        PaginationMode::None
    );

    let link = imported_rest_pagination(&ir, "link_list");
    assert_eq!(link.mode, PaginationMode::LinkHeader);
    assert_eq!(link.page_param.as_deref(), Some("page"));
    assert_eq!(link.page_start, 1);
    assert_eq!(
        link.page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("per_page")
    );

    let next_url = imported_rest_pagination(&ir, "next_url_header_list");
    assert_eq!(next_url.mode, PaginationMode::LinkHeader);
    assert_eq!(next_url.next_url_header.as_deref(), Some("X-Next-Page-Url"));
    assert_eq!(
        next_url
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("limit")
    );
}

#[test]
fn importer_keeps_opaque_link_header_page_token_public() {
    let manifest = parse_source_manifest_yaml(
        r"
name: link_page_token
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
        - {name: page, in: query, schema: {type: string}}
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
      responses:
        '200':
          headers:
            Link:
              schema: {type: string}
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
    .expect("import");

    let pagination = imported_rest_pagination(&ir, "items_list");
    assert_eq!(pagination.mode, PaginationMode::LinkHeader);
    assert_eq!(pagination.page_param, None);
    assert_eq!(
        pagination
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("per_page")
    );
}

/// Wrapped-list inference resolves `$ref` when it decides a response is an
/// envelope, so cursor discovery has to resolve it too: a row path without a
/// cursor is a table that silently stops after its first page.
#[test]
fn importer_finds_a_cursor_inside_a_referenced_envelope_metadata_object() {
    let manifest = parse_source_manifest_yaml(
        r"
name: referenced_meta
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /things:
    get:
      operationId: listThings
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/ThingPage'}
  /widgets:
    get:
      operationId: listWidgets
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items: {$ref: '#/components/schemas/Thing'}
                  pageInfo:
                    type: object
                    properties:
                      end_cursor: {$ref: '#/components/schemas/Cursor'}
components:
  schemas:
    Cursor:
      type: string
    Thing:
      type: object
      properties:
        id: {type: string}
    PageMeta:
      type: object
      properties:
        next_cursor: {type: string}
    ThingPage:
      type: object
      properties:
        data:
          type: array
          items: {$ref: '#/components/schemas/Thing'}
        meta: {$ref: '#/components/schemas/PageMeta'}
"
        .as_bytes(),
    )
    .expect("import");

    // A referenced `meta` sibling: the reference is what supplies the envelope
    // evidence, so the row path depends on resolving exactly what the cursor
    // walk must also see.
    assert_eq!(imported_row_path(&ir, "listthings"), ["data"]);
    let referenced_meta = imported_rest_pagination(&ir, "listthings");
    assert_eq!(referenced_meta.mode, PaginationMode::CursorQuery);
    assert_eq!(referenced_meta.cursor_param.as_deref(), Some("cursor"));
    assert_eq!(
        referenced_meta.response_cursor_path,
        ["meta", "next_cursor"]
    );

    // An inline `pageInfo` whose token is itself a reference.
    assert_eq!(imported_row_path(&ir, "listwidgets"), ["data"]);
    let referenced_token = imported_rest_pagination(&ir, "listwidgets");
    assert_eq!(referenced_token.mode, PaginationMode::CursorQuery);
    assert_eq!(
        referenced_token.response_cursor_path,
        ["pageInfo", "end_cursor"]
    );
}

/// Row-path inference asks the pagination detectors whether an operation is
/// paginated rather than predicting their answer, so a contract binding any
/// request input is envelope evidence — one case per way `binds_pagination_input`
/// can be satisfied. Predicting the answer used to deadlock the two inferences:
/// no row path because an alias was unknown, and no pagination because the gate
/// needs a row path. `skip`/`take` and `$skip`/`$top` are the aliases that
/// deadlocked.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Every mode shares one envelope fixture, which is what makes the inputs the only variable."
)]
fn importer_infers_row_paths_when_pagination_detection_binds_a_request_input() {
    let manifest = parse_source_manifest_yaml(
        r"
name: aliases
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /skip-take:
    get:
      operationId: skipTakeList
      parameters:
        - {name: skip, in: query, schema: {type: integer, default: 0}}
        - {name: take, in: query, schema: {type: integer, default: 25}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Envelope'}
  /odata:
    get:
      operationId: odataList
      parameters:
        - {name: $skip, in: query, schema: {type: integer, default: 0}}
        - {name: $top, in: query, schema: {type: integer, default: 25}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Envelope'}
  /cursor:
    get:
      operationId: cursorList
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
      responses:
        '200':
          headers:
            X-Next-Cursor:
              schema: {type: string}
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Envelope'}
  /page:
    get:
      operationId: pageList
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Envelope'}
  /link-header:
    get:
      operationId: linkHeaderList
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
      responses:
        '200':
          headers:
            Link:
              schema: {type: string}
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Envelope'}
  /unpaginated:
    get:
      operationId: unpaginatedGet
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Envelope'}
components:
  schemas:
    Envelope:
      type: object
      properties:
        success: {type: boolean}
        data:
          type: array
          items:
            type: object
            properties:
              id: {type: string}
"
        .as_bytes(),
    )
    .expect("import");

    // Every operation returns the same envelope, and it carries no metadata
    // sibling, so the operation's own inputs are the only evidence available.
    for (operation_id, mode) in [
        ("skiptakelist", PaginationMode::Offset),
        ("odatalist", PaginationMode::Offset),
        ("cursorlist", PaginationMode::CursorQuery),
        ("pagelist", PaginationMode::Page),
        // Link-header detection is tried first, so this reaches
        // `binds_pagination_input` by a different route than `pagelist` does —
        // and it is the shape most real paginated endpoints use.
        ("linkheaderlist", PaginationMode::LinkHeader),
    ] {
        assert_eq!(
            imported_row_path(&ir, operation_id),
            ["data"],
            "{operation_id} should be unwrapped"
        );
        assert_eq!(
            imported_rest_pagination(&ir, operation_id).mode,
            mode,
            "{operation_id} should paginate"
        );
    }

    // The same envelope without pagination inputs stays a singleton: nothing
    // says this response is a page rather than a resource.
    assert!(imported_row_path(&ir, "unpaginatedget").is_empty());
}

/// A `Link` response header alone is not envelope evidence. GitHub declares one
/// on singleton resources, where treating it as evidence would promote an
/// incidental array to the whole relation.
#[test]
fn importer_does_not_treat_a_bare_link_header_as_envelope_evidence() {
    let manifest = parse_source_manifest_yaml(
        r"
name: link_header_singleton
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /runners/{runner_id}:
    get:
      operationId: getRunner
      parameters:
        - {name: runner_id, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          headers:
            Link:
              schema: {type: string}
          content:
            application/json:
              schema:
                type: object
                properties:
                  name: {type: string}
                  public_ips:
                    type: array
                    items:
                      type: object
                      properties:
                        prefix: {type: string}
"
        .as_bytes(),
    )
    .expect("import");

    assert!(
        imported_row_path(&ir, "getrunner").is_empty(),
        "a runner is a resource, not a page of its IP addresses"
    );
}

#[test]
fn importer_treats_path_parameters_as_required_when_required_is_omitted() {
    let manifest = parse_source_manifest_yaml(
        r"
name: omitted_path_required
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /tenants/{tenant}/items/{id}:
    get:
      operationId: items/get
      parameters:
        - {name: id, in: path, required: false, schema: {type: string}}
        - {name: tenant, in: path, schema: {type: string, default: public}}
        - {name: include_archived, in: query, schema: {type: boolean}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  id: {type: string}
"
        .as_bytes(),
    )
    .expect("import");
    let operation = ir.operations.first().expect("operation");
    let required = operation
        .inputs
        .iter()
        .map(|input| (input.name.as_str(), input.required))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(required.get("id"), Some(&true));
    assert_eq!(required.get("tenant"), Some(&true));
    assert_eq!(required.get("include_archived"), Some(&false));
}

#[test]
fn importer_warns_for_invalid_parameters_and_unresolved_responses() {
    let manifest = parse_source_manifest_yaml(
        r"
name: broken
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
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
    assert!(
        operation.diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .contains("parameter without a string name")),
        "{:?}",
        operation.diagnostics
    );
    assert!(
        operation.diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .contains("response schema could not be resolved")),
        "{:?}",
        operation.diagnostics
    );
    assert_eq!(operation.output.cardinality, OutputCardinality::Unknown);
}
