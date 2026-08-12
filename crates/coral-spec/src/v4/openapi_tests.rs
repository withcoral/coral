use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::*;
use crate::v4::test_support::github_openapi_at_version;
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

/// A branch may be an alias — `{$ref: Alias}` where `Alias` is itself
/// `{$ref: Base}` — which one hop of resolution leaves still holding a `$ref`,
/// with nothing to contribute.
///
/// Inference resolves chains, so a branch dropped here would make a row path it
/// found look absent from the imported type, and `infer_row_path` would discard
/// the path.
#[test]
fn importer_folds_all_of_branches_through_alias_refs() {
    let manifest = parse_source_manifest_yaml(
        r"
name: aliased_all_of
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
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
              schema: {$ref: '#/components/schemas/Page'}
components:
  schemas:
    Base:
      type: object
      properties:
        next_cursor: {type: string}
    Alias: {$ref: '#/components/schemas/Base'}
    Page:
      type: object
      allOf:
        - {$ref: '#/components/schemas/Alias'}
        - type: object
          properties:
            items:
              type: array
              items: {type: object, properties: {id: {type: string}}}
"
        .as_bytes(),
    )
    .expect("import");

    let operation = ir.operations.first().expect("operation");
    let row_type = ir
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type");
    let IrTypeShape::Object { fields } = &row_type.shape else {
        panic!("the composed page should import as an object: {row_type:?}");
    };
    let names = fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();

    assert!(
        names.contains(&"next_cursor"),
        "the aliased base contributes its properties: {names:?}"
    );
    assert!(names.contains(&"items"), "{names:?}");
}

/// The fold tracks the refs it is resolving, so a self-referential branch is
/// named as a cycle rather than recursing until the depth cap stops it.
#[test]
fn importer_reports_a_cyclic_all_of_branch() {
    let manifest = parse_source_manifest_yaml(
        r"
name: cyclic_all_of
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
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
              schema: {$ref: '#/components/schemas/Loop'}
components:
  schemas:
    Loop:
      type: object
      allOf:
        - {$ref: '#/components/schemas/Loop'}
"
        .as_bytes(),
    )
    .expect("a cyclic allOf imports with diagnostics rather than recursing");

    let operation = ir.operations.first().expect("operation");
    assert!(
        operation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("cyclic")),
        "the diagnostic must name the cycle, not a depth ceiling: {:?}",
        operation.diagnostics
    );
    assert_eq!(operation.output.type_ref, "json");
}

/// Composition past the cap reports its own ceiling. Reusing the property
/// comparison error sent whoever read it to a constant eight times larger.
#[test]
fn importer_reports_all_of_composition_past_the_depth_cap() {
    let mut schemas = String::from(
        r"
components:
  schemas:
    Deep0:
      type: object
      properties:
        id: {type: string}
",
    );
    for level in 1..=12 {
        writeln!(
            schemas,
            "    Deep{level}:\n      type: object\n      allOf:\n        - {{$ref: '#/components/schemas/Deep{}'}}",
            level - 1
        )
        .expect("writing to a String cannot fail");
    }
    let manifest = parse_source_manifest_yaml(
        r"
name: deep_all_of
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let document = format!(
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
              schema: {{$ref: '#/components/schemas/Deep12'}}
{schemas}"
    );
    let ir = import_openapi_surface(v4, &v4.surface, document.as_bytes()).expect("import");

    let operation = ir.operations.first().expect("operation");
    assert!(
        operation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("nests past")),
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
/// request input is envelope evidence — one case per way `signals_page_envelope`
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
        // `signals_page_envelope` by a different route than `pagelist` does —
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

/// A body field holding a whole next-page URL is a stronger signal than a
/// guessed cursor parameter, so it outranks cursor-query and offset detection —
/// but only for names that actually denote a URL.
#[test]
fn importer_detects_body_next_url_pagination_above_cursor_and_offset() {
    let manifest = parse_source_manifest_yaml(
        r"
name: nexturl
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /next-url:
    get:
      operationId: nextUrlList
      parameters:
        - {name: skip, in: query, schema: {type: integer, default: 0}}
        - {name: limit, in: query, schema: {type: integer, default: 25}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  next_page_url: {type: string}
                  data:
                    type: array
                    items: {type: object, properties: {id: {type: string}}}
  /next-token:
    get:
      operationId: nextTokenList
      parameters:
        - {name: cursor, in: query, schema: {type: string}}
        - {name: limit, in: query, schema: {type: integer, default: 25}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  next_cursor: {type: string}
                  data:
                    type: array
                    items: {type: object, properties: {id: {type: string}}}
"
        .as_bytes(),
    )
    .expect("import");

    let next_url = imported_rest_pagination(&ir, "nexturllist");
    assert_eq!(next_url.mode, PaginationMode::NextUrlBody);
    assert_eq!(next_url.next_url_path, ["next_page_url"]);
    assert_eq!(
        next_url
            .page_size
            .as_ref()
            .and_then(|size| size.query_param.as_deref()),
        Some("limit"),
        "page one still asks for a page size; later pages inherit it from the URL"
    );
    assert!(
        next_url.offset_param.is_none(),
        "a whole next URL must beat offset detection, which would drive `skip` the server may reject"
    );
    assert_eq!(imported_row_path(&ir, "nexturllist"), ["data"]);

    // `next_cursor` is a token, not a URL: it belongs in the request parameter
    // that expects it, so it must keep falling through to cursor-query.
    let next_token = imported_rest_pagination(&ir, "nexttokenlist");
    assert_eq!(next_token.mode, PaginationMode::CursorQuery);
    assert_eq!(next_token.cursor_param.as_deref(), Some("cursor"));
    assert!(next_token.next_url_path.is_empty());
}

/// A declared `Link` header is cheaper and more standard than reading the body,
/// so it stays ahead of body next-URL detection.
#[test]
fn importer_prefers_a_declared_link_header_over_a_body_next_url() {
    let manifest = parse_source_manifest_yaml(
        r"
name: linkfirst
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /both:
    get:
      operationId: bothList
      parameters:
        - {name: per_page, in: query, schema: {type: integer, default: 30}}
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
                  next_link: {type: string}
                  data:
                    type: array
                    items: {type: object, properties: {id: {type: string}}}
"
        .as_bytes(),
    )
    .expect("import");

    let pagination = imported_rest_pagination(&ir, "bothlist");
    assert_eq!(pagination.mode, PaginationMode::LinkHeader);
    assert!(pagination.next_url_path.is_empty());
}

/// A next-page URL in the response body is envelope evidence in its own right.
///
/// Graph is the shape that needs it: `{"@odata.nextLink": ..., "value": [...]}`
/// on an operation that declares no `$top`. The response names no conventional
/// row property and carries no metadata sibling the lexicon recognizes, so
/// without the next-URL path there is nothing to unwrap `value` on — and a
/// contract only survives once the response reads as a list, so the pagination
/// would have been discarded along with the row path.
#[test]
fn importer_treats_a_body_next_url_as_envelope_evidence_without_page_inputs() {
    let manifest = parse_source_manifest_yaml(
        r"
name: odata
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://graph.microsoft.com/v1.0
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /users:
    get:
      operationId: listUsers
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  '@odata.nextLink': {type: string}
                  value:
                    type: array
                    items: {type: object, properties: {id: {type: string}}}
"
        .as_bytes(),
    )
    .expect("import");

    assert_eq!(imported_row_path(&ir, "listusers"), ["value"]);
    let pagination = imported_rest_pagination(&ir, "listusers");
    assert_eq!(pagination.mode, PaginationMode::NextUrlBody);
    assert_eq!(pagination.next_url_path, ["@odata.nextLink"]);
    assert!(
        pagination.page_size.is_none(),
        "nothing declares a page size here; the next URL carries the paging state"
    );
}

/// A body next-URL outranks the input-corroborated modes, so it has to be more
/// than a name match: the schema must declare the property a string.
///
/// Without that, an operation like this one got `mode: next_url_body` on the
/// strength of the name `nextLink`. At runtime `Value::as_str` on a non-string
/// reads `None`, `advance_pagination_state` stops, and the query returns page
/// one — no error, no diagnostic — where the `skip`/`limit` contract the server
/// actually declared would have fetched everything.
#[test]
fn importer_ignores_a_body_next_url_the_schema_does_not_declare_a_string() {
    let manifest = parse_source_manifest_yaml(
        r"
name: things
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /things:
    get:
      operationId: listThings
      parameters:
        - {name: skip, in: query, schema: {type: integer, default: 0}}
        - {name: limit, in: query, schema: {type: integer, default: 25}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  nextLink: {}
                  data:
                    type: array
                    items: {type: object, properties: {id: {type: string}}}
"
        .as_bytes(),
    )
    .expect("import");

    let pagination = imported_rest_pagination(&ir, "listthings");
    assert_eq!(
        pagination.mode,
        PaginationMode::Offset,
        "an undeclared type is not enough to displace the contract the server declared"
    );
    assert_eq!(pagination.offset_param.as_deref(), Some("skip"));
    assert!(pagination.next_url_path.is_empty());
}

/// ...but only at the response root, which is where what it unlocks applies.
///
/// `find_response_cursor_path` descends into nested objects up to depth 8. A
/// singleton resource that happens to carry a nested link — every pre-existing
/// detector was immune to this, because each needed a bound request input —
/// would otherwise reach the sole-array fallback and have its one incidental
/// array promoted to the whole relation.
#[test]
fn importer_ignores_a_body_next_url_nested_below_the_response_root() {
    let manifest = parse_source_manifest_yaml(
        r"
name: tracks
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /tracks/{id}:
    get:
      operationId: getTrack
      parameters:
        - {name: id, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  id: {type: string}
                  tags:
                    type: array
                    items: {type: string}
                  links:
                    type: object
                    properties:
                      next_href: {type: string}
"
        .as_bytes(),
    )
    .expect("import");

    assert!(
        imported_row_path(&ir, "gettrack").is_empty(),
        "the track is the resource; its tags are one of its fields, not the relation"
    );
    assert_eq!(
        imported_rest_pagination(&ir, "gettrack").mode,
        PaginationMode::None,
        "a contract only survives once the response reads as a list"
    );
}

/// End to end on the Microsoft Graph shape: an `allOf` envelope of a shared
/// pagination base and a `value` array, with `$top`/`$skip` both declared.
///
/// The `offset_param` assertion is the regression guard for the ordering
/// constraint. Graph declares `$skip` on collections that reject it at runtime,
/// so if body next-URL detection ever stopped winning, these tables would go
/// from returning one page to returning an error.
#[test]
fn importer_reads_odata_collections_as_paginated_row_tables() {
    let manifest = parse_source_manifest_yaml(
        r"
name: graph
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://graph.microsoft.com/v1.0
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /me/chats:
    get:
      operationId: me.listChats
      parameters:
        - {name: $top, in: query, schema: {type: integer}}
        - {name: $skip, in: query, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/microsoft.graph.chatCollectionResponse'
components:
  schemas:
    BaseCollectionPaginationCountResponse:
      title: Base collection pagination and count responses
      type: object
      properties:
        '@odata.count': {type: integer, format: int64, nullable: true}
        '@odata.nextLink': {type: string, nullable: true}
    microsoft.graph.chat:
      type: object
      properties:
        id: {type: string}
        topic: {type: string}
    microsoft.graph.chatCollectionResponse:
      title: Collection of chat
      type: object
      allOf:
        - $ref: '#/components/schemas/BaseCollectionPaginationCountResponse'
        - type: object
          properties:
            value:
              type: array
              items:
                $ref: '#/components/schemas/microsoft.graph.chat'
"
        .as_bytes(),
    )
    .expect("import");

    assert_eq!(imported_row_path(&ir, "me_listchats"), ["value"]);

    let pagination = imported_rest_pagination(&ir, "me_listchats");
    assert_eq!(pagination.mode, PaginationMode::NextUrlBody);
    assert_eq!(pagination.next_url_path, ["@odata.nextLink"]);
    assert_eq!(
        pagination
            .page_size
            .as_ref()
            .and_then(|size| size.query_param.as_deref()),
        Some("$top")
    );
    assert!(
        pagination.offset_param.is_none(),
        "Graph rejects $skip on several collections; the next link is the only contract that works"
    );
}

/// The fixture above declares only `$top` and `$skip`, which is not what a real
/// Graph collection looks like: they also declare a boolean `$count`.
///
/// `find_numeric_query_input` picks the first candidate-named input and only
/// then filters by type, so `$count` is chosen and rejected and `$top` is never
/// reached — page-size detection finds nothing. Pinning it here so the
/// follow-up that fixes the ordering has an assertion to flip; pagination
/// itself is unaffected, since the next link carries the paging state and Coral
/// just accepts Graph's server-side default page size.
#[test]
fn importer_misses_the_page_size_a_boolean_count_parameter_masks() {
    let manifest = parse_source_manifest_yaml(
        r"
name: graph
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://graph.microsoft.com/v1.0
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /me/chats:
    get:
      operationId: me.listChats
      parameters:
        - {name: $top, in: query, schema: {type: integer}}
        - {name: $skip, in: query, schema: {type: integer}}
        - {name: $count, in: query, schema: {type: boolean}}
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/microsoft.graph.chatCollectionResponse'
components:
  schemas:
    BaseCollectionPaginationCountResponse:
      title: Base collection pagination and count responses
      type: object
      properties:
        '@odata.count': {type: integer, format: int64, nullable: true}
        '@odata.nextLink': {type: string, nullable: true}
    microsoft.graph.chat:
      type: object
      properties:
        id: {type: string}
        topic: {type: string}
    microsoft.graph.chatCollectionResponse:
      title: Collection of chat
      type: object
      allOf:
        - $ref: '#/components/schemas/BaseCollectionPaginationCountResponse'
        - type: object
          properties:
            value:
              type: array
              items:
                $ref: '#/components/schemas/microsoft.graph.chat'
"
        .as_bytes(),
    )
    .expect("import");

    // The collection still reads as a paginated row table — only the page size
    // is lost.
    assert_eq!(imported_row_path(&ir, "me_listchats"), ["value"]);
    let pagination = imported_rest_pagination(&ir, "me_listchats");
    assert_eq!(pagination.mode, PaginationMode::NextUrlBody);
    assert_eq!(pagination.next_url_path, ["@odata.nextLink"]);
    assert!(
        pagination.page_size.is_none(),
        "boolean $count sorts ahead of $top and masks it; flip this when \
         find_numeric_query_input filters by type before choosing"
    );
}

/// Graph nests its envelope bases: a delta collection response composes
/// `BaseDeltaFunctionResponse`, which is itself an `allOf` over
/// `BaseCollectionPaginationCountResponse`. Row-path inference folds that whole
/// tree, so type import has to fold it too — otherwise the imported type is
/// missing the properties the inferred path names and the path is discarded.
#[test]
fn importer_folds_nested_all_of_envelope_bases() {
    let manifest = parse_source_manifest_yaml(
        r"
name: graph
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://graph.microsoft.com/v1.0
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /me/chats/delta:
    get:
      operationId: me.chats.delta
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/microsoft.graph.chatDeltaCollectionResponse'
components:
  schemas:
    BaseCollectionPaginationCountResponse:
      type: object
      properties:
        '@odata.count': {type: integer, format: int64, nullable: true}
        '@odata.nextLink': {type: string, nullable: true}
    BaseDeltaFunctionResponse:
      type: object
      allOf:
        - $ref: '#/components/schemas/BaseCollectionPaginationCountResponse'
        - type: object
          properties:
            '@odata.deltaLink': {type: string, nullable: true}
    microsoft.graph.chat:
      type: object
      properties:
        id: {type: string}
    microsoft.graph.chatDeltaCollectionResponse:
      type: object
      allOf:
        - $ref: '#/components/schemas/BaseDeltaFunctionResponse'
        - type: object
          properties:
            value:
              type: array
              items:
                $ref: '#/components/schemas/microsoft.graph.chat'
"
        .as_bytes(),
    )
    .expect("import");

    assert_eq!(imported_row_path(&ir, "me_chats_delta"), ["value"]);

    let pagination = imported_rest_pagination(&ir, "me_chats_delta");
    assert_eq!(pagination.mode, PaginationMode::NextUrlBody);
    assert_eq!(pagination.next_url_path, ["@odata.nextLink"]);

    // The nested base contributes `@odata.count`/`@odata.nextLink` and the
    // intermediate one `@odata.deltaLink`. Folding only the immediate branches
    // kept `value` alone.
    let IrTypeShape::Object { fields } = &ir
        .semantic_ir
        .types
        .iter()
        .find(|ty| ty.id == "me_chats_delta_row")
        .expect("response type")
        .shape
    else {
        panic!("expected an object response type");
    };
    let names: Vec<&str> = fields.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "@odata.count",
            "@odata.deltaLink",
            "@odata.nextLink",
            "value"
        ]
    );
}

/// `properties` declared alongside `allOf` are as much part of the schema as
/// the branches are.
///
/// Row-path inference reads both, so before type import did too, the inferred
/// `value` path was rejected as absent from the imported type and the whole
/// collection collapsed to a single JSON row.
#[test]
fn importer_keeps_properties_declared_beside_all_of() {
    let manifest = parse_source_manifest_yaml(
        r"
name: graph
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://graph.microsoft.com/v1.0
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /me/chats:
    get:
      operationId: me.listChats
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/microsoft.graph.chatCollectionResponse'
components:
  schemas:
    BaseCollectionPaginationCountResponse:
      type: object
      properties:
        '@odata.count': {type: integer, format: int64, nullable: true}
        '@odata.nextLink': {type: string, nullable: true}
    microsoft.graph.chat:
      type: object
      properties:
        id: {type: string}
    microsoft.graph.chatCollectionResponse:
      type: object
      properties:
        value:
          type: array
          items:
            $ref: '#/components/schemas/microsoft.graph.chat'
      allOf:
        - $ref: '#/components/schemas/BaseCollectionPaginationCountResponse'
"
        .as_bytes(),
    )
    .expect("import");

    assert_eq!(imported_row_path(&ir, "me_listchats"), ["value"]);
}

/// A subtype re-declaring an inherited property to pin an annotation is not a
/// conflict.
///
/// Every Graph type re-declares the `@odata.type` discriminator with its own
/// `default`. Comparing declarations byte-for-byte reads that as two branches
/// disagreeing and discards the whole type, which costs every column rather
/// than the one property — so the comparison is on validation semantics, and
/// [`importer_warns_for_openapi_all_of_property_conflicts`] still holds for branches
/// that genuinely disagree.
#[test]
fn importer_accepts_annotation_only_redeclaration_across_all_of_levels() {
    let manifest = parse_source_manifest_yaml(
        r"
name: graph
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://graph.microsoft.com/v1.0
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let ir = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.0.3
paths:
  /me/drive:
    get:
      operationId: me.getDrive
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/microsoft.graph.drive'
components:
  schemas:
    microsoft.graph.entity:
      type: object
      properties:
        id: {type: string}
        '@odata.type': {type: string}
    microsoft.graph.baseItem:
      type: object
      allOf:
        - $ref: '#/components/schemas/microsoft.graph.entity'
        - type: object
          properties:
            name: {type: string}
            webUrl: {type: string}
    microsoft.graph.drive:
      type: object
      allOf:
        - $ref: '#/components/schemas/microsoft.graph.baseItem'
        - type: object
          properties:
            driveType: {type: string}
            '@odata.type': {type: string, default: '#microsoft.graph.drive'}
"
        .as_bytes(),
    )
    .expect("import");

    let IrTypeShape::Object { fields } = &ir
        .semantic_ir
        .types
        .iter()
        .find(|ty| ty.id == "me_getdrive_row")
        .expect("drive response type")
        .shape
    else {
        panic!("a conflict would have left the type as opaque JSON");
    };
    let names: Vec<&str> = fields.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(
        names,
        ["@odata.type", "driveType", "id", "name", "webUrl"],
        "the inherited columns must survive two levels of composition"
    );
}

/// A minimal but complete document whose first line the caller chooses, so a
/// version test varies the version and nothing else.
fn version_probe_document(first_line: &str) -> String {
    format!(
        r"{first_line}
info:
  title: Demo
  description: Query demo data.
servers:
  - url: https://api.example.com/v1
paths:
  /items:
    get:
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
                    id: {{type: string}}
"
    )
}

#[expect(
    clippy::unwrap_in_result,
    reason = "Only the import's own result is under test; a manifest that will not parse is a bug in this file, not an outcome to return."
)]
fn import_version_probe(first_line: &str) -> crate::Result<ImportedSurface> {
    let manifest = parse_source_manifest_yaml(
        r"
name: version_probe
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    import_openapi_surface(
        v4,
        &v4.surface,
        version_probe_document(first_line).as_bytes(),
    )
}

#[test]
fn importer_accepts_every_spelling_of_a_supported_version() {
    // `3.0` carries no patch component, which the field's grammar allows and a
    // `"3.0."` prefix test rejected.
    for version in ["3.0.0", "3.0.3", "3.0.4", "3.0", "3.1.0", "3.1.1", "3.1"] {
        let imported = import_version_probe(&format!("openapi: '{version}'"))
            .unwrap_or_else(|error| panic!("version {version} should import: {error}"));
        assert_eq!(
            imported.operations.len(),
            1,
            "version {version} should import its one operation"
        );
    }
}

#[test]
fn importer_rejects_unsupported_versions_by_name() {
    // 3.2 was ratified in April 2026 but is not yet widely adopted, so it stays
    // out until there is something to import that uses it.
    for version in [
        "2.0",
        "3.2.0",
        "4.0.0",
        "3",
        // Well-formed prefix, malformed remainder. The version field holds one
        // optional numeric patch component and nothing else, under either
        // supported version.
        "3.0.",
        "3.0.banana",
        "3.0.1.2",
        "3.1.1-rc1",
        "3.1.x",
    ] {
        let error = import_version_probe(&format!("openapi: '{version}'"))
            .expect_err(&format!("version {version} should be rejected"));
        assert!(
            error
                .to_string()
                .contains(&format!("unsupported version '{version}'")),
            "version {version} should be named in its own rejection: {error}"
        );
    }
}

#[test]
fn importer_names_swagger_documents_rather_than_reporting_a_missing_field() {
    let error = import_version_probe("swagger: '2.0'").expect_err("Swagger should be rejected");
    let message = error.to_string();
    assert!(
        message.contains("Swagger version '2.0'"),
        "a Swagger document deserves to be told what it is: {message}"
    );
}

#[test]
fn importer_rejects_documents_declaring_no_version() {
    let error =
        import_version_probe("x-unversioned: true").expect_err("missing version should reject");
    assert!(
        error.to_string().contains("missing openapi version"),
        "{error}"
    );
}

#[test]
fn document_metadata_applies_the_same_version_gate_as_import() {
    let metadata = openapi_document_metadata(version_probe_document("openapi: '3.0'").as_bytes())
        .expect("3.0 metadata");
    assert_eq!(metadata.description.as_deref(), Some("Query demo data."));

    // Nothing read here is spelled differently in 3.1, so metadata extraction
    // needs no dialect — but it does have to accept exactly what import accepts.
    // Describing a document the importer would refuse, or refusing one it would
    // take, is the confusing outcome either way.
    let metadata = openapi_document_metadata(version_probe_document("openapi: '3.1.0'").as_bytes())
        .expect("3.1 metadata");
    assert_eq!(metadata.description.as_deref(), Some("Query demo data."));
    assert_eq!(
        metadata.server_url.as_deref(),
        Some("https://api.example.com/v1")
    );

    let error = openapi_document_metadata(version_probe_document("openapi: '3.2.0'").as_bytes())
        .expect_err("3.2 metadata should be rejected while import rejects it");
    assert!(
        error.to_string().contains("unsupported version '3.2.0'"),
        "{error}"
    );
}

/// Imports a document whose one operation returns `response_schema`.
#[expect(
    clippy::unwrap_in_result,
    reason = "Only the import's own result is under test; a manifest that will not parse is a bug in this file, not an outcome to return."
)]
fn import_response_schema(response_schema: &str) -> crate::Result<ImportedSurface> {
    let manifest = parse_source_manifest_yaml(
        r"
name: type_array_probe
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    import_openapi_surface(
        v4,
        &v4.surface,
        format!(
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
              schema:
{response_schema}
"
        )
        .as_bytes(),
    )
}

#[test]
fn importer_reads_a_nullable_collection_as_a_list() {
    // The type array is what a nullable schema looks like once `nullable` is
    // gone. Read as a string, `type` came back as `None` here and the schema
    // fell through to the typeless default, so the collection was imported as a
    // single object row and every item was lost.
    let imported = import_response_schema(
        r"                type: [array, 'null']
                items:
                  type: object
                  properties:
                    id: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::List);

    // The cardinality alone would still be satisfied if the item schema handed
    // to `import_schema` were the wrong one — the rows would be typed as opaque
    // JSON, which is the other half of the same data loss.
    let IrTypeShape::Object { fields } = &imported
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type")
        .shape
    else {
        panic!("the row type has to come from `items`, not from the collection itself");
    };
    let names: Vec<&str> = fields.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(names, ["id"]);
}

#[test]
fn response_and_schema_dispatch_agree_when_a_schema_claims_both_types() {
    // Only a schema declaring both types can tell the two dispatches apart, and
    // they have to answer alike: a response read as a list whose row type was
    // built as an object would describe a collection of one thing and rows of
    // another.
    let imported = import_response_schema(
        r"                type: [object, array]
                properties:
                  id: {type: string}
                items: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    assert_eq!(
        operation.output.cardinality,
        OutputCardinality::Singleton,
        "object wins in `classify_response_schema`, as it does in `import_schema`"
    );
    assert!(
        matches!(
            imported
                .types
                .iter()
                .find(|ty| ty.id == operation.output.type_ref)
                .expect("row type")
                .shape,
            IrTypeShape::Object { .. }
        ),
        "the row type must be the object the cardinality claims it is"
    );
}

#[test]
fn importer_reads_a_union_of_a_collection_and_a_scalar_as_json() {
    // `null` is the only type a collection may be unioned with and still be a
    // collection. This one also accepts a bare string, so importing `items`
    // would type every string response as a single row of item-derived columns,
    // each of them null. Neither shape is true of both instances, so the
    // response keeps the shape the importer uses when it cannot tell.
    let imported = import_response_schema(
        r"                type: [array, string]
                items:
                  type: object
                  properties:
                    id: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    assert_eq!(
        operation.output.cardinality,
        OutputCardinality::Unknown,
        "a union including `array` does not make the response a collection"
    );
    assert!(
        matches!(
            imported
                .types
                .iter()
                .find(|ty| ty.id == operation.output.type_ref)
                .expect("row type")
                .shape,
            IrTypeShape::Json
        ),
        "both dispatches have to decline: an `id` column here would come from \
         `items`, which only describes the collection branch"
    );
}

#[test]
fn importer_reads_a_nullable_object_as_an_object() {
    // This case survived the string-only read by luck rather than by design: an
    // unreadable `type` fell through to a default of "object", which is what a
    // nullable object needed anyway. Pinned because the new dispatch reaches the
    // same answer deliberately, and a later reshuffle of the branch order should
    // not be free to lose it.
    let imported = import_response_schema(
        r"                type: [object, 'null']
                properties:
                  id: {type: string}
                  name: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    let IrTypeShape::Object { fields } = &imported
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type")
        .shape
    else {
        panic!("a nullable object should keep its fields rather than fall back to opaque JSON");
    };
    let names: Vec<&str> = fields.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(names, ["id", "name"]);
}

#[test]
fn importer_reads_a_nullable_collection_of_objects_into_row_fields() {
    // Nullable rows inside a plain collection, which the "object" default also
    // happened to get right. Kept as the composed counterpart to the two cases
    // above, and as the anchor for the nullability assertion below.
    let imported = import_response_schema(
        r"                type: array
                items:
                  type: [object, 'null']
                  properties:
                    id: {type: string}
                    label: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::List);
    let row_type = imported
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type");
    assert!(
        !row_type.nullable,
        "reading 'null' out of a type array is the 3.1 dialect's job; 3.0 spells nullability with its own keyword"
    );
    let IrTypeShape::Object { fields } = &row_type.shape else {
        panic!("a nullable row should keep its fields rather than fall back to opaque JSON");
    };
    let names: Vec<&str> = fields.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(names, ["id", "label"]);
}

#[test]
fn importer_still_reads_a_typeless_schema_as_an_object() {
    // Declaring no type at all is not the same as declaring one this code has
    // no shape for: JSON Schema accepts any instance, and the importer has
    // always read that as an object.
    let imported = import_response_schema(
        r"                properties:
                  id: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
    let IrTypeShape::Object { fields } = &imported
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type")
        .shape
    else {
        panic!("a typeless schema with properties should still be an object");
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn importer_reads_a_nullable_array_property_as_a_list() {
    // The type-import half of the same fault. A nullable collection nested as a
    // property never reached the array branch: its `type` did not read as a
    // string, so it took the "object" default, found neither `properties` nor
    // `additionalProperties`, and collapsed to opaque JSON — losing the element
    // type of every optional list an API returns.
    let imported = import_response_schema(
        r"                type: object
                properties:
                  id: {type: string}
                  tags:
                    type: [array, 'null']
                    items: {type: string}",
    )
    .expect("import");

    let operation = imported.operations.first().expect("operation");
    let IrTypeShape::Object { fields } = &imported
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type")
        .shape
    else {
        panic!("expected an object row");
    };
    let tags = fields
        .iter()
        .find(|field| field.name == "tags")
        .expect("tags field");
    let IrTypeShape::List { item_type_ref } = &imported
        .types
        .iter()
        .find(|ty| ty.id == tags.type_ref)
        .expect("tags type")
        .shape
    else {
        panic!("a nullable array property should import as a list, not opaque JSON");
    };
    assert!(
        matches!(
            imported
                .types
                .iter()
                .find(|ty| &ty.id == item_type_ref)
                .expect("item type")
                .shape,
            IrTypeShape::Scalar(IrScalarType::String)
        ),
        "the element type has to survive, or the list is untyped"
    );
}

/// Imports a document at `version` whose one operation returns
/// `response_schema`, so a test can hold the schema fixed and vary the dialect.
#[expect(
    clippy::unwrap_in_result,
    reason = "Only the import's own result is under test; a manifest that will not parse is a bug in this file, not an outcome to return."
)]
fn import_versioned_response_schema(
    version: &str,
    response_schema: &str,
) -> crate::Result<ImportedSurface> {
    let manifest = parse_source_manifest_yaml(
        r"
name: dialect_probe
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    import_openapi_surface(
        v4,
        &v4.surface,
        format!(
            r"
openapi: {version}
paths:
  /items:
    get:
      operationId: items/get
      responses:
        '200':
          content:
            application/json:
              schema:
{response_schema}
"
        )
        .as_bytes(),
    )
}

fn probe_row_type<'a>(imported: &'a ImportedSurface, field: &str) -> &'a IrField {
    let operation = imported.operations.first().expect("operation");
    let IrTypeShape::Object { fields } = &imported
        .types
        .iter()
        .find(|ty| ty.id == operation.output.type_ref)
        .expect("row type")
        .shape
    else {
        panic!("expected an object row");
    };
    fields
        .iter()
        .find(|candidate| candidate.name == field)
        .unwrap_or_else(|| panic!("field {field}"))
}

fn probe_field_type<'a>(imported: &'a ImportedSurface, field: &str) -> &'a IrType {
    let type_ref = &probe_row_type(imported, field).type_ref;
    imported
        .types
        .iter()
        .find(|ty| &ty.id == type_ref)
        .expect("field type")
}

const NULLABILITY_PROBE_SCHEMA: &str = r"                type: object
                properties:
                  by_type_array:
                    type: [string, 'null']
                  by_keyword:
                    type: string
                    nullable: true";

#[test]
fn openapi_31_reads_nullability_out_of_the_type_array() {
    let imported = import_versioned_response_schema("3.1.0", NULLABILITY_PROBE_SCHEMA)
        .expect("3.1 should import");

    assert!(
        probe_field_type(&imported, "by_type_array").nullable,
        "3.1 spells nullability by listing 'null' in the type"
    );
    // The scalar still has to survive the extra type: a nullable string is a
    // string column, not an untyped one.
    assert!(
        matches!(
            probe_field_type(&imported, "by_type_array").shape,
            IrTypeShape::Scalar(IrScalarType::String)
        ),
        "a nullable string is still a string"
    );
    assert!(
        !probe_field_type(&imported, "by_keyword").nullable,
        "3.1 gives the 'nullable' keyword no meaning, so it must not confer nullability"
    );
}

#[test]
fn openapi_30_reads_nullability_out_of_the_nullable_keyword() {
    // The same schema under the other dialect, to pin that the two disagree in
    // exactly the way the specifications do rather than one being a superset.
    let imported = import_versioned_response_schema("3.0.3", NULLABILITY_PROBE_SCHEMA)
        .expect("3.0 should import");

    assert!(
        probe_field_type(&imported, "by_keyword").nullable,
        "3.0 spells nullability with its own keyword"
    );
    assert!(
        !probe_field_type(&imported, "by_type_array").nullable,
        "3.0 has no 'null' type to read"
    );
}

#[test]
fn openapi_31_warns_when_a_document_still_carries_nullable() {
    let imported = import_versioned_response_schema("3.1.0", NULLABILITY_PROBE_SCHEMA)
        .expect("3.1 should import");

    let operation = imported.operations.first().expect("operation");
    let warning = operation
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.message.contains("'nullable'"))
        .expect("a 3.1 document carrying 'nullable' should say so");
    assert!(
        warning.message.contains("removed in OpenAPI 3.1"),
        "the warning should name the version that removed it: {}",
        warning.message
    );
    assert_eq!(warning.operation_id.as_deref(), Some("items_get"));
}

#[test]
fn openapi_31_warns_about_a_removed_keyword_on_every_schema_path() {
    // Three placements the type import never sees. A parameter resolves to a
    // scalar without reaching `import_schema`, and a collection response hands
    // it `items` — so the schema carrying the keyword is set aside in both, and
    // the warning was dropped with it.
    let manifest = parse_source_manifest_yaml(
        r"
name: removed_keywords
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let imported = import_openapi_surface(
        v4,
        &v4.surface,
        r"
openapi: 3.1.0
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: since, in: query, schema: {type: string, nullable: true}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                nullable: true
                items:
                  type: object
                  properties:
                    id: {type: string}
"
        .as_bytes(),
    )
    .expect("3.1 should import");

    let operation = imported.operations.first().expect("operation");
    let warnings: Vec<&str> = operation
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.message.contains("'nullable'"))
        .map(|diagnostic| diagnostic.message.as_str())
        .collect();
    assert!(
        warnings
            .iter()
            .any(|message| message.contains("parameter 'since'")),
        "a parameter schema carrying the keyword has to be reported: {warnings:?}"
    );
    assert!(
        warnings
            .iter()
            .any(|message| message.contains("response schema")),
        "a collection response carrying the keyword has to be reported: {warnings:?}"
    );
    assert_eq!(
        warnings.len(),
        2,
        "one report each, and none for the item schema, which does not carry it: {warnings:?}"
    );
}

#[test]
fn openapi_31_reports_a_singleton_response_keyword_once() {
    // The counterpart to setting the schema aside: a singleton hands
    // `import_schema` the very schema the response resolved to, so both would
    // report the same keyword in the same place.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                nullable: true
                properties:
                  id: {type: string}",
    )
    .expect("3.1 should import");

    let operation = imported.operations.first().expect("operation");
    let warnings: Vec<&str> = operation
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.message.contains("'nullable'"))
        .map(|diagnostic| diagnostic.message.as_str())
        .collect();
    assert_eq!(warnings.len(), 1, "said once, not twice: {warnings:?}");
}

#[test]
fn openapi_30_does_not_warn_about_nullable() {
    let imported = import_versioned_response_schema("3.0.3", NULLABILITY_PROBE_SCHEMA)
        .expect("3.0 should import");

    let operation = imported.operations.first().expect("operation");
    assert!(
        !operation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("'nullable'")),
        "'nullable' is how 3.0 spells this: {:?}",
        operation.diagnostics
    );
}

#[test]
fn openapi_31_reads_const_as_a_single_value_enum() {
    // 3.1 documents pin a discriminator with `const` where a 3.0 one wrote a
    // one-element `enum`.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  kind:
                    const: invoice
                  status:
                    enum: [open, paid]",
    )
    .expect("3.1 should import");

    let IrTypeShape::Enum { values } = &probe_field_type(&imported, "kind").shape else {
        panic!("a const should import as the enum it is equivalent to");
    };
    assert_eq!(values, &["invoice"]);

    let IrTypeShape::Enum { values } = &probe_field_type(&imported, "status").shape else {
        panic!("enum should still import as an enum");
    };
    assert_eq!(values, &["open", "paid"]);
}

#[test]
fn openapi_31_reads_nullability_out_of_const_and_enum() {
    // Neither of these names a type, so `type` had nothing to say about them
    // and the columns came out non-nullable while the document was constraining
    // them to a value that is null, or to a set that includes it.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  always_null:
                    const: null
                  sometimes_null:
                    enum: [null, open]
                  never_null:
                    enum: [open, paid]",
    )
    .expect("3.1 should import");

    assert!(
        probe_field_type(&imported, "always_null").nullable,
        "a schema constrained to null admits null"
    );
    assert!(
        probe_field_type(&imported, "sometimes_null").nullable,
        "an enum listing null admits null"
    );
    assert!(
        !probe_field_type(&imported, "never_null").nullable,
        "an enum that does not list null still forbids it"
    );

    // The shapes are the other half: deriving nullability must not change what
    // the values themselves import as.
    let IrTypeShape::Enum { values } = &probe_field_type(&imported, "sometimes_null").shape else {
        panic!("an enum listing null is still an enum");
    };
    assert_eq!(values, &["null", "open"]);
    assert!(
        matches!(
            probe_field_type(&imported, "always_null").shape,
            IrTypeShape::Json
        ),
        "a null constant constrains the value, not the shape, and `const_enum_values` \
         already declines to read it as a one-value enum"
    );
}

#[test]
fn openapi_30_leaves_const_alone() {
    // `const` is not a 3.0 keyword, so a 3.0 document using it gets no special
    // reading — the schema is typeless and stays opaque.
    let imported = import_versioned_response_schema(
        "3.0.3",
        r"                type: object
                properties:
                  kind:
                    const: invoice",
    )
    .expect("3.0 should import");

    assert!(
        matches!(probe_field_type(&imported, "kind").shape, IrTypeShape::Json),
        "3.0 has no const, so nothing should read one"
    );
}

#[test]
fn openapi_31_imports_a_realistic_document_exactly_as_30_does() {
    // The point of the dialect split is that only the keywords the versions
    // disagree about are version-specific. Everything else — `$ref` resolution,
    // response selection, wrapped-list row paths, pagination detection,
    // `format: date-time` — is one traversal, so a document using none of the
    // contested keywords has to import identically under both.
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

    let as_30 = import_openapi_surface(
        v4,
        &v4.surface,
        github_openapi_at_version("3.0.3").as_bytes(),
    )
    .expect("3.0 import");
    let as_31 = import_openapi_surface(
        v4,
        &v4.surface,
        github_openapi_at_version("3.1.0").as_bytes(),
    )
    .expect("3.1 import");

    assert!(
        !as_31.operations.is_empty() && !as_31.types.is_empty(),
        "a fixture importing nothing would make this vacuous"
    );

    // Serialized whole rather than compared by id. Names matching says only that
    // both versions found the same operations and types — it would hold just as
    // well if 3.1 gave every one of those types a different shape, different
    // nullability, or different fields, which is exactly the kind of divergence
    // this test exists to catch. The same for the metadata catalog, which
    // carries row paths, pagination, and lookup keys.
    assert_eq!(
        serde_json::to_value(&as_30.semantic_ir).expect("3.0 semantic IR"),
        serde_json::to_value(&as_31.semantic_ir).expect("3.1 semantic IR"),
    );
    assert_eq!(
        serde_json::to_value(&as_30.operation_metadata).expect("3.0 metadata"),
        serde_json::to_value(&as_31.operation_metadata).expect("3.1 metadata"),
    );

    // Pinned rather than merely compared: an inference that silently stopped
    // firing under both versions would satisfy every equality above.
    assert_eq!(
        imported_row_path(&as_31, "search_issues_and_pull_requests"),
        ["items"],
        "the wrapped-list row path has to survive under 3.1"
    );
    assert_eq!(
        imported_rest_pagination(&as_31, "issues_list_for_repo").mode,
        PaginationMode::Page,
        "page pagination has to be detected under 3.1"
    );
}

#[test]
fn openapi_31_stays_quiet_about_a_leftover_nullable_false() {
    // `nullable: false` asked for the default under 3.0 too, so ignoring it
    // changes nothing and the author has lost nothing. Worth pinning because
    // real documents carry these: GitHub's own 3.1 publication has five, and
    // warning on each would be noise on every import of it.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  html_url:
                    type: string
                    nullable: false",
    )
    .expect("3.1 should import");

    let operation = imported.operations.first().expect("operation");
    assert!(
        !operation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("'nullable'")),
        "a no-op keyword is not worth a diagnostic: {:?}",
        operation.diagnostics
    );
}

#[test]
fn openapi_31_leaves_a_structured_const_to_the_shape_dispatch() {
    // `const` may hold any JSON value. An object or array one describes a shape
    // rather than a value, so reading it as an enum would stand the stringified
    // constant where the declared fields belong and drop every column.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  settings:
                    type: object
                    const: {theme: dark}
                    properties:
                      theme: {type: string}
                  tags:
                    type: array
                    const: [a, b]
                    items: {type: string}",
    )
    .expect("3.1 should import");

    let IrTypeShape::Object { fields } = &probe_field_type(&imported, "settings").shape else {
        panic!("an object with a const still declares fields, and they have to survive");
    };
    let names: Vec<&str> = fields.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(names, ["theme"]);

    assert!(
        matches!(
            probe_field_type(&imported, "tags").shape,
            IrTypeShape::List { .. }
        ),
        "an array with a const is still a list"
    );
}

#[test]
fn openapi_31_reads_a_scalar_const_the_way_it_reads_a_one_value_enum() {
    // `const: 4` and `enum: [4]` say the same thing, so they have to import the
    // same way — the newer keyword does not get its own reading of a value.
    let as_const = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  pinned:
                    type: integer
                    const: 4",
    )
    .expect("const import");
    let as_enum = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  pinned:
                    type: integer
                    enum: [4]",
    )
    .expect("enum import");

    let IrTypeShape::Enum { values } = &probe_field_type(&as_const, "pinned").shape else {
        panic!("a scalar const is a single-value enum");
    };
    assert_eq!(values, &["4"]);
    assert_eq!(
        serde_json::to_value(&probe_field_type(&as_enum, "pinned").shape).expect("enum shape"),
        serde_json::to_value(&probe_field_type(&as_const, "pinned").shape).expect("const shape"),
    );
}

#[test]
fn a_const_narrows_the_enum_it_is_declared_beside() {
    // How a 2020-12 document pins one branch of a union: the branch re-declares
    // the discriminator it shares with the rest of the family — `enum` listing
    // every tag — and adds the `const` saying which one it is. Both constrain
    // the schema, so the narrower has to win; reading `enum` first left the
    // branch claiming every tag in the family instead of its own.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  kind:
                    type: string
                    enum: [invoice, receipt, credit_note]
                    const: invoice",
    )
    .expect("3.1 should import");

    let IrTypeShape::Enum { values } = &probe_field_type(&imported, "kind").shape else {
        panic!("a pinned discriminator is still an enum");
    };
    assert_eq!(
        values,
        &["invoice"],
        "the const pins the branch; the enum only says what the family allows"
    );
}

#[test]
fn a_structured_const_leaves_a_neighbouring_enum_to_be_read() {
    // The other side of that ordering. `const_enum_values` reads only scalars,
    // so a structured constant must fall past it to the `enum` arm rather than
    // shadowing it — otherwise moving `const` ahead of `enum` would lose the
    // values a schema declaring both had before.
    let imported = import_versioned_response_schema(
        "3.1.0",
        r"                type: object
                properties:
                  layout:
                    enum: [compact, roomy]
                    const: {theme: dark}",
    )
    .expect("3.1 should import");

    let IrTypeShape::Enum { values } = &probe_field_type(&imported, "layout").shape else {
        panic!("the enum is still the readable constraint here");
    };
    assert_eq!(values, &["compact", "roomy"]);
}
