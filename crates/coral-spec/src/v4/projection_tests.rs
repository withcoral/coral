use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::json;

use super::test_support::github_openapi;
use super::*;
use crate::backends::mcp::McpPaginationSpec;
use crate::{
    ManifestDataType, PaginationMode, SourceTableFunctionKind, parse_source_manifest_yaml,
};

#[test]
fn imports_and_generates_github_issue_slice() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: github
dsl_version: 4
inputs:
  GITHUB_API_BASE:
    kind: variable
    default: https://api.github.com
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: "{{input.GITHUB_API_BASE}}"
"#,
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let published = catalog
        .projections
        .iter()
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
        .map(|projection| projection.name.as_str())
        .collect::<Vec<_>>();
    assert!(published.contains(&"issue"), "{published:?}");
    assert!(published.contains(&"search_issues"), "{published:?}");
    assert!(published.contains(&"get_issues"), "{published:?}");
}

fn items_api_catalog(lookup_keys: Option<(bool, &[&str])>) -> ProjectionCatalog {
    let manifest = parse_source_manifest_yaml(
        r"
name: items_api
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
    let spec = r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: list_items
      parameters:
        - {name: state, in: query, schema: {type: string}}
        - {name: order_by, in: query, schema: {type: string}}
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: object, properties: {id: {type: string}, state: {type: string}}}}}}}}
  /projects/{project_id}/items:
    get:
      operationId: list_project_items
      parameters:
        - {name: project_id, in: path, required: true, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: object, properties: {id: {type: string}}}}}}}}
";
    let mut ir = import_openapi_surface(v4, surface, spec.as_bytes()).expect("import");
    if let Some((enabled, exclude)) = lookup_keys {
        for metadata in ir.operation_metadata.operations.values_mut() {
            if let OperationMetadata::Rest { lookup_keys, .. } = metadata {
                if enabled {
                    lookup_keys.retain(|key| !exclude.iter().any(|excluded| *excluded == key));
                } else {
                    lookup_keys.clear();
                }
            }
        }
    }
    generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog")
}

fn exposure(catalog: &ProjectionCatalog, operation_id: &str, input_name: &str) -> SqlInputExposure {
    catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == operation_id)
        .expect("projection")
        .inputs
        .iter()
        .find(|input| input.name == input_name)
        .expect("input")
        .sql_exposure
}

#[test]
fn lookup_key_allowlist_controls_joinability_not_exposure() {
    let filter_lookup_key = |catalog: &ProjectionCatalog, filter_name: &str| {
        let list_items = catalog
            .projections
            .iter()
            .find(|projection| projection.operation_id == "list_items")
            .expect("projection");
        projection_filter_specs(list_items)
            .iter()
            .find(|spec| spec.name == filter_name)
            .expect("filter spec")
            .lookup_key
    };

    let catalog = items_api_catalog(Some((true, &["order_by", "project_id"])));

    // A parameter omitted from the allowlist keeps its exposure and pushdown;
    // it only loses the dependent-join completeness flag.
    assert_eq!(
        exposure(&catalog, "list_items", "order_by"),
        SqlInputExposure::Filter
    );
    assert!(!filter_lookup_key(&catalog, "order_by"));
    assert_eq!(
        exposure(&catalog, "list_items", "state"),
        SqlInputExposure::Filter
    );
    assert!(filter_lookup_key(&catalog, "state"));

    // Function arguments never carry the flag, allowlisted or not: 'state' is
    // an allowlisted query input on this table function and 'project_id' is a
    // non-allowlisted path input, yet neither is flagged.
    let project_items = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_project_items")
        .expect("projection");
    assert_eq!(
        exposure(&catalog, "list_project_items", "project_id"),
        SqlInputExposure::FunctionArg
    );
    assert_eq!(
        exposure(&catalog, "list_project_items", "state"),
        SqlInputExposure::FunctionArg
    );
    assert!(project_items.inputs.iter().all(|input| !input.lookup_key));

    // Disabling lookup keys withholds the flag surface-wide without touching
    // exposure.
    let catalog = items_api_catalog(Some((false, &[])));
    assert_eq!(
        exposure(&catalog, "list_items", "state"),
        SqlInputExposure::Filter
    );
    assert!(!filter_lookup_key(&catalog, "state"));
    assert!(!filter_lookup_key(&catalog, "order_by"));

    // Generated metadata is present immediately after OpenAPI import, before
    // app materialization writes the operation-metadata artifact.
    let catalog = items_api_catalog(None);
    assert_eq!(
        exposure(&catalog, "list_items", "state"),
        SqlInputExposure::Filter
    );
    assert!(filter_lookup_key(&catalog, "state"));
    assert!(!filter_lookup_key(&catalog, "order_by"));
}

#[test]
fn top_level_scalar_rows_use_scalar_projection_types() {
    let manifest = parse_source_manifest_yaml(
        r"
name: scalar_rows
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
  /strings:
    get:
      operationId: list_strings
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: string}}}}}}
  /integers:
    get:
      operationId: list_integers
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: integer}}}}}}
  /numbers:
    get:
      operationId: list_numbers
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: number}}}}}}
  /booleans:
    get:
      operationId: list_booleans
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: boolean}}}}}}
  /timestamps:
    get:
      operationId: list_timestamps
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: string, format: date-time}}}}}}
".as_bytes(),
    )
    .expect("import");

    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let column_types = catalog
        .projections
        .iter()
        .map(|projection| {
            let [column] = projection.columns.as_slice() else {
                panic!("expected one value column for {}", projection.operation_id);
            };
            assert_eq!(column.name, "value", "{}", projection.operation_id);
            assert!(
                column.source_path.is_empty(),
                "{} should read the scalar row itself",
                projection.operation_id
            );
            (projection.operation_id.as_str(), column.data_type)
        })
        .collect::<HashMap<_, _>>();

    assert_eq!(
        column_types,
        HashMap::from([
            ("list_booleans", ManifestDataType::Boolean),
            ("list_integers", ManifestDataType::Int64),
            ("list_numbers", ManifestDataType::Float64),
            ("list_strings", ManifestDataType::Utf8),
            ("list_timestamps", ManifestDataType::Timestamp),
        ])
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The OpenAPI fixture keeps issue regression cases together."
)]
fn tagged_openapi_operations_use_grouped_operation_names() {
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
      tags: [billing]
      operationId: billing/get-github-billing-ai-credit-usage-report-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Usage'}
  /users/{username}/settings/billing/ai-credit-usage:
    get:
      tags: [billing]
      operationId: billing/get-github-billing-ai-credit-usage-report-user
      parameters:
        - {name: username, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/Usage'}
  /users/{username}/repos:
    get:
      tags: [repos]
      operationId: repos/list-for-user
      parameters:
        - {name: username, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Repository'}
  /users/{username}/subscriptions:
    get:
      tags: [activity]
      operationId: activity/list-repos-watched-by-user
      parameters:
        - {name: username, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Repository'}
  /orgs/{org}/projects/items:
    get:
      tags: [projects]
      operationId: projects/list-items-for-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/ProjectV2ItemWithContent'}
  /users/{username}/projects/items:
    get:
      tags: [projects]
      operationId: projects/list-items-for-user
      parameters:
        - {name: username, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/ProjectV2ItemWithContent'}
  /orgs/{org}/projects/views/{view_id}/items:
    get:
      tags: [projects]
      operationId: projects/list-view-items-for-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
        - {name: view_id, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/ProjectV2ItemWithContent'}
  /users/{username}/projects/views/{view_id}/items:
    get:
      tags: [projects]
      operationId: projects/list-view-items-for-user
      parameters:
        - {name: username, in: path, required: true, schema: {type: string}}
        - {name: view_id, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/ProjectV2ItemWithContent'}
components:
  schemas:
    Usage:
      type: object
      properties:
        total: {type: integer}
    Repository:
      type: object
      properties:
        id: {type: integer}
    ProjectV2ItemWithContent:
      type: object
      properties:
        id: {type: string}
"
        .as_bytes(),
    )
    .expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let names = catalog
        .projections
        .iter()
        .map(|projection| projection.name.as_str())
        .collect::<BTreeSet<_>>();
    let expected = [
        "billing_get_github_billing_ai_credit_usage_report_org",
        "billing_get_github_billing_ai_credit_usage_report_user",
        "repos_list_for_user",
        "activity_list_repos_watched_by_user",
        "projects_list_items_for_org",
        "projects_list_items_for_user",
        "projects_list_view_items_for_org",
        "projects_list_view_items_for_user",
    ];
    for name in expected {
        assert!(names.contains(name), "missing {name}: {names:?}");
    }
    assert!(
        names.iter().all(|name| !name.contains("__")),
        "tag-grouped names should not need hash suffixes: {names:?}"
    );
    assert!(
        catalog
            .diagnostics
            .iter()
            .all(|diagnostic| !diagnostic.message.contains("projection name collision")),
        "tag-grouped names should not need collision diagnostics: {:?}",
        catalog.diagnostics
    );
}

#[test]
fn projection_generation_keeps_pagination_inputs_internal() {
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
    let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "issues_list_for_repo")
        .expect("repo issues projection");
    let operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == projection.operation_id)
        .expect("repo issues operation");

    let plan = ir.validated_plan().expect("plan");
    let pagination = plan.rest_pagination(&operation.id);
    assert_eq!(pagination.mode, PaginationMode::Page);
    assert_eq!(pagination.page_param.as_deref(), Some("page"));
    assert_eq!(
        pagination
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("per_page")
    );

    let exposures = projection
        .inputs
        .iter()
        .map(|input| (input.wire_name.as_str(), input.sql_exposure))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(exposures.get("owner"), Some(&SqlInputExposure::FunctionArg));
    assert_eq!(exposures.get("repo"), Some(&SqlInputExposure::FunctionArg));
    assert_eq!(exposures.get("state"), Some(&SqlInputExposure::FunctionArg));
    assert_eq!(exposures.get("page"), Some(&SqlInputExposure::Internal));
    assert_eq!(exposures.get("per_page"), Some(&SqlInputExposure::Internal));

    let filter_names = projection_filter_specs(projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert!(filter_names.is_empty());

    let arg_names = projection_arg_specs(projection)
        .into_iter()
        .map(|arg| arg.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        arg_names,
        BTreeSet::from(["owner".to_string(), "repo".to_string(), "state".to_string()])
    );

    let column_names = projection_column_specs(projection)
        .into_iter()
        .map(|column| column.name)
        .collect::<BTreeSet<_>>();
    assert!(!column_names.contains("owner"));
    assert!(!column_names.contains("repo"));
    assert!(column_names.contains("state"));
    assert!(!column_names.contains("page"));
    assert!(!column_names.contains("per_page"));

    let request = request_spec_for_projection(projection, operation).expect("request");
    let query_names = request
        .query
        .iter()
        .map(|param| param.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(query_names, BTreeSet::from(["state"]));
    assert!(matches!(
        &request
            .query
            .iter()
            .find(|param| param.name == "state")
            .expect("state query")
            .value,
        crate::ValueSourceSpec::Arg { .. }
    ));
    assert!(
        !projection.guide.contains("paginate"),
        "projection guide should not describe pagination: {}",
        projection.guide
    );
}

#[test]
fn required_header_sharing_pagination_param_name_stays_unsupported() {
    let manifest = parse_source_manifest_yaml(
        r"
name: items_api
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
    let spec = r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: list_items
      parameters:
        - {name: page, in: query, schema: {type: integer}}
        - {name: per_page, in: query, schema: {type: integer}}
        - {name: page, in: header, required: true, schema: {type: string}}
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: object, properties: {id: {type: string}}}}}}}}
";
    let ir = import_openapi_surface(v4, surface, spec.as_bytes()).expect("import");
    let plan = ir.validated_plan().expect("plan");
    let operation = plan
        .semantic_ir()
        .operations
        .iter()
        .find(|operation| operation.id == "list_items")
        .expect("operation");

    let pagination = plan.rest_pagination(&operation.id);
    assert_eq!(pagination.mode, PaginationMode::Page);
    assert_eq!(pagination.page_param.as_deref(), Some("page"));
    assert!(plan.pagination_owns_input(operation, "page", IrInputLocation::Query));
    assert!(!plan.pagination_owns_input(operation, "page", IrInputLocation::Header));

    let catalog = generate_projection_catalog(v4, &plan).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_items")
        .expect("projection");
    assert_eq!(projection.visibility, ProjectionVisibility::Hidden);
    assert!(
        projection.diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .contains("required Header input 'page' cannot be exposed in SQL")),
        "required header sharing the pagination param name must stay unsupported: {:?}",
        projection.diagnostics
    );
}

#[test]
fn optional_header_and_cookie_inputs_stay_published_with_dropped_input_diagnostics() {
    let manifest = parse_source_manifest_yaml(
        r"
name: items_api
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
    let spec = r"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: list_items
      parameters:
        - {name: state, in: query, schema: {type: string}}
        - {name: X-Api-Version, in: header, schema: {type: string}}
        - {name: session, in: cookie, schema: {type: string}}
      responses: {'200': {content: {application/json: {schema: {type: array, items: {type: object, properties: {id: {type: string}}}}}}}}
";
    let ir = import_openapi_surface(v4, surface, spec.as_bytes()).expect("import");
    let plan = ir.validated_plan().expect("plan");
    let catalog = generate_projection_catalog(v4, &plan).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_items")
        .expect("projection");

    assert_eq!(projection.visibility, ProjectionVisibility::Published);
    for dropped in ["Header input 'X-Api-Version'", "Cookie input 'session'"] {
        assert!(
            projection.diagnostics.iter().any(|diagnostic| {
                diagnostic.message.contains(dropped)
                    && diagnostic
                        .message
                        .contains("not sent by generated requests")
            }),
            "dropped optional {dropped} must be diagnosed: {:?}",
            projection.diagnostics
        );
    }
}

#[test]
fn projection_generation_keeps_link_header_page_inputs_internal() {
    let manifest = parse_source_manifest_yaml(
        r"
name: link_pages
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
"
        .as_bytes(),
    )
    .expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "items_list")
        .expect("items projection");
    let operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == projection.operation_id)
        .expect("items operation");

    let plan = ir.validated_plan().expect("plan");
    let pagination = plan.rest_pagination(&operation.id);
    assert_eq!(pagination.mode, PaginationMode::LinkHeader);
    assert_eq!(pagination.page_param.as_deref(), Some("page"));
    assert_eq!(
        pagination
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("per_page")
    );
    assert_eq!(projection.visibility, ProjectionVisibility::Published);

    let exposures = projection
        .inputs
        .iter()
        .map(|input| (input.wire_name.as_str(), input.sql_exposure))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(exposures.get("page"), Some(&SqlInputExposure::Internal));
    assert_eq!(exposures.get("per_page"), Some(&SqlInputExposure::Internal));

    let filter_names = projection_filter_specs(projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert!(filter_names.is_empty());

    let request = request_spec_for_projection(projection, operation).expect("request");
    assert!(request.query.is_empty());
}

#[test]
fn projection_generation_keeps_opaque_link_header_page_tokens_public() {
    let manifest = parse_source_manifest_yaml(
        r"
name: link_page_tokens
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
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "items_list")
        .expect("items projection");
    let operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == projection.operation_id)
        .expect("items operation");

    let plan = ir.validated_plan().expect("plan");
    let pagination = plan.rest_pagination(&operation.id);
    assert_eq!(pagination.mode, PaginationMode::LinkHeader);
    assert_eq!(pagination.page_param, None);
    assert_eq!(
        pagination
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("per_page")
    );
    assert_eq!(projection.visibility, ProjectionVisibility::Published);

    let exposures = projection
        .inputs
        .iter()
        .map(|input| (input.wire_name.as_str(), input.sql_exposure))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(exposures.get("page"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("per_page"), Some(&SqlInputExposure::Internal));

    let filter_names = projection_filter_specs(projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(filter_names, BTreeSet::from(["page".to_string()]));

    let request = request_spec_for_projection(projection, operation).expect("request");
    let query_names = request
        .query
        .iter()
        .map(|param| param.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(query_names, BTreeSet::from(["page"]));
}

#[test]
fn projection_generation_uses_omitted_path_required_for_table_function_args() {
    let manifest = parse_source_manifest_yaml(
        r"
name: path_required
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
        - {name: id, in: path, required: false, schema: {type: string, default: public}}
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
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "items_get")
        .expect("projection");
    assert!(matches!(
        projection.kind,
        ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Table
        }
    ));
    let id_input = projection
        .inputs
        .iter()
        .find(|input| input.wire_name == "id")
        .expect("id input");
    assert_eq!(id_input.sql_exposure, SqlInputExposure::FunctionArg);
    assert!(!id_input.required);
    assert_eq!(id_input.default_value.as_deref(), Some("public"));

    let id_arg = projection_arg_specs(projection)
        .into_iter()
        .find(|arg| arg.name == "id")
        .expect("id arg");
    assert!(!id_arg.required);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The fixture keeps related path default escaping cases together."
)]
fn request_paths_preserve_path_parameter_defaults() {
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
  /tenants/{tenant}/items:
    get:
      operationId: list-items
      parameters:
        - name: tenant
          in: path
          required: true
          schema:
            type: string
            default: '|public}}'
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Item'}
  /search/{tenant}/items:
    get:
      operationId: search-items
      parameters:
        - name: tenant
          in: path
          required: true
          schema:
            type: string
            default: '..'
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Item'}
  /namespaces/{namespace}/items:
    get:
      operationId: list-namespace-items
      parameters:
        - name: namespace
          in: path
          required: true
          schema:
            type: string
            default: '.'
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/Item'}
components:
  schemas:
    Item:
      type: object
      properties:
        id: {type: integer}
"
        .as_bytes(),
    )
    .expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let list_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_items")
        .expect("list projection");
    let list_operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == list_projection.operation_id)
        .expect("list operation");
    let list_request =
        request_spec_for_projection(list_projection, list_operation).expect("list request");
    assert_eq!(
        list_request.path.raw(),
        "/tenants/{{arg.tenant|%7Cpublic%7D%7D}}/items"
    );
    assert!(projection_filter_specs(list_projection).is_empty());
    let list_arg = projection_arg_specs(list_projection)
        .into_iter()
        .find(|arg| arg.name == "tenant")
        .expect("tenant arg");
    assert!(!list_arg.required);

    let search_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "search_items")
        .expect("search projection");
    let search_operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == search_projection.operation_id)
        .expect("search operation");
    let search_request =
        request_spec_for_projection(search_projection, search_operation).expect("search request");
    assert_eq!(
        search_request.path.raw(),
        "/search/{{arg.tenant|%252E%252E}}/items"
    );
    let search_arg = projection_arg_specs(search_projection)
        .into_iter()
        .find(|arg| arg.name == "tenant")
        .expect("tenant arg");
    assert!(!search_arg.required);

    let namespace_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_namespace_items")
        .expect("namespace projection");
    let namespace_operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == namespace_projection.operation_id)
        .expect("namespace operation");
    let namespace_request = request_spec_for_projection(namespace_projection, namespace_operation)
        .expect("namespace request");
    assert_eq!(
        namespace_request.path.raw(),
        "/namespaces/{{arg.namespace|%252E}}/items"
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The OpenAPI fixture keeps related collision cases together."
)]
fn projection_names_use_path_context_for_collisions() {
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
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /orgs/{org}/issues:
    get:
      operationId: issues/list-for-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /repos/{owner}/{repo}/pulls:
    get:
      operationId: pulls/list
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/PullRequestSimple'}}
  /repos/{owner}/{repo}/commits:
    get:
      operationId: repos/list-commits
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Commit'}}
  /repos/{owner}/{repo}/pulls/{pull_number}/commits:
    get:
      operationId: pulls/list-commits
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: pull_number, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Commit'}}
components:
  schemas:
    Issue:
      type: object
      properties:
        id: {type: integer}
    PullRequestSimple:
      type: object
      properties:
        id: {type: integer}
    Commit:
      type: object
      properties:
        sha: {type: string}
"
        .as_bytes(),
    )
    .expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let names_by_operation = catalog
        .projections
        .iter()
        .map(|projection| {
            (
                projection.operation_id.as_str(),
                (projection.name.as_str(), &projection.kind),
            )
        })
        .collect::<HashMap<_, _>>();

    let issues_list = names_by_operation
        .get("issues_list")
        .expect("issues_list projection");
    assert_eq!(issues_list.0, "issue");
    let org_issues = names_by_operation
        .get("issues_list_for_org")
        .expect("issues_list_for_org projection");
    assert_eq!(org_issues.0, "orgs_issue");
    let repo_issues = names_by_operation
        .get("issues_list_for_repo")
        .expect("issues_list_for_repo projection");
    assert_eq!(repo_issues.0, "repos_issue");
    let pulls = names_by_operation
        .get("pulls_list")
        .expect("pulls_list projection");
    assert_eq!(pulls.0, "pull_request");
    assert!(matches!(
        pulls.1,
        ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Table
        }
    ));
    let commits = names_by_operation
        .get("repos_list_commits")
        .expect("repos_list_commits projection");
    assert_eq!(commits.0, "commit");
    let pull_commits = names_by_operation
        .get("pulls_list_commits")
        .expect("pulls_list_commits projection");
    assert_eq!(pull_commits.0, "repos_pulls_commit");
    let pull_commits_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "pulls_list_commits")
        .expect("pull commits projection");
    let pull_number_arg = projection_arg_specs(pull_commits_projection)
        .into_iter()
        .find(|arg| arg.name == "pull_number")
        .expect("pull_number arg");
    assert_eq!(pull_number_arg.data_type, ManifestDataType::Int64);

    let catalog_collision_diagnostics = catalog
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.message.contains("projection name collision"))
        .collect::<Vec<_>>();
    assert_eq!(catalog_collision_diagnostics.len(), 3);
    let projection_collision_diagnostics = catalog
        .projections
        .iter()
        .flat_map(|projection| &projection.diagnostics)
        .filter(|diagnostic| diagnostic.message.contains("projection name collision"))
        .count();
    assert_eq!(
        projection_collision_diagnostics,
        catalog_collision_diagnostics.len()
    );
}

fn mcp_manifest() -> crate::ValidatedSourceManifest {
    parse_source_manifest_yaml(
        r"
name: github_mcp
dsl_version: 4
surface:
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect("manifest")
}

fn search_issues_mcp_catalog() -> McpToolCatalog {
    McpToolCatalog {
        tools: vec![McpToolDescriptor {
            name: "search_issues".to_string(),
            title: None,
            description: None,
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"}
                },
                "required": ["query"]
            }),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer"},
                                "title": {"type": "string"}
                            }
                        }
                    }
                }
            })),
            read_only_hint: Some(true),
        }],
    }
}

#[test]
fn rest_projection_input_names_keep_legacy_normalization() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.example.com
",
    )
    .expect("manifest");
    let openapi = r"
openapi: 3.0.3
paths:
  /issues:
    get:
      operationId: issues/search
      parameters:
        - name: perPage
          in: query
          schema: { type: integer }
        - name: pullNumber
          in: query
          required: true
          schema: { type: integer }
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: { type: integer }
";
    let v4 = manifest.as_v4().expect("v4");
    let surface = &v4.surface;
    let ir = import_openapi_surface(v4, surface, openapi.as_bytes()).expect("import");
    let catalog =
        generate_projection_catalog(v4, &ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog.projections.first().expect("projection");
    let sql_name_by_wire = projection
        .inputs
        .iter()
        .map(|input| (input.wire_name.as_str(), input.name.as_str()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(sql_name_by_wire.get("perPage"), Some(&"perpage"));
    assert_eq!(sql_name_by_wire.get("pullNumber"), Some(&"pullnumber"));
}

#[test]
fn generated_mcp_projection_exposes_current_row_result_columns() {
    let manifest = mcp_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = &v4.surface;
    let mcp_ir =
        import_mcp_surface(v4, mcp_surface, &search_issues_mcp_catalog()).expect("mcp import");

    let catalog =
        generate_projection_catalog(v4, &mcp_ir.validated_plan().expect("plan")).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "search_issues")
        .expect("mcp search projection");

    let columns = projection
        .columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                column.data_type,
                column.source_path.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        columns,
        vec![
            ("result".to_string(), ManifestDataType::Utf8, Vec::new()),
            (
                "result_json".to_string(),
                ManifestDataType::Json,
                Vec::new()
            )
        ]
    );
}

#[test]
fn generated_mcp_projection_keeps_an_inferred_cursor_internal() {
    let manifest = mcp_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = &v4.surface;
    let catalog = McpToolCatalog {
        tools: vec![McpToolDescriptor {
            name: "list_items".to_string(),
            title: None,
            description: None,
            input_schema: json!({
                "type": "object",
                "properties": {
                    "cursor": {"type": "string"},
                    "query": {"type": "string"}
                },
                "required": ["query"]
            }),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"}
                            }
                        }
                    },
                    "meta": {
                        "type": "object",
                        "properties": {
                            "nextCursor": {"type": ["string", "null"]}
                        }
                    }
                }
            })),
            read_only_hint: Some(true),
        }],
    };
    let mcp_ir = import_mcp_surface(v4, mcp_surface, &catalog).expect("mcp import");

    let projections =
        generate_projection_catalog(v4, &mcp_ir.validated_plan().expect("plan")).expect("catalog");
    let projection = projections
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_items")
        .expect("mcp projection");
    let cursor = projection
        .inputs
        .iter()
        .find(|input| input.wire_name == "cursor")
        .expect("cursor input");

    // Pagination owns the cursor of a wrapped-list tool, so SQL never binds it.
    assert_eq!(cursor.sql_exposure, SqlInputExposure::Internal);
    assert!(
        mcp_projection_arg_specs(projection)
            .iter()
            .all(|arg| arg.bind.arg != "cursor")
    );
}

#[test]
fn generated_mcp_projection_with_only_an_inferred_cursor_is_a_table() {
    let manifest = mcp_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = &v4.surface;
    let catalog = McpToolCatalog {
        tools: vec![McpToolDescriptor {
            name: "list_items".to_string(),
            title: None,
            description: None,
            input_schema: json!({
                "type": "object",
                "properties": {
                    "cursor": {"type": "string"}
                }
            }),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"}
                            }
                        }
                    },
                    "meta": {
                        "type": "object",
                        "properties": {
                            "nextCursor": {"type": ["string", "null"]}
                        }
                    }
                }
            })),
            read_only_hint: Some(true),
        }],
    };
    let mcp_ir = import_mcp_surface(v4, mcp_surface, &catalog).expect("mcp import");

    let projections =
        generate_projection_catalog(v4, &mcp_ir.validated_plan().expect("plan")).expect("catalog");
    let projection = projections
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_items")
        .expect("mcp projection");

    // The cursor is the tool's only argument and pagination owns it, so the
    // projection has nothing left to take as a function argument.
    assert!(matches!(projection.kind, ProjectionKind::Table));
    assert_eq!(
        projection
            .inputs
            .iter()
            .filter(|input| input.sql_exposure != SqlInputExposure::Internal)
            .count(),
        0
    );
}

#[test]
fn generated_mcp_projection_snake_cases_camel_input_names() {
    let manifest = mcp_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = &v4.surface;
    let catalog = McpToolCatalog {
        tools: vec![McpToolDescriptor {
            name: "pull_request_read".to_string(),
            title: None,
            description: None,
            input_schema: json!({
                "type": "object",
                "properties": {
                    "perPage": {"type": "number"},
                    "pullNumber": {"type": "number"},
                    "alertNumber": {"type": "number"},
                    "discussionNumber": {"type": "number"},
                    "ghsaId": {"type": "string"},
                    "notificationID": {"type": "string"}
                },
                "required": ["pullNumber"]
            }),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "title": {"type": "string"}
                }
            })),
            read_only_hint: Some(true),
        }],
    };
    let mcp_ir = import_mcp_surface(v4, mcp_surface, &catalog).expect("mcp import");

    let projections =
        generate_projection_catalog(v4, &mcp_ir.validated_plan().expect("plan")).expect("catalog");
    let projection = projections
        .projections
        .iter()
        .find(|projection| projection.operation_id == "pull_request_read")
        .expect("mcp projection");
    let sql_name_by_wire = projection
        .inputs
        .iter()
        .map(|input| (input.wire_name.as_str(), input.name.as_str()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(sql_name_by_wire.get("perPage"), Some(&"per_page"));
    assert_eq!(sql_name_by_wire.get("pullNumber"), Some(&"pull_number"));
    assert_eq!(sql_name_by_wire.get("alertNumber"), Some(&"alert_number"));
    assert_eq!(
        sql_name_by_wire.get("discussionNumber"),
        Some(&"discussion_number")
    );
    assert_eq!(sql_name_by_wire.get("ghsaId"), Some(&"ghsa_id"));
    assert_eq!(
        sql_name_by_wire.get("notificationID"),
        Some(&"notification_id")
    );
}

fn imported_items_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: per_page, in: query, schema: {type: integer, default: 25, maximum: 100}}
        - {name: state, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {type: object}}
",
    )
    .expect("import");
    (manifest, imported)
}

fn imported_mcp_items_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = mcp_manifest().as_v4().expect("v4").clone();
    let catalog = McpToolCatalog {
        tools: vec![McpToolDescriptor {
            name: "list_items".to_string(),
            title: None,
            description: None,
            input_schema: json!({
                "type": "object",
                "properties": {
                    "cursor": {"type": "string"},
                    "query": {"type": "string"}
                }
            }),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"id": {"type": "string"}}
                        }
                    },
                    "meta": {
                        "type": "object",
                        "properties": {"nextCursor": {"type": ["string", "null"]}}
                    }
                }
            })),
            read_only_hint: Some(true),
        }],
    };
    let mut imported =
        import_mcp_surface(&manifest, &manifest.surface, &catalog).expect("MCP import");
    let OperationMetadata::Mcp { pagination, .. } = imported
        .operation_metadata
        .operations
        .get_mut("list_items")
        .expect("list_items metadata")
    else {
        panic!("expected MCP metadata");
    };
    pagination.cursor = Some(McpPaginationSpec {
        cursor_arg: "cursor".to_string(),
        response_cursor_path: vec!["meta".to_string(), "nextCursor".to_string()],
        max_pages: None,
    });
    (manifest, imported)
}

fn imported_wrapped_items_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  total_count: {type: integer}
                  items:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                        title: {type: string}
",
    )
    .expect("import");
    (manifest, imported)
}

/// An envelope offering row paths of three different shapes: `items` yields
/// objects, `tags` yields scalars, and `blobs` yields rows whose item type the
/// importer could not resolve.
fn imported_mixed_row_shapes_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  total_count: {type: integer}
                  tags:
                    type: array
                    items: {type: string}
                  blobs:
                    type: array
                    items: {$ref: '#/components/schemas/Missing'}
                  items:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                        title: {type: string}
",
    )
    .expect("import");
    (manifest, imported)
}

/// Rows whose fields cover every `IrTypeShape` a nested source path can be
/// walked through: an object, a list, a map, an opaque payload, and a scalar.
fn imported_nested_row_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  total_count: {type: integer}
                  items:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: string}
                        owner:
                          type: object
                          properties:
                            login: {type: string}
                            '0': {type: string}
                        tags:
                          type: array
                          items: {type: string}
                        extra: {}
                        labels:
                          type: object
                          additionalProperties: {type: string}
",
    )
    .expect("import");
    (manifest, imported)
}

/// Replaces the catalog's columns with a single column reading `source_path`,
/// standing in for a hand-authored projection override.
fn compatibility_of_source_path(
    catalog: &ProjectionCatalog,
    source_path: &[&str],
) -> ProjectionCatalog {
    let mut catalog = catalog.clone();
    let projection = catalog.projections.first_mut().expect("projection");
    projection.columns = vec![ProjectionColumn {
        name: "probe".to_string(),
        data_type: ManifestDataType::Json,
        source_path: source_path
            .iter()
            .map(|segment| (*segment).to_string())
            .collect(),
        nullable: true,
        description: String::new(),
        do_not_index: false,
    }];
    catalog
}

fn override_row_path(imported: &mut ImportedSurface, operation_id: &str, path: &[&str]) {
    let OperationMetadata::Rest { row_path, .. } = imported
        .operation_metadata
        .operations
        .get_mut(operation_id)
        .expect("operation metadata")
    else {
        panic!("expected REST metadata");
    };
    *row_path = path.iter().map(|segment| (*segment).to_string()).collect();
}

fn imported_required_account_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: account, in: query, required: true, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {type: object}}
",
    )
    .expect("import");
    (manifest, imported)
}

fn imported_shadowed_header_surface() -> (V4SourceManifest, ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: state, in: query, schema: {type: string}}
        - {name: state, in: header, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {type: object}}
",
    )
    .expect("import");
    (manifest, imported)
}

fn projection_input_mut<'a>(
    catalog: &'a mut ProjectionCatalog,
    wire_name: &str,
) -> &'a mut ProjectionInput {
    catalog
        .projections
        .first_mut()
        .expect("projection")
        .inputs
        .iter_mut()
        .find(|input| input.wire_name == wire_name)
        .expect("projection input")
}

fn projection_input_at_mut<'a>(
    catalog: &'a mut ProjectionCatalog,
    wire_name: &str,
    location: IrInputLocation,
) -> &'a mut ProjectionInput {
    catalog
        .projections
        .first_mut()
        .expect("projection")
        .inputs
        .iter_mut()
        .find(|input| input.wire_name == wire_name && input.source_location == location)
        .expect("projection input")
}

#[test]
fn generated_projection_columns_come_from_the_wrapped_list_row_type() {
    let (manifest, imported) = imported_wrapped_items_surface();
    let plan = imported.validated_plan().expect("plan");
    assert_eq!(plan.output_row_path("items_list"), ["items"]);

    let catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    let projection = catalog.projections.first().expect("projection");
    assert_eq!(
        projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "title"]
    );
}

#[test]
fn projection_compatibility_rejects_columns_the_effective_row_path_cannot_yield() {
    let (manifest, mut imported) = imported_wrapped_items_surface();
    let catalog = generate_projection_catalog(&manifest, &imported.validated_plan().expect("plan"))
        .expect("projections");

    // Overriding the row path back to the envelope root leaves the snapshot's
    // columns describing rows the operation no longer yields.
    let OperationMetadata::Rest { row_path, .. } = imported
        .operation_metadata
        .operations
        .get_mut("items_list")
        .expect("items_list metadata")
    else {
        panic!("expected REST metadata");
    };
    row_path.clear();
    let plan = imported.validated_plan().expect("plan");

    let error =
        validate_projection_compatibility(&plan, &catalog).expect_err("stale columns must fail");

    assert!(
        error.to_string().contains("column 'id' reads field 'id'"),
        "unexpected error: {error}"
    );
}

/// A row path may legitimately select an array of scalars, so the plan accepts
/// the override. The snapshot's field columns cannot survive it: a source path
/// against a string row resolves to null on every row rather than failing.
#[test]
fn projection_compatibility_rejects_field_columns_when_the_rows_are_scalars() {
    let (manifest, mut imported) = imported_mixed_row_shapes_surface();
    let catalog = generate_projection_catalog(&manifest, &imported.validated_plan().expect("plan"))
        .expect("projections");
    override_row_path(&mut imported, "items_list", &["tags"]);
    let plan = imported.validated_plan().expect("plan");

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("field columns must not survive a scalar row path");

    assert!(
        error
            .to_string()
            .contains("is not an object and names no fields"),
        "unexpected error: {error}"
    );
}

/// The same hole one step earlier: a row type the semantic IR carries no entry
/// for names no fields either, and an unresolved item type reaches exactly that
/// state through the `json` sentinel.
#[test]
fn projection_compatibility_rejects_field_columns_when_the_row_type_is_absent() {
    let (manifest, mut imported) = imported_mixed_row_shapes_surface();
    let catalog = generate_projection_catalog(&manifest, &imported.validated_plan().expect("plan"))
        .expect("projections");
    override_row_path(&mut imported, "items_list", &["blobs"]);
    let plan = imported.validated_plan().expect("plan");
    assert_eq!(plan.rest_output_type_ref("items_list"), "json");

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("field columns must not survive an opaque row path");

    assert!(
        error
            .to_string()
            .contains("is not an object and names no fields"),
        "unexpected error: {error}"
    );
}

/// The rule is about source paths, not row shapes: a catalog generated for
/// non-object rows projects them whole, and stays compatible.
#[test]
fn projection_compatibility_accepts_whole_row_columns_for_non_object_rows() {
    for row_path in [["tags"], ["blobs"]] {
        let (manifest, mut imported) = imported_mixed_row_shapes_surface();
        override_row_path(&mut imported, "items_list", &row_path);
        let plan = imported.validated_plan().expect("plan");
        let catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
        assert!(
            catalog
                .projections
                .iter()
                .flat_map(|projection| &projection.columns)
                .all(|column| column.source_path.is_empty()),
            "non-object rows are projected whole"
        );

        validate_projection_compatibility(&plan, &catalog)
            .expect("a catalog generated for these rows is compatible with them");
    }
}

/// The generator never nests a source path, but an authored override may, and
/// runtime follows every segment. Each case is the shape reached *before* the
/// final segment deciding whether that segment is selectable.
#[test]
fn projection_compatibility_walks_every_segment_of_a_nested_source_path() {
    let (manifest, imported) = imported_nested_row_surface();
    let plan = imported.validated_plan().expect("plan");
    let generated = generate_projection_catalog(&manifest, &plan).expect("projections");

    for (source_path, expected) in [
        // An object names its fields, so a real one resolves and a bogus one
        // cannot.
        (&["owner", "login"][..], Ok(())),
        (
            &["owner", "nope"][..],
            Err("type 'items_list_row_items_item_owner' has no field 'nope'"),
        ),
        // A scalar has nothing below it at all.
        (
            &["id", "nope"][..],
            Err("names no fields, so segment 'nope' cannot be selected"),
        ),
        // `get_path_value` indexes arrays numerically, and only numerically.
        (&["tags", "0"][..], Ok(())),
        (
            &["tags", "name"][..],
            Err("is a list, so segment 'name' must be a numeric index"),
        ),
        // A map admits any key, and an opaque payload admits anything at all.
        (&["labels", "any_key"][..], Ok(())),
        (&["extra", "anything", "deep"][..], Ok(())),
        // ...but not a numeric one: runtime reads that as an array index, so it
        // selects nothing however the payload is shaped. `owner` really does
        // declare a field named `0`, so this is the numeric rule rejecting it
        // rather than the missing-field rule.
        (
            &["owner", "0"][..],
            Err("is read as an array index and so selects nothing"),
        ),
        (
            &["labels", "0"][..],
            Err("is read as an array index and so selects nothing"),
        ),
        // The first segment is still checked as before.
        (&["nope", "deeper"][..], Err("which has no such field")),
    ] {
        let catalog = compatibility_of_source_path(&generated, source_path);
        let outcome = validate_projection_compatibility(&plan, &catalog);
        match expected {
            Ok(()) => {
                outcome.unwrap_or_else(|error| {
                    panic!("source path {source_path:?} should be compatible: {error}")
                });
            }
            Err(fragment) => {
                let error =
                    outcome.expect_err(&format!("source path {source_path:?} should be rejected"));
                assert!(
                    error.to_string().contains(fragment),
                    "source path {source_path:?}: unexpected error: {error}"
                );
            }
        }
    }
}

#[test]
fn projection_compatibility_rejects_missing_operation() {
    let (manifest, imported) = imported_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    let projection = catalog.projections.first_mut().expect("projection");
    projection.operation_id = "items/missing".to_string();
    let projection_name = projection.name.clone();

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("missing operation must fail");

    assert_eq!(
        error.to_string(),
        format!("projection '{projection_name}' references missing operation 'items/missing'")
    );
}

#[test]
fn projection_compatibility_rejects_input_missing_from_operation() {
    let (manifest, imported) = imported_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    let projection = catalog.projections.first_mut().expect("projection");
    let mut stale = projection.inputs.first().expect("input").clone();
    stale.name = "stale".to_string();
    stale.wire_name = "renamed_upstream".to_string();
    projection.inputs.push(stale);

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("stale projection input must fail");

    assert!(
        error
            .to_string()
            .contains("input 'stale' does not match a Query input named 'renamed_upstream'"),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_rejects_public_rest_pagination_input() {
    let (manifest, imported) = imported_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    projection_input_mut(&mut catalog, "page").sql_exposure = SqlInputExposure::Filter;

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("public pagination input must fail");

    assert!(
        error.to_string().contains(
            "input 'page' on operation 'items_list' is owned by pagination but has sql_exposure 'filter'"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_rejects_public_mcp_pagination_input() {
    let (manifest, imported) = imported_mcp_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    projection_input_mut(&mut catalog, "cursor").sql_exposure = SqlInputExposure::FunctionArg;

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("public MCP pagination input must fail");

    assert!(
        error.to_string().contains(
            "input 'cursor' on operation 'list_items' is owned by pagination but has sql_exposure 'function_arg'"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_accepts_internal_pagination_input() {
    let (manifest, imported) = imported_mcp_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let catalog = generate_projection_catalog(&manifest, &plan).expect("projections");

    assert_eq!(
        catalog
            .projections
            .first()
            .expect("projection")
            .inputs
            .iter()
            .find(|input| input.wire_name == "cursor")
            .expect("cursor")
            .sql_exposure,
        SqlInputExposure::Internal
    );
    validate_projection_compatibility(&plan, &catalog)
        .expect("internal pagination input must remain compatible");
}

#[test]
fn projection_compatibility_rejects_unauthorised_lookup_key() {
    let (manifest, mut imported) = imported_items_surface();
    let OperationMetadata::Rest { lookup_keys, .. } = imported
        .operation_metadata
        .operations
        .get_mut("items_list")
        .expect("items_list metadata")
    else {
        panic!("expected REST metadata");
    };
    lookup_keys.clear();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    projection_input_mut(&mut catalog, "state").lookup_key = true;

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("unauthorised lookup key must fail");

    assert!(
        error
            .to_string()
            .contains("wire input 'state' is not authorised"),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_rejects_non_filter_lookup_key() {
    let (manifest, imported) = imported_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    let input = projection_input_mut(&mut catalog, "state");
    input.sql_exposure = SqlInputExposure::FunctionArg;
    assert!(input.lookup_key, "state should be an inferred lookup key");

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("non-filter lookup key must fail");

    assert!(
        error
            .to_string()
            .contains("lookup_key=true with sql_exposure 'function_arg'"),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_rejects_non_rest_lookup_key() {
    let (manifest, imported) = imported_mcp_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    projection_input_mut(&mut catalog, "query").lookup_key = true;

    let error =
        validate_projection_compatibility(&plan, &catalog).expect_err("MCP lookup key must fail");

    assert!(
        error
            .to_string()
            .contains("lookup keys are only valid for REST inputs"),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_accepts_conservative_choices_without_mutation() {
    let (manifest, imported) = imported_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    let projection = catalog.projections.first_mut().expect("projection");
    projection.name = "authored_items".to_string();
    projection.guide = "Keep this guide".to_string();
    let input = projection
        .inputs
        .iter_mut()
        .find(|input| input.wire_name == "state")
        .expect("state");
    input.sql_exposure = SqlInputExposure::Internal;
    input.lookup_key = false;
    let before = serde_yaml::to_string(&catalog).expect("serialize before validation");

    validate_projection_compatibility(&plan, &catalog).expect("conservative policy is compatible");

    let after = serde_yaml::to_string(&catalog).expect("serialize after validation");
    assert_eq!(after, before);
}

#[test]
fn projection_compatibility_rejects_published_required_internal_input() {
    let (manifest, imported) = imported_required_account_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    assert_eq!(
        catalog.projections.first().expect("projection").visibility,
        ProjectionVisibility::Published,
        "generator should publish a projection whose required input is exposable"
    );
    projection_input_mut(&mut catalog, "account").sql_exposure = SqlInputExposure::Internal;

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("published projection must not internalize a required input");

    assert!(
        error.to_string().contains(
            "input 'account' on operation 'items_list' is required by the operation but has sql_exposure 'internal'"
        ),
        "unexpected error: {error}"
    );

    // Hiding the projection is the generator's own escape hatch for a required
    // input that SQL cannot bind, so it stays compatible.
    catalog
        .projections
        .first_mut()
        .expect("projection")
        .visibility = ProjectionVisibility::Hidden;
    validate_projection_compatibility(&plan, &catalog)
        .expect("hidden projections may internalize required inputs");
}

#[test]
fn projection_compatibility_rejects_exposure_mismatched_with_projection_kind() {
    let (manifest, imported) = imported_items_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    assert!(matches!(
        catalog.projections.first().expect("projection").kind,
        ProjectionKind::Table
    ));
    let state = projection_input_mut(&mut catalog, "state");
    state.sql_exposure = SqlInputExposure::FunctionArg;
    state.lookup_key = false;

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("function argument on a table must fail");

    assert!(
        error.to_string().contains(
            "input 'state' on operation 'items_list' has sql_exposure 'function_arg'; table projections expose non-internal inputs as filters"
        ),
        "unexpected error: {error}"
    );

    let (tool_manifest, tool_imported) = imported_mcp_items_surface();
    let tool_plan = tool_imported.validated_plan().expect("plan");
    let mut tool_catalog =
        generate_projection_catalog(&tool_manifest, &tool_plan).expect("projections");
    assert!(matches!(
        tool_catalog.projections.first().expect("projection").kind,
        ProjectionKind::TableFunction { .. }
    ));
    projection_input_mut(&mut tool_catalog, "query").sql_exposure = SqlInputExposure::Filter;

    let error = validate_projection_compatibility(&tool_plan, &tool_catalog)
        .expect_err("filter on a table function must fail");

    assert!(
        error.to_string().contains(
            "input 'query' on operation 'list_items' has sql_exposure 'filter'; table function projections expose non-internal inputs as function arguments"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_rejects_non_query_lookup_key() {
    let (manifest, imported) = imported_shadowed_header_surface();
    let plan = imported.validated_plan().expect("plan");
    let mut catalog = generate_projection_catalog(&manifest, &plan).expect("projections");
    assert!(
        plan.input_is_lookup_key("items_list", "state"),
        "the query input named 'state' should be allowlisted"
    );
    let header = projection_input_at_mut(&mut catalog, "state", IrInputLocation::Header);
    header.sql_exposure = SqlInputExposure::Filter;
    header.lookup_key = true;

    let error = validate_projection_compatibility(&plan, &catalog)
        .expect_err("header lookup key sharing an allowlisted query name must fail");

    assert!(
        error.to_string().contains(
            "has lookup_key=true with source location Header; lookup keys are only valid for query inputs"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn projection_compatibility_accepts_empty_operation_and_projection_catalogs() {
    let (manifest, mut imported) = imported_items_surface();
    imported.semantic_ir.operations.clear();
    imported.operation_metadata.operations.clear();
    let plan = imported.validated_plan().expect("empty plan");
    let catalog = generate_projection_catalog(&manifest, &plan).expect("empty projections");

    assert!(catalog.projections.is_empty());
    validate_projection_compatibility(&plan, &catalog)
        .expect("empty operation and projection catalogs are compatible");
}
