use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::naming::{pluralize, singularize};
use super::test_support::github_openapi;
use super::*;
use crate::{
    ManifestDataType, PaginationMode, SourceTableFunctionKind, parse_source_manifest_yaml,
};

#[test]
fn imports_and_generates_github_issue_slice() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      GITHUB_API_BASE:
        kind: variable
        default: https://api.github.com
    base_url: "{{input.GITHUB_API_BASE}}"
"#,
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
    let published = catalog
        .projections
        .iter()
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
        .map(|projection| projection.name.as_str())
        .collect::<Vec<_>>();
    assert!(published.contains(&"issues"), "{published:?}");
    assert!(published.contains(&"search_issues"), "{published:?}");
    assert!(published.contains(&"get_issue"), "{published:?}");
}

#[test]
fn top_level_scalar_rows_use_scalar_projection_types() {
    let manifest = parse_source_manifest_yaml(
        r"
name: scalar_rows
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

    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
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
    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
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
            .all(|diagnostic| diagnostic.code != "PROJECTION_NAME_COLLISION_RESOLVED"),
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
    let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
    let catalog = generate_projection_catalog(v4, std::slice::from_ref(&ir)).expect("catalog");
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

    let IrExecutionAttachment::Rest(rest) = &operation.execution else {
        panic!("expected REST execution");
    };
    assert_eq!(rest.pagination.mode, PaginationMode::Page);
    assert_eq!(rest.pagination.page_param.as_deref(), Some("page"));
    assert_eq!(
        rest.pagination
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
fn projection_generation_uses_omitted_path_required_for_table_function_args() {
    let manifest = parse_source_manifest_yaml(
        r"
name: path_required
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
    let catalog = generate_projection_catalog(v4, std::slice::from_ref(&ir)).expect("catalog");
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
    let catalog = generate_projection_catalog(v4, std::slice::from_ref(&ir)).expect("catalog");
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
    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
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
    assert_eq!(issues_list.0, "issues");
    let org_issues = names_by_operation
        .get("issues_list_for_org")
        .expect("issues_list_for_org projection");
    assert_eq!(org_issues.0, "orgs_issues");
    let repo_issues = names_by_operation
        .get("issues_list_for_repo")
        .expect("issues_list_for_repo projection");
    assert_eq!(repo_issues.0, "repos_issues");
    let pulls = names_by_operation
        .get("pulls_list")
        .expect("pulls_list projection");
    assert_eq!(pulls.0, "pull_requests");
    assert!(matches!(
        pulls.1,
        ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Table
        }
    ));
    let commits = names_by_operation
        .get("repos_list_commits")
        .expect("repos_list_commits projection");
    assert_eq!(commits.0, "commits");
    let pull_commits = names_by_operation
        .get("pulls_list_commits")
        .expect("pulls_list_commits projection");
    assert_eq!(pull_commits.0, "repos_pulls_commits");
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
        .filter(|diagnostic| diagnostic.code == "PROJECTION_NAME_COLLISION_RESOLVED")
        .collect::<Vec<_>>();
    assert_eq!(catalog_collision_diagnostics.len(), 3);
    let projection_collision_diagnostics = catalog
        .projections
        .iter()
        .flat_map(|projection| &projection.diagnostics)
        .filter(|diagnostic| diagnostic.code == "PROJECTION_NAME_COLLISION_RESOLVED")
        .count();
    assert_eq!(
        projection_collision_diagnostics,
        catalog_collision_diagnostics.len()
    );
}

fn rest_mcp_collision_manifest() -> crate::ValidatedSourceManifest {
    parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect("manifest")
}

fn two_rest_collision_manifest() -> crate::ValidatedSourceManifest {
    parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surfaces:
  - id: rest_primary
    namespace_suffix: rest_primary
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
  - id: rest_secondary
    namespace_suffix: rest_secondary
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest")
}

fn rest_search_openapi() -> &'static [u8] {
    r"
openapi: 3.0.3
paths:
  /search/issues:
    get:
      operationId: issues/search
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  items:
                    type: array
                    items:
                      type: object
                      properties:
                        id: {type: integer}
                        title: {type: string}
"
    .as_bytes()
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
surfaces:
  - id: rest
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
    let surface = v4.surfaces.first().expect("surface");
    let ir = import_openapi_surface(v4, surface, openapi.as_bytes()).expect("import");
    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
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
fn different_surface_namespaces_keep_colliding_projection_names() {
    let manifest = rest_mcp_collision_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let rest_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "rest")
        .expect("rest surface");
    let mcp_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "mcp")
        .expect("mcp surface");
    let rest_ir =
        import_openapi_surface(v4, rest_surface, rest_search_openapi()).expect("rest import");
    let mcp_ir =
        import_mcp_surface(v4, mcp_surface, &search_issues_mcp_catalog()).expect("mcp import");

    let catalog = generate_projection_catalog(v4, &[rest_ir, mcp_ir]).expect("catalog");
    let rest_projection = catalog
        .projections
        .iter()
        .find(|projection| {
            projection.surface_id == "rest" && projection.operation_id == "issues_search"
        })
        .expect("rest search projection");
    assert_eq!(rest_projection.name, "search_issues");
    assert_eq!(rest_projection.namespace, "github_rest");

    let mcp_projection = catalog
        .projections
        .iter()
        .find(|projection| {
            projection.surface_id == "mcp" && projection.operation_id == "search_issues"
        })
        .expect("mcp search projection");
    assert_eq!(mcp_projection.name, "search_issues");
    assert_eq!(mcp_projection.namespace, "github_mcp");

    assert!(
        !catalog
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "PROJECTION_NAME_COLLISION_RESOLVED")
    );
}

#[test]
fn generated_mcp_projection_exposes_current_row_result_columns() {
    let manifest = rest_mcp_collision_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "mcp")
        .expect("mcp surface");
    let mcp_ir =
        import_mcp_surface(v4, mcp_surface, &search_issues_mcp_catalog()).expect("mcp import");

    let catalog = generate_projection_catalog(v4, &[mcp_ir]).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| {
            projection.surface_id == "mcp" && projection.operation_id == "search_issues"
        })
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
fn generated_mcp_projection_keeps_pagination_cursor_internal() {
    let manifest = rest_mcp_collision_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "mcp")
        .expect("mcp surface");
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

    let projections = generate_projection_catalog(v4, &[mcp_ir]).expect("catalog");
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

    assert_eq!(cursor.sql_exposure, SqlInputExposure::Internal);
    assert!(
        mcp_projection_arg_specs(projection)
            .iter()
            .all(|arg| arg.bind.arg != "cursor")
    );
}

#[test]
fn generated_mcp_projection_with_only_pagination_cursor_is_table() {
    let manifest = rest_mcp_collision_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "mcp")
        .expect("mcp surface");
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

    let projections = generate_projection_catalog(v4, &[mcp_ir]).expect("catalog");
    let projection = projections
        .projections
        .iter()
        .find(|projection| projection.operation_id == "list_items")
        .expect("mcp projection");

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
    let manifest = rest_mcp_collision_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let mcp_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "mcp")
        .expect("mcp surface");
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

    let projections = generate_projection_catalog(v4, &[mcp_ir]).expect("catalog");
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

#[test]
fn same_type_surface_namespaces_keep_colliding_projection_names() {
    let manifest = two_rest_collision_manifest();
    let v4 = manifest.as_v4().expect("v4");
    let primary_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "rest_primary")
        .expect("primary rest surface");
    let secondary_surface = v4
        .surfaces
        .iter()
        .find(|surface| surface.id == "rest_secondary")
        .expect("secondary rest surface");
    let primary_ir = import_openapi_surface(v4, primary_surface, rest_search_openapi())
        .expect("primary rest import");
    let secondary_ir = import_openapi_surface(v4, secondary_surface, rest_search_openapi())
        .expect("secondary rest import");

    let catalog = generate_projection_catalog(v4, &[primary_ir, secondary_ir]).expect("catalog");
    let primary_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.surface_id == "rest_primary")
        .expect("primary projection");
    assert_eq!(primary_projection.name, "search_issues");
    assert_eq!(primary_projection.namespace, "github_rest_primary");

    let secondary_projection = catalog
        .projections
        .iter()
        .find(|projection| projection.surface_id == "rest_secondary")
        .expect("secondary projection");
    assert_eq!(secondary_projection.name, "search_issues");
    assert_eq!(secondary_projection.namespace, "github_rest_secondary");

    assert!(
        !catalog
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "PROJECTION_NAME_COLLISION_RESOLVED")
    );
}

#[test]
fn projection_names_avoid_obvious_bad_singulars() {
    assert_eq!(singularize("status"), "status");
    assert_eq!(singularize("news"), "news");
    assert_eq!(singularize("analytics"), "analytics");
    assert_eq!(singularize("addresses"), "address");
    assert_eq!(pluralize("box"), "boxes");
}
