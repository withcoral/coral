use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::{PaginationMode, parse_source_manifest_yaml};

use super::identifiers::{pluralize, singularize};
use super::*;

#[test]
fn parses_v4_manifest_and_unions_surface_inputs() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ZZZ_TOKEN:
        kind: secret
      AAA_BASE:
        kind: variable
        default: https://api.example.com
    base_url: "{{input.AAA_BASE}}"
    auth:
      type: HeaderAuth
      headers:
        - name: Authorization
          from: template
          template: Bearer {{input.ZZZ_TOKEN}}
"#,
    )
    .expect("v4 manifest");
    assert_eq!(manifest.dsl_version(), 4);
    assert!(manifest.as_v4().is_some());
    assert_eq!(manifest.declared_inputs().len(), 2);
    let keys = manifest
        .declared_inputs()
        .iter()
        .map(|input| input.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys, ["ZZZ_TOKEN", "AAA_BASE"]);
}

#[test]
fn parses_v4_openapi_surface_without_base_url() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");
    assert_eq!(
        v4.surfaces
            .first()
            .expect("surface")
            .openapi_runtime
            .base_url
            .raw(),
        ""
    );
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

    assert_eq!(projection.pagination.mode, PaginationMode::Page);
    assert_eq!(projection.pagination.page_param.as_deref(), Some("page"));
    assert_eq!(
        projection
            .pagination
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
    assert_eq!(exposures.get("owner"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("repo"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("state"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("page"), Some(&SqlInputExposure::Internal));
    assert_eq!(exposures.get("per_page"), Some(&SqlInputExposure::Internal));

    let filter_names = projection_filter_specs(projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        filter_names,
        BTreeSet::from(["owner".to_string(), "repo".to_string(), "state".to_string()])
    );

    let column_names = projection_column_specs(projection)
        .into_iter()
        .map(|column| column.name)
        .collect::<BTreeSet<_>>();
    assert!(column_names.contains("owner"));
    assert!(column_names.contains("repo"));
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

    let mut stale_projection = projection.clone();
    for input in &mut stale_projection.inputs {
        if matches!(input.wire_name.as_str(), "page" | "per_page") {
            input.sql_exposure = SqlInputExposure::Filter;
        }
    }
    let stale_filter_names = projection_filter_specs(&stale_projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(stale_filter_names, filter_names);

    let stale_request =
        request_spec_for_projection(&stale_projection, operation).expect("stale request");
    let stale_query_names = stale_request
        .query
        .iter()
        .map(|param| param.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(stale_query_names, query_names);

    for input in &mut stale_projection.inputs {
        if matches!(input.wire_name.as_str(), "page" | "per_page") {
            input.sql_exposure = SqlInputExposure::FunctionArg;
        }
    }
    let stale_arg_names = projection_arg_specs(&stale_projection)
        .into_iter()
        .map(|arg| arg.name)
        .collect::<BTreeSet<_>>();
    assert!(!stale_arg_names.contains("page"));
    assert!(!stale_arg_names.contains("per_page"));
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
    assert!(matches!(pulls.1, ProjectionKind::Table));
    let commits = names_by_operation
        .get("repos_list_commits")
        .expect("repos_list_commits projection");
    assert_eq!(commits.0, "commits");
    let pull_commits = names_by_operation
        .get("pulls_list_commits")
        .expect("pulls_list_commits projection");
    assert_eq!(pull_commits.0, "repos_pulls_commits");
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
    assert!(ir.types.iter().any(|ty| ty.id == "tree"));
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

#[test]
fn projection_names_avoid_obvious_bad_singulars() {
    assert_eq!(singularize("status"), "status");
    assert_eq!(singularize("news"), "news");
    assert_eq!(singularize("analytics"), "analytics");
    assert_eq!(singularize("addresses"), "address");
    assert_eq!(pluralize("box"), "boxes");
}

fn github_openapi() -> &'static str {
    r"
openapi: 3.0.3
paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
        - {name: page, in: query, schema: {type: integer}}
        - {name: per_page, in: query, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/issue'}
  /search/issues:
    get:
      operationId: search/issues-and-pull-requests
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
        - {name: sort, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  items:
                    type: array
                    items: {$ref: '#/components/schemas/issue'}
  /repos/{owner}/{repo}/issues/{issue_number}:
    get:
      operationId: issues/get
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: issue_number, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/issue'}
    patch:
      operationId: issues/update
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                title: {type: string}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/issue'}
components:
  schemas:
    issue:
      type: object
      properties:
        id: {type: integer}
        number: {type: integer}
        title: {type: string}
        state: {type: string}
        html_url: {type: string}
        created_at: {type: string, format: date-time}
        updated_at: {type: string, format: date-time}
        body: {type: string}
        user: {type: object}
"
}
